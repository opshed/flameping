# Flameping design research

Status: phase-one consensus accepted after three review rounds, 2026-08-31

## Outcome

Flameping should be a Linux-first, single-process Go daemon with an embedded web UI and an embedded SQLite database. It should use shared ICMP sockets, a monotonic central scheduler, one bounded event pipeline, one owner for all database writes, read-only database connections for the web API, Linux rtnetlink interface counters, and an optional in-process UDP traceroute engine that receives ICMP errors through a raw socket.

No external TSDB or second service is justified at the intended starting scale. The design must keep the storage boundary narrow enough to add a server database later, but the first implementation will support SQLite only.

The two unspecified scale choices have safe answers and do not block implementation:

- The global defaults are a 5-second probe interval and 1-second display timeout. Each target can override both, down to a 100-millisecond interval.
- Raw samples default to seven days, one-minute rollups to 90 days, and one-hour rollups to five years. A 5 GiB database budget and free-space reserve may shorten raw retention before they sacrifice rollups.

## Runtime architecture

```text
monotonic scheduler -> bounded send queues -> shared ICMP sockets
                              |                     |
                              |                 reply readers
                              v                     v
                     local outcome events ---> bounded event queue
Linux interface collector --------------------------|
UDP traceroute collector ----------------------------|
                                                     v
                                      single SQLite write owner
                                      / rollups / retention
                                                     |
embedded HTTP API + UI -> bounded read-only SQLite connections
```

One process is the right fault and operations boundary. Go goroutines provide enough isolation for these mostly asynchronous jobs, while multiple daemons would add IPC, deployment, and consistency problems without a demonstrated scaling need. Every long-lived component will be supervised by a root context and report readiness; bounded queues prevent a stalled database from consuming memory without limit.

## Echo probing

### Scheduling

Use one min-heap of target deadlines and one resettable timer. Target phases are spread deterministically over their interval so startup does not create a packet burst. The scheduler advances from the intended monotonic deadline, not from the completion time, which prevents drift. When execution is behind, it records `scheduler_missed` attempts and advances to the next future slot; it never emits a catch-up burst.

The scheduler hands sends to bounded IPv4 and IPv6 queues. There is one shared socket and receive loop per address family rather than a socket or read goroutine per target. Hostnames are resolved into a pinned endpoint, the resolved IP is stored on every sample, and an endpoint-change event is recorded when DNS causes the selected address to change.

### Privileges and portability

Use [`golang.org/x/net/icmp`](https://pkg.go.dev/golang.org/x/net/icmp). Its non-privileged `udp4` and `udp6` ICMP endpoints are supported on Linux and Darwin; Linux may require the service group to be included in `net.ipv4.ping_group_range`. The core ping feature should try these endpoints first and should not require root.

Raw ICMP sockets require `CAP_NET_RAW` on Linux. Traceroute may use that single capability, preferably supplied by systemd with `AmbientCapabilities=CAP_NET_RAW`, `CapabilityBoundingSet=CAP_NET_RAW`, and `NoNewPrivileges=true`. If raw reception is unavailable, ping and the rest of the dashboard continue while traceroute reports an explicit disabled reason. The service must never recommend running the whole daemon as root.

Linux is the full-featured first platform. Build-tagged adapters let other operating systems compile and report unsupported interface or trace capabilities instead of returning fabricated zero values.

### Correlation, loss, and late replies

ICMP's 16-bit identifier and sequence fields are insufficient for high-rate, long-lived correlation. RFC 792 also guarantees that echo payload data is returned, so each payload will contain a version, random per-process run ID, 64-bit sequence, monotonic send offset, timeout, and a truncated authentication tag. This permits authenticated reply matching after an in-memory deadline entry is gone and makes sequence wrap harmless.

A raw sample is inserted once when the send is attempted. It stores its scheduled wall time, actual send wall time, timeout, target, and resolved endpoint. Outcomes are represented without conflating local overload with the network:

- `on_time`: a reply was received at or before the sample's timeout.
- `late`: a reply was received after its timeout.
- `unanswered`: no reply is known and the timeout has elapsed.
- `pending`: no reply is known but the timeout has not elapsed.
- `send_error`: the local send failed.
- `scheduler_missed`: the scheduler intentionally skipped an overdue send.

There is no timeout write for each unanswered probe: the query and rollup logic derives pending versus unanswered from `sent_at + timeout`. A reply updates the original row. Late replies remain acceptable until raw retention removes the row; an update that no longer finds a raw row increments an internal diagnostic counter.

The primary loss graph is the deadline-miss rate (`late + unanswered`) because that matches the requested display timeout. A separate no-reply rate shows only `unanswered`. Late RTT points stay visible. The timeout used by the individual sample is immutable, so a config change cannot rewrite history.

RTT is measured from Go monotonic clock offsets so NTP or manual wall-clock changes do not distort live measurements. Wall timestamps remain in storage for ordering and display.

## SQLite storage

Use the pinned `modernc.org/sqlite` driver so `CGO_ENABLED=0` still produces one executable. Its [v1.57.0 package documentation](https://pkg.go.dev/modernc.org/sqlite@v1.57.0) identifies SQLite 3.53.3 for the supported build targets, and its [v1.57.0 changelog](https://gitlab.com/cznic/sqlite/-/blob/v1.57.0/CHANGELOG.md) records both that embedded version and an additional journal-recovery patch carried by the driver. Flameping must require SQLite 3.51.3 or newer because SQLite's [WAL-reset bug advisory](https://sqlite.org/wal.html#wal_reset_bug) identifies the rare checkpoint/write corruption race through 3.51.2 and the fixed releases.

The initial database settings are:

- a local filesystem only;
- `journal_mode=WAL`;
- `synchronous=NORMAL` by default, with a `full` durability option;
- one logical writer, committing at most every 250 ms or 512 events;
- commit-triggered auto-checkpoints disabled and periodic application-controlled PASSIVE checkpoints;
- short, deadline-bounded read transactions;
- `busy_timeout`, foreign keys, and defensive schema migrations;
- a final truncate checkpoint during clean shutdown.

SQLite documents that WAL normally makes writes sequential, uses fewer `fsync` operations, and permits readers alongside a writer. Under `synchronous=NORMAL`, commits do not normally sync the WAL; checkpoints perform the sync. A power failure can therefore lose recent transactions without corrupting a consistent database. Operators who value the newest samples over lower I/O can select `full`.

Routine full `VACUUM` is specifically rejected. Retention deletes in small chunks and lets SQLite reuse free pages, producing a stable high-water file without periodic rewrite storms. `PRAGMA optimize` is periodic. A deliberate offline `db compact` command can be added for an operator who needs the file to shrink.

### Data model and rollups

Raw ping rows are mutable only to attach the first valid reply. They include the run/sequence key, target ID, scheduled and sent timestamps, timeout, resolved IP, RTT and received time if present, local outcome, and late classification.

One-minute and one-hour rollups store scheduled, sent, on-time, late, unanswered, send-error, and scheduler-missed counts; sum/min/max RTT; and a mergeable sparse logarithmic RTT histogram. Mergeable histograms are required because averages and percentiles-of-percentiles cannot accurately answer arbitrary zoom ranges. A late update marks its minute bucket dirty for recomputation. Hour buckets merge minute histograms.

Interface samples store cumulative 64-bit kernel counters plus an interface generation. Route tables store every trace run and every hop/probe observation; a separate event records confirmed route transitions.

### Retention and capacity

Defaults:

| Tier | Retention |
|---|---:|
| Raw probes | 7 days |
| One-minute rollups | 90 days |
| One-hour rollups | 5 years |
| Soft database budget | 5 GiB |
| Filesystem reserve | greater of 1 GiB or 10% |

Assuming roughly 100-160 bytes per indexed raw row:

| Interval | Rows per target/day | Seven raw days | Thirty raw days |
|---|---:|---:|---:|
| 5 s | 17,280 | 12-19 MiB | 52-83 MiB |
| 100 ms | 864,000 | 0.60-0.97 GiB | 2.6-4.1 GiB |

Ten targets at 100 ms create 100 probes/s and about 0.86-1.38 GiB of final raw rows per day. The database budget therefore cannot promise seven raw days for every configuration. Before the page or free-space reserve is threatened, maintenance first verifies rollup coverage and then prunes the oldest raw rows in chunks. The status API and UI expose the effective raw horizon and any capacity pressure. Free database pages are reusable even though the file does not shrink.

This is preferable to making the default installation depend on Prometheus, InfluxDB, or PostgreSQL. Revisit an external store only for distributed collectors, multiple writers, high availability, or long raw retention at thousands of probes per second.

## Interface statistics

On Linux, collect `RTM_GETLINK`/`IFLA_STATS64` data from `rtnl_link_stats64` every five seconds. The kernel documentation calls rtnetlink the preferred API; sysfs is convenient but performs a complete stats dump for every individual counter file.

Store cumulative bytes, packets, errors, drops, missed, FIFO, CRC, frame, carrier, and collision counters. Rates and deltas are derived during queries and rollups. A boot-ID, ifindex, name, or MAC generation change—or any counter decrease—creates a reset marker rather than a huge wrapped delta. The UI must not label `rx_dropped` alone as physical network loss; Linux exposes distinct missed/error counters with different meanings.

## Traceroute and route changes

Use native UDP probes with increasing TTL/hop limit and a raw ICMP receiver. [RFC 5388 section 3](https://www.rfc-editor.org/rfc/rfc5388.html#section-3) recommends UDP because many routers do not send ICMP Time Exceeded in response to ICMP Echo, so ICMP-echo traceroute is not the default.

Keep flow-identifying fields stable where practical (Paris-style UDP) to reduce ECMP-induced false changes. Correlate probes through quoted headers/checksum compensation; if that proves non-portable, fall back to classic varying-port UDP and label the method. Defaults are a trace every 15 minutes with deterministic jitter, three probes per hop, 30 hops, a one-second per-probe timeout, a global rate limit, and an overall deadline.

Each TTL is modeled as a set of responders because paths can be multipath. Route signatures exclude RTT, PTR names, and responder order. Missing replies are unknown rather than proof of change. A route change is confirmed only when the same candidate signature appears in two consecutive completed traces. Reverse DNS is asynchronous display metadata and never affects identity.

## Web interface and API

Use the standard Go HTTP server with explicit read, header, write, and idle timeouts. The UI is small TypeScript/vanilla JavaScript using a pinned, vendored uPlot build; its compiled assets are included with `go:embed`, so Node is a build-time tool only and deployment remains one executable.

The API chooses raw, minute, or hour data from the requested range and point/pixel budget. It caps result size, applies query deadlines, and re-queries at higher resolution after zoom. The initial dashboard provides:

- target status and current endpoint;
- median and percentile latency bands, min/max, timeout, and distinct late points;
- deadline-miss, no-reply, send-error, and scheduler-skip series;
- time-range presets and click/drag drill-down;
- interface throughput/error/drop rates with reset markers;
- trace history and hop-by-hop route diffs;
- storage horizon, queue pressure, and capability diagnostics.

Poll every five seconds. Server-Sent Events can be added later if polling proves inadequate. Bind to `127.0.0.1:8080` by default. Public binding must be explicit; TLS and authentication stay at a reverse proxy rather than becoming a home-grown security subsystem.

## Configuration and operations

Use strict YAML and reject unknown keys. Targets have a stable string ID, display name, address, and optional interval/timeout overrides. Configuration reload is deferred; a restart is simpler and stable IDs preserve history.

The CLI will provide `run`, `check-config`, `doctor`, and `version`. The repository will include an example config and a hardened systemd unit. The server provides `/healthz` and `/readyz`, structured logs, graceful shutdown, schema migrations, and restrictive database-file permissions.

## Verification strategy

- Fake-clock scheduler tests cover phase spreading, drift, overload, skipped slots, and wall-clock corrections.
- Fake packet connections cover reorder, duplicates, spoofed/authentication failures, late replies, IPv4/IPv6, and sequence wrap.
- SQLite integration tests cover migrations, reply upserts, dirty rollups, retention, checkpoints, and reopen integrity.
- Interface and traceroute parsers use fixtures and fuzz tests.
- API tests cover bounds, invalid ranges, resolution selection, and cancellation.
- `go test -race` exercises concurrency paths.
- Privileged Linux integration tests can later use namespaces, veth, and `tc netem` for delay, loss, duplication, and reorder.
- Ingest/rollup benchmarks at 100, 1,000, and 5,000 probes/s must precede published scale claims.

## Alternatives rejected

- RRD: its fixed-size model is attractive, but consolidation and file-oriented updates are awkward for late mutable samples and route/interface events.
- Prometheus as the primary store: another service and append-oriented samples are a poor fit for updating a timed-out probe with a late reply.
- PostgreSQL/Timescale or InfluxDB by default: additional operations without demonstrated need.
- One socket, timer, or process per probe: unnecessary descriptor, timer, and scheduler pressure under loss.
- Shelling out to `ping` or `traceroute`: process overhead, brittle parsing, and weak late-reply correlation.
- ICMP-echo traceroute by default: contradicted by RFC 5388 operational guidance.
- sysfs as the primary Linux counter source: simple but officially documented as inefficient for multiple counters.
- routine `VACUUM`: it recreates avoidable disk write bursts.
- multiple Flameping services: no present scale or isolation requirement justifies the added consistency and deployment cost.

## Primary references

- [Go `x/net/icmp` package](https://pkg.go.dev/golang.org/x/net/icmp)
- [RFC 792: ICMP echo matching and returned data](https://www.rfc-editor.org/rfc/rfc792.html)
- [RFC 5388 section 3: traceroute measurement model and UDP recommendation](https://www.rfc-editor.org/rfc/rfc5388.html#section-3)
- [Linux interface-statistics documentation](https://www.kernel.org/doc/html/latest/networking/statistics.html)
- [Linux raw sockets](https://man7.org/linux/man-pages/man7/raw.7.html)
- [SQLite write-ahead logging and WAL-reset advisory](https://sqlite.org/wal.html#wal_reset_bug)
- [SQLite synchronous modes](https://sqlite.org/pragma.html#pragma_synchronous)
- [`modernc.org/sqlite` v1.57.0 driver documentation](https://pkg.go.dev/modernc.org/sqlite@v1.57.0)
- [`modernc.org/sqlite` v1.57.0 changelog](https://gitlab.com/cznic/sqlite/-/blob/v1.57.0/CHANGELOG.md)
- [Go embedded files](https://pkg.go.dev/embed)
- [Go race detector](https://go.dev/doc/articles/race_detector)
