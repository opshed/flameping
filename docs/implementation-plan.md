# Flameping implementation plan

Status: phase-two consensus accepted after three review rounds, 2026-08-31

Implementation status: phase-three consensus accepted after five review rounds and final remediation verification, 2026-08-31

This plan implements the complete v1 described in [design-research.md](design-research.md): IPv4/IPv6 echo monitoring, late-reply-aware latency and loss graphs, SQLite retention and rollups, Linux interface statistics, UDP traceroute and route-change history, an embedded UI, and operational tooling. Every stage leaves a runnable, tested program.

## 1. Repository and toolchain

Use Go 1.25 as the module compatibility floor and test with the workspace's Go 1.26 toolchain. The production build is `CGO_ENABLED=0`.

```text
cmd/flameping/main.go
internal/
  app/                 lifecycle, supervision, readiness
  buildinfo/           version/commit/build metadata
  cli/                 run, check-config, doctor, version, db commands
  config/              strict YAML, defaults, validation, byte/duration types
  model/               shared probe/interface/trace/query types
  clock/               real and deterministic fake clock interface
  resolver/            pinned target endpoint selection and refresh
  scheduler/           monotonic min-heap scheduler and gap coalescing
  echo/                payload, socket adapters, sender/receiver engine
  eventbus/            ordered bounded measurement stream
  histogram/           versioned sparse logarithmic RTT histogram
  ifstats/             collector API, Linux rtnetlink, unsupported stub
  trace/               UDP sender, raw ICMP parser, correlation, route changes
  store/
    store.go           query/write-facing interfaces
    sqlite/            connections, writer, queries, rollups, retention
    sqlite/migrations/ embedded checksummed up-only SQL migrations
  httpapi/             server, middleware, handlers, JSON DTOs
  webui/
    embed.go            embedded built frontend
    dist/               committed generated assets
web/
  src/                  TypeScript/HTML/CSS source
  package.json
  package-lock.json
  esbuild.mjs
configs/flameping.example.yaml
packaging/systemd/flameping.service
tests/integration/     opt-in privileged network tests
docs/operations.md
Makefile
README.md
go.mod
go.sum
```

Committed `internal/webui/dist` assets allow `go build` without Node. The frontend build regenerates them with pinned tools and CI rejects a diff.

### Pinned dependencies

Resolve and pin exact current versions when scaffolding:

- `modernc.org/sqlite` v1.57.0;
- `golang.org/x/net/icmp`, `ipv4`, and `ipv6`;
- `golang.org/x/sync/errgroup`;
- `go.yaml.in/yaml/v3` for strict YAML;
- `github.com/jsimonetti/rtnetlink/v2` and its netlink dependencies;
- a small fakeable clock library only if the local clock abstraction cannot deterministically drive resettable timers.

Frontend-only dependencies are TypeScript, esbuild, and uPlot. Do not add a CLI framework, ORM, HTTP framework, frontend runtime framework, or migration framework.

## 2. Configuration and CLI

Decode YAML with unknown-field rejection, apply defaults, then validate the resolved structure. Use `time.Duration`-compatible text and IEC byte values.

```yaml
server:
  listen: 127.0.0.1:8080
  allow_public: false

storage:
  path: ./flameping.db
  durability: normal
  raw_retention: 168h
  minute_retention: 2160h
  hour_retention: 43800h
  trace_retention: 720h
  max_bytes: 5GiB
  free_space_reserve: 1GiB
  free_space_reserve_percent: 10

ping:
  interval: 5s
  timeout: 1s
  min_interval: 100ms
  event_queue: 8192
  send_queue: 1024
  max_probes_per_second: 5000

dns:
  refresh_interval: 5m

targets:
  - id: gateway
    name: Gateway
    address: 192.0.2.1
    family: auto
    interval: 1s
    timeout: 500ms
    traceroute: true

interfaces:
  - name: eth0
    interval: 5s

traceroute:
  enabled: true
  method: auto
  interval: 15m
  probes_per_hop: 3
  max_hops: 30
  hop_timeout: 1s
  overall_timeout: 10s
  pipeline_hops: 4
  max_packets_per_second: 50
```

Validation includes unique stable target IDs, exact interface names, `interval >= min_interval >= 100ms`, positive timeouts, aggregate-rate limits, retention ordering, usable database/WAL headroom, and valid address families. Every effective target timeout must be shorter than raw retention and the emergency raw floor is `max(24h, maximum effective timeout + 1m)`; retention never removes a sample whose individual deadline has not elapsed. Minute and hour retention may not be configured below the non-negotiable 30-day review horizon; this is the minimum historical review promised by v1, even if raw retention is shortened under pressure. The filesystem reserve is the greater of `free_space_reserve` and `free_space_reserve_percent` of filesystem capacity. A non-loopback listener is rejected unless `allow_public` is true.

Target config is synchronized into storage on startup. Removed IDs become inactive but their history remains. For a DNS target, retain the chosen IP while it remains in the answer set; otherwise choose a deterministic family-compatible replacement and record an endpoint-change event. `family: auto` prefers IPv4 initially unless only IPv6 is available; operators can force either family.

Commands use the standard `flag` package:

- `flameping run --config PATH`
- `flameping check-config --config PATH`
- `flameping doctor --config PATH`
- `flameping version`
- `flameping db backup --config PATH --output PATH`
- `flameping db compact --config PATH` as an explicit offline operation

`doctor` validates config and storage, reports filesystem capacity, tests IPv4/IPv6 ping endpoints and raw ICMP trace reception, checks Linux `ping_group_range`, and prints exact capability remediation. It never changes host settings.

## 3. Lifecycle and event invariants

`internal/app` creates a root context with signal cancellation and supervises storage, resolver, echo, interface, traceroute, and HTTP components through `errgroup`. Startup order is migrations, config synchronization, writer, collectors, then readiness. Shutdown order is stop HTTP admission, stop schedulers/collectors, drain the event stream, stop readers, run the writer's final checkpoint, and close connections.

Persistent measurement events are lossless while the process is healthy; only explicitly modeled scheduler gaps may replace individual attempts.

Event types:

- `ProbeSent`
- `ProbeSendError`
- `ProbeReply`
- `ProbeDuplicate`
- `SchedulerGap` (target, first scheduled time, interval, count)
- `EndpointChanged`
- `InterfaceSnapshot`
- `InterfaceReset`
- `TraceCompleted`

All producers publish to one bounded event bus with capacity reservations. Publishing applies backpressure; it never silently drops a persistent event. Before a network send, the echo sender blocks only to reserve one event slot. Once admitted, it captures the monotonic/wall send times immediately adjacent to `WriteEcho`, performs the write, and publishes exactly one `ProbeSent` or `ProbeSendError` into its reservation without blocking. Queue wait therefore affects scheduler admission, not measured RTT or timeout, and a crash before the socket write cannot leave a stored row that later looks like network loss.

A very fast reply can reach the writer before its `ProbeSent`. The writer keeps a strictly bounded reply-before-row map keyed by run/sequence and applies the reply in the same transaction when the corresponding sent event arrives. The map consumes reserved event-budget capacity, has a short safety expiry, and treats expiry as a visible internal consistency failure rather than network loss. This is tested with forced sender/receiver reordering. Replies may otherwise block briefly behind storage but cannot disappear by policy.

The target send queues are also bounded. When the writer/event queue backs up, senders stall, send queues fill, and the scheduler stops creating network traffic for those due slots. It coalesces them in memory into `SchedulerGap` ranges, which a reporter persists when capacity returns. If the writer has a permanent failure, readiness becomes false and the root context shuts the process down; Flameping does not keep transmitting while losing history.

The SQLite owner drains up to 512 events or 250 milliseconds per transaction. Transient busy errors use bounded retry; all other write failures are fatal and visible.

## 4. Scheduler, resolution, and echo engine

### Scheduler

Maintain a min-heap of intended target deadlines and one resettable monotonic timer. Initial phase is a stable hash of target ID modulo interval. Each completed slot advances from its intended deadline rather than the completion time.

On wake:

1. Pop due targets.
2. Non-blockingly offer one job to the appropriate family send queue.
3. If that queue is full or the configured aggregate rate is exhausted, add the due slot to its coalesced scheduler gap.
4. Advance directly to the next future deadline, recording every skipped slot in the gap count; never send catch-up bursts.
5. Reset the single timer to the new heap minimum.

The clock and job sink are injected. Deterministic tests control deadlines without sleeping.

### Probe identity

Use a fixed 56-byte, network-order echo payload:

```text
magic[4] | version[1] | flags[1] | header_len[2]
run_id[8] | sequence[8] | monotonic_send_ns[8]
timeout_ns[8] | HMAC-SHA256-truncated[16]
```

The run ID and HMAC secret are generated from `crypto/rand` at startup; the run ID is recorded in `probe_runs` and the secret stays in memory. Sequence is process-global and constrained below signed 63-bit range. The echo engine verifies magic, version, length, HMAC, current run, source address, and expected endpoint. ICMP identifier/sequence values are not trusted for uniqueness.

`monotonic_send_ns` is an offset from a retained process-start `time.Time`, so RTT remains monotonic without retaining every outstanding probe in memory. The receiver obtains target, endpoint, and timeout from an in-memory active index for normal replies; for very late replies it can submit the authenticated run/sequence and let the writer match the stored row. A reply from a prior process run is ignored.

### Socket abstraction

```go
type PacketIO interface {
    WriteEcho(payload []byte, dst netip.Addr) error
    ReadEcho(context.Context) (Packet, error)
    Close() error
}
```

One actor owns writes for each IPv4/IPv6 socket and one receive loop reads each socket. Real adapters prefer `icmp.ListenPacket("udp4", ...)` and `udp6`; raw fallback is attempted only when permitted. Fakes inject reorder, duplicates, spoofing, late replies, family differences, and I/O errors.

## 5. SQLite storage

### Connections and migrations

Open one writer connection and a read pool capped at four connections. Set WAL and persistent database settings through the writer before opening readers. Apply connection-local pragmas such as `busy_timeout` and `query_only` to the appropriate connections; never ask a read-only connection to change journal mode.

Settings:

- local filesystem validation;
- SQLite runtime version check `>= 3.51.3`;
- `journal_mode=WAL`;
- configurable `synchronous=NORMAL|FULL`;
- `wal_autocheckpoint=0`;
- `foreign_keys=ON`;
- `busy_timeout=5000`;
- PASSIVE checkpoints issued by the write owner between transactions;
- final TRUNCATE checkpoint only after HTTP readers stop.

Migrations are embedded, checksummed, up-only SQL files. Apply each in a transaction and store version, checksum, and application time. Reject a database with a newer schema or changed applied checksum.

### Core schema

Timestamps use signed integer Unix microseconds; RTTs, timeouts, and intervals use integer nanoseconds. Endpoint addresses are normalized once and referenced by ID.

```text
schema_migrations(version PK, checksum, applied_at_us)

probe_runs(run_id BLOB PK, started_at_us, boot_id, build_version)

targets(
  id INTEGER PK, stable_id TEXT UNIQUE, display_name, configured_address,
  address_family, interval_ns, timeout_ns, active,
  first_seen_us, last_seen_us
)

endpoints(
  id INTEGER PK, target_id FK, family, address, zone,
  first_seen_us, last_seen_us,
  UNIQUE(target_id, family, address, zone)
)

target_events(
  id INTEGER PK, target_id FK, at_us, kind,
  old_endpoint_id, new_endpoint_id, detail_json
)

ping_samples(
  run_id BLOB, sequence INTEGER,
  target_id FK, endpoint_id FK,
  scheduled_at_us, sent_at_us, timeout_ns,
  local_outcome, send_error_code,
  reply_at_us, rtt_ns, reply_class,
  responder_address, icmp_type, icmp_code, duplicate_count DEFAULT 0,
  PRIMARY KEY(run_id, sequence)
) WITHOUT ROWID

scheduler_gaps(
  id INTEGER PK, target_id FK,
  first_scheduled_at_us, interval_ns, missed_count
)

ping_rollups(
  target_id, resolution_s, bucket_start_us,
  scheduled_count, attempted_count, sent_count,
  on_time_count, late_count, unanswered_count,
  send_error_count, scheduler_missed_count,
  rtt_count, rtt_sum_ns, rtt_min_ns, rtt_max_ns,
  histogram BLOB, updated_at_us,
  PRIMARY KEY(target_id, resolution_s, bucket_start_us)
) WITHOUT ROWID

dirty_rollups(
  kind, entity_id, resolution_s, bucket_start_us,
  PRIMARY KEY(kind, entity_id, resolution_s, bucket_start_us)
) WITHOUT ROWID
```

`local_outcome` is null for a socket write that succeeded and otherwise `send_error`; scheduler gaps live in their own compact table. `reply_class` is null, `on_time`, or `late`. Pending and unanswered are derived from `reply_at_us IS NULL` and the individual `sent_at_us + timeout_ns`; they are not timeout-write events.

Indexes cover `(target_id, scheduled_at_us)`, time retention scans, dirty work, interface time, and trace target/time.

### Interface schema

```text
interface_generations(
  id INTEGER PK, boot_id, ifindex, name, mac,
  started_at_us, ended_at_us
)

interface_samples(
  generation_id, sampled_at_us,
  rx/tx bytes, packets, errors, dropped,
  rx_missed, rx_fifo, tx_fifo, rx_crc, rx_frame,
  tx_carrier, collisions,
  PRIMARY KEY(generation_id, sampled_at_us)
) WITHOUT ROWID

interface_rollups(
  generation_id, resolution_s, bucket_start_us,
  elapsed_ns, sample_count, reset_count,
  counter deltas for every stored counter, peak byte/packet rates,
  PRIMARY KEY(generation_id, resolution_s, bucket_start_us)
) WITHOUT ROWID
```

### Trace schema

```text
trace_runs(
  id INTEGER PK, target_id FK, endpoint_id FK,
  method, flow_id, started_at_us, ended_at_us,
  status, reached, reached_hop, signature
)

trace_probes(
  run_id FK, ttl, probe_index, token,
  sent_at_us, reply_at_us, responder_address, rtt_ns,
  icmp_type, icmp_code,
  PRIMARY KEY(run_id, ttl, probe_index)
) WITHOUT ROWID

route_changes(
  id INTEGER PK, target_id FK, endpoint_id FK,
  old_trace_id, candidate_trace_id, confirming_trace_id,
  old_signature, new_signature,
  first_seen_us, confirmed_at_us
)
```

Every trace is retained independently of whether it confirms a route change.

## 6. Histograms, rollups, and retention

Implement a versioned sparse logarithmic histogram with a documented relative-error bound before storing production data. Encode sorted `(bucket-index delta, count)` pairs as unsigned varints. Merging is a linear sum of like buckets; percentile lookup uses cumulative counts. Golden, merge, quantile-error, malformed-input, and compatibility tests lock the format.

Rollup dirtiness is durable and transactional:

1. Inserting a `ProbeSent` or `ProbeSendError`, or persisting a `SchedulerGap`, inserts every affected minute key into `dirty_rollups` in the same transaction. This guarantees that completely unanswered targets and local failures still create loss rollups.
2. Attaching the first reply also inserts its minute key, even if that bucket was rolled previously.
3. A minute is initially eligible when all of its sent rows have individually passed `sent_at + timeout`; there is no global-timeout delay. An ineligible dirty key remains queued for a later sweep.
4. Recompute the minute and clear its dirty key in one transaction; that transaction dirties the containing hour.
5. Recompute an hour by merging minute counters/histograms and clear its key in one transaction.
6. A duplicate increments diagnostics/duplicate count but never changes first RTT or histograms.

Inserting an `InterfaceSnapshot` dirties its containing minute. An interface minute becomes eligible after the bucket has closed and a same-generation sample at or beyond its end exists, allowing the boundary delta to be assigned correctly. Recomputing it dirties the containing hour. Interface minute rollups derive counter deltas only within a generation; hour rollups merge complete eligible minutes. A reset closes the prior generation's last eligible interval and dirties both sides without ever deriving a cross-generation delta.

Retention invariants:

- Delete raw ping/interface data only after its minute rollup exists and is not dirty.
- Delete minute rollups only after their hour exists and is not dirty.
- Delete complete buckets in small transactions.
- A late reply after raw retirement only increments a diagnostic.
- Retain full trace runs for `trace_retention` (30 days by default) and confirmed route-change snapshots for the hour-tier horizon unless the storage budget reaches the documented pressure stage.

### Budget behavior

Track separately:

- physical bytes: database, WAL, and SHM files;
- live bytes: `(page_count - freelist_count) * page_size`;
- reusable bytes: `freelist_count * page_size`;
- filesystem free bytes.

Use `auto_vacuum=NONE`. Reserve about ten percent of `max_bytes` for WAL and set `max_page_count` for the main database below the remaining hard limit. At startup, reject `max_bytes` if its main-file allowance is smaller than the existing SQLite high-water file plus required WAL headroom; `max_page_count` cannot shrink an existing file, so the error directs the operator to increase the budget or run the offline compact command. Begin early pruning from live-page pressure; deletion creates reusable pages but does not claim to return filesystem space.

Pressure order is:

1. Normally expired, non-pending raw samples, interface samples, detailed trace runs, and rollup tiers.
2. Raw ping/interface samples younger than their configured horizon, oldest complete covered buckets first, down to the emergency floor `max(24h, maximum effective timeout + 1m)`. A pending probe is never pruned.
3. Detailed trace probe/run data oldest first, while retaining at least one completed signature per target/day and every run needed by a confirmed change within 30 days. Confirmed `route_changes` store self-contained old/new signature snapshots so older trace rows can eventually be removed.
4. Minute rollups older than the fixed 30-day review horizon.
5. Hour rollups and route-change events older than that horizon, oldest first.
6. Pause new measurements and report not-ready rather than delete the last 30 days of review data.

Configured minute/hour retention cannot be shorter than the 30-day floor. Never delete raw data lacking a safe lower-resolution replacement.

If unrelated files consume the filesystem reserve, pruning cannot necessarily restore free bytes because the SQLite high-water file remains allocated. Pause admission, perform a safe checkpoint, and require operator action. Only the explicit offline compact command shrinks the main file.

Maintenance also runs periodic `PRAGMA optimize`, exposes long readers and WAL size, and prevents checkpoint work from racing the logical writer.

## 7. Linux interface statistics

The Linux collector uses `rtnetlink.Link.List`/`IFLA_STATS64`, filters configured exact names, and emits cumulative `rtnl_link_stats64` values every configured interval. It does not require `CAP_NET_ADMIN`.

Generation identity is boot ID + ifindex + name + MAC. Create a new generation on identity change or any counter decrease; never calculate deltas across generations. Interface disappearance is a typed status/event. Build-tagged non-Linux code compiles and reports unsupported capability.

Fixture tests cover every counter mapping, missing attributes, counter reset, rename/recreation, disappearance, and values near the signed storage limit.

## 8. UDP traceroute and route changes

Use one long-lived UDP sender and one raw ICMP receiver per supported family. A single family actor owns TTL/hop-limit socket option changes and sends, preventing races. All trace activity shares a token-bucket limiter and deterministic target jitter.

The v1 implementation order is:

1. Implement and integration-test classic UDP correlation with unique destination ports/tokens.
2. Implement Paris-style UDP on Linux: keep addresses and ports stable, select the routed source address, adjust a payload word to produce the intended nonzero checksum token, and correlate ICMP quotes by destination, ports, and checksum.
3. `method: auto` selects Paris only after its startup/platform capability checks pass; otherwise it records and uses classic UDP. Never silently return all `*` from a broken advanced mode.

Use a four-hop pipeline within the overall ten-second deadline, up to three probes per hop and 30 hops. Stop scheduling higher TTLs once the destination is reliably reached. Store partial and timed-out runs as well as completed ones.

Signatures map TTL to a sorted responder set and exclude RTT, PTR names, response order, and missing hops. Compare only compatible endpoints/flow IDs. A candidate change requires a meaningful nonempty responder-set or reached-hop difference and must repeat in two consecutive completed traces before a `route_changes` row is written. Reverse DNS is an asynchronous, bounded display cache only.

If raw ICMP is unavailable, only traceroute is disabled and its reason is visible; echo, interface monitoring, storage, API, and UI remain ready.

## 9. HTTP API and embedded UI

Use a private Go `http.ServeMux`, strict methods, request query deadlines, a 64 KiB header limit, security headers, and explicit read-header/read/write/idle timeouts. Do not expose pprof or mutation routes.

Every series request captures one `as_of` timestamp and executes against one SQLite read transaction/snapshot. The query planner uses clean, sealed rollups for complete buckets, then overlays any dirty or unsealed bucket from the next-finer tier (ultimately raw rows) and classifies pending/unanswered relative to that same `as_of`. It never serves a known-dirty persisted total as final. The response identifies its `as_of`, effective resolution, and newest partial bucket, so live minutes and late-repaired historical buckets are internally consistent.

Routes:

- `GET /healthz`
- `GET /readyz`
- `GET /api/v1/status`
- `GET /api/v1/targets`
- `GET /api/v1/targets/{id}/ping?from=&to=&max_points=`
- `GET /api/v1/interfaces`
- `GET /api/v1/interfaces/{name}/series?from=&to=&max_points=`
- `GET /api/v1/targets/{id}/traces?limit=`
- `GET /api/v1/traces/{id}`
- `GET /api/v1/route-changes?target=&limit=`

`from`/`to` accept RFC 3339 or Unix milliseconds. `max_points` defaults near the chart pixel width and is capped at 20,000. Queries choose raw/minute/hour input, merge adjacent histogram buckets to the budget, and return column-oriented arrays plus effective resolution, raw/rollup horizons, and partial-data warnings. A 90-day request must not scan raw rows.

The embedded UI includes target selection/status, 1h/6h/24h/7d/30d presets, uPlot latency percentile/min-max bands, timeout and late markers, deadline-miss and no-reply rates, send-error/scheduler-gap indicators, click-drag zoom with a finer server query, interface rates/errors/drops with reset markers, trace history/diffs, and capacity/capability status. It polls every five seconds and remains useful without JavaScript-generated server state.

## 10. Security and packaging

The example systemd service uses a dedicated `flameping` user, state directory, restrictive umask, only `CAP_NET_RAW`, `NoNewPrivileges`, a capability bounding set, `ProtectSystem=strict`, `ProtectHome`, and `PrivateTmp`. The data directory is `0750`; database-related files are accessible only to the service account/group.

The default standalone path is local `./flameping.db`; the packaged example uses `/var/lib/flameping/flameping.db`. No payload secret is stored. Public HTTP binding is explicit and logged; TLS and authentication are documented reverse-proxy responsibilities. Containers require `NET_RAW` for traceroute and observe their own network namespace unless host networking is chosen.

## 11. Test and benchmark matrix

- Strict config tests: defaults, unknown keys, durations/bytes, public bind, duplicate IDs, aggregate rates.
- Fake-clock scheduler: phase spread, fixed cadence, overload, coalesced gaps, no catch-up, cancellation.
- Echo payload/socket: malformed lengths, HMAC failure, wrong run/source/endpoint, reorder, duplicate, late, IPv4/IPv6, sequence boundary.
- Event/store saturation: reserved send admission, forced reply-before-row buffering, no silent loss, scheduler admission pauses, and permanent writer failure cancels the app.
- SQLite integration: migrations/checksums, config synchronization, reply upsert, dirty propagation, retention coverage, live/freelist budget, WAL/checkpoint bounds, shutdown/reopen, and simulated full-disk/page-limit errors.
- Histogram: fuzz decode, merge associativity, deterministic encoding, relative quantile error, version compatibility.
- API: invalid ranges, result caps, resolution selection, cancellation, unknown IDs, security headers.
- Interface fixtures and Linux namespace tests: mappings, reset, disappearance, dropped versus missed.
- Traceroute parser fuzzing and privileged namespace/veth tests: classic/Paris correlation, destination reached, loss, delay, multipath, deadline, capability degradation, two-run confirmation.
- Frontend unit tests plus browser smoke tests for presets, zoom re-query, target/interface switching, and route diff.
- `go test -race ./...`, `go vet ./...`, `CGO_ENABLED=0 go build ./cmd/flameping`, cross-compilation of unsupported stubs, and a 24-hour soak without goroutine, descriptor, queue, or WAL growth.

Benchmark ingestion and rollups at sustained 100, 1,000, and 5,000 probes/s before publishing a supported scale. Record CPU, allocations, commit latency, WAL growth, checkpoint latency, query latency, and bytes per raw/rollup row.

## 12. Staged delivery and acceptance

1. **Foundation** — module, build info, strict config, CLI, lifecycle, structured logging, health/readiness, example config, initial systemd unit. Acceptance: config tests pass; Linux and unsupported-platform stubs compile; `run` starts and shuts down cleanly.
2. **SQLite core** — migrations, target/endpoint sync, ordered event bus, batched writer, query pool, checkpointing. Acceptance: migrations are idempotent/checksummed; saturation preserves ordering; kill/reopen is consistent; WAL checkpoints after readers close.
3. **Echo engine** — resolver, scheduler, authenticated payload, IPv4/IPv6 shared sockets, send/reply/late/duplicate handling. Acceptance: 100 ms fake cadence without catch-up; loopback smoke succeeds when permitted; timeout, local error, and scheduler gaps remain distinct; race test passes.
4. **Rollups and retention** — histogram, minute/hour repair, interface-ready rollup primitives, budget state machine. Acceptance: tier totals agree; a late reply repairs both tiers; no uncovered bucket is deleted; page-limit pressure becomes visible and bounded.
5. **Ping API and UI** — bounded series queries, target/status dashboard, latency/loss drill-down. Acceptance: zoom selects finer data; every response respects `max_points`; 90-day queries avoid raw scans; committed UI builds reproducibly.
6. **Interface statistics** — rtnetlink collector, generations, rollups, API, UI. Acceptance: live/fixture collection and reset handling pass; unsupported platforms are explicit; dropped and missed counters remain distinct.
7. **Traceroute** — raw receiver, classic fallback, Paris mode, route confirmation, API, UI. Acceptance: privileged namespace topology discovers hops/destination; broken/unavailable raw access degrades only trace; loss/multipath does not produce an immediate false change.
8. **Release hardening** — doctor, backup/compact, operations/README, systemd review, browser checks, benchmarks, soak. Acceptance: non-root service uses only required capability; files are restrictive; disk pressure pauses safely; all default and privileged test suites pass.

Implementation proceeds only after both phase-two reviewers accept this plan. During implementation, any unavoidable deviation is recorded in this document before the affected code is merged.
