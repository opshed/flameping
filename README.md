# Flameping

Flameping is a small, single-process network monitor inspired by SmokePing. It sends continuous authenticated ICMP echo probes (including sub-second schedules), preserves late replies, graphs latency and deadline loss, records Linux interface counters, and periodically captures UDP traceroutes and confirmed route changes. Everything—including the responsive web UI—is embedded in one static Go executable, with SQLite as the only data store.

## Build and run

Go 1.25 or newer is required. The committed web assets mean Node is not required for a normal build.

```sh
make build
cp configs/flameping.example.yaml flameping.yaml
./flameping check-config --config flameping.yaml
./flameping doctor --config flameping.yaml
./flameping run --config flameping.yaml
```

Open `http://127.0.0.1:8080`. The example monitors `1.1.1.1`; edit it before deploying.

On Linux, unprivileged echo requires the service user's group to fall within `net.ipv4.ping_group_range`. Traceroute reception requires `CAP_NET_RAW` (included in the sample systemd unit) or root. `doctor` reports both capabilities without changing the host.

## Per-target ping settings

The top-level `ping` block supplies defaults. Override individual timing settings
inside a target's `ping` block:

```yaml
ping:
  interval: 5s
  timeout: 1s
  min_interval: 100ms

targets:
  - id: gateway
    address: 1.1.1.1
    ping:
      interval: 1s
      timeout: 750ms
      min_interval: 250ms
    obsess: {}  # Uses this target's 250ms minimum for faster probing.
  - id: backup
    address: 9.9.9.9  # Inherits the global ping settings.
```

Omitted or null settings inherit independently; `ping: {}` inherits all timings.
Existing target-level `interval` and `timeout` fields remain supported. For each
field, choose either the direct form or the nested form; specifying both is an
error. A missing nested field also inherits its existing direct target value.

Each target may raise or lower the default minimum interval, with a hard floor
of 100ms. Its normal and obsess intervals must satisfy that target's minimum.
`event_queue`, `send_queue`, and `max_probes_per_second` configure the shared
engine and remain global. Restart Flameping after editing configuration.

## Why SQLite works here

One logical writer batches measurements into WAL transactions while up to four read connections serve the UI. Raw samples are rolled into mergeable one-minute and one-hour histograms. Defaults retain raw detail for 7 days, minute detail for 90 days, hourly history for 5 years, and impose a 5 GiB hard database budget. Pruning only removes a raw bucket after its replacement rollup is complete and clean.

Loss has explicit semantics: a reply past its target timeout counts as a deadline miss but remains a measured late reply; no reply and local send errors are separate; scheduler backpressure is recorded as a scheduler gap rather than falsely blamed on the network.

## Commands

```text
flameping run --config PATH
flameping check-config --config PATH
flameping doctor --config PATH
flameping version
flameping db backup --config PATH --output PATH
flameping db compact --config PATH
```

`db backup` uses SQLite's consistent `VACUUM INTO`. Stop the daemon before `db compact`.

See [operations.md](docs/operations.md) for deployment and recovery details, [design-research.md](docs/design-research.md) for the design rationale, and [implementation-plan.md](docs/implementation-plan.md) for component invariants.

This branch experiments with a **Route activity** timeline beneath the latency/loss chart. Warm marks show confirmed route changes; a separate lane shows retained trace observations. Select an interval to compare changed hops and inspect individual probes, or use **Zoom charts here** to align the charts to that interval. See the [research and experiment notes](docs/route-timeline-experiment.md) for alternatives, sources, and interpretation.

**Interface activity** surfaces host counter signals above and alongside the graphs. Aligned lanes show errors, drops/missed packets, and resets; interface details open automatically, including when only one interface is configured. Select an interval for counter specifics and reset reasons, with peak RX/TX traffic on its own scale. See the [interface experiment notes](docs/interface-timeline-experiment.md) for the design and measurement semantics.

## Development

Targets can optionally [obsess over loss or latency spikes](docs/obsess-experiment.md):
temporarily probe faster, then return to their normal interval after a continuous
healthy period against the saved pre-event threshold. Configure `obsess: {}` on
a target to try the defaults; live status appears in the dashboard.

```sh
make test
make race
make web       # requires Node/npm; regenerates embedded assets
make browser-smoke  # requires Node/npm and headless Chrome
```

The HTTP API is under `/api/v1`; `/healthz` is process liveness and `/readyz` reflects storage-writer readiness. The server listens on loopback by default and refuses a public listener unless `server.allow_public` is explicitly enabled. Flameping has no built-in authentication; put an authenticated reverse proxy in front of any public deployment.
