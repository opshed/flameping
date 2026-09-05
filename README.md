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

## Development

```sh
make test
make race
make web       # requires Node/npm; regenerates embedded assets
make browser-smoke  # requires Node/npm and headless Chrome
```

The HTTP API is under `/api/v1`; `/healthz` is process liveness and `/readyz` reflects storage-writer readiness. The server listens on loopback by default and refuses a public listener unless `server.allow_public` is explicitly enabled. Flameping has no built-in authentication; put an authenticated reverse proxy in front of any public deployment.
