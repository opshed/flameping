# Operations

## Deployment

Install the static `flameping` binary at `/usr/local/bin/flameping`, the example configuration as `/etc/flameping/config.yaml`, and the sample unit as `/etc/systemd/system/flameping.service`. With `StateDirectory=flameping`, use `storage.path: /var/lib/flameping/flameping.db`.

Validate before restart:

```sh
flameping check-config --config /etc/flameping/config.yaml
flameping doctor --config /etc/flameping/config.yaml
systemctl daemon-reload
systemctl enable --now flameping
```

The unit grants only `CAP_NET_RAW`, used by the raw ICMP traceroute receiver. Echo probes normally use Linux ping sockets controlled by `/proc/sys/net/ipv4/ping_group_range`. The database, WAL, and backup files are created mode 0640; keep the containing directory private.

## Health and diagnosis

- `GET /healthz`: the HTTP process is responding.
- `GET /readyz`: the SQLite write owner is healthy and accepting measurements.
- `GET /api/v1/status`: readiness/writer errors, file sizes, reusable pages, retention horizons, dirty rollup backlog, and per-family traceroute/interface capability state.
- `journalctl -u flameping`: structured component and capability errors.

A permanent non-capacity write error drops readiness and terminates the daemon. Flameping deliberately stops sending probes rather than silently collecting incomplete history. On SQLite or filesystem capacity exhaustion, the writer retains its current batch, attempts a bounded WAL checkpoint and safe retention ladder, and retries that exact batch. If space remains exhausted, measurement admission pauses and readiness drops while HTTP diagnostics stay available; restore headroom, then restart once the retained queue has drained or stop explicitly if the batch cannot be recovered.

## Backup and recovery

Create an online, transactionally consistent backup:

```sh
flameping db backup --config /etc/flameping/config.yaml --output /srv/backup/flameping-$(date +%F).db
```

Test backups periodically with `flameping doctor` against a temporary configuration. To restore, stop Flameping, preserve the current `.db`, `.db-wal`, and `.db-shm` files together, copy the verified backup into place, confirm ownership, and start the service.

SQLite reuses freed pages, so the main file does not normally shrink after retention. To return unused pages to the filesystem, stop the daemon and run:

```sh
flameping db compact --config /etc/flameping/config.yaml
```

Compaction needs temporary disk headroom and should not be placed in a routine timer.

## Tuning

The configured aggregate probe rate is validated against `ping.max_probes_per_second`. Increase queue sizes only after examining storage latency; a larger queue delays backpressure but uses more memory. `storage.max_bytes` is enforced through SQLite's page limit with headroom for WAL. Retention is incremental. Under budget pressure, raw data is shortened first; finer and then coarser rollups can be shortened only to the non-negotiable 30-day review floor before measurement pauses.

### Per-target ping settings

The top-level `ping` block supplies default timings. A target can override
`ping.interval`, `ping.timeout`, and `ping.min_interval` independently. Omitted
or null fields inherit their defaults; `ping: {}` inherits all three timings.

The existing direct target-level `interval` and `timeout` fields are also
supported. For each field, use either the direct form or the nested form;
specifying both is an error. An omitted nested field inherits a direct
target-level value before falling back to the global default.

Each target may raise or lower the global minimum interval, with a hard floor
of 100 ms. Both its normal and obsess intervals must satisfy that target's
minimum. The obsess interval must also be shorter than its normal interval.
`event_queue`, `send_queue`, and `max_probes_per_second` configure the shared
engine and remain global. Restart Flameping after editing configuration.

See the [README configuration examples](../README.md#per-target-ping-settings)
and [obsess behavior](obsess-experiment.md) for usage and recovery details.

## Build metadata

`make build` records the source commit and UTC build time in `flameping version`.
Set `VERSION` for a versioned build; it defaults to `dev`. You can also supply
`COMMIT` and `BUILD_DATE` when building from a source archive or reproducing a
build. The normal build embeds the committed web assets, so it does not require
Node. Frontend development commands are listed in the
[README](../README.md#development).

## Web access

The UI/API is unauthenticated. Keep the default loopback listener or use TLS and authentication at a reverse proxy. Setting `allow_public: true` is an explicit acknowledgement, not a security control.
