# Interface activity experiment

The old interface view required selecting a name before seeing any measurements, even with one configured interface. Its chart also put peak Mbps, counter increments, and reset markers on the same vertical scale. A small but useful error signal could disappear beside normal traffic.

This experiment makes local counter signals visible alongside latency, loss, and route activity, with automatic interface details and a shared time window.

## Research and alternatives

| Source / pattern | Useful idea | Application here |
| --- | --- | --- |
| [Grafana status history](https://grafana.com/docs/grafana/latest/visualizations/panels-visualizations/visualizations/status-history/) | Periodic observations appear as horizontal rows; missing values remain gaps. | One compact row per interface, aligned in time. Preserve unknown intervals instead of interpolating a healthy state. |
| [Netdata interface tables](https://learn.netdata.cloud/docs/network-performance-monitoring/device-metrics/integrations/snmp-devices) | Per-interface rows expose traffic, errors, and discards with further detail available. | Surface every configured interface and the affected interfaces immediately; keep throughput context separate from diagnostic counters. |
| [Linux networking statistics](https://docs.kernel.org/networking/statistics.html) | Error totals include diagnostic subcounters; dropped and missed counters have different meanings and driver-dependent interpretations. | Keep individual counters visible, avoid double counting, and describe recorded observations without inferring target loss or utilization. |

The chosen design combines a categorical timeline with a numeric inspector. An error-rate line would require packet-denominator semantics that the existing API does not provide. A single combined severity score would hide which counters increased and double count some diagnostics. A green/red continuous health ribbon would imply link-state and sampling coverage that the collector does not establish. Traffic spike detection would need an explicit baseline and threshold policy; ordinary throughput variation alone is not classified as anomalous.

## Interaction

- A summary above the flame graph reports interfaces with recorded signals, missing/unobserved interfaces, and unavailable history. It links directly to the interface overview.
- The compact **Interface activity** panel sits between the latency and route panels. Each interface is visible without selection. Separate rows within each timeline mark errors/diagnostics, drops/missed, and resets. Fixed categorical marks preserve small signals; their heights do not pretend to measure severity. Hatching denotes no counter interval, including reset-only buckets. Dashed edges denote clipped buckets.
- All three overviews use the same requested time bounds and horizontal plot area. Interface density is limited to the available width, capped at 180 buckets; zooming requests a finer view without creating hundreds of subpixel controls.
- The sole interface opens automatically and its name is plain text. With multiple interfaces, an interface containing recorded signals is chosen initially; later polls preserve the user's choice. Names switch details, while timeline rows continue to show every interface.
- Interface details appear automatically below the overview panels. RX/TX errors, dropped counts, missed counts, resets, and any diagnostic subcounter increases are directly visible. A separate throughput graph shows **peak observed Mbps per bucket**, with point marks for isolated observations and gaps for unknown values.
- Selecting an interval or **Next signal** highlights it on the flame and traffic plots, scrolls to the details, and focuses their heading. **Zoom charts here** changes the shared time window. **Whole window** restores aggregate details. Arrow keys, Home, and End navigate intervals; Enter or Space selects them. **Back to activity** returns to the overview.
- A disclosure lists all diagnostic counters and recorded reset times/reasons. CRC, FIFO, frame, carrier, and collision increases also appear above the disclosure when nonzero, including when a driver's aggregate error counters remain zero.
- Selected counters are a snapshot. Polling refreshes the overview and synchronized traffic without discarding that snapshot or its open disclosure. Transient list/series failures preserve selected evidence while marking current traffic unavailable. A changed time window clears selection. Host interface selection is independent of the selected destination.

## API and interpretation

The collector, schema, rollup writer, and retention behavior are unchanged. The existing `GET /api/v1/interfaces/{name}/series` response gains:

| Field | Meaning |
| --- | --- |
| `bucket_ms` | Native display bucket width; timestamps are epoch-aligned except the single-point aggregate. |
| `source_resolution_ms` | Planned historical tier: zero for raw samples, 60000 for minute data, 3600000 for hourly data. Dirty buckets can use finer observations. |
| `points[].has_deltas` | At least one usable counter pair contributed, or a rollup has positive elapsed time. A singleton baseline or a reset record alone does not establish measured zeros. |
| `points[].partial` | The display bucket extends beyond the requested edges. This flag does not describe sampling coverage. |
| `points[].rx_fifo`, `tx_fifo`, `rx_crc`, `rx_frame`, `tx_carrier`, `collisions` | Counter increments already retained in raw samples and historical rollups. |
| `points[].reset_count` | Authoritative count of reset records in the requested half-open interval, assigned to the display bucket. Never added to rollup reset counts. |
| `resets`, `resets_truncated` | Latest 100 reset observations with `at_ms` and `reason`; truncation does not reduce timeline marker counts. Narrowing the time window retrieves older details. |

All fields are additive; the existing rate/counter names and `reset` marker remain available. The response is assembled within the existing read transaction.

Counter increases are assigned to the bucket containing the endpoint observation. A sample pair can begin before the selected interval or span a collection gap. Historical edge rollups can include increments outside a requested zoom. The inspector explains this and shows full native timestamps for a selected clipped bucket. Neither a raw observation nor a rollup reveals the exact time an individual packet error happened.

`present` means a sampled interface generation remains open, not that its link is up or collection is fresh. The UI displays the last sample timestamp and missing/unobserved status without inferring uptime. No counter interval means unknown, not an outage or measured zero. Zero reported driver counters are not a guarantee that every error type is supported.

Error aggregates, diagnostic subcounters, dropped packets, missed packets, and collisions are kept distinct. Overview summaries count affected interfaces and buckets, not allegedly unique lost packets. Interfaces describe the monitored host; Flameping does not identify the egress interface for each target.

## Review and validation

An ultra-reasoning subagent reviewed the design, plan, backend semantics, and implementation. Its independent Chrome harness checked sparse/all-null traffic, snapshot/focus persistence, and production Content Security Policy. Review corrections included preserving diagnostic-only signals, coalescing slow polls, keeping failures distinct from empty configuration, preserving snapshots on transient list failures, and positioning timeline cells through CSSOM so production CSP permits them.

Store tests cover raw and rollup diagnostics, initial baselines, reset-only intervals, clipped historical zoom, half-open reset bounds, exact reset counts with truncated details, and single-point aggregation. The browser workflow exercises automatic single/multiple interface selection, all anomaly lanes, unknown values, actual plot alignment, keyboard navigation, interval zoom, polling snapshots, request failures and stale-response isolation under the production CSP. The existing flame and route regression workflow remains in place. Synthetic desktop/mobile previews complement the automated checks.
