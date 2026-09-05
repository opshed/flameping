# Route timeline experiment

Branch: `experiment/route-timeline` · research and experiment: 2026-09-04

## Problem

The previous traceroute panel listed the latest 12 runs and six confirmed changes. It ignored the selected latency/loss time window. Seeing whether a loss incident coincided with a route change required mentally comparing timestamps, and the detailed view offered little context for a selected run.

The experiment makes **when did the path change?** the first question the route view answers, then reveals **what changed?** and **what did the probes actually observe?** on request.

## Research

These are product patterns documented by their authors. The proposed Flameping design is our inference from those patterns, not a measured usability result.

| Tool | Documented approach | Useful lesson |
| --- | --- | --- |
| [RIPE Atlas Path Analysis](https://atlas.ripe.net/docs/tools-and-code/path-analysis/) | A timeline marks detected changes, with navigation between events and a separate before/after comparison mode. | Put events on the time axis and make comparison the next layer of detail. |
| [PingPlotter](https://www.pingplotter.com/manual/route_changes/) | Moving the focus period through the time graph updates the displayed route; a smaller focus period reveals finer route detail. | Routes and loss should share one time context. |
| [ThousandEyes](https://www.thousandeyes.com/blog/path-quality-understanding-impact-packet-loss-applications) | Selecting times around a loss spike reveals the corresponding historical path visualization. | Preserve the relationship between an incident and its observed path; do not force timestamp matching between disconnected panels. |
| [RIPE Khipu](https://atlas.ripe.net/docs/tools-and-code/khipu/) | IP/ASN/country views, filters, and pinned node details offer different levels of detail. | Keep the overview compact and reveal identifiers as needed. ASN grouping needs additional data that Flameping does not collect. |
| [Historical TraceMON](https://atlas.ripe.net/docs/tools-and-code/tracemon/) | A latency/loss chart selects a historical traceroute, with raw textual detail available separately. Its documentation now identifies it as retired in favor of Khipu. | Use the time correlation pattern, rather than adopting a retired widget. |

Measurement semantics constrain visualization. [Paris traceroute's authors](https://paris-traceroute.net/about/) explain how load balancing can make classic traceroute infer false links; keeping flow identity stable helps, but does not provide an exhaustive path map. Responders at successive TTLs therefore should not be connected into every possible apparent path. [PingPlotter's explanation of intermediate-hop loss](https://www.pingman.com/kb/5) also distinguishes missing router replies from forwarding loss. A silent intermediate hop should read as an unknown observation.

## Alternatives considered

| Idea | Strength | Limitation | Decision |
| --- | --- | --- | --- |
| Route identity ribbon, such as A → B → A | Very fast recognition of returning routes and long stable periods. | Raw signatures vary with missing replies and responder subsets. Reliable persistent identities require context boundaries and explicit coverage; interpolated bands can overstate stability. | Possible later experiment after defining those semantics. |
| Hop-by-time matrix | Shows which TTLs vary and where observations disappear. | Tall and visually noisy with many TTLs; needs careful treatment of unknown replies and bounded historical data. | Possible expanded view, not the initial overview. |
| Node/link topology | Useful for inspecting complex simultaneous paths. | Harder to scan over time; current responder sets cannot establish all individual links. | Defer. |
| Change activity strip + observation lane + inspector | Makes timing and concentration of confirmed events visible, with gaps and raw evidence kept explicit. | Does not name persistent route identities or claim exact change times. | Implemented experiment. |

## Implemented interaction

- A full-width **Route activity** panel sits immediately below the Flameping latency/loss plot. Both use the same requested time bounds and horizontal plot area. Presets and drag-to-zoom query both views.
- The upper lane has warm marks for confirmed changes. Height increases with the number of events in that interval, using a logarithmic scale relative to the busiest interval in the visible window. Empty upper cells mean no recorded confirmations, not proof of route stability. Counts remain available in the interval label.
- The lower lane independently indicates retained completed traces that reached the destination, completed traces that did not, and errors/timeouts. Multiple categories can appear in one interval. Hatching means no retained traces; the display does not assume an expected sampling cadence.
- Clicking an interval opens an inspector for that interval and highlights it on the loss plot. Arrow keys move between intervals; Enter or Space opens one. **Inspect history** opens the entire window. **Zoom charts here** applies an interval to the charts so even dense history can be examined.
- Confirmed events show first-observed and confirmation timestamps. Selecting an event shows the stored before/after responder sets at changed TTLs, with unchanged TTLs collapsed and older/newer event navigation. Evidence buttons open the baseline, first-observation, or confirming trace.
- Individual trace runs are available in a disclosure. Raw detail includes destination, method, flow identity, outcome, TTL, probe number, responder, RTT, and ICMP type/code.
- An open inspector is a snapshot. Periodic refresh updates the overview without replacing the selected record or expanded details. Changing target or time window clears selection and cancels dependent requests.

## Data and interpretation

The experiment uses the existing detector and stored events. It does not infer new confirmations in JavaScript. The detector ignores missing-only differences and overlapping responder sets when reached-hop observations agree; endpoint, method, and flow changes reset its comparison baseline. A completed run can still fail to reach the destination. Errors/timeouts are separate from completed runs in overview counts.

Confirmation requires two matching eligible observations; intervening error runs do not reset the current implementation's candidate. First observed and confirmed are observation timestamps, not a precise bracket proving the physical transition time. The linked old trace is the stored baseline, which can predate the most recent observation before the event.

`GET /api/v1/targets/{target}/route-history` accepts `from`, `to`, `max_points` (1–300) and `limit` (1–500). Time bounds are half-open `[from, to)`. Trace observations are assigned by start time, confirmed events by confirmation time.

The response includes requested bounds, bucket width, exact totals for retained records, bounded aggregate buckets including empty intervals, and bounded lists of recent traces and changes. Each list has an explicit truncation flag. The UI requests at most 120 buckets and 100 records per list. Bounded lists never replace aggregate counts; selecting and zooming into smaller intervals retrieves earlier records. Queries share a read transaction so totals and lists describe the same database snapshot.

Ordinary traces and change records have different retention behavior. Neither zero traces nor zero changes proves uninterrupted monitoring. Raw evidence links can be unavailable after pruning. The UI reports these conditions and request errors instead of leaving stale details under a different target.

## Review and validation

Ultra-reasoning subagents reviewed the primary-source research, refined the plan and backend contract, and reviewed the implementation. Independent browser validation exercises the complete interactive path using synthetic route histories, including selection, range alignment, raw detail, refresh, and request races. Store/API tests cover range boundaries, counts despite capped records, identity, and invalid requests. Desktop and mobile screenshots support visual inspection.

The experiment is successful if a user can locate a route-change cluster relative to a loss incident at a glance, reveal the differing responders in one selection, and inspect probe evidence without losing the selected time context. Live network observations and user feedback are still needed to decide whether this should replace the original view permanently.
