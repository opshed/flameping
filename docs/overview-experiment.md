# Tactical overview experiment

Status: implemented and validated as an experiment, 2026-09-09.

## Problem and intended outcome

The existing start page immediately selects a target and opens its detailed
latency, interface, and route charts. The sidebar shows the latest probe result,
but comparing recent evidence across targets requires opening each one. It is
also easy to confuse one latest result with behavior over a longer window.

The experiment adds an **Overview** landing page that answers three questions:
which targets have recent signals, how current is the evidence, and where should
the operator inspect next? The existing target charts remain the place to
examine latency distributions and correlate loss with interface and route
observations.

## Research and alternatives

These are patterns documented by the tools' authors. The proposed application
to Flameping is a design inference, not a measured usability result.

| Official source | Documented pattern | Application here |
| --- | --- | --- |
| [Checkmk dashboards](https://docs.checkmk.com/latest/en/dashboards.html) and [views](https://docs.checkmk.com/latest/en/views.html) | Status totals, problem lists, and history provide an overview; counts and table entries lead to narrower views. | Use compact target counts and a table with a direct path to evidence. Do not imply acknowledgment, incident tracking, or a service-health policy that Flameping does not implement. |
| [PingPlotter summary graphs](https://www.pingplotter.com/manual/summary_graphs/) | Sortable summaries compare targets and link to trace details. The same focus period applies across different probing intervals; sample counts can differ and statistics can be blank. | Give every row the same time bounds, preserve different sample counts, and carry those bounds into detail. |
| [SmokePing graph documentation](https://oss.oetiker.ch/smokeping/doc/reading.en.html) | Overview graphs compare measurements while a single-target detail graph exposes the latency distribution and interactive zoom. | Keep detailed distribution rendering in the target view; use a small trend only as navigation context on the landing page. |
| [LibreNMS dashboards](https://docs.librenms.org/Extensions/Dashboards/) | Availability tiles and separate device, component, and alert summaries provide compact status displays. | Borrow compact grouping, while retaining Flameping's distinct network outcomes, local collection failures, and missing evidence. |
| [Grafana dashboard guidance](https://grafana.com/docs/grafana/latest/dashboards/build-dashboards/best-practices/) | Dashboards should answer focused questions, progress from general to specific, and avoid aggregation that hides important differences. | Use one overview and the existing detail view. Do not invent a fleet-average RTT or combine unrelated signals into a health score. |
| [Grafana No Data and Error states](https://grafana.com/docs/grafana/latest/alerting/fundamentals/alert-rule-evaluation/nodata-and-error-states/) | A successful query with no observations differs from a failed query. | Keep no retained evidence, stale observations, and refresh failure visibly distinct. |

| Layout | Strength | Limitation | Decision |
| --- | --- | --- | --- |
| Compact triage table with a small trend per target | Compares names, counts, latency, freshness, and collection state in one scan; supports search and direct drilldown. | Requires concise combined cells and a usable narrow-screen layout. | Selected. |
| Colored target grid or target-by-time heatmap | Dense and useful for recognizing widespread changes. | A single color hides overlapping evidence; a continuous ribbon can imply healthy coverage where no probes exist. A richer matrix needs additional legend and interaction work. | Possible later correlation experiment. |
| Wall of full target charts | Preserves the familiar latency-distribution presentation for several targets at once. | Repeated legends and axes consume space; many charts and detail queries increase browser and database work. | Keep full charts in target detail. |

## Selected page and interaction

- **Overview** is the default landing page, including with a single target. The
  target sidebar remains available in the detail view, and the overview has an
  explicit title, selected recent window, and refresh status.
- A compact strip reports target counts: configured targets, targets with
  recorded deadline misses, actively obsessing targets, targets with local send
  errors or scheduler gaps, and targets needing current observations. Labels
  describe targets rather than packets. The
  categories overlap and must not be added into a total incident count.
- The target table combines related information into six columns: target
  identity; latest observation and age; recent latency and trend; deadline,
  no-reply, late, and pending evidence; local send errors and scheduler gaps;
  and retained route changes with trace-observation counts. Obsess state and
  requested interval accompany the target identity.
- Stable name ordering is the default. Search, sorting, and a signal filter
  narrow the table without assigning an unexplained severity score. Refreshes
  must preserve keyboard focus and the selected filter/sort.
- The rolling overview offers fixed **5m**, **15m**, and **1h** windows, with
  **15m** selected initially. Its small trends are bounded; absent observations
  remain gaps rather than interpolated measurements.
- Selecting a target opens the existing charts with the exact overview
  `from`/`to` bounds that were displayed. This fixes the historical time window,
  allowing the operator to inspect the same period even as time advances.
  It is not an immutable database snapshot: pending outcomes can settle and
  late replies can still update samples within that window.
  Choosing a chart preset returns that detail view to a rolling window.
  Returning to Overview resumes its selected rolling window. Target hash
  navigation identifies the configured stable target ID.
- Host interface context reports recorded errors, drops/missed counters,
  resets, traffic, and observation recency. Monitor context separately reports
  writer readiness, storage pressure, and collection capabilities.

The page should remain useful on a narrow screen without making exact counts
or unknown states available only through hover. Color supplements text labels.

## Measurement and status semantics

### Latest result versus recent history

The latest observation is one probe outcome or scheduler gap, with its age. A
single missing reply at its deadline does not establish a confirmed outage.
Use **No reply by deadline**, **Late reply**, **On-time reply**, **Pending**,
**Local send error**, or **Scheduler gap** as appropriate. Stale and unobserved
targets remain distinct from current network outcomes. The age rule must apply
to scheduler-gap observations as well as ordinary probe samples.

Recent columns summarize the selected window, independently of the latest
result. A target can therefore show a current on-time reply and retained recent
deadline misses. Likewise, an active obsess incident can be gathering recovery
evidence while its latest reply is on time. Obsessing means faster requested
probing, not proof of a continuing outage; ready, learning, paused, and limited
tracking states must retain their existing meanings.

### Counts, denominators, and latency

For successfully sent probes in the selected window:

```text
sent = on_time + late + unanswered + pending
deadline_miss_percent = 100 * (late + unanswered) / sent
no_reply_percent = 100 * unanswered / sent
```

These percentages retain the existing chart's sent-probe denominator. Pending
probes are explicitly reported and make the current result provisional. Pending
is neither a deadline miss nor a reason to classify a target as needing data.
An all-pending sample set is awaiting outcomes, not evidence that every probe
succeeded. When no probes were sent, percentages are unavailable rather than
measured zero.

A late reply remains a measured RTT and a deadline miss. When an unanswered
probe later replies, its no-reply classification can change while its deadline
miss remains. Local send errors and skipped scheduler slots stay outside network
loss counts and retain separate counts.

Latency percentiles use the combined RTT histogram for the selected target and
window, including retained late replies, consistently with the existing charts.
They are not averages of bucket percentiles. No measured replies means no RTT
value. Probe-count weighting also means an obsess period contributes more
observations than a normal-cadence period. Neither these percentages nor the
small trends estimate time-based availability.

### Route, interface, and monitor context

Route cells count retained confirmed changes and retained trace observations.
Zero confirmations does not prove route stability, particularly when no traces
were retained. Trace errors, missing intermediate-hop replies, and an unobserved
destination are not interchangeable with destination ping loss. The existing
route detector and confirmation semantics remain authoritative.

Interfaces describe the monitored host. Flameping does not establish a target's
egress interface or causation between an interface counter increase and a ping
incident. Error aggregates, diagnostic subcounters, drops, and missed packets
can overlap and must not be summed as unique lost packets. Reset-only or
singleton observations do not establish a valid counter interval. Interface
presence is not link health or proof of fresh collection.

Monitor readiness describes the collection/storage process. It must not be
presented as an overall network-health result. Disabled or unsupported
capabilities differ from collection failures.

## Bounded data plan

`GET /api/v1/overview?window=5m|15m|1h` accepts only those fixed recent windows;
omitting `window` selects 15 minutes. The response supplies `as_of_ms`, `from_ms`,
`to_ms`, `window_ms`, `targets`, and `interfaces`. Bounds are aligned to whole
milliseconds and half-open `[from, to)`, so chart requests can reproduce them.
Each target contains its latest summary, `recent` ping counts and percentiles,
`trend` buckets, and retained `route` counts. Percentages are JSON `null` when
their sent-probe denominator is zero. Pending probes are explicit and set the
ping aggregate's `partial` flag.

The server assembles stored observations within one read transaction. The
browser does not issue a detailed ping/route request for every target. Clean,
complete minute rollups provide the middle of each ping window; raw samples
provide exact edge minutes and contiguous ranges of dirty or missing minutes.
Histograms are merged across those non-overlapping sources. One-minute trend
buckets cover 5m and 15m windows, and four-minute buckets cover 1h. Epoch-aligned
edge buckets are clipped to the requested bounds, producing at most 16 buckets
per target. Their bounds describe the clipped interval, not complete sampling
coverage. Live obsess state accompanies the result without claiming it is a
persisted history of incidents.

Windows of at most one hour fit within Flameping's minimum 24-hour raw retention,
including its emergency raw-pruning floor. The raw fallback can therefore reuse
the current probe and scheduler-gap semantics without introducing a new
historical rollup tier or migration. Interface aggregates use sample pairs ending
inside the window; a predecessor before the start can contribute a valid delta
and sets `partial`. `has_deltas` distinguishes measured zero values from absent
counter intervals, and traffic reports peak observed pair rates. Configuration,
collector cadence, and retention policy do not change for this experiment.

Overview polling follows the existing five-second cadence while the page is
visible, with bounded query work and coalesced refreshes. Responses for an old
window or navigation context must not replace newer evidence. A failed refresh
may preserve the previous snapshot, but must identify it as old and show its
timestamp; it must not replace unknown values with zeros or display an all-clear
message. An empty configuration and a successful query without samples require
their own explanations.

## Implementation plan

1. Add the overview query and HTTP contract with fixed-window validation,
   consistent time bounds, merged latency histograms, and bounded trends.
2. Add overview/detail navigation, the target-count strip, six-column table,
   filters, and separate host/monitor context.
3. Preserve current target charts and pass exact snapshot bounds into drilldown.
4. Verify outcome counts, pending and late updates, stale scheduler gaps, empty
   and failed data, retained route counts, and sparse interface observations.
5. Exercise overview navigation, search/sort/filter behavior, polling races,
   focus preservation, narrow layouts, and existing chart interactions under
   the production content security policy.
6. Review the implementation independently and run the relevant Go and browser
   checks before recording the results.

## Review and validation

Independent ultra-effort reviews covered the source research, layout alternatives,
API plan, backend aggregation, and frontend implementation. Review corrections
included matching the chart's sent-probe denominator, aging scheduler-gap
observations, retaining keyboard focus across refreshes, positioning clipped
trend buckets by their actual time bounds, and distinguishing never-sampled
interfaces from missing interfaces. Target navigation now clears and reloads its
chart context immediately, independently of the target-list metadata request,
so a failed metadata refresh cannot leave the previous target's evidence under
the new target's heading. The final code review found no blocking issues.

Validation passed:

- The full Go test suite, targeted storage/API race tests, TypeScript checking,
  the frontend asset build, and the binary build.
- Storage and API regressions for fixed-window bounds, pending outcomes,
  zero-denominator values, raw/rollup agreement, late replies that dirty a clean
  minute, scheduler-gap records spanning raw edges and clean minutes, active
  target filtering, stale gaps, interface diagnostic counters and resets, and
  retained trace/confirmation counts.
- The extended browser smoke workflow under the production content security
  policy: overview landing, target counts, search/filter controls, escaped
  names, pending evidence, interface diagnostics, refresh focus, failed-refresh
  preservation and recovery, delayed-window responses, exact-bound drilldown,
  back navigation, and target switching while metadata fails. The existing
  flame, route, and interface workflows also passed.
- Independent inspection of the generated desktop and mobile overview previews.
  The narrow layout retains labeled evidence without horizontal clipping;
  compact count cards leave the first target visible in the initial viewport.
- A temporary loopback instance of the built binary: all three overview windows,
  live obsess annotation, exact counts, the trend-bucket cap, the embedded
  landing page, and clean shutdown.

A local benchmark with 200 targets, 720,000 raw probe rows, and clean minute
rollups measured about 144 milliseconds per one-hour overview query, with about
18 MB allocated. This fixture exercises the recent-window query path; the
experiment still needs normal operator use to assess whether its information
hierarchy makes triage faster.
