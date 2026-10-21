# Outlier flames experiment

Status: proposed 2026-10-20 (variant C chosen from the mockup); not yet implemented.

## Problem and intended outcome

The target detail chart ("Latency distribution & loss") hides rare bad pings
when zoomed out. On the JacksTailscale target, the 6h preset covered 4,320 pings
with 14 replies over 1 s, four over 4 s, 14 late replies, and 14 unanswered
pings. Yet the chart showed only a nearly invisible dark-red dash near 4,300 ms
and 1 px slivers in the loss lanes. One dash sat directly on the 2,000 ms grid
line and was effectively hidden.

The operator's framing: a single slow ping is not the most important thing the
chart must show (the typical distribution, the "smoke", still is), but rare
outliers and small loss counts must never be *hidden*. They should be a little
more obvious, drawn as an ember with a flame trail rather than a dark line
floating in a dark field.

## Diagnosis

1. **Tail grains are never emitted.** `aggregatesToPoints`
   (`internal/store/sqlite/query.go`) returns up to 17 midpoint quantiles,
   `(i+0.5)/17`, per bucket. The top grain is rank `ceil(0.9706 n)`, so for
   n ≥ 35 a bucket's single largest reply is never drawn; a bucket needs two
   tail replies before one appears. At 6h (5-minute buckets, ~60 pings at a 5 s
   interval) the 12:52:52 reply of 4,115 ms vanished entirely, and only one of
   the three 4–5 s replies at 12:47 survived.
2. **Rarity is drawn faintest.** `flameSmokePlugin` (`web/src/app.ts`) colors
   each grain by local neighbor density; an isolated grain is
   `rgba(205,48,31,.28)` and 2.7 px tall, drawn under the grid-line layer's
   colors. The rarest, most diagnostic values are the least visible.
3. **Linear axis.** `flameLatencyRange` scales linearly to the highest grain.
   One 4 s grain crushes the 5–15 ms body into a flat line; without that grain
   the axis does not reach the outlier at all.
4. **Loss lanes scale by percent.** Lane height is `pct/100` of the lane with a
   1 device-pixel floor: one lost ping of 60 is 0.4 px, and at the 30d preset
   (4 h buckets, ~2,880 pings) it is the same 1 px sliver.
5. Late replies are in the RTT histogram, so a 4 s reply is legitimately both a
   latency value and a deadline-lane count. The design keeps that dual identity.
6. Side note: for buckets with fewer than 100 replies, the current `p99` equals
   `max` (rank `ceil(.99 n) = n`).

Bucket widths per preset (`seriesPointWidth`): 1h → 60 s, 6h → 5 min, 24h →
15 min, 7d → 1 h, 30d → 4 h. Presets of 6h and longer are served from minute or
hour rollups through the merged histogram; drag-zoom windows can be served from
raw samples.

## Research and alternatives

Four variants were mocked up with real data (JacksTailscale 1h, 6h, 24h,
drag-zoom, 4 h buckets; Cloudflare 24h; fw1-internal 24h): A reproduced today's
rendering, B used a log axis, C ("body + sky") used a piecewise axis, and D
moved lost pings into an ember row at the top. **C was selected.**

| Idea | Decision |
| --- | --- |
| Pure log axis (B) | Rejected as default: a 5 → 7 ms shift vanishes and the smoke flattens into a band. |
| Symlog or square-root axis | Rejected: unfamiliar ticks, same mid-range squashing. |
| Pin outliers at a ceiling with value labels | Subsumed by the sky; loses relative magnitude. |
| **Body + sky piecewise axis (C)** | Selected: linear where the smoke lives, log where rare values live, with a visible seam. |
| Always emit min/max as extra grains | Partial fix only; still density-colored and squashed. Folded into tail bins. |
| Ship the full histogram per bucket | Rejected: 5–10× payload, client re-derives quantiles. |
| **Tail bins with counts** | Selected: histogram-derived, works for every storage tier, never implies more pings than existed. |
| **Exact worst samples with timestamps** | Selected for hover, within raw retention and a row budget. |
| Severity → brightness (instead of density → brightness) | Selected for sparks, gated so mild tails stay smoke. |
| **Ember with a flame trail** | Selected: ties the spark to its bucket and is the product's visual identity. |
| p99/max envelope lines | Rejected: jagged at 181 points; trails do the job per bucket. |
| Loss as heat color only (SmokePing style) | Rejected: reproduces "one loss is invisible". |
| **Square-root lanes, 3 px floor, count numerals** | Selected. |
| Lost pings as embers in a top row (D) | Rejected by the operator in favor of keeping the two lanes. |
| Sparks in linear (no-sky) mode | Rejected: 20 ms pings on a 3.4 ms target lit up as flames. Tail values there become guaranteed-visible grains instead. |

## Selected design

### API changes

Add to each `PingPoint` (`/api/v1/targets/{target}/ping`):

- `tail: [{ms, n}]` — histogram bins of that bucket that qualify, highest
  first, at most 24 entries, plus `tail_truncated: bool` when more bins
  qualified. A bin qualifies when either:
  - its value is above the window seam (`window.seam_ms`, below), in any
    bucket. This guarantees every potential ember is sent, including a lone
    reply in a one-ping bucket and a cluster of slow replies that shares one
    histogram bin; or
  - the bucket has more replies than smoke grains (`rtt_count >
    distribution_cap`, 17) and the bin's value is at or above
    `max(4 × p50, p95)` of the bucket. At or above, not strictly above, so a
    slow cluster that contains the bucket's p95 still qualifies. Buckets with
    17 or fewer replies already draw every reply as its own smoke grain, so
    they get no below-seam tail entries and drag-zoom keeps its smoke look.

  `ms` is the bin midpoint clamped to `[min_ms, max_ms]` (so the bucket max is
  exact); `n` is the bin's count. Computed from the same merged histogram as
  the quantiles, so raw, minute, and hour tiers behave identically.
  Invariant: `sum(n) ≤ rtt_count`.
- `worst: [{time_ms, rtt_ms, late}]` — the bucket's highest RTT replies, at most
  5, exact values and scheduled times from `ping_samples`.
- `misses: [time_ms]` — scheduled times of the bucket's unanswered pings, at
  most 5 (the count is already in `unanswered`).

`worst` and `misses` are present only for buckets fully inside raw retention,
and only when the window's attempted-probe count (known from the aggregates
before the scan) is within a row budget; the scan is one ordered range read of
`ping_samples` for the window, separate from the aggregation pass (raw-served
windows are short drag-zooms, so the second read is cheap and keeps one code
path).

Add to `PingSeries`:

- `window: {rtt_count, p50_ms, p95_ms, p99_ms, max_ms, seam_ms}` — pooled over
  the merge of all returned bucket histograms (a second, in-memory pass over
  the aggregates). The server owns the seam formula below and computes
  `seam_ms` before extracting tails; the client uses `seam_ms` as given.

Existing fields (`distribution_ms`, counts, `timeout_min_ms`, `timeout_max_ms`,
`distribution_method`, `distribution_cap`) are unchanged. No storage or
migration changes.

### Y axis

- Seam `S = window.seam_ms`, computed by the server as
  `niceCeil(2 × window p95)` using the set
  {1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8} × 10ᵏ.
- **Sky mode** exists iff `window.max_ms ≥ 3 × S`. The latency area is then
  62% linear body (0 → S) and 38% logarithmic sky
  (S → `niceCeil(1.15 × max)`). The space reserved below zero for the loss
  rail is unchanged.
- Without sky mode: linear 0 → `niceCeil(1.08 × max)` (the current look), where
  `max` includes tail values, so the axis always reaches the worst reply.
- Seam rendering: dashed seam line across the plot, a zig-zag break glyph on
  the axis at the seam tick, sky tick labels in warmer ink (`#c99a6a`), sky
  ticks within 12 px of the seam dropped, a faint warm wash over the sky
  (`rgba(255,120,40,.035)`), and a right-aligned label
  "LOG SCALE ABOVE 200 ms ↑".
- Deadline: a dashed line at `timeout_max_ms` when it is inside the y range,
  labelled at the left "DEADLINE 1.0 s · late above".
- The mean, p50, p95, and best series share the piecewise scale, so a bucket
  whose mean is pulled up by an outlier rises into the sky. That is honest and
  matches the mockup; the lines are not clipped at the seam.

### Smoke, tail grains, and embers

- **Smoke**: the 17 body grains render as today, except that the isolated-grain
  color's alpha is raised from .28 to .42.
- **Tail grains**: tail entries that are not embers (all tail values without
  sky mode, tail values below the seam, and mild sky values) are drawn as
  guaranteed-visible grains: brighter than smoke (orange
  `rgba(255,190,110,.7)`; red `rgba(255,120,90,.85)` when late), 4 px tall,
  slot-width like smoke grains. Mild sky values use alpha `.55 + .6·sev`.
- **Embers** (sky mode only), for each tail entry with `v > S`:
  - `sev = clamp(ln(v/S) / ln(max(top/S, 20)), 0, 1)`, `late = v > timeout_max_ms`.
  - Mild: `!late && sev < .35` → tail grain as above, no trail or glow.
  - Otherwise a tapered flame trail from `y(min(p95, S))` up to `v`: base width
    1.8 / 3 / 4 px for n = 1 / 2–3 / ≥ 4, plus `1.6·sev`, capped at 55% of the
    slot; vertical gradient alpha 0 → `.5a` → `a` with `a = .2 + .7·sev`;
    orange `255,157,66` on time, red `255,90,46` late.
  - A core ellipse at `v`: radius `(1.3 / 1.8 / 2.2 by n) + 1.1·sev`, height
    1.7× width; fill `#ffb86a` / `#ffdcae` / `#fff6ea` by sev thirds; alpha
    `.7 + .3·sev`; glow `3 + 9·sev` px (+3 px, color `#ff3b1e`, when late;
    otherwise `#ff8a2e`).
  - Labels: the value on the highest ember of each of the three worst buckets;
    `×n` when n ≥ 3, or n ≥ 2 and not mild, and the slot is ≥ 10 px. Text in
    ink color on a dark backdrop, to the right of the ember, or to the left
    when the right side would leave the plot, overlap a placed label, or cover
    another bucket's ember core or tail grain; a label that fits on neither
    side is skipped (hover still has the value). Use the exact `worst` value
    when present, otherwise prefix "≈".
- **Layering**: embers, tail grains, and labels are drawn after the grid,
  seam, deadline, and series lines, so no line can hide one. Embers and tail
  grains get a thin dark keyline so they separate from a line beneath them.
  Every trail keyline is drawn before any trail, and every core keyline before
  any core, so one ember's keyline never darkens another's trail or glow in
  the same bucket.

### Loss lanes

- Bar height = inner lane height × √(pct/100), with a 3 px minimum and full
  opacity, so one lost or late ping is always visible at every preset.
- When the slot is ≥ 13 px, the literal count is centred above or inside the
  bar (dark ink when the bar is tall).
- A small right-aligned note on the rail: "√ scale · min 3 px · numerals = pings".
- Legend values read as counts with percentages, e.g. "3 of 60 (5.0%)".
- The full-loss hatch, send-error, scheduler-gap, and partial markers are unchanged.

### Hover

A bucket tooltip next to the cursor shows: the bucket's time range and sent
count; p50 / p95 / p99 / max; late and no-reply counts with percentages; local
send errors when present; the `worst` list with exact times and "late" marks;
the `misses` times (with "…" when `unanswered` exceeds the list); and the first
few tail bins with counts when `worst` is absent. The existing legend readout
row stays.

### Behaviour by range

- **1h, 6h, 24h**: sky mode whenever the window has replies ≥ 3× the seam; lane
  numerals visible; exact worst samples in hover.
- **7d, 30d**: tail bins come from hour-rollup histograms, so one 4 s reply among
  2,880 pings is still a bright ember at the right height. Lane numerals are
  hidden (slots < 13 px) but bars keep the 3 px floor; counts are in hover.
  Exact samples appear in hover only while the window is within raw retention
  and the row budget.
- **Drag-zoom to about one ping per bucket**: every reply is already its own
  smoke grain, and only replies above the seam become tail entries, so a 2.5 s
  ping is still a labelled ember while normal replies stay smoke. The seam rises
  toward the spikes because the window's p95 is itself a spike; that is
  expected.
- **Clean targets** (for example Cloudflare at 3–4 ms with a 23 ms maximum): no
  sky; tail values show as guaranteed-visible grains, not flames.
- **LAN targets**: the threshold is relative to the window's own p95. On
  fw1-internal (0.7 ms typical) a single 48 ms reply becomes a labelled ember.
  This was proposed during review and the operator did not object.

## Calls left to the builder

Pixel constants above come from the approved mockup and may be tuned slightly
for uPlot's coordinate system and device-pixel ratio, as long as the visual
result matches. Also left to the builder: code organization within `app.ts`,
how the piecewise scale is expressed in uPlot (`distr: 100` with custom
`fwd`/`bwd` and `splits` is known to work in 1.6.32), the exact row budget for
the `worst` scan, the tooltip's positioning and styling (matching the existing
dark theme), and the test design.

The approved mockup was rendered from a copy of an operator's database and is
not committed, because it contains real network data. Automated tests use
synthetic fixtures that reproduce the reviewed shapes: a ~8 ms target with a
lone 4,115 ms reply among 60 pings, a cluster of three 4–5 s replies in one
bucket, five slow replies sharing one histogram bin, a clean 3–4 ms target with
a 23 ms maximum (no sky), a 0.7 ms LAN target with a 48 ms blip, a drag-zoom of
one to three pings per bucket, and one lost ping among 2,880. Before calling the
work done, the built UI is also checked by eye against a copy of that database.

## Implementation plan

See [the implementation plan](superpowers/plans/2026-10-20-outlier-flames.md).

## Review and validation

To be recorded after implementation.
