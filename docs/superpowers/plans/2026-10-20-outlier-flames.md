# Outlier Flames Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make rare slow replies and small loss counts visible at every zoom level of the target latency chart, using a "body + sky" piecewise axis, ember-with-flame-trail outliers, and count-aware loss lanes.

**Architecture:** The ping series API gains per-bucket tail bins (from the existing merged histograms), exact worst/missed samples (from raw rows, budgeted), and a pooled window summary that owns the seam. The uPlot chart switches its y scale to a custom piecewise distribution and draws embers, tail grains, labels, √ loss lanes, and a bucket tooltip from that data.

**Tech Stack:** Go 1.25+, SQLite (modernc), TypeScript 5.9, uPlot 1.6.32, esbuild, headless Chrome smoke test.

**Spec:** `docs/outlier-flames-experiment.md`

## Global Constraints

- Work on branch `outlier-flames`, never directly on `master`.
- Never alter files in `internal/store/sqlite/migrations/`; no schema changes.
- Run Go tests as `TMPDIR=/dev/shm/flameping-test go test ./...` (create the dir first). The main disk is 98% full and trips the 10% free-space reserve in store tests.
- `internal/webui/dist/` is committed: every task that changes `web/src` runs `cd web && npm run build` and commits the rebuilt dist with the source.
- No new npm or Go dependencies. uPlot stays at 1.6.32.
- Production CSP is `style-src 'self'`: no inline `style=""` attributes or `<style>` in markup; positioning via `element.style.*` from script is fine.
- Match the surrounding style: dense one-line TypeScript in `web/src`, gofmt Go.
- Add `AIDEV-NOTE:` anchors at the tail rule, the server-owned seam, the exact-sample budget, the piecewise scale, and the ember severity mapping.
- Spec values, verbatim: tail cap 24; seam = niceCeil(2 × window p95) with nice set {1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8} × 10ᵏ; sky iff window max ≥ 3 × seam; body 62% / sky 38%; top niceCeil(1.15 × max) with sky, niceCeil(1.08 × max) without; isolated smoke alpha .42; lane height inner × √(pct/100), 3 px minimum; numerals when slot ≥ 13 px; labels "LOG SCALE ABOVE {seam} ↑", "DEADLINE {timeout} · late above", "√ scale · min 3 px · numerals = pings".

## Review Focus

1. A window where every ping went unanswered: no `window`, no sky, the chart still renders the loss-only rail (Task 2 Go test; existing smoke `noReplyOnly` point).
2. Live refresh and target switching reuse the same uPlot instance via `setData`; the scale, seam, and sky state must come from current state, not values captured at construction (Task 4 smoke: gateway with sky → backup without sky flips `data-sky`).
3. A window straddling the raw-retention boundary (7d preset): exact samples only for buckets starting at or after the oldest raw row, no error (Task 3 Go test).
4. A high-rate (obsess) window over the exact-sample row budget: series still returned with tails, no `worst`/`misses` (Task 3 Go test).
5. Sub-millisecond LAN targets: seam and tick/label formatting for values like 0.3 ms and 3 ms (Task 2 `niceCeil` table; Task 8 fw1-internal real-data screenshot, 0.7 ms typical).

---

### Task 1: Histogram tail-bin extraction

**Files:**
- Modify: `internal/histogram/histogram.go`
- Test: `internal/histogram/histogram_test.go`

**Interfaces:**
- Produces: `type Bin struct { Value, Count uint64 }`; `func (h *Histogram) TopBins(minValue uint64, limit int) ([]Bin, bool)` — bins whose midpoint value is ≥ `minValue`, highest first, at most `limit`; the bool is true when more bins qualified. `Value` is the same midpoint `Quantiles` returns for that bin.

- [ ] **Step 1: Write the failing tests**

```go
func TestTopBinsReturnsLoneOutlierHighestFirst(t *testing.T) {
	h := New()
	for i := 0; i < 59; i++ { h.Observe(uint64(8 * time.Millisecond)) }
	h.Observe(uint64(4115 * time.Millisecond))
	bins, truncated := h.TopBins(uint64(32*time.Millisecond), 24)
	// len(bins)==1, truncated==false, bins[0].Count==1,
	// |bins[0].Value-4115ms| <= 2% of 4115ms, bins[0].Value == h.Quantile(1)
}
func TestTopBinsLimitAndOrder(t *testing.T) {
	// observe 30 values 100ms,200ms,...,3000ms (distinct bins)
	// TopBins(1, 24): len==24, truncated==true, strictly descending Values
	// TopBins(uint64(10*time.Second), 24): len==0, truncated==false
}
```

- [ ] **Step 2: Run** `go test ./internal/histogram/ -run TopBins` — Expected: FAIL, `h.TopBins undefined`.
- [ ] **Step 3: Implement `TopBins`** and extract the existing midpoint computation in `Quantiles` into `func binValue(idx uint32) uint64` so both share it. Walk `h.indices()` from the end.
- [ ] **Step 4: Run** `go test ./internal/histogram/` — Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "Add highest-first tail bin extraction to histograms"`

---

### Task 2: Window summary, server-owned seam, and tail bins in the ping series

**Files:**
- Modify: `internal/store/sqlite/query.go` (types near line 30, `PingSeries` ~153, `aggregatesToPoints` ~570)
- Modify: `internal/store/sqlite/store_test.go` (existing `aggregatesToPoints(...)` calls gain a `0` argument)
- Create: `internal/store/sqlite/outliers_test.go`

**Interfaces:**
- Consumes: `Histogram.TopBins`, `Histogram.Merge`, `Histogram.Quantiles` (Task 1).
- Produces (JSON names are the API contract the frontend uses):
  - `type TailBin struct { MS float64 `json:"ms"`; N int64 `json:"n"` }`
  - `PingPoint.Tail []TailBin `json:"tail,omitempty"``, `PingPoint.TailTruncated bool `json:"tail_truncated,omitempty"``
  - `type PingWindow struct { RTTCount int64 `json:"rtt_count"`; P50MS, P95MS, P99MS, MaxMS, SeamMS float64 (json p50_ms, p95_ms, p99_ms, max_ms, seam_ms) }`
  - `PingSeries.Window *PingWindow `json:"window,omitempty"`` (nil when the window has no replies)
  - `func niceCeil(x float64) float64`
  - `func pingWindow(aggregates map[int64]*queryAggregate) *PingWindow`
  - `func aggregatesToPoints(aggregates map[int64]*queryAggregate, seamNS uint64) []PingPoint` — `seamNS == 0` disables the above-seam rule.
  - `const pingTailCap = 24`

- [ ] **Step 1: Write the failing tests** in `outliers_test.go`, building aggregates with `observeRTT` as `TestPingPointMeanAndAdaptiveDistribution` does:

```go
func TestNiceCeil(t *testing.T) // table: 168→200, 2.6→3, 0.26→0.3, 11→12, 0.9→1, 1000→1000, 1001→1200, 8.1→10
func TestTailKeepsLoneOutlierInLargeBucket(t *testing.T)
	// 59×8ms + 1×4115ms, seamNS=0 → len(Tail)==1, Tail[0].N==1, Tail[0].MS within 2% of 4115 and <= *MaxMS
func TestTailKeepsClusterSharingP95Bin(t *testing.T)
	// 55×8ms + 5×4000ms (one bin), seamNS=0 → a Tail entry with N==5
func TestTailAboveSeamInSmallBucket(t *testing.T)
	// 1×2500ms: seamNS=800ms → Tail==[{~2500,1}]; seamNS=0 → Tail==nil
func TestTailSkipsBodyValuesInSmallBucket(t *testing.T)
	// 8ms, 8ms, 40ms with seamNS=200ms → Tail==nil
func TestTailCapOrderAndCount(t *testing.T)
	// 10×1ms + 30 distinct values 100ms..3000ms, seamNS=0 → len==24, TailTruncated, descending MS, sum(N) <= RTTCount
func TestPingWindowPoolsBucketsAndOwnsSeam(t *testing.T)
	// bucket A 60×8ms, bucket B 59×8ms+1×4115ms → RTTCount==120, MaxMS==4115,
	// SeamMS==niceCeil(2*P95MS); pingWindow of loss-only aggregates == nil
func TestPingSeriesJSONOmitsEmptyOutlierFields(t *testing.T)
	// marshal a loss-only series: no "window", "tail", "tail_truncated" keys
```

- [ ] **Step 2: Run** `TMPDIR=/dev/shm/flameping-test go test ./internal/store/sqlite/ -run 'NiceCeil|Tail|PingWindow|OutlierFields'` — Expected: FAIL (undefined symbols).
- [ ] **Step 3: Implement** the types, `niceCeil`, `pingWindow` (merge all bucket histograms into a fresh one; `MaxMS` = max `rttMax`; `SeamMS = niceCeil(2*P95MS)`), and the tail rule inside `aggregatesToPoints`. The rule, with its AIDEV-NOTE, is the one non-obvious algorithm:

```go
threshold := uint64(math.MaxUint64)
if seamNS > 0 { threshold = seamNS + 1 }            // strictly above the seam, any bucket
if a.rttCount > pingDistributionCap {                // grains are lossy only above 17 replies
	threshold = min(threshold, max(4*quantiles[0], quantiles[1])) // ≥, so a p95 cluster qualifies
}
bins, truncated := a.hist.TopBins(threshold, pingTailCap)
// MS = clamp(bin.Value, rttMin, rttMax)/1e6; qualification uses the unclamped midpoint
```

  Wire `PingSeries`: compute `window := pingWindow(aggregates)` after the `maxPoints == 1` collapse, derive `seamNS` from `window.SeamMS` (0 when nil), call `aggregatesToPoints(aggregates, seamNS)`, set `Window: window`. Add `, 0` to existing test calls.
- [ ] **Step 4: Run** `TMPDIR=/dev/shm/flameping-test go test ./internal/store/sqlite/ ./internal/httpapi/` — Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "Report window seam and per-bucket tail bins in ping series"`

---

### Task 3: Exact worst replies and missed pings within raw retention

**Files:**
- Modify: `internal/store/sqlite/query.go`
- Test: `internal/store/sqlite/outliers_test.go`

**Interfaces:**
- Consumes: `PingSeries` wiring from Task 2 (the already-queried `rawStart sql.NullInt64` and `pointWidth`).
- Produces:
  - `type WorstSample struct { TimeMS int64 `json:"time_ms"`; RTTMS float64 `json:"rtt_ms"`; Late bool `json:"late"` }`
  - `PingPoint.Worst []WorstSample `json:"worst,omitempty"`` (highest RTT first), `PingPoint.Misses []int64 `json:"misses,omitempty"`` (scheduled ms, time order)
  - `const pingExactSampleCap = 5`, `const pingExactSampleBudget = 200_000`
  - `func attachExactSamples(ctx context.Context, q *sql.Tx, targetID int64, to, asOf time.Time, width time.Duration, rawStart sql.NullInt64, budget int64, points []PingPoint) error`

- [ ] **Step 1: Write the failing tests** with a local `recordProbe(t, db, runID, target, at time.Time, rtt time.Duration, answered bool)` helper modelled on the `probe` closure in `overview_test.go` (late when rtt > 1 s):

```go
func TestPingSeriesReportsExactWorstAndMisses(t *testing.T)
	// 6h-style window from minute rollups is not needed: record 60 probes at 5 s in one 5-min bucket,
	// 8ms each except one 4115ms (late) and two unanswered; PingSeries(from,to,73)
	// → bucket Worst[0]=={TimeMS: that probe's scheduled ms, RTTMS≈4115, Late:true}, len(Worst)==5,
	//   Misses == the two scheduled ms values in time order
func TestExactSamplesOnlyInsideRawRetention(t *testing.T)
	// call attachExactSamples with rawStart after the first point's TimeMS → first point Worst==nil, later points filled
func TestExactSamplesSkippedOverBudget(t *testing.T)
	// attachExactSamples with budget smaller than sum(Attempted) → every point Worst==nil && Misses==nil, err==nil
```

- [ ] **Step 2: Run** `TMPDIR=/dev/shm/flameping-test go test ./internal/store/sqlite/ -run 'ExactWorst|RawRetention|OverBudget'` — Expected: FAIL.
- [ ] **Step 3: Implement `attachExactSamples`.** Eligible points: `TimeMS*1000 >= rawStart.Int64` (none when `!rawStart.Valid`). Skip entirely when the eligible points' `Attempted` sum exceeds `budget`. One ordered scan of `ping_samples` (`scheduled_at_us` from the first eligible bucket to `to`), bucket key `scheduled - scheduled%widthUS`. Replies: keep the 5 highest `rtt_ns` (ties: earlier first), `Late = reply_class=='late'`. Misses: first 5 where `local_outcome IS NULL AND reply_at_us IS NULL AND sent+timeout/1000 <= asOf`. Call it from `PingSeries` after `aggregatesToPoints`, skipped when `maxPoints == 1`, with `pingExactSampleBudget`.
- [ ] **Step 4: Run** `TMPDIR=/dev/shm/flameping-test go test ./...` — Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "Attach exact worst replies and missed pings to recent ping buckets"`

---

### Task 4: Body + sky piecewise y axis

**Files:**
- Create: `web/src/flame-scale.ts`
- Modify: `web/src/app.ts` (`Point` type, `flameLatencyRange`, `flameGraph` scale/axis config, `FlameState`)
- Modify: `web/browser-smoke.mjs` (ping fixture + assertions)
- Rebuild: `internal/webui/dist/`

**Interfaces:**
- Consumes: API `window`, `tail`, `timeout_max_ms` from Tasks 2–3.
- Produces (in `flame-scale.ts`):
  - `export type FlameWindow={rtt_count:number,p50_ms?:number,p95_ms?:number,p99_ms?:number,max_ms?:number,seam_ms?:number}`
  - `export type SkyLayout={sky:boolean,seam:number,top:number}`
  - `export function niceCeil(x:number):number`
  - `export function skyLayout(window:FlameWindow|undefined,fallbackMax:number):SkyLayout`
  - `export function flameFwd(layout:SkyLayout,v:number):number`, `export function flameBwd(layout:SkyLayout,u:number):number`
  - `export function flameSplits(layout:SkyLayout):{value:number,kind:"body"|"seam"|"sky"}[]`
  - `export function formatMs(v:number):string` ("0.3 ms", "48 ms", "2.5 s")
  - `export type Point` moves here from `app.ts` and gains `timeout_min_ms?:number,timeout_max_ms?:number,tail?:{ms:number,n:number}[],tail_truncated?:boolean,worst?:{time_ms:number,rtt_ms:number,late:boolean}[],misses?:number[]`; `app.ts` imports it.
  - `FlameState` gains `layout:SkyLayout` and `window?:FlameWindow`; `app.ts` exports nothing new.

- [ ] **Step 1: Extend the smoke fixture and add failing assertions.** In `fixtureResponse`, gateway responses get `window:{rtt_count:1200,p50_ms:3,p95_ms:9,p99_ms:15,max_ms:4115,seam_ms:20}`, a point with `tail:[{ms:4115,n:1}]`, `worst:[{time_ms,rtt_ms:4115,late:true}]`, `max_ms:4115`, `timeout_max_ms:1000`, and a point with `tail:[{ms:4300,n:3}]`; backup responses get `window:{rtt_count:1200,p50_ms:3,p95_ms:9,p99_ms:13,max_ms:15,seam_ms:20}` (consistent with the fixture's ~1.5–15 ms grains, so no sky). All gateway points carry `timeout_max_ms:1000`. Assert on `#flame` after the gateway detail loads: `dataset.sky==="on"`, `dataset.seam==="20"`; after clicking backup: `dataset.sky==="off"` (Review Focus 2); the y-axis tick text contains "4k"-style sky ticks only for gateway.
- [ ] **Step 2: Run** `cd web && npm run build && npm run browser-smoke` — Expected: FAIL on the `sky` assertion.
- [ ] **Step 3: Implement `flame-scale.ts`** and switch the `y` scale in `flameGraph` to `{distr:100,fwd:v=>flameFwd(flameState.layout,v),bwd:u=>flameBwd(flameState.layout,u),range:...}` reading `flameState.layout` at call time (never a captured layout). The mapping (AIDEV-NOTE), in "latency units" where the latency area is [0,1] and the existing loss-rail reservation is [-0.38,0]:

```ts
const B=.62; // body share of the latency area
fwd(v) = !sky ? v/top : v<=seam ? v/seam*B : B+(Math.log10(v)-Math.log10(seam))/(Math.log10(top)-Math.log10(seam))*(1-B)
range  = [bwd(-.38), top]   // bwd is the exact inverse of fwd; below zero is linear
```

  `skyLayout`: `seam=window.seam_ms`, `sky=max_ms>=3*seam`, `top=niceCeil((sky?1.15:1.08)*max)`; without a window fall back to today's grain maximum. Axis `splits` from `flameSplits` (body: 4 linear ticks below the seam; the seam; sky: 1-2-5 log ticks, dropping any within 12 px of the seam at draw time); `values` via `formatMs`-style short numbers; sky tick labels drawn in `#c99a6a`. In the smoke plugin's `drawAxes` hook draw: the dashed seam line, the zig-zag break glyph, the sky wash `rgba(255,120,40,.035)`, the right-aligned "LOG SCALE ABOVE {formatMs(seam)} ↑" label, and the dashed deadline line at the window's highest `timeout_max_ms` labelled "DEADLINE {formatMs(timeout)} · late above" when inside the range. Set `dataset.sky` ("on"/"off") and `dataset.seam`.
- [ ] **Step 4: Run** `cd web && npx tsc --noEmit && npm run build && npm run browser-smoke` — Expected: PASS.
- [ ] **Step 5: Commit** source and rebuilt dist: `git commit -m "Split the latency axis into a linear body and logarithmic sky"`

---

### Task 5: Embers, tail grains, labels, and brighter isolated smoke

**Files:**
- Create: `web/src/flame-embers.ts`
- Modify: `web/src/app.ts` (`flameSmokePlugin`: grain colors, new `draw` hook)
- Modify: `web/browser-smoke.mjs`
- Rebuild: `internal/webui/dist/`

**Interfaces:**
- Consumes: `SkyLayout`, `flameState` (Task 4), `Point.tail`, `Point.worst`, `Point.timeout_max_ms`.
- Produces: `export type Ember={index:number,value:number,count:number,late:boolean,severity:number,mild:boolean}`; `export function classifyTail(points:Point[],layout:SkyLayout):{embers:Ember[],grains:{index:number,value:number,late:boolean,alpha:number}[]}`; `export function drawEmbers(u:uPlot,points:Point[],slots:readonly (readonly [number,number])[],result:ReturnType<typeof classifyTail>):number` (returns labels drawn). `Point` comes from `flame-scale.ts` (Task 4). `dataset.embers` counts non-mild embers; mild ones are counted in `dataset.tailGrains`.

- [ ] **Step 1: Add failing smoke assertions** (gateway fixture from Task 4): `dataset.embers==="2"`, `Number(dataset.emberLabels)>=1`; backup fixture with a body tail `tail:[{ms:14,n:1}]` on one point with `rtt_count:60`: `dataset.embers==="0"`, `dataset.tailGrains==="1"`.
- [ ] **Step 2: Run** the smoke test — Expected: FAIL on `embers`.
- [ ] **Step 3: Implement.** `classifyTail` per spec (AIDEV-NOTE on severity): ember iff `layout.sky && v>seam`; `severity=clamp(Math.log(v/seam)/Math.log(Math.max(top/seam,20)),0,1)`; `late=v>point.timeout_max_ms`; `mild=!late&&severity<.35`; everything else (and mild embers) is a grain with the spec's colors and alpha. Draw in a uPlot `draw` hook (after grid and series) so nothing covers them: trails, then cores with glow, then labels, each with a thin dark keyline; tail grains 4 px tall. Constants from the spec's "Smoke, tail grains, and embers" section. Labels: highest ember of each of the three worst buckets, `×n` rule, ink text on a dark backdrop, collisions skipped, exact `worst[0].rtt_ms` when present else "≈" prefix. Raise `grainColors[0]` alpha to .42. Set `dataset.embers`, `dataset.tailGrains`, `dataset.emberLabels`.
- [ ] **Step 4: Run** `cd web && npx tsc --noEmit && npm run build && npm run browser-smoke` — Expected: PASS.
- [ ] **Step 5: Commit** source and dist: `git commit -m "Draw outlier embers with flame trails and guaranteed-visible tail grains"`

---

### Task 6: Count-aware loss lanes

**Files:**
- Modify: `web/src/app.ts` (`fillLane`, lane labels, `pctValue` for deadline/no-reply series)
- Modify: `web/browser-smoke.mjs`
- Rebuild: `internal/webui/dist/`

**Interfaces:**
- Consumes: `Point.sent`, `late`, `unanswered` (existing).
- Produces: `dataset.laneNumerals` (count of numerals drawn); legend values for "deadline miss" and "no reply" formatted `"{count} of {sent} ({pct.toFixed(1)}%)"`.

- [ ] **Step 1: Add failing smoke assertions:** with the existing fixture (60 attempted per point, `late:2` or `unanswered:1` on some points) at the 1h preset, `Number(dataset.laneNumerals)>0`; after a hover, the "no reply" legend value matches `/^\d+ of \d+ \(\d+\.\d%\)$/`.
- [ ] **Step 2: Run** the smoke test — Expected: FAIL.
- [ ] **Step 3: Implement:** bar height `inner*Math.sqrt(pct/100)` with a `3*ratio` minimum and full-opacity fill; the count numeral centred in the slot when the slot is ≥ 13 CSS px (dark ink when the bar is taller than the numeral); the right-aligned rail note "√ scale · min 3 px · numerals = pings". The full-loss hatch and local markers are unchanged.
- [ ] **Step 4: Run** `cd web && npx tsc --noEmit && npm run build && npm run browser-smoke` — Expected: PASS.
- [ ] **Step 5: Commit** source and dist: `git commit -m "Scale loss lanes by square root with a visible floor and counts"`

---

### Task 7: Bucket tooltip and chart key

**Files:**
- Create: `web/src/flame-tooltip.ts`
- Modify: `web/src/app.ts` (cursor hook), `web/src/index.html` (flame key), `web/src/extra.css` (tooltip and key styles)
- Modify: `web/browser-smoke.mjs`
- Rebuild: `internal/webui/dist/`

**Interfaces:**
- Consumes: `Point` (with `tail`, `worst`, `misses`), `formatMs`, the series `bucket_ms`.
- Produces: `export function bucketTooltip(point:Point,bucketMs:number):string` (escaped HTML) and a `#flame-tooltip` element inside `#flame`, shown on cursor move over a bucket and hidden on leave.

- [ ] **Step 1: Add failing smoke assertions:** dispatch a `mousemove` over the gateway bucket that has `worst`; `#flame-tooltip` is visible and its text contains the worst sample's local time, "4.1 s", and "late"; after `mouseleave` it is hidden. The key contains "spark", "late spark", "seam", and "deadline" entries.
- [ ] **Step 2: Run** the smoke test — Expected: FAIL.
- [ ] **Step 3: Implement** the tooltip content per the spec's Hover section (time range and sent; p50/p95/p99/max; late and no-reply counts with %; send errors when present; `worst` with exact times and "late"; `misses` with "…" when `unanswered` exceeds the list; first six tail bins with `×n` when `worst` is absent). Position with `element.style.left/top` clamped inside the chart. Update the key in `index.html` with the spec's legend entries (add spark ≥ seam, late spark, seam, deadline; keep RTT density, lanes, local gap, partial bucket), with CSS-only swatches in `extra.css`. Update the `#flame` `aria-label` to mention outlier embers.
- [ ] **Step 4: Run** `cd web && npx tsc --noEmit && npm run build && npm run browser-smoke` — Expected: PASS.
- [ ] **Step 5: Commit** source and dist: `git commit -m "Add bucket outlier tooltip and update the flame chart key"`

---

### Task 8: Real-data check, docs, and full verification

**Files:**
- Modify: `docs/outlier-flames-experiment.md` (Status, Implementation plan, Review and validation)

- [ ] **Step 1: Full checks.** `TMPDIR=/dev/shm/flameping-test go test ./...`, `TMPDIR=/dev/shm/flameping-test go test -race ./internal/store/sqlite/ ./internal/httpapi/`, `cd web && npx tsc --noEmit && npm run build && npm run browser-smoke`, `make build`. Expected: all pass; `git status` shows dist matches source.
- [ ] **Step 2: Eyeball against real data without probing the network.** Copy `/tmp/jackflameping/flameping.db` and `flameping.yaml` into `/dev/shm/flameping-real/` (never touch the originals); point `storage.path` at the copy and `server.listen` at `127.0.0.1:18080`. Run inside an isolated network namespace so no pings leave the host: `unshare -rn sh -c 'ip link set lo up; ./flameping run --config /dev/shm/flameping-real/flameping.yaml & sleep 3; google-chrome --headless --disable-gpu --no-sandbox --window-size=1840,1400 --virtual-time-budget=8000 --screenshot=/dev/shm/flameping-real/jacks-6h.png "http://127.0.0.1:18080/#target=JacksTailscale&from=1792514700000&to=1792536300000"; kill %1'` (6h window 10:45–16:45 MDT 2026-10-20). Repeat for JacksTailscale 24h (`from=1792449900000&to=1792536300000`), the zoom (`from=1792535647000&to=1792536296000`), and `cloudflare` and `fw1-internal` over the same 24h. Read each screenshot. Expected: every reply over 1 s at 12:28–12:29, 12:47, 12:52, 15:24, 15:56, 16:14, 16:28, 16:34, 16:42 is a visible ember; lane numerals sum to 14 late + 14 no-reply at 6h; Cloudflare shows no sky; fw1-internal shows the 48 ms ember.
- [ ] **Step 3: Update the experiment doc**: Status "implemented as an experiment, {date}"; replace the plan placeholder with a short numbered summary of Tasks 1–7; record validation results (commands and real-data checks) in the repo's existing "Review and validation" style.
- [ ] **Step 4: Commit** `git commit -m "Record outlier flames implementation and validation"`
