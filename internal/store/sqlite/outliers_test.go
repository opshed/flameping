package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/opshed/flameping/internal/histogram"
	"github.com/opshed/flameping/internal/model"
)

func newTestAggregate() pingAggregate {
	return pingAggregate{hist: histogram.New(), rttMin: math.MaxInt64}
}

func observeMany(a *pingAggregate, count int, rtt time.Duration) {
	for i := 0; i < count; i++ {
		observeRTT(a, rtt.Nanoseconds())
	}
}

func singleBucket(a pingAggregate) map[int64]*queryAggregate {
	return map[int64]*queryAggregate{0: {pingAggregate: a}}
}

func TestNiceCeil(t *testing.T) {
	// Exact equality: the seam is serialized as seam_ms, so float noise such as
	// 3 × 0.1 = 0.30000000000000004 must not reach the JSON.
	for _, tc := range []struct{ in, want float64 }{
		{168, 200}, {2.6, 3}, {0.26, 0.3}, {11, 12}, {0.9, 1}, {1000, 1000}, {1001, 1200}, {8.1, 10},
		{0.011, 0.012}, {0.0026, 0.003}, {0.7, 0.8}, {0.00055, 0.0006}, {0.13, 0.15}, {0.081, 0.1},
	} {
		if got := niceCeil(tc.in); got != tc.want {
			t.Errorf("niceCeil(%v)=%v, want exactly %v", tc.in, got, tc.want)
		}
	}
	data, err := json.Marshal(PingWindow{SeamMS: niceCeil(0.26)})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"seam_ms":0.3}`) {
		t.Fatalf("json %s, want seam_ms 0.3", data)
	}
}

func TestTailKeepsLoneOutlierInLargeBucket(t *testing.T) {
	a := newTestAggregate()
	observeMany(&a, 59, 8*time.Millisecond)
	observeMany(&a, 1, 4115*time.Millisecond)
	point := aggregatesToPoints(singleBucket(a), 0)[0]
	if len(point.Tail) != 1 || point.Tail[0].N != 1 {
		t.Fatalf("tail=%+v, want one single-reply bin", point.Tail)
	}
	if got := point.Tail[0].MS; math.Abs(got-4115) > 4115*0.02 || got > *point.MaxMS {
		t.Fatalf("tail ms=%v, want ~4115 and <= max %v", got, *point.MaxMS)
	}
}

func TestTailKeepsClusterSharingP95Bin(t *testing.T) {
	a := newTestAggregate()
	observeMany(&a, 55, 8*time.Millisecond)
	observeMany(&a, 5, 4000*time.Millisecond)
	point := aggregatesToPoints(singleBucket(a), 0)[0]
	for _, bin := range point.Tail {
		if bin.N == 5 {
			return
		}
	}
	t.Fatalf("tail=%+v, want an entry with n=5", point.Tail)
}

func TestTailAboveSeamInSmallBucket(t *testing.T) {
	a := newTestAggregate()
	observeMany(&a, 1, 2500*time.Millisecond)
	point := aggregatesToPoints(singleBucket(a), uint64(800*time.Millisecond))[0]
	if len(point.Tail) != 1 || point.Tail[0].N != 1 || math.Abs(point.Tail[0].MS-2500) > 2500*0.02 {
		t.Fatalf("tail=%+v, want [{~2500 1}]", point.Tail)
	}
	if point := aggregatesToPoints(singleBucket(a), 0)[0]; point.Tail != nil {
		t.Fatalf("tail without seam=%+v, want nil", point.Tail)
	}
}

func TestTailSkipsBodyValuesInSmallBucket(t *testing.T) {
	a := newTestAggregate()
	observeMany(&a, 2, 8*time.Millisecond)
	observeMany(&a, 1, 40*time.Millisecond)
	point := aggregatesToPoints(singleBucket(a), uint64(200*time.Millisecond))[0]
	if point.Tail != nil {
		t.Fatalf("tail=%+v, want nil", point.Tail)
	}
}

func TestTailCapOrderAndCount(t *testing.T) {
	a := newTestAggregate()
	observeMany(&a, 10, time.Millisecond)
	for i := 1; i <= 30; i++ {
		observeMany(&a, 1, time.Duration(100+(2900*(i-1))/29)*time.Millisecond)
	}
	point := aggregatesToPoints(singleBucket(a), uint64(50*time.Millisecond))[0]
	if len(point.Tail) != pingTailCap || !point.TailTruncated {
		t.Fatalf("len=%d truncated=%v, want %d and truncated", len(point.Tail), point.TailTruncated, pingTailCap)
	}
	var sum int64
	for i, bin := range point.Tail {
		sum += bin.N
		if i > 0 && bin.MS >= point.Tail[i-1].MS {
			t.Fatalf("tail not strictly descending: %+v", point.Tail)
		}
	}
	if sum > point.RTTCount {
		t.Fatalf("sum(n)=%d exceeds rtt_count=%d", sum, point.RTTCount)
	}
}

// withTimeouts records the bucket's probe timeouts and late count as the store
// does: a reply is late iff its RTT exceeds its own probe's timeout.
func withTimeouts(a *pingAggregate, minTimeout, maxTimeout time.Duration, late int64) {
	a.timeoutMin, a.timeoutMax, a.late = minTimeout.Nanoseconds(), maxTimeout.Nanoseconds(), late
}

func tailLate(tail []TailBin) []bool {
	flags := make([]bool, len(tail))
	for i, bin := range tail {
		flags[i] = bin.Late
	}
	return flags
}

// A 995 ms on-time reply shares the histogram bin whose midpoint is just past
// the 1,000 ms deadline. Judged by value it would be drawn as a late spark that
// never happened; only the bucket's one late reply may be flagged.
func TestTailLateFlagIgnoresBinMidpointPastDeadline(t *testing.T) {
	a := newTestAggregate()
	observeMany(&a, 58, 8*time.Millisecond)
	observeMany(&a, 1, 995*time.Millisecond)
	observeMany(&a, 1, 4115*time.Millisecond)
	withTimeouts(&a, time.Second, time.Second, 1)
	tail := aggregatesToPoints(singleBucket(a), uint64(200*time.Millisecond))[0].Tail
	if len(tail) != 2 || tail[1].N != 1 || tail[1].MS <= 1000 {
		t.Fatalf("tail=%+v, want the 995 ms reply's bin reported above the 1,000 ms deadline", tail)
	}
	if got := tailLate(tail); !reflect.DeepEqual(got, []bool{true, false}) {
		t.Fatalf("late=%v for tail %+v, want only the 4,115 ms bin late", got, tail)
	}
}

func TestTailLateFlagOnLoneLateReply(t *testing.T) {
	a := newTestAggregate()
	observeMany(&a, 59, 8*time.Millisecond)
	observeMany(&a, 1, 4115*time.Millisecond)
	withTimeouts(&a, time.Second, time.Second, 1)
	tail := aggregatesToPoints(singleBucket(a), uint64(200*time.Millisecond))[0].Tail
	if len(tail) != 1 || !tail[0].Late {
		t.Fatalf("tail=%+v, want one late bin", tail)
	}
}

// One late (1,005 ms) and one on-time (999 ms) reply share a bin. Flagging it
// would imply two late pings, so it is not flagged; the lane count stays exact.
func TestTailLateFlagSkipsBinMixingLateAndOnTime(t *testing.T) {
	a := newTestAggregate()
	observeMany(&a, 58, 8*time.Millisecond)
	observeMany(&a, 1, 999*time.Millisecond)
	observeMany(&a, 1, 1005*time.Millisecond)
	withTimeouts(&a, time.Second, time.Second, 1)
	tail := aggregatesToPoints(singleBucket(a), uint64(200*time.Millisecond))[0].Tail
	if len(tail) != 1 || tail[0].N != 2 || tail[0].Late {
		t.Fatalf("tail=%+v, want one shared bin with n=2, not late", tail)
	}
}

// With 1 s and 2 s probes in one bucket the highest replies need not be the
// late ones, so only bins wholly above the largest timeout can be late. Here
// 1,995 ms is on time under 2 s yet its bin's midpoint is past 2,000 ms, and
// 1,500 ms is late under 1 s but below the largest timeout.
func TestTailLateFlagWithMixedTimeouts(t *testing.T) {
	a := newTestAggregate()
	observeMany(&a, 57, 8*time.Millisecond)
	observeMany(&a, 1, 1500*time.Millisecond)
	observeMany(&a, 1, 1995*time.Millisecond)
	observeMany(&a, 1, 2500*time.Millisecond)
	withTimeouts(&a, time.Second, 2*time.Second, 2)
	tail := aggregatesToPoints(singleBucket(a), uint64(200*time.Millisecond))[0].Tail
	if len(tail) != 3 || tail[1].MS <= 2000 {
		t.Fatalf("tail=%+v, want three bins with the 1,995 ms bin reported above 2,000 ms", tail)
	}
	if got := tailLate(tail); !reflect.DeepEqual(got, []bool{true, false, false}) {
		t.Fatalf("late=%v for tail %+v, want only the 2,500 ms bin late", got, tail)
	}
}

func TestPingWindowPoolsBucketsAndOwnsSeam(t *testing.T) {
	a, b := newTestAggregate(), newTestAggregate()
	observeMany(&a, 60, 8*time.Millisecond)
	observeMany(&b, 59, 8*time.Millisecond)
	observeMany(&b, 1, 4115*time.Millisecond)
	window := pingWindow(map[int64]*queryAggregate{0: {pingAggregate: a}, 60_000_000: {pingAggregate: b}})
	if window == nil || window.RTTCount != 120 || window.MaxMS != 4115 {
		t.Fatalf("window=%+v, want rtt_count=120 max_ms=4115", window)
	}
	if want := niceCeil(2 * window.P95MS); window.SeamMS != want {
		t.Fatalf("seam=%v, want niceCeil(2*p95)=%v", window.SeamMS, want)
	}
	loss := newTestAggregate()
	loss.attempted, loss.unanswered = 3, 3
	if w := pingWindow(singleBucket(loss)); w != nil {
		t.Fatalf("loss-only window=%+v, want nil", w)
	}
}

func TestPingSeriesJSONOmitsEmptyOutlierFields(t *testing.T) {
	loss := newTestAggregate()
	loss.attempted, loss.unanswered = 3, 3
	aggregates := singleBucket(loss)
	series := PingSeries{Window: pingWindow(aggregates), Points: aggregatesToPoints(aggregates, 0)}
	data, err := json.Marshal(series)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"window"`, `"tail"`, `"tail_truncated"`} {
		if strings.Contains(string(data), key) {
			t.Fatalf("json %s contains %s", data, key)
		}
	}
}

// recordProbe stores one probe; rtt is only used when answered, and is late
// when it exceeds the 1 s timeout.
func recordProbe(t *testing.T, db *DB, runID model.RunID, target Target, sequence uint64, at time.Time, rtt time.Duration, answered bool) {
	t.Helper()
	p := model.ProbeEvent{Key: model.ProbeKey{RunID: runID, Sequence: sequence}, TargetID: target.ID,
		Endpoint: netip.MustParseAddr("127.0.0.1"), ScheduledAt: at, SentAt: at, Timeout: time.Second}
	events := []model.Event{{Kind: model.EventProbeSent, Probe: p}}
	if answered {
		p.ReplyAt, p.RTT, p.Responder, p.ReplyClass = at.Add(rtt), rtt, p.Endpoint, model.ReplyOnTime
		if rtt > p.Timeout {
			p.ReplyClass = model.ReplyLate
		}
		events = append(events, model.Event{Kind: model.EventProbeReply, Probe: p})
	}
	if err := db.applyBatch(context.Background(), events, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}
}

func TestPingSeriesReportsExactWorstAndMisses(t *testing.T) {
	db, _, target, runID := openTestDB(t)
	start := time.Now().Add(-30 * time.Minute).Truncate(5 * time.Minute)
	slow := 17
	for i := 0; i < 60; i++ {
		at := start.Add(time.Duration(i) * 5 * time.Second)
		switch i {
		case slow:
			recordProbe(t, db, runID, target, uint64(i), at, 4115*time.Millisecond, true)
		case 3, 40:
			recordProbe(t, db, runID, target, uint64(i), at, 0, false)
		default:
			recordProbe(t, db, runID, target, uint64(i), at, 8*time.Millisecond+time.Duration(i)*time.Microsecond, true)
		}
	}
	series, err := db.PingSeries(context.Background(), target.StableID, start.Add(-330*time.Minute), start.Add(30*time.Minute), 73)
	if err != nil {
		t.Fatal(err)
	}
	if series.ResolutionS != 300 {
		t.Fatalf("resolution=%d, want 300", series.ResolutionS)
	}
	var point *PingPoint
	for i := range series.Points {
		if series.Points[i].TimeMS == start.UnixMilli() {
			point = &series.Points[i]
		}
	}
	if point == nil {
		t.Fatalf("no bucket at %d in %+v", start.UnixMilli(), series.Points)
	}
	want := WorstSample{TimeMS: start.Add(time.Duration(slow) * 5 * time.Second).UnixMilli(), RTTMS: 4115, Late: true}
	if len(point.Worst) != 5 || point.Worst[0] != want {
		t.Fatalf("worst=%+v, want first %+v and 5 entries", point.Worst, want)
	}
	for i := 1; i < len(point.Worst); i++ {
		if point.Worst[i].RTTMS > point.Worst[i-1].RTTMS || point.Worst[i].Late {
			t.Fatalf("worst not descending/on-time: %+v", point.Worst)
		}
	}
	if len(point.Tail) == 0 || point.Tail[0].N != 1 || !point.Tail[0].Late {
		t.Fatalf("tail=%+v, want the lone 4,115 ms reply first and flagged late", point.Tail)
	}
	wantMisses := []int64{start.Add(15 * time.Second).UnixMilli(), start.Add(200 * time.Second).UnixMilli()}
	if !reflect.DeepEqual(point.Misses, wantMisses) {
		t.Fatalf("misses=%v, want %v", point.Misses, wantMisses)
	}
}

// A drag-zoom requests 900 points over an arbitrary span, so the raw bucket
// width is usually not a whole number of milliseconds (here 300 s / 899 =
// 333.705 ms) while PingPoint.TimeMS is truncated to milliseconds. Exact
// samples must still reach their buckets.
func TestExactSamplesInFractionalMillisecondBuckets(t *testing.T) {
	db, _, target, runID := openTestDB(t)
	start := time.Now().Add(-30 * time.Minute).Truncate(5 * time.Minute)
	// Earlier raw history, so the straddling first bucket is inside raw retention.
	recordProbe(t, db, runID, target, 1000, start.Add(-time.Minute), 9*time.Millisecond, true)
	for i := 0; i < 60; i++ {
		at := start.Add(time.Duration(i) * 5 * time.Second)
		recordProbe(t, db, runID, target, uint64(i), at, 8*time.Millisecond+time.Duration(i)*time.Millisecond, i != 7)
	}
	series, err := db.PingSeries(context.Background(), target.StableID, start, start.Add(5*time.Minute), 900)
	if err != nil {
		t.Fatal(err)
	}
	if width := time.Duration(series.BucketMS * float64(time.Millisecond)); width%time.Millisecond == 0 {
		t.Fatalf("bucket width %v is whole milliseconds; fixture no longer exercises fractional widths", width)
	}
	worst, misses := 0, 0
	for _, point := range series.Points {
		if point.RTTCount > 0 && len(point.Worst) != 1 {
			t.Fatalf("bucket %d has %d replies but worst=%+v", point.TimeMS, point.RTTCount, point.Worst)
		}
		for _, sample := range point.Worst {
			// Both times are truncated to milliseconds, so a sample in the
			// bucket's last fractional millisecond can sit BucketMS+<1 after it.
			if sample.TimeMS < point.TimeMS || float64(sample.TimeMS-point.TimeMS) >= series.BucketMS+1 {
				t.Fatalf("worst %+v outside bucket %d", sample, point.TimeMS)
			}
		}
		worst += len(point.Worst)
		misses += len(point.Misses)
	}
	if worst != 59 || misses != 1 {
		t.Fatalf("worst=%d misses=%d, want 59 and 1", worst, misses)
	}
}

// A rollup-served window reads whole minute rows, so its first bucket counts
// the minute that contains from and its last counts the minute that contains
// to. The exact-sample scan must cover the same samples: a spike in the minute
// before from (or after to) is drawn as an ember and must lead worst, and a
// reply in an earlier minute that only shares the bucket's key is not counted
// and must not appear.
func TestExactSamplesMatchRollupServedEdgeBuckets(t *testing.T) {
	db, _, target, runID := openTestDB(t)
	ctx := context.Background()
	base := time.Now().Add(-2 * time.Hour).Truncate(10 * time.Minute)
	for i, probe := range []struct {
		offset, rtt time.Duration
	}{
		{-30 * time.Second, 8 * time.Millisecond},    // earlier raw history: the first bucket is inside raw retention
		{20 * time.Second, 5000 * time.Millisecond},  // minute before the first counted row; shares only the bucket key
		{70 * time.Second, 4115 * time.Millisecond},  // the minute containing from, before from
		{100 * time.Second, 8 * time.Millisecond},    // from = base+90s
		{150 * time.Second, 9 * time.Millisecond},    //
		{210 * time.Second, 10 * time.Millisecond},   //
		{270 * time.Second, 11 * time.Millisecond},   //
		{345 * time.Second, 3000 * time.Millisecond}, // the minute containing to, after to = base+330s
		{370 * time.Second, 6000 * time.Millisecond}, // after the last counted row
	} {
		recordProbe(t, db, runID, target, uint64(i), base.Add(probe.offset), probe.rtt, true)
	}
	for i := 0; i < 4; i++ {
		if err := db.recomputeDirty(ctx, 1000, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	from, to := base.Add(90*time.Second), base.Add(330*time.Second)
	series, err := db.PingSeries(ctx, target.StableID, from, to, 3)
	if err != nil {
		t.Fatal(err)
	}
	if series.ResolutionS != 120 || len(series.Points) != 3 || series.Points[0].TimeMS != base.UnixMilli() {
		t.Fatalf("resolution=%d points=%+v, want three 2-minute rollup-served buckets from %d", series.ResolutionS, series.Points, base.UnixMilli())
	}
	for _, point := range series.Points {
		if point.RTTCount == 0 || point.MaxMS == nil {
			t.Fatalf("bucket %d has no replies: %+v", point.TimeMS, point)
		}
		// Every bucket holds at most five replies, so worst lists all of them.
		if int64(len(point.Worst)) != point.RTTCount || point.Worst[0].RTTMS != *point.MaxMS {
			t.Fatalf("bucket %d: rtt_count=%d max=%v worst=%+v, want worst to list exactly the counted replies", point.TimeMS, point.RTTCount, *point.MaxMS, point.Worst)
		}
	}
	if got := series.Points[0].Worst[0]; got.RTTMS != 4115 || got.TimeMS != base.Add(70*time.Second).UnixMilli() {
		t.Fatalf("first bucket worst=%+v, want the 4,115 ms spike before from", got)
	}
	if got := series.Points[2].Worst[0]; got.RTTMS != 3000 || got.TimeMS != base.Add(345*time.Second).UnixMilli() {
		t.Fatalf("last bucket worst=%+v, want the 3,000 ms spike after to", got)
	}
}

func readTx(t *testing.T, db *DB) *sql.Tx {
	t.Helper()
	tx, err := db.readers.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

func TestExactSamplesOnlyInsideRawRetention(t *testing.T) {
	db, _, target, runID := openTestDB(t)
	start := time.Now().Add(-10 * time.Minute).Truncate(time.Minute)
	for i := 0; i < 4; i++ {
		recordProbe(t, db, runID, target, uint64(i), start.Add(time.Duration(i)*30*time.Second), 20*time.Millisecond, true)
	}
	second := start.Add(time.Minute)
	points := []PingPoint{{TimeMS: start.UnixMilli(), Attempted: 2}, {TimeMS: second.UnixMilli(), Attempted: 2}}
	rawStart := sql.NullInt64{Int64: second.UnixMicro(), Valid: true}
	err := attachExactSamples(context.Background(), readTx(t, db), target.ID, start, start.Add(2*time.Minute), time.Now(), time.Minute, 0, rawStart, 1000, points)
	if err != nil {
		t.Fatal(err)
	}
	if points[0].Worst != nil || len(points[1].Worst) != 2 {
		t.Fatalf("points=%+v", points)
	}
	none := []PingPoint{{TimeMS: start.UnixMilli(), Attempted: 2}}
	if err := attachExactSamples(context.Background(), readTx(t, db), target.ID, start, start.Add(2*time.Minute), time.Now(), time.Minute, 0, sql.NullInt64{}, 1000, none); err != nil || none[0].Worst != nil {
		t.Fatalf("no raw rows: err=%v points=%+v", err, none)
	}
}

func TestExactSamplesSkippedOverBudget(t *testing.T) {
	db, _, target, runID := openTestDB(t)
	start := time.Now().Add(-10 * time.Minute).Truncate(time.Minute)
	recordProbe(t, db, runID, target, 1, start, 20*time.Millisecond, true)
	recordProbe(t, db, runID, target, 2, start.Add(time.Second), 0, false)
	points := []PingPoint{{TimeMS: start.UnixMilli(), Attempted: 2}}
	rawStart := sql.NullInt64{Int64: start.UnixMicro(), Valid: true}
	err := attachExactSamples(context.Background(), readTx(t, db), target.ID, start, start.Add(time.Minute), time.Now(), time.Minute, 0, rawStart, 1, points)
	if err != nil || points[0].Worst != nil || points[0].Misses != nil {
		t.Fatalf("err=%v points=%+v", err, points)
	}
	// The detail page re-requests the series every 5 s, so the production
	// budget must keep the scan to short windows: the 1h, 6h, and 24h presets
	// at a 5 s interval and 1h and 6h at 1 s (with a bucket of slack for the
	// straddling edge buckets) get exact samples; a 7d preset at 5 s does not.
	for _, tc := range []struct {
		span, interval, bucket time.Duration
		attached               bool
	}{
		{time.Hour, 5 * time.Second, time.Minute, true},
		{6 * time.Hour, 5 * time.Second, 5 * time.Minute, true},
		{24 * time.Hour, 5 * time.Second, 15 * time.Minute, true},
		{time.Hour, time.Second, time.Minute, true},
		{6 * time.Hour, time.Second, 5 * time.Minute, true},
		{7 * 24 * time.Hour, 5 * time.Second, time.Hour, false},
		{24 * time.Hour, time.Second, 15 * time.Minute, false},
	} {
		tx, err := db.readers.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		points := []PingPoint{{TimeMS: start.UnixMilli(), Attempted: int64((tc.span + tc.bucket) / tc.interval)}}
		err = attachExactSamples(context.Background(), tx, target.ID, start, start.Add(time.Minute), time.Now(), time.Minute, 0, rawStart, pingExactSampleBudget, points)
		_ = tx.Rollback()
		if err != nil || (points[0].Worst != nil) != tc.attached {
			t.Errorf("%v at %v (%d probes): err=%v worst=%+v, want attached=%v", tc.span, tc.interval, points[0].Attempted, err, points[0].Worst, tc.attached)
		}
	}
}
