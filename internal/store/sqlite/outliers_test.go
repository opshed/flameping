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
	for _, tc := range []struct{ in, want float64 }{
		{168, 200}, {2.6, 3}, {0.26, 0.3}, {11, 12}, {0.9, 1}, {1000, 1000}, {1001, 1200}, {8.1, 10},
	} {
		if got := niceCeil(tc.in); math.Abs(got-tc.want) > 1e-9*tc.want {
			t.Errorf("niceCeil(%v)=%v, want %v", tc.in, got, tc.want)
		}
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
	err := attachExactSamples(context.Background(), readTx(t, db), target.ID, start, start.Add(2*time.Minute), time.Now(), time.Minute, rawStart, 1000, points)
	if err != nil {
		t.Fatal(err)
	}
	if points[0].Worst != nil || len(points[1].Worst) != 2 {
		t.Fatalf("points=%+v", points)
	}
	none := []PingPoint{{TimeMS: start.UnixMilli(), Attempted: 2}}
	if err := attachExactSamples(context.Background(), readTx(t, db), target.ID, start, start.Add(2*time.Minute), time.Now(), time.Minute, sql.NullInt64{}, 1000, none); err != nil || none[0].Worst != nil {
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
	err := attachExactSamples(context.Background(), readTx(t, db), target.ID, start, start.Add(time.Minute), time.Now(), time.Minute, rawStart, 1, points)
	if err != nil || points[0].Worst != nil || points[0].Misses != nil {
		t.Fatalf("err=%v points=%+v", err, points)
	}
}
