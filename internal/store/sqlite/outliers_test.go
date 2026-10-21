package sqlite

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/opshed/flameping/internal/histogram"
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
