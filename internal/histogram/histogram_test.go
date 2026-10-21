package histogram

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestRoundTripMergeAndQuantile(t *testing.T) {
	a := New()
	b := New()
	for i := uint64(1); i <= 1000; i++ {
		if i%2 == 0 {
			a.Observe(i * 1_000_000)
		} else {
			b.Observe(i * 1_000_000)
		}
	}
	a.Merge(b)
	data, err := a.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalBinary(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Count() != 1000 || !reflect.DeepEqual(a.buckets, got.buckets) {
		t.Fatalf("round trip mismatch: count=%d", got.Count())
	}
	median := got.Quantile(0.5)
	if relative(median, 500_000_000) > 0.011 {
		t.Fatalf("median %d outside relative error", median)
	}
}

func TestMalformed(t *testing.T) {
	for _, data := range [][]byte{nil, {2, 0}, {1, 1, 1}, {1, 1, 1, 0}, {1, 0, 1}} {
		if _, err := UnmarshalBinary(data); err == nil {
			t.Fatalf("accepted %v", data)
		}
	}
}

func TestQuantilesMatchRepeatedQueriesAndPreserveOrder(t *testing.T) {
	h := New()
	for i := uint64(1); i <= 250; i++ {
		h.Observe(i * 1_000_000)
	}
	qs := []float64{0.99, -1, 0.5, 1.5, 0.1, 0.5, 0}
	want := make([]uint64, len(qs))
	for i, q := range qs {
		want[i] = h.Quantile(q)
	}
	if got := h.Quantiles(qs); !reflect.DeepEqual(got, want) {
		t.Fatalf("Quantiles(%v)=%v, want %v", qs, got, want)
	}
	if got := h.Quantiles([]float64{-1, 0, 1, 2}); got[0] != got[1] || got[2] != got[3] {
		t.Fatalf("clamped quantiles=%v", got)
	}
}

func TestQuantilesUseNearestRanks(t *testing.T) {
	// Ten observations occupy three known buckets: ranks 1-2 estimate to 1,
	// ranks 3-5 estimate to 7, and ranks 6-10 estimate to 53.
	h := &Histogram{buckets: map[uint32]uint64{0: 2, 100: 3, 200: 5}, count: 10}
	qs := []float64{1, 0.21, 0.2, 0.5001, 0, 0.5, -5, 3}
	want := []uint64{53, 7, 1, 53, 1, 7, 1, 53}
	if got := h.Quantiles(qs); !reflect.DeepEqual(got, want) {
		t.Fatalf("Quantiles(%v)=%v, want hand-calculated nearest ranks %v", qs, got, want)
	}
}

func TestQuantilesEmptyInputs(t *testing.T) {
	h := New()
	if got := h.Quantiles(nil); len(got) != 0 {
		t.Fatalf("nil query returned %v", got)
	}
	if got := h.Quantiles([]float64{0, 0.5, 1}); !reflect.DeepEqual(got, []uint64{0, 0, 0}) {
		t.Fatalf("empty histogram returned %v", got)
	}
}

func TestTopBinsReturnsLoneOutlierHighestFirst(t *testing.T) {
	h := New()
	for i := 0; i < 59; i++ {
		h.Observe(uint64(8 * time.Millisecond))
	}
	h.Observe(uint64(4115 * time.Millisecond))
	bins, truncated := h.TopBins(uint64(32*time.Millisecond), 24)
	if len(bins) != 1 || truncated {
		t.Fatalf("bins=%v truncated=%v, want one bin and no truncation", bins, truncated)
	}
	if bins[0].Count != 1 {
		t.Fatalf("count=%d, want 1", bins[0].Count)
	}
	if relative(bins[0].Value, uint64(4115*time.Millisecond)) > 0.02 {
		t.Fatalf("value %d outside 2%% of 4115ms", bins[0].Value)
	}
	if bins[0].Value != h.Quantile(1) {
		t.Fatalf("value %d != Quantile(1) %d", bins[0].Value, h.Quantile(1))
	}
	// Low bounds every value the bin can hold: at or below the observed reply
	// and within one bin width (gamma) of it.
	if low := bins[0].Low; low > uint64(4115*time.Millisecond) || float64(low)*gamma <= float64(4115*time.Millisecond) || low > bins[0].Value {
		t.Fatalf("low %d does not bound the 4115ms reply within one bin (value %d)", low, bins[0].Value)
	}
}

func TestTopBinsLimitAndOrder(t *testing.T) {
	h := New()
	for i := 1; i <= 30; i++ {
		h.Observe(uint64(i) * uint64(100*time.Millisecond))
	}
	bins, truncated := h.TopBins(1, 24)
	if len(bins) != 24 || !truncated {
		t.Fatalf("len=%d truncated=%v, want 24 and true", len(bins), truncated)
	}
	for i := 1; i < len(bins); i++ {
		if bins[i].Value >= bins[i-1].Value {
			t.Fatalf("bins not strictly descending at %d: %v", i, bins)
		}
	}
	bins, truncated = h.TopBins(uint64(10*time.Second), 24)
	if len(bins) != 0 || truncated {
		t.Fatalf("bins=%v truncated=%v, want none", bins, truncated)
	}
}

func relative(a, b uint64) float64 {
	return math.Abs(float64(a)-float64(b)) / float64(b)
}
