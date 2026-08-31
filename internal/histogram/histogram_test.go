package histogram

import (
	"math"
	"reflect"
	"testing"
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

func relative(a, b uint64) float64 {
	return math.Abs(float64(a)-float64(b)) / float64(b)
}
