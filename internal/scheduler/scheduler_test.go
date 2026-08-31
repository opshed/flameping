package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	clockpkg "github.com/benbjohnson/clock"

	"flameping/internal/model"
)

type fakeSink struct {
	mu     sync.Mutex
	accept bool
	jobs   []Job
	gaps   []model.SchedulerGap
}

func (s *fakeSink) TrySubmit(j Job) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.accept {
		s.jobs = append(s.jobs, j)
	}
	return s.accept
}
func (s *fakeSink) RecordGap(g model.SchedulerGap) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gaps = append(s.gaps, g)
}

func TestNoCatchUpAndGapCoalescing(t *testing.T) {
	clk := clockpkg.NewMock()
	sink := &fakeSink{}
	target := Target{ID: 1, StableID: "a", Interval: time.Second}
	s := New(clk, []Target{target}, sink)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	time.Sleep(time.Millisecond)
	clk.Add(3500 * time.Millisecond)
	time.Sleep(time.Millisecond)
	sink.mu.Lock()
	sink.accept = true
	sink.mu.Unlock()
	clk.Add(time.Second)
	time.Sleep(time.Millisecond)
	cancel()
	<-done
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.jobs) > 1 {
		t.Fatalf("catch-up burst: %d jobs", len(sink.jobs))
	}
	var missed uint64
	for _, gap := range sink.gaps {
		missed += gap.MissedCount
	}
	if missed < 2 {
		t.Fatalf("missed count = %d", missed)
	}
}

func TestPhaseStable(t *testing.T) {
	if a, b := phaseFor("same", 5*time.Second), phaseFor("same", 5*time.Second); a != b || a < 0 || a >= 5*time.Second {
		t.Fatalf("phase %v, %v", a, b)
	}
}

func TestSustainedRejectionFlushesGapWithoutLaterSuccess(t *testing.T) {
	clk := clockpkg.NewMock()
	sink := &fakeSink{}
	s := New(clk, []Target{{ID: 1, StableID: "unresolved", Interval: time.Second}}, sink)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	time.Sleep(time.Millisecond)
	clk.Add(6 * time.Second)
	time.Sleep(time.Millisecond)
	sink.mu.Lock()
	flushed := len(sink.gaps)
	sink.mu.Unlock()
	if flushed == 0 {
		t.Fatal("sustained unresolved target kept every scheduler gap in memory")
	}
	cancel()
	<-done
}

func TestRejectedLongIntervalTargetFlushesOnIndependentDeadline(t *testing.T) {
	clk := clockpkg.NewMock()
	sink := &fakeSink{}
	interval := time.Minute
	stableID := ""
	for i := 0; i < 1000; i++ {
		candidate := fmt.Sprintf("long-%d", i)
		if phaseFor(candidate, interval) < time.Second {
			stableID = candidate
			break
		}
	}
	if stableID == "" {
		t.Fatal("could not find a deterministic short phase")
	}
	s := New(clk, []Target{{ID: 1, StableID: stableID, Interval: interval}}, sink)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	time.Sleep(time.Millisecond)
	clk.Add(phaseFor(stableID, interval) + time.Millisecond)
	time.Sleep(time.Millisecond)
	clk.Add(gapFlushInterval + time.Second)
	time.Sleep(time.Millisecond)
	sink.mu.Lock()
	flushed := len(sink.gaps)
	sink.mu.Unlock()
	if flushed != 1 {
		t.Fatalf("long-interval rejection flushes=%d, want 1", flushed)
	}
	cancel()
	<-done
}
