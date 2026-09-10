package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	clockpkg "github.com/benbjohnson/clock"

	"github.com/opshed/flameping/internal/model"
)

type fakeSink struct {
	mu       sync.Mutex
	accept   bool
	jobs     []Job
	gaps     []model.SchedulerGap
	observed []model.SchedulerGap
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
func (s *fakeSink) ObserveGap(g model.SchedulerGap) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observed = append(s.observed, g)
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

func TestIntervalUpdateWakesSleepingScheduler(t *testing.T) {
	clk := clockpkg.NewMock()
	sink := &fakeSink{accept: true}
	target := Target{ID: 1, StableID: "sleeping-target", Interval: time.Hour}
	if phaseFor(target.StableID, target.Interval) <= time.Second {
		t.Fatal("test requires a normal phase more than one second away")
	}
	s := New(clk, []Target{target}, sink)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	defer func() { cancel(); <-done }()
	time.Sleep(time.Millisecond)
	if !s.UpdateInterval(1, 100*time.Millisecond) {
		t.Fatal("valid update rejected")
	}
	time.Sleep(time.Millisecond)
	clk.Add(200 * time.Millisecond)
	time.Sleep(time.Millisecond)
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.jobs) == 0 {
		t.Fatal("cadence update did not wake the original hour-long schedule")
	}
	for _, job := range sink.jobs {
		if job.Target.Interval != 100*time.Millisecond {
			t.Fatalf("submitted interval = %v", job.Target.Interval)
		}
	}
}

func TestCadenceChangeAndRecoveryRespectLastSubmission(t *testing.T) {
	clk := clockpkg.NewMock()
	now := clk.Now()
	sink := &fakeSink{accept: true}
	target := Target{ID: 1, StableID: "target", Interval: time.Second}
	s := New(clk, []Target{target}, sink)
	// The prior normal send was delayed until just before its next scheduled
	// phase. Changing cadence must not produce two nearly simultaneous sends.
	h := deadlineHeap{{target: target, next: now.Add(time.Millisecond), lastSubmitted: now}}
	pending := make(map[int64]model.SchedulerGap)
	s.UpdateInterval(1, 100*time.Millisecond)
	s.applyUpdates(&h, pending, now)
	if want := now.Add(100 * time.Millisecond); !h[0].next.Equal(want) {
		t.Fatalf("fast deadline = %v, want %v", h[0].next, want)
	}
	clk.Add(100 * time.Millisecond)
	s.submitDue(&h, pending, clk.Now())
	if len(sink.jobs) != 1 {
		t.Fatalf("fast submissions = %d", len(sink.jobs))
	}
	s.UpdateInterval(1, time.Second)
	s.applyUpdates(&h, pending, clk.Now())
	if want := now.Add(1100 * time.Millisecond); !h[0].next.Equal(want) {
		t.Fatalf("restored deadline = %v, want %v", h[0].next, want)
	}
	clk.Add(999 * time.Millisecond)
	s.submitDue(&h, pending, clk.Now())
	if len(sink.jobs) != 1 {
		t.Fatal("restoration produced an early send")
	}
	clk.Add(time.Millisecond)
	s.submitDue(&h, pending, clk.Now())
	if len(sink.jobs) != 2 || sink.jobs[1].Target.Interval != time.Second {
		t.Fatalf("restored jobs = %+v", sink.jobs)
	}
}

func TestCadenceChangesFlushSeparateGapsAndObserveImmediately(t *testing.T) {
	clk := clockpkg.NewMock()
	sink := &fakeSink{}
	target := Target{ID: 1, StableID: "target", Interval: time.Second}
	s := New(clk, []Target{target}, sink)
	h := deadlineHeap{{target: target, next: clk.Now()}}
	pending := make(map[int64]model.SchedulerGap)
	s.submitDue(&h, pending, clk.Now())
	if len(sink.observed) != 1 || len(sink.gaps) != 0 {
		t.Fatal("rejection must be observed before its coalesced gap is persisted")
	}
	s.UpdateInterval(1, 100*time.Millisecond)
	s.applyUpdates(&h, pending, clk.Now())
	clk.Add(100 * time.Millisecond)
	s.submitDue(&h, pending, clk.Now())
	s.UpdateInterval(1, time.Second)
	s.applyUpdates(&h, pending, clk.Now())
	if len(sink.gaps) != 2 || len(sink.observed) != 2 || len(pending) != 0 {
		t.Fatalf("persisted=%+v observed=%+v pending=%+v", sink.gaps, sink.observed, pending)
	}
	if sink.gaps[0].Interval != time.Second || sink.gaps[1].Interval != 100*time.Millisecond || sink.gaps[0].MissedCount != 1 || sink.gaps[1].MissedCount != 1 {
		t.Fatalf("cadences were coalesced together: %+v", sink.gaps)
	}
}

func TestDelayedOldCadenceAccountsSkippedSlotsBeforeUpdate(t *testing.T) {
	clk := clockpkg.NewMock()
	sink := &fakeSink{accept: true}
	target := Target{ID: 1, StableID: "target", Interval: time.Second}
	s := New(clk, []Target{target}, sink)
	h := deadlineHeap{{target: target, next: clk.Now()}}
	heap.Init(&h)
	pending := make(map[int64]model.SchedulerGap)
	clk.Add(3500 * time.Millisecond)
	s.UpdateInterval(1, 100*time.Millisecond)
	s.submitDue(&h, pending, clk.Now())
	s.applyUpdates(&h, pending, clk.Now())
	s.submitDue(&h, pending, clk.Now())
	if len(sink.jobs) != 1 || len(sink.observed) != 1 || sink.observed[0].MissedCount != 3 || sink.observed[0].Interval != time.Second {
		t.Fatalf("old cadence accounting: jobs=%+v observed=%+v", sink.jobs, sink.observed)
	}
	if !h[0].next.Equal(clk.Now().Add(100 * time.Millisecond)) {
		t.Fatalf("updated next deadline = %v", h[0].next)
	}
}

func TestIntervalUpdatesAreBoundedConcurrentAndRejectInvalidTargets(t *testing.T) {
	clk := clockpkg.NewMock()
	s := New(clk, []Target{{ID: 1, StableID: "a", Interval: time.Second}}, &fakeSink{})
	if s.UpdateInterval(9, time.Second) || s.UpdateInterval(1, 0) || s.UpdateInterval(1, -time.Second) {
		t.Fatal("invalid cadence update accepted")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.UpdateInterval(1, time.Duration(j+1)*time.Millisecond)
			}
		}()
	}
	wg.Wait()
	if len(s.updates) != 1 || len(s.wake) != 1 {
		t.Fatalf("updates were not coalesced: updates=%d wakes=%d", len(s.updates), len(s.wake))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = s.Run(ctx)
	if s.UpdateInterval(1, time.Second) {
		t.Fatal("update accepted after scheduler shutdown")
	}
}
