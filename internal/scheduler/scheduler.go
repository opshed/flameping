package scheduler

import (
	"container/heap"
	"context"
	"hash/fnv"
	"sync"
	"time"

	clockpkg "github.com/benbjohnson/clock"

	"flameping/internal/model"
)

type Target struct {
	ID       int64
	StableID string
	Interval time.Duration
	Value    any
}

type Job struct {
	Target      Target
	ScheduledAt time.Time
}

type Sink interface {
	TrySubmit(Job) bool
	RecordGap(model.SchedulerGap)
}

type Scheduler struct {
	clock    clockpkg.Clock
	targets  []Target
	sink     Sink
	updateMu sync.Mutex
	updates  map[int64]time.Duration
	known    map[int64]struct{}
	wake     chan struct{}
	stopped  bool
}

const gapFlushInterval = 5 * time.Second

func New(clock clockpkg.Clock, targets []Target, sink Sink) *Scheduler {
	if clock == nil {
		clock = clockpkg.New()
	}
	known := make(map[int64]struct{}, len(targets))
	for _, target := range targets {
		known[target.ID] = struct{}{}
	}
	return &Scheduler{clock: clock, targets: append([]Target(nil), targets...), sink: sink, updates: make(map[int64]time.Duration), known: known, wake: make(chan struct{}, 1)}
}

// UpdateInterval coalesces cadence changes without waiting for the scheduler or
// its sink. All deadline and gap accounting remains owned by Run.
func (s *Scheduler) UpdateInterval(targetID int64, interval time.Duration) bool {
	if interval <= 0 {
		return false
	}
	s.updateMu.Lock()
	defer s.updateMu.Unlock()
	if _, ok := s.known[targetID]; !ok || s.stopped {
		return false
	}
	s.updates[targetID] = interval
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return true
}

type entry struct {
	target        Target
	next          time.Time
	lastSubmitted time.Time
}

type deadlineHeap []entry

func (h deadlineHeap) Len() int           { return len(h) }
func (h deadlineHeap) Less(i, j int) bool { return h[i].next.Before(h[j].next) }
func (h deadlineHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *deadlineHeap) Push(x any)        { *h = append(*h, x.(entry)) }
func (h *deadlineHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (s *Scheduler) Run(ctx context.Context) error {
	defer func() {
		s.updateMu.Lock()
		s.stopped = true
		clear(s.updates)
		s.updateMu.Unlock()
	}()
	if len(s.targets) == 0 {
		<-ctx.Done()
		return ctx.Err()
	}
	now := s.clock.Now()
	h := make(deadlineHeap, 0, len(s.targets))
	for _, target := range s.targets {
		phase := phaseFor(target.StableID, target.Interval)
		h = append(h, entry{target: target, next: now.Add(phase)})
	}
	heap.Init(&h)
	pending := make(map[int64]model.SchedulerGap)
	lastGapFlush := now
	timer := s.clock.Timer(h[0].next.Sub(now))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			for _, gap := range pending {
				s.sink.RecordGap(gap)
			}
			return ctx.Err()
		case <-timer.C:
		case <-s.wake:
		}
		now = s.clock.Now()
		// Account for due slots at their existing cadence before changing it.
		// Delayed targets submit once and report all earlier skipped slots.
		s.submitDue(&h, pending, now)
		s.applyUpdates(&h, pending, now)
		// A faster cadence may now be due, even when its old deadline was far
		// away. lastSubmitted prevents a second immediate accepted submission.
		s.submitDue(&h, pending, now)
		if now.Sub(lastGapFlush) >= gapFlushInterval {
			for id, gap := range pending {
				s.sink.RecordGap(gap)
				delete(pending, id)
			}
			lastGapFlush = now
		}
		nextWake := h[0].next
		if len(pending) > 0 {
			flushAt := lastGapFlush.Add(gapFlushInterval)
			if flushAt.Before(nextWake) {
				nextWake = flushAt
			}
		}
		timer.Reset(max(0, nextWake.Sub(s.clock.Now())))
	}
}

func (s *Scheduler) submitDue(h *deadlineHeap, pending map[int64]model.SchedulerGap, now time.Time) {
	for h.Len() > 0 && !(*h)[0].next.After(now) {
		item := heap.Pop(h).(entry)
		if behind := now.Sub(item.next); behind >= item.target.Interval {
			skipped := uint64(behind / item.target.Interval)
			s.observeGap(pending, item.target, item.next, skipped)
			item.next = item.next.Add(time.Duration(skipped) * item.target.Interval)
		}
		job := Job{Target: item.target, ScheduledAt: item.next}
		if s.sink.TrySubmit(job) {
			item.lastSubmitted = s.clock.Now()
			if gap, ok := pending[item.target.ID]; ok {
				s.sink.RecordGap(gap)
				delete(pending, item.target.ID)
			}
		} else {
			s.observeGap(pending, item.target, item.next, 1)
		}
		item.next = item.next.Add(item.target.Interval)
		heap.Push(h, item)
	}
}

func (s *Scheduler) applyUpdates(h *deadlineHeap, pending map[int64]model.SchedulerGap, now time.Time) {
	s.updateMu.Lock()
	updates := s.updates
	s.updates = make(map[int64]time.Duration)
	s.updateMu.Unlock()
	if len(updates) == 0 {
		return
	}
	for i := range *h {
		item := &(*h)[i]
		interval, ok := updates[item.target.ID]
		if !ok || interval == item.target.Interval {
			continue
		}
		if gap, ok := pending[item.target.ID]; ok {
			s.sink.RecordGap(gap)
			delete(pending, item.target.ID)
		}
		next := now.Add(interval)
		if !item.lastSubmitted.IsZero() {
			next = item.lastSubmitted.Add(interval)
		}
		if item.lastSubmitted.IsZero() && interval < item.target.Interval && item.next.Before(next) {
			next = item.next
		}
		if next.Before(now) {
			next = now
		}
		item.target.Interval = interval
		item.next = next
	}
	heap.Init(h)
}

func (s *Scheduler) observeGap(pending map[int64]model.SchedulerGap, target Target, first time.Time, count uint64) {
	if observer, ok := s.sink.(interface{ ObserveGap(model.SchedulerGap) }); ok {
		observer.ObserveGap(model.SchedulerGap{TargetID: target.ID, FirstScheduled: first, Interval: target.Interval, MissedCount: count})
	}
	addGap(pending, target, first, count)
}

func addGap(pending map[int64]model.SchedulerGap, target Target, first time.Time, count uint64) {
	if count == 0 {
		return
	}
	gap, ok := pending[target.ID]
	if !ok {
		pending[target.ID] = model.SchedulerGap{TargetID: target.ID, FirstScheduled: first, Interval: target.Interval, MissedCount: count}
		return
	}
	gap.MissedCount += count
	pending[target.ID] = gap
}

func phaseFor(id string, interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return time.Duration(h.Sum64() % uint64(interval))
}
