package scheduler

import (
	"container/heap"
	"context"
	"hash/fnv"
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
	clock   clockpkg.Clock
	targets []Target
	sink    Sink
}

const gapFlushInterval = 5 * time.Second

func New(clock clockpkg.Clock, targets []Target, sink Sink) *Scheduler {
	if clock == nil {
		clock = clockpkg.New()
	}
	return &Scheduler{clock: clock, targets: append([]Target(nil), targets...), sink: sink}
}

type entry struct {
	target Target
	next   time.Time
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
			now = s.clock.Now()
			for h.Len() > 0 && !h[0].next.After(now) {
				item := heap.Pop(&h).(entry)
				if behind := now.Sub(item.next); behind >= item.target.Interval {
					skipped := uint64(behind / item.target.Interval)
					addGap(pending, item.target, item.next, skipped)
					item.next = item.next.Add(time.Duration(skipped) * item.target.Interval)
				}
				job := Job{Target: item.target, ScheduledAt: item.next}
				if s.sink.TrySubmit(job) {
					if gap, ok := pending[item.target.ID]; ok {
						s.sink.RecordGap(gap)
						delete(pending, item.target.ID)
					}
				} else {
					addGap(pending, item.target, item.next, 1)
				}
				item.next = item.next.Add(item.target.Interval)
				heap.Push(&h, item)
			}
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
			delay := nextWake.Sub(s.clock.Now())
			if delay < 0 {
				delay = 0
			}
			timer.Reset(delay)
		}
	}
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
