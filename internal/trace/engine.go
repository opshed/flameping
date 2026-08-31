package trace

import (
	"context"
	"hash/fnv"
	"net/netip"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"flameping/internal/eventbus"
	"flameping/internal/model"
)

type Engine struct {
	runner  *Runner
	bus     *eventbus.Bus
	targets []Target
	period  time.Duration
	v4mu    sync.Mutex
	v6mu    sync.Mutex
	mu      sync.RWMutex
	current map[int64]timeTarget
}

type timeTarget struct{ endpoint netip.Addr }

func NewEngine(runner *Runner, bus *eventbus.Bus, targets []Target, period time.Duration) *Engine {
	current := make(map[int64]timeTarget, len(targets))
	for _, target := range targets {
		current[target.ID] = timeTarget{endpoint: target.Endpoint}
	}
	return &Engine{runner: runner, bus: bus, targets: append([]Target(nil), targets...), period: period, current: current}
}

func (e *Engine) UpdateEndpoint(targetID int64, endpoint netip.Addr) {
	e.mu.Lock()
	e.current[targetID] = timeTarget{endpoint: endpoint}
	e.mu.Unlock()
}

func (e *Engine) Run(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	for _, target := range e.targets {
		target := target
		group.Go(func() error { return e.runTarget(ctx, target) })
	}
	if len(e.targets) == 0 {
		<-ctx.Done()
		return ctx.Err()
	}
	return group.Wait()
}

func (e *Engine) runTarget(ctx context.Context, target Target) error {
	delayWindow := min(e.period, 30*time.Second)
	timer := time.NewTimer(tracePhase(target.StableID, delayWindow))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			e.mu.RLock()
			if current, ok := e.current[target.ID]; ok {
				target.Endpoint = current.endpoint
			}
			e.mu.RUnlock()
			if !target.Endpoint.IsValid() {
				timer.Reset(min(e.period, time.Minute))
				continue
			}
			if !e.runner.Available(target.Endpoint) {
				timer.Reset(e.period)
				continue
			}
			mutex := &e.v6mu
			if target.Endpoint.Is4() {
				mutex = &e.v4mu
			}
			mutex.Lock()
			result, err := e.runner.Trace(ctx, target)
			mutex.Unlock()
			if err != nil {
				result.Status = "error"
				result.ErrorDetail = err.Error()
				result.EndedAt = time.Now()
			}
			if publishErr := e.bus.Publish(ctx, model.Event{Kind: model.EventTraceCompleted, Trace: result}); publishErr != nil {
				return publishErr
			}
			next := e.period
			if err != nil {
				next = min(next, time.Minute)
			}
			timer.Reset(next)
		}
	}
}

func tracePhase(id string, window time.Duration) time.Duration {
	if window <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return time.Duration(h.Sum64() % uint64(window))
}
