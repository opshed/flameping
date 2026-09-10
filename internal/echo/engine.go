package echo

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/opshed/flameping/internal/eventbus"
	"github.com/opshed/flameping/internal/model"
	"github.com/opshed/flameping/internal/scheduler"
)

type Target struct {
	ID       int64
	StableID string
	Endpoint netip.Addr
	Interval time.Duration
	Timeout  time.Duration
}

// Observer receives live probe evidence independently of the lossless storage
// queue. Implementations must return promptly and must not publish to that queue.
type Observer interface {
	BeginProbe(model.ProbeEvent)
	CompleteProbe(model.ProbeKey, bool)
	ObserveReply(model.ProbeEvent)
	ObserveGap(model.SchedulerGap)
	UpdateEndpoint(int64, netip.Addr)
}

type Engine struct {
	bus          *eventbus.Bus
	runID        model.RunID
	secret       []byte
	started      time.Time
	sequence     atomic.Uint64
	v4           PacketIO
	v6           PacketIO
	v4Failed     atomic.Bool
	v6Failed     atomic.Bool
	v4CloseOnce  sync.Once
	v6CloseOnce  sync.Once
	v4CloseErr   error
	v6CloseErr   error
	v4jobs       chan scheduler.Job
	v6jobs       chan scheduler.Job
	gaps         chan model.SchedulerGap
	mu           sync.RWMutex
	current      map[int64]netip.Addr
	done         chan struct{}
	inputMu      sync.RWMutex
	inputWG      sync.WaitGroup
	inputsDone   chan struct{}
	runStarted   chan struct{}
	runOnce      sync.Once
	inputsClosed bool
	closing      atomic.Bool
	failures     chan error
	observer     Observer
}

// SetObserver must be called before Run or any concurrent use of the engine.
func (e *Engine) SetObserver(observer Observer) { e.observer = observer }

func NewEngine(bus *eventbus.Bus, runID model.RunID, v4, v6 PacketIO, sendQueue int) (*Engine, error) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, err
	}
	engine := &Engine{bus: bus, runID: runID, secret: secret, started: time.Now(), v4: v4, v6: v6,
		v4jobs: make(chan scheduler.Job, sendQueue), v6jobs: make(chan scheduler.Job, sendQueue), gaps: make(chan model.SchedulerGap, sendQueue), current: make(map[int64]netip.Addr), done: make(chan struct{}), inputsDone: make(chan struct{}), runStarted: make(chan struct{}), failures: make(chan error, 1)}
	inputLoops := 1
	if v4 != nil {
		inputLoops++
	}
	if v6 != nil {
		inputLoops++
	}
	engine.inputWG.Add(inputLoops)
	return engine, nil
}

func (e *Engine) UpdateEndpoint(targetID int64, endpoint netip.Addr) {
	e.mu.Lock()
	e.current[targetID] = endpoint
	if e.observer != nil {
		e.observer.UpdateEndpoint(targetID, endpoint)
	}
	e.mu.Unlock()
}

// ObserveGap reports missing evidence immediately; RecordGap separately persists
// the scheduler's coalesced counts.
func (e *Engine) ObserveGap(gap model.SchedulerGap) {
	if e.observer != nil {
		e.observer.ObserveGap(gap)
	}
}

func (e *Engine) TrySubmit(job scheduler.Job) bool {
	e.inputMu.RLock()
	defer e.inputMu.RUnlock()
	if e.inputsClosed {
		return false
	}
	select {
	case <-e.done:
		return false
	default:
	}
	target, ok := job.Target.Value.(Target)
	if !ok {
		return false
	}
	e.mu.RLock()
	if endpoint, exists := e.current[target.ID]; exists {
		target.Endpoint = endpoint
	}
	e.mu.RUnlock()
	if !target.Endpoint.IsValid() {
		return false
	}
	job.Target.Value = target
	queue := e.v6jobs
	socket := e.v6
	failed := &e.v6Failed
	if target.Endpoint.Is4() {
		queue, socket = e.v4jobs, e.v4
		failed = &e.v4Failed
	}
	if socket == nil || failed.Load() {
		return false
	}
	select {
	case queue <- job:
		return true
	default:
		return false
	}
}

func (e *Engine) RecordGap(gap model.SchedulerGap) {
	e.inputMu.RLock()
	defer e.inputMu.RUnlock()
	if e.inputsClosed {
		return
	}
	select {
	case <-e.done:
		return
	default:
	}
	select {
	case e.gaps <- gap:
	case <-e.done:
	}
}

func (e *Engine) Run(ctx context.Context) error {
	defer close(e.done)
	e.runOnce.Do(func() { close(e.runStarted) })
	group, ctx := errgroup.WithContext(ctx)
	if e.v4 != nil {
		group.Go(func() error { defer e.inputWG.Done(); return e.sendLoop(ctx, e.v4, e.v4jobs, &e.v4Failed) })
		group.Go(func() error { return e.runReceiver(ctx, e.v4, 4, &e.v4Failed) })
	}
	if e.v6 != nil {
		group.Go(func() error { defer e.inputWG.Done(); return e.sendLoop(ctx, e.v6, e.v6jobs, &e.v6Failed) })
		group.Go(func() error { return e.runReceiver(ctx, e.v6, 6, &e.v6Failed) })
	}
	group.Go(func() error {
		defer e.inputWG.Done()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case gap, ok := <-e.gaps:
				if !ok {
					return nil
				}
				if err := e.bus.Publish(ctx, model.Event{Kind: model.EventSchedulerGap, Gap: gap}); err != nil {
					return err
				}
			}
		}
	})
	go func() {
		e.inputWG.Wait()
		close(e.inputsDone)
	}()
	return group.Wait()
}

func (e *Engine) sendLoop(ctx context.Context, socket PacketIO, jobs <-chan scheduler.Job, failed *atomic.Bool) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case job, ok := <-jobs:
			if !ok {
				return nil
			}
			if failed.Load() {
				if err := e.publishUnsentGap(ctx, job); err != nil {
					return err
				}
				continue
			}
			target := job.Target.Value.(Target)
			reservation, err := e.bus.Reserve(ctx)
			if err != nil {
				return err
			}
			sequence := e.sequence.Add(1)
			if sequence > uint64(^uint64(0)>>1) {
				reservation.Cancel()
				return errors.New("probe sequence exhausted")
			}
			sentAt := time.Now()
			offset := time.Since(e.started)
			payload, err := EncodePayload(Identity{RunID: e.runID, Sequence: sequence, SendOffset: offset, Timeout: target.Timeout}, e.secret)
			if err != nil {
				reservation.Cancel()
				return err
			}
			// Capacity acquisition can wait through a DNS change. Resolve the
			// queued endpoint only after it succeeds, and register the probe in
			// the same endpoint generation before releasing the endpoint lock.
			e.mu.RLock()
			if endpoint, changed := e.current[target.ID]; changed {
				if !endpoint.IsValid() || endpoint.Is4() != target.Endpoint.Is4() {
					e.mu.RUnlock()
					reservation.Cancel()
					if err := e.publishUnsentGap(ctx, job); err != nil {
						return err
					}
					continue
				}
				target.Endpoint = endpoint
			}
			event := model.Event{Kind: model.EventProbeSent, Probe: model.ProbeEvent{
				Key: model.ProbeKey{RunID: e.runID, Sequence: sequence}, TargetID: target.ID,
				Endpoint: target.Endpoint, ScheduledAt: job.ScheduledAt, SentAt: sentAt, Timeout: target.Timeout,
			}}
			if e.observer != nil {
				e.observer.BeginProbe(event.Probe)
			}
			e.mu.RUnlock()
			writeErr := socket.WriteEcho(payload, target.Endpoint, sequence)
			if e.observer != nil {
				e.observer.CompleteProbe(event.Probe.Key, writeErr == nil)
			}
			if writeErr != nil {
				event.Kind = model.EventProbeSendError
				event.Probe.SendErrorCode = "socket_write"
				event.Probe.SendErrorMessage = writeErr.Error()
			}
			if err := reservation.Publish(event); err != nil {
				return err
			}
		}
	}
}

func (e *Engine) publishUnsentGap(ctx context.Context, job scheduler.Job) error {
	gap := model.SchedulerGap{
		TargetID: job.Target.ID, FirstScheduled: job.ScheduledAt, Interval: job.Target.Interval, MissedCount: 1,
	}
	e.ObserveGap(gap)
	return e.bus.Publish(ctx, model.Event{Kind: model.EventSchedulerGap, Gap: gap})
}

// DrainInputs closes measurement admission after the scheduler has stopped and
// waits until every accepted probe attempt and scheduler gap has reached the
// lossless event bus. Receive loops remain active until Close is called.
func (e *Engine) DrainInputs(ctx context.Context) error {
	select {
	case <-e.runStarted:
	case <-ctx.Done():
		return ctx.Err()
	}
	e.inputMu.Lock()
	if !e.inputsClosed {
		e.inputsClosed = true
		close(e.v4jobs)
		close(e.v6jobs)
		close(e.gaps)
	}
	e.inputMu.Unlock()
	select {
	case <-e.inputsDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) receiveLoop(ctx context.Context, socket PacketIO) error {
	for {
		packet, err := socket.ReadEcho(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		identity, err := DecodePayload(packet.Payload, e.secret)
		if err != nil || identity.RunID != e.runID {
			continue
		}
		rtt := time.Since(e.started) - identity.SendOffset
		if rtt < 0 {
			continue
		}
		class := model.ReplyOnTime
		if rtt > identity.Timeout {
			class = model.ReplyLate
		}
		event := model.Event{Kind: model.EventProbeReply, Probe: model.ProbeEvent{
			Key: model.ProbeKey{RunID: e.runID, Sequence: identity.Sequence}, SentAt: e.started.Add(identity.SendOffset), Timeout: identity.Timeout, ReplyAt: time.Now(), RTT: rtt,
			ReplyClass: class, Responder: packet.Source, ICMPType: packet.Type, ICMPCode: packet.Code,
		}}
		if e.observer != nil {
			e.observer.ObserveReply(event.Probe)
		}
		if err := e.bus.Publish(ctx, event); err != nil {
			return fmt.Errorf("publish reply: %w", err)
		}
	}
}

func (e *Engine) runReceiver(ctx context.Context, socket PacketIO, family int, failed *atomic.Bool) error {
	err := e.receiveLoop(ctx, socket)
	if err == nil || ctx.Err() != nil || e.closing.Load() {
		return nil
	}
	failed.Store(true)
	closeErr := e.closeFamily(family)
	failure := fmt.Errorf("IPv%d echo receiver: %w", family, err)
	if closeErr != nil {
		failure = errors.Join(failure, fmt.Errorf("close failed family socket: %w", closeErr))
	}
	select {
	case e.failures <- failure:
	default:
	}
	return nil
}

func (e *Engine) Failures() <-chan error { return e.failures }

func (e *Engine) closeFamily(family int) error {
	if family == 4 {
		e.v4CloseOnce.Do(func() {
			if e.v4 != nil {
				e.v4CloseErr = e.v4.Close()
			}
		})
		return e.v4CloseErr
	}
	e.v6CloseOnce.Do(func() {
		if e.v6 != nil {
			e.v6CloseErr = e.v6.Close()
		}
	})
	return e.v6CloseErr
}

func (e *Engine) Close() error {
	e.closing.Store(true)
	return errors.Join(e.closeFamily(4), e.closeFamily(6))
}
