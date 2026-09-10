package echo

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opshed/flameping/internal/eventbus"
	"github.com/opshed/flameping/internal/model"
	"github.com/opshed/flameping/internal/scheduler"
)

type drainPacketIO struct {
	release chan struct{}
	closed  chan struct{}
	once    sync.Once
	writes  atomic.Int64
}

type failingReceiverPacketIO struct {
	failRead chan struct{}
	closed   chan struct{}
	once     sync.Once
}

func (f *failingReceiverPacketIO) WriteEcho([]byte, netip.Addr, uint64) error {
	<-f.closed
	return net.ErrClosed
}

func (f *failingReceiverPacketIO) ReadEcho(ctx context.Context) (Packet, error) {
	select {
	case <-ctx.Done():
		return Packet{}, ctx.Err()
	case <-f.failRead:
		return Packet{}, errors.New("injected receive failure")
	case <-f.closed:
		return Packet{}, net.ErrClosed
	}
}

func (f *failingReceiverPacketIO) Close() error {
	f.once.Do(func() { close(f.closed) })
	return nil
}

func (f *drainPacketIO) WriteEcho([]byte, netip.Addr, uint64) error {
	select {
	case <-f.release:
		f.writes.Add(1)
		return nil
	case <-f.closed:
		return net.ErrClosed
	}
}

func (f *drainPacketIO) ReadEcho(ctx context.Context) (Packet, error) {
	select {
	case <-ctx.Done():
		return Packet{}, ctx.Err()
	case <-f.closed:
		return Packet{}, net.ErrClosed
	}
}

func (f *drainPacketIO) Close() error {
	f.once.Do(func() { close(f.closed) })
	return nil
}

func TestDrainInputsAccountsForSaturatedAcceptedWork(t *testing.T) {
	bus := eventbus.New(32)
	socket := &drainPacketIO{release: make(chan struct{}), closed: make(chan struct{})}
	engine, err := NewEngine(bus, model.RunID{1}, socket, nil, 4)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- engine.Run(ctx) }()

	target := Target{ID: 1, StableID: "target", Endpoint: netip.MustParseAddr("127.0.0.1"), Interval: time.Second, Timeout: time.Second}
	accepted := 0
	for i := 0; i < 16; i++ {
		if !engine.TrySubmit(scheduler.Job{Target: scheduler.Target{ID: 1, StableID: "target", Interval: time.Second, Value: target}, ScheduledAt: time.Now()}) {
			break
		}
		accepted++
	}
	if accepted < 4 || accepted >= 16 {
		t.Fatalf("accepted=%d, queue was not saturated", accepted)
	}
	engine.RecordGap(model.SchedulerGap{TargetID: 1, FirstScheduled: time.Now(), Interval: time.Second, MissedCount: 3})

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer drainCancel()
	drained := make(chan error, 1)
	go func() { drained <- engine.DrainInputs(drainCtx) }()
	select {
	case err := <-drained:
		t.Fatalf("drain completed before blocked accepted writes were accounted for: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(socket.release)
	if err := <-drained; err != nil {
		t.Fatal(err)
	}

	var sent, gaps int
	for i := 0; i < accepted+1; i++ {
		event, err := bus.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		switch event.Kind {
		case model.EventProbeSent:
			sent++
		case model.EventSchedulerGap:
			gaps++
		}
	}
	if sent != accepted || gaps != 1 || int(socket.writes.Load()) != accepted {
		t.Fatalf("sent=%d gaps=%d writes=%d accepted=%d", sent, gaps, socket.writes.Load(), accepted)
	}
	_ = engine.Close()
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
}

func TestReceiveFailureConvertsSaturatedQueuedWorkToGaps(t *testing.T) {
	bus := eventbus.New(32)
	socket := &failingReceiverPacketIO{failRead: make(chan struct{}), closed: make(chan struct{})}
	engine, err := NewEngine(bus, model.RunID{2}, socket, nil, 4)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- engine.Run(ctx) }()
	target := Target{ID: 1, StableID: "target", Endpoint: netip.MustParseAddr("127.0.0.1"), Interval: time.Second, Timeout: time.Second}
	job := func() scheduler.Job {
		return scheduler.Job{Target: scheduler.Target{ID: 1, StableID: "target", Interval: time.Second, Value: target}, ScheduledAt: time.Now()}
	}
	accepted := 0
	for i := 0; i < 16; i++ {
		if !engine.TrySubmit(job()) {
			break
		}
		accepted++
	}
	if accepted < 4 || accepted >= 16 {
		t.Fatalf("accepted=%d, queue was not saturated", accepted)
	}
	close(socket.failRead)
	select {
	case failure := <-engine.Failures():
		if !strings.Contains(failure.Error(), "injected receive failure") {
			t.Fatalf("failure=%v", failure)
		}
	case <-time.After(time.Second):
		t.Fatal("receive failure was not reported")
	}
	if engine.TrySubmit(job()) {
		t.Fatal("failed address family continued accepting jobs")
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer drainCancel()
	if err := engine.DrainInputs(drainCtx); err != nil {
		t.Fatal(err)
	}

	accounted := 0
	for accounted < accepted {
		event, err := bus.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		switch event.Kind {
		case model.EventProbeSendError:
			accounted++
		case model.EventSchedulerGap:
			accounted += int(event.Gap.MissedCount)
		default:
			t.Fatalf("unexpected event kind %d", event.Kind)
		}
	}
	if accounted != accepted {
		t.Fatalf("accounted=%d, accepted=%d", accounted, accepted)
	}
	_ = engine.Close()
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
}
