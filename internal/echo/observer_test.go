package echo

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/opshed/flameping/internal/eventbus"
	"github.com/opshed/flameping/internal/model"
	"github.com/opshed/flameping/internal/scheduler"
)

type observerSpy struct {
	events chan string
	reply  chan struct{}
	once   sync.Once
	gaps   chan model.SchedulerGap
}

func newObserverSpy() *observerSpy {
	return &observerSpy{events: make(chan string, 16), reply: make(chan struct{}), gaps: make(chan model.SchedulerGap, 16)}
}
func (s *observerSpy) BeginProbe(model.ProbeEvent) { s.events <- "begin" }
func (s *observerSpy) CompleteProbe(_ model.ProbeKey, success bool) {
	if success {
		s.events <- "sent"
	} else {
		s.events <- "send_error"
	}
}
func (s *observerSpy) ObserveReply(model.ProbeEvent) {
	s.events <- "reply"
	s.once.Do(func() { close(s.reply) })
}
func (s *observerSpy) ObserveGap(g model.SchedulerGap)  { s.gaps <- g }
func (s *observerSpy) UpdateEndpoint(int64, netip.Addr) {}

type observerSocket struct {
	write   func([]byte, netip.Addr, uint64) error
	packets chan Packet
	closed  chan struct{}
	once    sync.Once
}

func newObserverSocket() *observerSocket {
	return &observerSocket{packets: make(chan Packet, 8), closed: make(chan struct{})}
}
func (s *observerSocket) WriteEcho(b []byte, a netip.Addr, n uint64) error { return s.write(b, a, n) }
func (s *observerSocket) ReadEcho(ctx context.Context) (Packet, error) {
	select {
	case p := <-s.packets:
		return p, nil
	case <-ctx.Done():
		return Packet{}, ctx.Err()
	case <-s.closed:
		return Packet{}, net.ErrClosed
	}
}
func (s *observerSocket) Close() error { s.once.Do(func() { close(s.closed) }); return nil }

func TestObserverSeesAuthenticatedEarlyReplyBeforeBlockedPublication(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "successful write", true: "failed write"}[fail], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			bus := eventbus.New(1) // The send reservation blocks reply publication.
			socket := newObserverSocket()
			spy := newObserverSpy()
			engine, err := NewEngine(bus, model.RunID{1}, socket, nil, 1)
			if err != nil {
				t.Fatal(err)
			}
			engine.SetObserver(spy)
			socket.write = func(payload []byte, endpoint netip.Addr, _ uint64) error {
				spy.events <- "write"
				bad := append([]byte(nil), payload...)
				bad[len(bad)-1] ^= 1
				socket.packets <- Packet{Payload: bad, Source: endpoint}
				foreign, _ := EncodePayload(Identity{RunID: model.RunID{2}, Sequence: 42, SendOffset: 0, Timeout: time.Second}, engine.secret)
				socket.packets <- Packet{Payload: foreign, Source: endpoint}
				socket.packets <- Packet{Payload: append([]byte(nil), payload...), Source: endpoint}
				select {
				case <-spy.reply:
				case <-ctx.Done():
					return ctx.Err()
				}
				if fail {
					return errors.New("injected write failure")
				}
				return nil
			}
			done := make(chan error, 1)
			go func() { done <- engine.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				_ = engine.Close()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("echo observer shutdown hung")
				}
			})
			target := Target{ID: 1, Endpoint: netip.MustParseAddr("127.0.0.1"), Interval: time.Second, Timeout: time.Second}
			if !engine.TrySubmit(scheduler.Job{Target: scheduler.Target{ID: 1, Interval: time.Second, Value: target}, ScheduledAt: time.Now()}) {
				t.Fatal("submission rejected")
			}
			last := "sent"
			if fail {
				last = "send_error"
			}
			for _, want := range []string{"begin", "write", "reply", last} {
				select {
				case got := <-spy.events:
					if got != want {
						t.Fatalf("observer event=%q want %q", got, want)
					}
				case <-ctx.Done():
					t.Fatal("observer blocked behind storage publication")
				}
			}
			for i := 0; i < 2; i++ {
				if _, err := bus.Next(ctx); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case extra := <-spy.events:
				t.Fatalf("unauthenticated/foreign reply reached observer: %s", extra)
			default:
			}
		})
	}
}

func TestQueuedProbeHandlesEndpointChanges(t *testing.T) {
	for _, address := range []string{"127.0.0.2", "::1"} {
		t.Run(address, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			bus := eventbus.New(1)
			// Hold admission across the DNS update, even after the queued job is read.
			if err := bus.Publish(ctx, model.Event{Kind: model.EventSchedulerGap}); err != nil {
				t.Fatal(err)
			}
			socket := newObserverSocket()
			spy := newObserverSpy()
			writes := make(chan netip.Addr, 1)
			socket.write = func(_ []byte, a netip.Addr, _ uint64) error { writes <- a; return nil }
			engine, err := NewEngine(bus, model.RunID{1}, socket, nil, 1)
			if err != nil {
				t.Fatal(err)
			}
			engine.SetObserver(spy)
			target := Target{ID: 1, Endpoint: netip.MustParseAddr("127.0.0.1"), Interval: time.Second, Timeout: time.Second}
			if !engine.TrySubmit(scheduler.Job{Target: scheduler.Target{ID: 1, Interval: time.Second, Value: target}, ScheduledAt: time.Now()}) {
				t.Fatal("submission rejected")
			}
			next := netip.MustParseAddr(address)
			done := make(chan error, 1)
			go func() { done <- engine.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				_ = engine.Close()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("echo shutdown hung")
				}
			})
			for len(engine.v4jobs) > 0 && ctx.Err() == nil {
				time.Sleep(time.Millisecond)
			}
			time.Sleep(time.Millisecond)
			engine.UpdateEndpoint(1, next)
			if _, err := bus.Next(ctx); err != nil {
				t.Fatal(err)
			}
			event, err := bus.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if next.Is4() {
				if event.Kind != model.EventProbeSent || event.Probe.Endpoint != next {
					t.Fatalf("queued probe used stale endpoint: %+v", event)
				}
				select {
				case actual := <-writes:
					if actual != next {
						t.Fatal(actual)
					}
				case <-ctx.Done():
					t.Fatal("no write")
				}
			} else {
				if event.Kind != model.EventSchedulerGap || event.Gap.MissedCount != 1 {
					t.Fatalf("cross-family work not accounted: %+v", event)
				}
				select {
				case gap := <-spy.gaps:
					if gap.MissedCount != 1 {
						t.Fatal(gap)
					}
				case <-ctx.Done():
					t.Fatal("no immediate gap")
				}
				select {
				case a := <-writes:
					t.Fatalf("wrong-family write: %s", a)
				default:
				}
				engine.RecordGap(event.Gap)
				select {
				case gap := <-spy.gaps:
					t.Fatalf("persisted gap notified twice: %+v", gap)
				default:
				}
			}
		})
	}
}
