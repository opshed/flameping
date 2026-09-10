package eventbus

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/opshed/flameping/internal/model"
)

var ErrClosed = errors.New("event bus closed")

type Bus struct {
	slots     chan struct{}
	queue     chan model.Event
	closed    chan struct{}
	closeOnce sync.Once
	depth     atomic.Int64
}

func New(capacity int) *Bus {
	if capacity < 1 {
		panic("event bus capacity must be positive")
	}
	return &Bus{
		slots:  make(chan struct{}, capacity),
		queue:  make(chan model.Event, capacity),
		closed: make(chan struct{}),
	}
}

type Reservation struct {
	bus  *Bus
	once sync.Once
}

func (b *Bus) Reserve(ctx context.Context) (*Reservation, error) {
	select {
	case b.slots <- struct{}{}:
		select {
		case <-b.closed:
			<-b.slots
			return nil, ErrClosed
		default:
		}
		return &Reservation{bus: b}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.closed:
		return nil, ErrClosed
	}
}

func (r *Reservation) Publish(event model.Event) error {
	var published bool
	r.once.Do(func() {
		select {
		case r.bus.queue <- event:
			r.bus.depth.Add(1)
			published = true
		case <-r.bus.closed:
			<-r.bus.slots
		}
	})
	if !published {
		return ErrClosed
	}
	return nil
}

func (r *Reservation) Cancel() {
	r.once.Do(func() { <-r.bus.slots })
}

func (b *Bus) Publish(ctx context.Context, event model.Event) error {
	r, err := b.Reserve(ctx)
	if err != nil {
		return err
	}
	return r.Publish(event)
}

func (b *Bus) Next(ctx context.Context) (model.Event, error) {
	select {
	case event := <-b.queue:
		b.depth.Add(-1)
		<-b.slots
		return event, nil
	case <-ctx.Done():
		return model.Event{}, ctx.Err()
	case <-b.closed:
		select {
		case event := <-b.queue:
			b.depth.Add(-1)
			<-b.slots
			return event, nil
		default:
			return model.Event{}, ErrClosed
		}
	}
}

func (b *Bus) Close()        { b.closeOnce.Do(func() { close(b.closed) }) }
func (b *Bus) Depth() int    { return int(b.depth.Load()) }
func (b *Bus) Capacity() int { return cap(b.queue) }
