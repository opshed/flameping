package eventbus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/opshed/flameping/internal/model"
)

func TestReservationPreservesCapacity(t *testing.T) {
	b := New(1)
	r, err := b.Reserve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := b.Reserve(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second reserve error = %v", err)
	}
	if err := r.Publish(model.Event{Kind: model.EventProbeSent}); err != nil {
		t.Fatal(err)
	}
	got, err := b.Next(context.Background())
	if err != nil || got.Kind != model.EventProbeSent {
		t.Fatalf("Next = %#v, %v", got, err)
	}
}

func TestCanceledReservationReleasesSlot(t *testing.T) {
	b := New(1)
	r, err := b.Reserve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r.Cancel()
	if err := b.Publish(context.Background(), model.Event{Kind: model.EventProbeReply}); err != nil {
		t.Fatal(err)
	}
}
