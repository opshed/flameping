package ifstats

import (
	"context"
	"testing"
	"time"

	"flameping/internal/config"
	"flameping/internal/eventbus"
	"flameping/internal/model"
)

func TestResetReason(t *testing.T) {
	base := Link{Name: "eth0", IfIndex: 2, MAC: "00:11:22:33:44:55", Counters: model.InterfaceCounters{RXBytes: 10, TXBytes: 20}}
	if got := resetReason(base, base); got != "" {
		t.Fatalf("unchanged reset = %q", got)
	}
	decreased := base
	decreased.Counters.RXBytes = 9
	if got := resetReason(base, decreased); got != "counter_decreased" {
		t.Fatalf("decreased reset = %q", got)
	}
	replaced := base
	replaced.IfIndex = 3
	if got := resetReason(base, replaced); got != "identity_changed" {
		t.Fatalf("identity reset = %q", got)
	}
}

type fakeSource struct{ links []Link }

func (f *fakeSource) List() ([]Link, error) { return f.links, nil }
func (f *fakeSource) Close() error          { return nil }

func TestCollectMissingInterface(t *testing.T) {
	source := &fakeSource{}
	bus := eventbus.New(4)
	c := New(source, bus, []config.InterfaceConfig{{Name: "eth0", Interval: config.Duration(time.Second)}})
	at := time.Now()
	if err := c.collect(context.Background(), at); err != nil {
		t.Fatal(err)
	}
	event, err := bus.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if event.Kind != model.EventInterfaceReset || event.Interface.ResetReason != "interface_missing" || event.Interface.Name != "eth0" {
		t.Fatalf("first missing observation=%+v", event)
	}
	if err := c.collect(context.Background(), at.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if bus.Depth() != 0 {
		t.Fatal("unchanged missing interface emitted another event")
	}
	source.links = []Link{{Name: "eth0", IfIndex: 2, MAC: "00:11:22:33:44:55"}}
	if err := c.collect(context.Background(), at.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	event, err = bus.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if event.Kind != model.EventInterfaceSnapshot {
		t.Fatalf("reappeared interface event=%+v", event)
	}
}
