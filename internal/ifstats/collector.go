package ifstats

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/opshed/flameping/internal/config"
	"github.com/opshed/flameping/internal/eventbus"
	"github.com/opshed/flameping/internal/model"
)

type Link struct {
	Name     string
	IfIndex  int
	MAC      string
	Counters model.InterfaceCounters
}

type Source interface {
	List() ([]Link, error)
	Close() error
}

type Collector struct {
	source     Source
	bus        *eventbus.Bus
	interfaces []config.InterfaceConfig
	bootID     string
	previous   map[string]Link
	lastSample map[string]time.Time
	missing    map[string]bool
}

func New(source Source, bus *eventbus.Bus, interfaces []config.InterfaceConfig) *Collector {
	bootID := "unknown"
	if data, err := os.ReadFile("/proc/sys/kernel/random/boot_id"); err == nil {
		bootID = strings.TrimSpace(string(data))
	}
	return &Collector{source: source, bus: bus, interfaces: append([]config.InterfaceConfig(nil), interfaces...), bootID: bootID, previous: make(map[string]Link), lastSample: make(map[string]time.Time), missing: make(map[string]bool)}
}

func (c *Collector) Run(ctx context.Context) error {
	if len(c.interfaces) == 0 {
		<-ctx.Done()
		return ctx.Err()
	}
	interval := 5 * time.Second
	for _, item := range c.interfaces {
		if item.Interval > 0 && item.Interval.Value() < interval {
			interval = item.Interval.Value()
		}
	}
	if interval > time.Second {
		interval = time.Second
	}
	if err := c.collect(ctx, time.Now()); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case at := <-ticker.C:
			if err := c.collect(ctx, at); err != nil {
				return err
			}
		}
	}
}

func (c *Collector) collect(ctx context.Context, at time.Time) error {
	if c.lastSample == nil {
		c.lastSample = make(map[string]time.Time)
	}
	if c.missing == nil {
		c.missing = make(map[string]bool)
	}
	links, err := c.source.List()
	if err != nil {
		return err
	}
	wanted := make(map[string]struct{}, len(c.interfaces))
	for _, item := range c.interfaces {
		interval := item.Interval.Value()
		if interval <= 0 {
			interval = 5 * time.Second
		}
		if last := c.lastSample[item.Name]; !last.IsZero() && at.Sub(last) < interval {
			continue
		}
		wanted[item.Name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(links))
	for _, link := range links {
		if _, ok := wanted[link.Name]; !ok {
			continue
		}
		seen[link.Name] = struct{}{}
		if old, ok := c.previous[link.Name]; ok {
			reason := resetReason(old, link)
			if reason != "" {
				if err := c.bus.Publish(ctx, model.Event{Kind: model.EventInterfaceReset, Interface: model.InterfaceEvent{Name: link.Name, BootID: c.bootID, IfIndex: link.IfIndex, MAC: link.MAC, SampledAt: at, ResetReason: reason}}); err != nil {
					return err
				}
			}
		}
		event := model.Event{Kind: model.EventInterfaceSnapshot, Interface: model.InterfaceEvent{Name: link.Name, BootID: c.bootID, IfIndex: link.IfIndex, MAC: link.MAC, SampledAt: at, Counters: link.Counters}}
		if err := c.bus.Publish(ctx, event); err != nil {
			return err
		}
		c.previous[link.Name] = link
		delete(c.missing, link.Name)
		c.lastSample[link.Name] = at
	}
	for name := range wanted {
		if _, ok := seen[name]; !ok {
			if !c.missing[name] {
				old := c.previous[name]
				if err := c.bus.Publish(ctx, model.Event{Kind: model.EventInterfaceReset, Interface: model.InterfaceEvent{Name: name, BootID: c.bootID, IfIndex: old.IfIndex, MAC: old.MAC, SampledAt: at, ResetReason: "interface_missing"}}); err != nil {
					return err
				}
				c.missing[name] = true
			}
			delete(c.previous, name)
			c.lastSample[name] = at
		}
	}
	return nil
}

func resetReason(old, current Link) string {
	if old.IfIndex != current.IfIndex || old.MAC != current.MAC {
		return "identity_changed"
	}
	a, b := old.Counters, current.Counters
	oldValues := []uint64{a.RXBytes, a.TXBytes, a.RXPackets, a.TXPackets, a.RXErrors, a.TXErrors, a.RXDropped, a.TXDropped, a.RXMissed, a.RXFIFO, a.TXFIFO, a.RXCRC, a.RXFrame, a.TXCarrier, a.Collisions}
	newValues := []uint64{b.RXBytes, b.TXBytes, b.RXPackets, b.TXPackets, b.RXErrors, b.TXErrors, b.RXDropped, b.TXDropped, b.RXMissed, b.RXFIFO, b.TXFIFO, b.RXCRC, b.RXFrame, b.TXCarrier, b.Collisions}
	for i := range oldValues {
		if newValues[i] < oldValues[i] {
			return "counter_decreased"
		}
	}
	return ""
}

func (c *Collector) Close() error {
	if c.source == nil {
		return errors.New("interface source is nil")
	}
	return c.source.Close()
}
