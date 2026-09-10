// Package obsess temporarily increases a target's probe rate when its network
// behavior deteriorates. It observes live probes independently of persistence.
package obsess

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"sync"
	"time"

	clockpkg "github.com/benbjohnson/clock"

	"github.com/opshed/flameping/internal/config"
	"github.com/opshed/flameping/internal/model"
)

// The configuration validator budgets pending probes, retained baseline
// samples, and queued-send bursts against this same limit.
const maxTrackedSamples = 100000

type Target struct {
	ID       int64
	Endpoint netip.Addr
	Interval time.Duration
	Config   *config.ObsessConfig
}

type Status struct {
	Enabled         bool     `json:"enabled"`
	Monitoring      bool     `json:"monitoring"`
	State           string   `json:"state"`
	IntervalMS      float64  `json:"interval_ms"`
	Reason          string   `json:"reason,omitempty"`
	SinceMS         *int64   `json:"since_ms,omitempty"`
	BaselineMS      *float64 `json:"baseline_ms,omitempty"`
	ThresholdMS     *float64 `json:"threshold_ms,omitempty"`
	BaselineSamples int      `json:"baseline_samples"`
	HealthyForMS    float64  `json:"healthy_for_ms"`
	RecoverAfterMS  float64  `json:"recover_after_ms"`
	// TrackingLimited records lost observation capacity until the endpoint is
	// reset or a complete recovery is observed, rather than instantaneous usage.
	TrackingLimited bool `json:"tracking_limited,omitempty"`
}

type sample struct {
	sentAt time.Time
	rtt    float64
}

type targetState struct {
	target          Target
	absolute        time.Duration
	percent         float64
	active          bool
	reason          string
	since           time.Time
	baseline        float64
	baselineCount   int
	threshold       float64
	hasThreshold    bool
	healthySince    time.Time
	healthyThrough  time.Time
	lastBad         time.Time
	history         []sample
	historyHead     int
	head            *probe
	tail            *probe
	trackingLimited bool
}

// A probe remains in its target's FIFO until all earlier probes have settled.
// Only accepted, unresolved probes also belong to the deadline heap.
type probe struct {
	event         model.ProbeEvent
	target        *targetState
	prev, next    *probe
	heapIndex     int
	accepted      bool
	done          bool
	good          bool
	rtt           float64
	early         *model.ProbeEvent
	baseline      float64
	baselineCount int
	threshold     float64
	hasThreshold  bool
}

func (p *probe) deadline() time.Time { return p.event.SentAt.Add(p.event.Timeout) }

type deadlineHeap []*probe

func (h deadlineHeap) Len() int           { return len(h) }
func (h deadlineHeap) Less(i, j int) bool { return h[i].deadline().Before(h[j].deadline()) }
func (h deadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex, h[j].heapIndex = i, j
}
func (h *deadlineHeap) Push(v any) {
	p := v.(*probe)
	p.heapIndex = len(*h)
	*h = append(*h, p)
}
func (h *deadlineHeap) Pop() any {
	old := *h
	p := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	p.heapIndex = -1
	return p
}

// Transition captures evidence at the state change. Consumers must use this
// snapshot rather than looking up mutable endpoint or probe state later.
type Transition struct {
	TargetID int64
	Status   Status
	Endpoint netip.Addr
	Reason   string
	At       time.Time
	// Trigger is a private copy of the entering probe, absent on recovery/reset.
	Trigger *model.ProbeEvent
}

type Controller struct {
	mu         sync.Mutex
	clock      clockpkg.Clock
	targets    map[int64]*targetState
	probes     map[model.ProbeKey]*probe
	deadlines  deadlineHeap
	tracked    int
	samples    int
	wake       chan struct{}
	running    bool
	change     func(int64, time.Duration)
	transition func(Transition)
}

// New expects effective, validated obsess settings. change is called while the
// controller lock is held to preserve ordering between concurrent transitions;
// it must be nonblocking and must not call back into Controller. transition runs
// outside that lock and may read snapshots; it should do only lightweight work.
func New(clock clockpkg.Clock, targets []Target, change func(int64, time.Duration), onTransition func(Transition)) (*Controller, error) {
	if clock == nil {
		clock = clockpkg.New()
	}
	c := &Controller{clock: clock, targets: make(map[int64]*targetState), probes: make(map[model.ProbeKey]*probe), wake: make(chan struct{}, 1), change: change, transition: onTransition}
	for _, target := range targets {
		if _, exists := c.targets[target.ID]; exists {
			return nil, fmt.Errorf("duplicate obsess target %d", target.ID)
		}
		state := &targetState{target: target}
		if target.Config != nil {
			policy := *target.Config
			state.target.Config = &policy
			if policy.Interval <= 0 || target.Interval <= policy.Interval.Value() || policy.BaselineWindow <= 0 || policy.RecoverAfter <= 0 || policy.MinSamples < 1 {
				return nil, fmt.Errorf("target %d has invalid effective obsess settings", target.ID)
			}
			absolute, percent, err := policy.Threshold()
			if err != nil {
				return nil, fmt.Errorf("target %d: %w", target.ID, err)
			}
			state.absolute, state.percent = absolute, percent
		}
		c.targets[target.ID] = state
	}
	return c, nil
}

// Run owns the single deadline timer. Probe hooks remain safe during startup
// and shutdown; Monitoring reports whether this timer is running.
func (c *Controller) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return errors.New("obsess controller already running")
	}
	c.running = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()
	timer := c.clock.Timer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for {
		c.mu.Lock()
		changes := c.expire(c.clock.Now())
		var next time.Time
		if len(c.deadlines) > 0 {
			next = c.deadlines[0].deadline()
		}
		c.mu.Unlock()
		c.emit(changes)
		var timeout <-chan time.Time
		if !next.IsZero() {
			timer.Reset(max(0, next.Sub(c.clock.Now())))
			timeout = timer.C
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.wake:
		case <-timeout:
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

// BeginProbe must be called before the socket write so a fast reply can be
// correlated. A writing probe does not acquire a deadline until CompleteProbe
// confirms that the write succeeded.
func (c *Controller) BeginProbe(event model.ProbeEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.targets[event.TargetID]
	if t == nil || t.target.Config == nil || event.Endpoint != t.target.Endpoint || !event.Endpoint.IsValid() || event.Timeout <= 0 {
		return
	}
	if _, exists := c.probes[event.Key]; exists {
		return
	}
	c.pruneHistory(t, event.SentAt)
	if !c.makeRoom() {
		t.trackingLimited = true
		// Its result cannot be observed, so no recovery window may cross
		// this probe's outstanding lifetime, even if capacity frees sooner.
		c.interrupt(t, event.SentAt.Add(event.Timeout))
		return
	}
	baseline, count := c.average(t)
	threshold, known := c.limit(t, baseline, count)
	if t.active {
		baseline, count = t.baseline, t.baselineCount
		threshold, known = t.threshold, t.hasThreshold
	}
	p := &probe{event: event, target: t, heapIndex: -1, baseline: baseline, baselineCount: count, threshold: threshold, hasThreshold: known, prev: t.tail}
	if t.tail != nil {
		t.tail.next = p
	} else {
		t.head = p
	}
	t.tail = p
	c.probes[event.Key] = p
	c.tracked++
}

func (c *Controller) CompleteProbe(key model.ProbeKey, success bool) {
	c.mu.Lock()
	p := c.probes[key]
	var changes []Transition
	if p != nil && !p.accepted {
		p.accepted = true
		if !success {
			c.interrupt(p.target, c.clock.Now())
			changes = c.finish(p, false, 0)
		} else if p.early != nil {
			changes = c.reply(p, *p.early)
		} else {
			heap.Push(&c.deadlines, p)
			c.signal()
		}
	}
	c.mu.Unlock()
	c.emit(changes)
}

func (c *Controller) ObserveReply(event model.ProbeEvent) {
	c.mu.Lock()
	p := c.probes[event.Key]
	var changes []Transition
	if p != nil && sameResponder(p.event.Endpoint, event.Responder) && event.RTT >= 0 {
		if !p.accepted {
			if p.early == nil {
				copy := event
				p.early = &copy
			}
		} else {
			changes = c.reply(p, event)
		}
	}
	c.mu.Unlock()
	c.emit(changes)
}

// ObserveGap interrupts recovery immediately, before persistence coalesces the
// gap. A local scheduling failure does not itself start an obsess incident.
func (c *Controller) ObserveGap(gap model.SchedulerGap) {
	if gap.MissedCount == 0 {
		return
	}
	c.mu.Lock()
	if t := c.targets[gap.TargetID]; t != nil && t.target.Config != nil {
		c.interrupt(t, c.clock.Now())
	}
	c.mu.Unlock()
}

// UpdateEndpoint starts a new baseline at the normal interval. Old in-flight
// probes are forgotten here but remain available to the persistence pipeline.
func (c *Controller) UpdateEndpoint(id int64, endpoint netip.Addr) {
	c.mu.Lock()
	t := c.targets[id]
	var changes []Transition
	if t != nil && t.target.Endpoint != endpoint {
		t.target.Endpoint = endpoint
		for p := t.head; p != nil; p = p.next {
			delete(c.probes, p.event.Key)
			if p.heapIndex >= 0 {
				heap.Remove(&c.deadlines, p.heapIndex)
			}
			c.tracked--
		}
		t.head, t.tail = nil, nil
		c.clearHistory(t)
		wasActive := t.active
		t.active, t.trackingLimited = false, false
		t.reason, t.since = "", time.Time{}
		t.baseline, t.baselineCount, t.threshold, t.hasThreshold = 0, 0, 0, false
		t.healthySince, t.healthyThrough, t.lastBad = time.Time{}, time.Time{}, time.Time{}
		if wasActive {
			changes = append(changes, c.changed(t, "endpoint_changed", c.clock.Now(), nil))
		}
		c.signal()
	}
	c.mu.Unlock()
	c.emit(changes)
}

func (c *Controller) Snapshot(id int64) (Status, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.targets[id]
	if !ok {
		return Status{}, false
	}
	if t.target.Config != nil {
		c.pruneHistory(t, c.clock.Now())
	}
	return c.status(t), true
}

func (c *Controller) reply(p *probe, event model.ProbeEvent) []Transition {
	var changes []Transition
	good := true
	p.event.RTT, p.event.ReplyClass, p.event.Responder = event.RTT, event.ReplyClass, event.Responder
	p.event.ICMPType, p.event.ICMPCode = event.ICMPType, event.ICMPCode
	p.event.ReplyAt = event.ReplyAt
	if p.event.ReplyAt.IsZero() {
		p.event.ReplyAt = c.clock.Now()
	}
	if event.ReplyClass == model.ReplyLate || event.RTT > p.event.Timeout {
		good = false
		changes = c.fault(p, "loss", p.deadline())
	} else {
		limit, known := p.threshold, p.hasThreshold
		if p.target.active {
			limit, known = p.target.threshold, p.target.hasThreshold
		}
		if known && float64(event.RTT) > limit {
			good = false
			changes = c.fault(p, "latency", p.event.ReplyAt)
		}
	}
	return append(changes, c.finish(p, good, float64(event.RTT))...)
}

func (c *Controller) expire(now time.Time) []Transition {
	var changes []Transition
	for len(c.deadlines) > 0 && !c.deadlines[0].deadline().After(now) {
		p := c.deadlines[0]
		changes = append(changes, c.fault(p, "loss", p.deadline())...)
		changes = append(changes, c.finish(p, false, 0)...)
	}
	return changes
}

func (c *Controller) fault(p *probe, reason string, at time.Time) []Transition {
	t := p.target
	first := !t.active
	if first {
		t.active = true
		t.reason, t.since = reason, at
		t.baseline, t.baselineCount = p.baseline, p.baselineCount
		t.threshold, t.hasThreshold = c.limit(t, t.baseline, t.baselineCount)
	}
	c.interrupt(t, at)
	if first {
		return []Transition{c.changed(t, reason, at, &p.event)}
	}
	return nil
}

func (c *Controller) interrupt(t *targetState, at time.Time) {
	if at.After(t.lastBad) {
		t.lastBad = at
	}
	if t.active {
		t.healthySince, t.healthyThrough = time.Time{}, time.Time{}
		c.clearHistory(t)
	}
}

func (c *Controller) finish(p *probe, good bool, rtt float64) []Transition {
	p.done, p.good, p.rtt, p.early = true, good, rtt, nil
	delete(c.probes, p.event.Key)
	if p.heapIndex >= 0 {
		heap.Remove(&c.deadlines, p.heapIndex)
		c.signal()
	}
	t := p.target
	var changes []Transition
	for t.head != nil && t.head.done {
		next := t.head
		t.head = next.next
		if t.head == nil {
			t.tail = nil
		} else {
			t.head.prev = nil
		}
		c.tracked--
		if !next.good {
			continue
		}
		if t.active {
			// A reply can have settled before an earlier probe's timeout
			// froze a different threshold. Reclassify that buffered result
			// against the incident's fixed limit before using it as evidence.
			if t.hasThreshold && next.rtt > t.threshold {
				changes = append(changes, c.fault(next, "latency", next.event.ReplyAt)...)
				continue
			}
			// Pre-event probes and probes from before a recovery interruption
			// cannot provide evidence that the network has recovered.
			if next.event.SentAt.Before(t.lastBad) {
				continue
			}
			// Admission can stall before a probe reaches BeginProbe without
			// filling the scheduler queue. Require observed send continuity,
			// allowing up to twice the fast cadence for ordinary jitter.
			if !t.healthyThrough.IsZero() {
				gap := next.event.SentAt.Sub(t.healthyThrough)
				cadence := t.target.Config.Interval.Value()
				if gap > cadence && gap-cadence > cadence {
					c.interrupt(t, next.event.SentAt)
				}
			}
			if t.healthySince.IsZero() {
				t.healthySince = next.event.SentAt
			}
			t.healthyThrough = next.event.SentAt
			c.addSample(t, sample{sentAt: next.event.SentAt, rtt: next.rtt})
			if t.healthyThrough.Sub(t.healthySince) >= t.target.Config.RecoverAfter.Value() {
				t.active, t.trackingLimited = false, false
				t.reason, t.since = "", time.Time{}
				t.healthySince, t.healthyThrough = time.Time{}, time.Time{}
				changes = append(changes, c.changed(t, "recovered", c.clock.Now(), nil))
			}
		} else {
			c.addSample(t, sample{sentAt: next.event.SentAt, rtt: next.rtt})
		}
	}
	return changes
}

func (c *Controller) addSample(t *targetState, value sample) {
	c.pruneHistory(t, c.clock.Now())
	if value.sentAt.Before(c.clock.Now().Add(-t.target.Config.BaselineWindow.Value())) || !c.makeRoom() {
		return
	}
	if t.historyHead > 0 && len(t.history) == cap(t.history) {
		copy(t.history, t.history[t.historyHead:])
		t.history = t.history[:len(t.history)-t.historyHead]
		t.historyHead = 0
	}
	t.history = append(t.history, value)
	c.samples++
}

func (c *Controller) pruneHistory(t *targetState, now time.Time) {
	cutoff := now.Add(-t.target.Config.BaselineWindow.Value())
	for t.historyHead < len(t.history) && t.history[t.historyHead].sentAt.Before(cutoff) {
		t.historyHead++
		c.samples--
	}
	if t.historyHead == len(t.history) {
		t.history, t.historyHead = nil, 0
	}
}

func (c *Controller) clearHistory(t *targetState) {
	c.samples -= len(t.history) - t.historyHead
	t.history, t.historyHead = nil, 0
}

func (c *Controller) average(t *targetState) (float64, int) {
	values := t.history[t.historyHead:]
	var mean float64
	for i, value := range values {
		// An incremental mean avoids overflowing a duration sum.
		mean += (value.rtt - mean) / float64(i+1)
	}
	return mean, len(values)
}

func (c *Controller) limit(t *targetState, mean float64, count int) (float64, bool) {
	if t.absolute > 0 {
		return float64(t.absolute), true
	}
	if count < t.target.Config.MinSamples {
		return 0, false
	}
	threshold := mean * (1 + t.percent/100)
	if math.IsInf(threshold, 0) || threshold > float64(math.MaxInt64) {
		threshold = float64(math.MaxInt64)
	}
	return threshold, true
}

// makeRoom preferentially evicts history, preserving every tracked probe and
// the incident's frozen threshold. Configuration normally prevents this guard
// from being reached, even when queued socket writes are released in a burst.
func (c *Controller) makeRoom() bool {
	if c.tracked+c.samples < maxTrackedSamples {
		return true
	}
	for _, t := range c.targets {
		if t.historyHead < len(t.history) {
			t.historyHead++
			c.samples--
			if t.historyHead == len(t.history) {
				t.history, t.historyHead = nil, 0
			}
			t.trackingLimited = true
			c.interrupt(t, c.clock.Now())
			return true
		}
	}
	return false
}

func (c *Controller) status(t *targetState) Status {
	s := Status{State: "disabled", IntervalMS: milliseconds(t.target.Interval)}
	if t.target.Config == nil {
		return s
	}
	s.Enabled, s.Monitoring = true, c.running
	s.State = "normal"
	s.RecoverAfterMS = milliseconds(t.target.Config.RecoverAfter.Value())
	s.TrackingLimited = t.trackingLimited
	baseline, count := c.average(t)
	threshold, known := c.limit(t, baseline, count)
	if t.active {
		s.State, s.Reason = "obsessing", t.reason
		s.IntervalMS = milliseconds(t.target.Config.Interval.Value())
		baseline, count = t.baseline, t.baselineCount
		threshold, known = t.threshold, t.hasThreshold
		since := t.since.UnixMilli()
		s.SinceMS = &since
		if !t.healthySince.IsZero() {
			s.HealthyForMS = milliseconds(t.healthyThrough.Sub(t.healthySince))
		}
	} else if !known {
		s.State = "warming"
	}
	s.BaselineSamples = count
	if count > 0 {
		value := baseline / float64(time.Millisecond)
		s.BaselineMS = &value
	}
	if known {
		value := threshold / float64(time.Millisecond)
		s.ThresholdMS = &value
	}
	return s
}

func (c *Controller) changed(t *targetState, reason string, at time.Time, trigger *model.ProbeEvent) Transition {
	status := c.status(t)
	if c.change != nil {
		interval := t.target.Interval
		if t.active {
			interval = t.target.Config.Interval.Value()
		}
		c.change(t.target.ID, interval)
	}
	result := Transition{TargetID: t.target.ID, Status: status, Endpoint: t.target.Endpoint, Reason: reason, At: at}
	if trigger != nil {
		copy := *trigger
		result.Trigger = &copy
		result.Endpoint = copy.Endpoint
	}
	return result
}

func (c *Controller) emit(changes []Transition) {
	if c.transition != nil {
		for _, change := range changes {
			c.transition(change)
		}
	}
}

func (c *Controller) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func sameResponder(endpoint, responder netip.Addr) bool {
	return endpoint.Unmap().WithZone("") == responder.Unmap().WithZone("")
}

func milliseconds(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}
