package obsess

import (
	"context"
	"math"
	"net/netip"
	"sync"
	"testing"
	"time"

	clockpkg "github.com/benbjohnson/clock"

	"github.com/opshed/flameping/internal/config"
	"github.com/opshed/flameping/internal/model"
)

type fixture struct {
	t          *testing.T
	clock      *clockpkg.Mock
	c          *Controller
	start      time.Time
	endpoint   netip.Addr
	sequence   uint64
	intervalMu sync.Mutex
	intervals  []time.Duration
}

func newFixture(t *testing.T, threshold string) *fixture {
	t.Helper()
	f := &fixture{t: t, clock: clockpkg.NewMock(), start: time.Unix(1700000000, 0), endpoint: netip.MustParseAddr("192.0.2.1")}
	f.clock.Set(f.start)
	policy := &config.ObsessConfig{Interval: config.Duration(100 * time.Millisecond), LatencyThreshold: threshold, BaselineWindow: config.Duration(time.Minute), MinSamples: 3, RecoverAfter: config.Duration(time.Minute)}
	var err error
	f.c, err = New(f.clock, []Target{{ID: 1, Endpoint: f.endpoint, Interval: 5 * time.Second, Config: policy}}, func(_ int64, interval time.Duration) {
		f.intervalMu.Lock()
		f.intervals = append(f.intervals, interval)
		f.intervalMu.Unlock()
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fixture) move(at time.Time) {
	if at.After(f.clock.Now()) {
		f.clock.Set(at)
	}
}

func (f *fixture) begin(at time.Time, timeout time.Duration) model.ProbeEvent {
	f.move(at)
	f.sequence++
	p := model.ProbeEvent{Key: model.ProbeKey{Sequence: f.sequence}, TargetID: 1, Endpoint: f.endpoint, SentAt: at, Timeout: timeout}
	f.c.BeginProbe(p)
	return p
}

func (f *fixture) sent(at time.Time, timeout time.Duration) model.ProbeEvent {
	p := f.begin(at, timeout)
	f.c.CompleteProbe(p.Key, true)
	return p
}

func (f *fixture) reply(p model.ProbeEvent, rtt time.Duration) {
	event := p
	event.ReplyAt, event.RTT, event.Responder = p.SentAt.Add(rtt), rtt, p.Endpoint
	event.ReplyClass = model.ReplyOnTime
	if rtt > p.Timeout {
		event.ReplyClass = model.ReplyLate
	}
	f.move(event.ReplyAt)
	f.c.ObserveReply(event)
}

func (f *fixture) success(at time.Time, rtt time.Duration) model.ProbeEvent {
	p := f.sent(at, time.Second)
	f.reply(p, rtt)
	return p
}

func (f *fixture) expire(at time.Time) {
	f.move(at)
	f.c.mu.Lock()
	changes := f.c.expire(f.clock.Now())
	f.c.mu.Unlock()
	f.c.emit(changes)
}

func (f *fixture) status() Status {
	f.t.Helper()
	status, ok := f.c.Snapshot(1)
	if !ok {
		f.t.Fatal("target missing")
	}
	return status
}

func (f *fixture) warm() {
	for i := 0; i < 3; i++ {
		f.success(f.start.Add(time.Duration(i)*5*time.Second), 10*time.Millisecond)
	}
}

func (f *fixture) obsess() time.Time {
	f.warm()
	f.success(f.start.Add(15*time.Second), 40*time.Millisecond)
	if state := f.status().State; state != "obsessing" {
		f.t.Fatalf("state=%s, want obsessing", state)
	}
	return f.start.Add(16 * time.Second)
}

func requireMS(t *testing.T, got *float64, want float64) {
	t.Helper()
	if got == nil || math.Abs(*got-want) > 0.000001 {
		t.Fatalf("milliseconds=%v, want %g", got, want)
	}
}

func TestRelativeThresholdWarmupAndStrictComparison(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rtt   time.Duration
		state string
	}{{"equal", 15 * time.Millisecond, "normal"}, {"above", 15*time.Millisecond + time.Nanosecond, "obsessing"}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, "50%")
			if f.status().State != "warming" {
				t.Fatal("relative target did not start warming")
			}
			f.warm()
			status := f.status()
			if status.State != "normal" || status.BaselineSamples != 3 {
				t.Fatalf("warm status=%+v", status)
			}
			requireMS(t, status.BaselineMS, 10)
			requireMS(t, status.ThresholdMS, 15)
			f.success(f.start.Add(15*time.Second), tc.rtt)
			if state := f.status().State; state != tc.state {
				t.Fatalf("state=%s, want %s", state, tc.state)
			}
		})
	}
}

func TestAbsoluteThresholdWorksBeforeWarmup(t *testing.T) {
	f := newFixture(t, "100ms")
	if status := f.status(); status.State != "normal" || status.BaselineSamples != 0 {
		t.Fatalf("absolute initial status=%+v", status)
	}
	f.success(f.start, 100*time.Millisecond)
	if f.status().State != "normal" {
		t.Fatal("threshold equality triggered")
	}
	f.success(f.start.Add(5*time.Second), 101*time.Millisecond)
	status := f.status()
	if status.State != "obsessing" || status.Reason != "latency" {
		t.Fatalf("absolute failure status=%+v", status)
	}
	requireMS(t, status.ThresholdMS, 100)
}

func TestFrozenBaselineRejectsDegradationThenSeedsHealthyRecovery(t *testing.T) {
	f := newFixture(t, "50%")
	start := f.obsess()
	for i := 0; i < 700; i++ {
		f.success(start.Add(time.Duration(i)*100*time.Millisecond), 20*time.Millisecond)
	}
	status := f.status()
	if status.State != "obsessing" || status.HealthyForMS != 0 {
		t.Fatalf("degradation became normal: %+v", status)
	}
	requireMS(t, status.BaselineMS, 10)
	requireMS(t, status.ThresholdMS, 15)
	start = start.Add(70 * time.Second)
	for i := 0; i < 600; i++ {
		f.success(start.Add(time.Duration(i)*100*time.Millisecond), 10*time.Millisecond)
	}
	if f.status().State != "obsessing" {
		t.Fatal("recovered before a full minute")
	}
	f.success(start.Add(time.Minute), 10*time.Millisecond)
	status = f.status()
	if status.State != "normal" || status.IntervalMS != 5000 || status.BaselineSamples < 3 {
		t.Fatalf("recovery status=%+v", status)
	}
	requireMS(t, status.BaselineMS, 10)
	requireMS(t, status.ThresholdMS, 15)
	if len(f.intervals) != 2 || f.intervals[0] != 100*time.Millisecond || f.intervals[1] != 5*time.Second {
		t.Fatalf("interval transitions=%v", f.intervals)
	}
	if f.c.tracked != 0 || len(f.c.probes) != 0 || len(f.c.deadlines) != 0 {
		t.Fatal("completed probes retained")
	}
}

func TestUnknownBaselineLossCanRecoverFromOnTimeReplies(t *testing.T) {
	f := newFixture(t, "50%")
	f.sent(f.start, time.Second)
	f.expire(f.start.Add(time.Second))
	status := f.status()
	if status.State != "obsessing" || status.Reason != "loss" || status.BaselineMS != nil || status.ThresholdMS != nil {
		t.Fatalf("startup loss=%+v", status)
	}
	start := f.start.Add(2 * time.Second)
	for i := 0; i <= 600; i++ {
		f.success(start.Add(time.Duration(i)*100*time.Millisecond), 90*time.Millisecond)
	}
	if f.status().State != "normal" {
		t.Fatal("unknown baseline prevented recovery")
	}
	requireMS(t, f.status().BaselineMS, 90)
}

func TestLateReplyTriggersBeforeTimerAndDuplicatesDoNotExtendIncident(t *testing.T) {
	f := newFixture(t, "50%")
	p := f.sent(f.start, time.Second)
	f.reply(p, 2*time.Second)
	status := f.status()
	if status.State != "obsessing" || status.Reason != "loss" {
		t.Fatalf("late reply=%+v", status)
	}
	since := *status.SinceMS
	f.success(f.start.Add(3*time.Second), 10*time.Millisecond)
	f.success(f.start.Add(3100*time.Millisecond), 10*time.Millisecond)
	f.reply(p, 4*time.Second)
	status = f.status()
	if *status.SinceMS != since || status.HealthyForMS != 100 || len(f.intervals) != 1 {
		t.Fatalf("duplicate changed incident=%+v", status)
	}
	if len(f.c.probes) != 0 || len(f.c.deadlines) != 0 {
		t.Fatal("late reply retained deadline")
	}
}

func TestWritingProbeWaitsForSuccessfulCompletion(t *testing.T) {
	t.Run("early reply followed by failed write", func(t *testing.T) {
		f := newFixture(t, "1ms")
		p := f.begin(f.start, time.Second)
		f.reply(p, 10*time.Millisecond)
		if f.status().State != "normal" {
			t.Fatal("reply was processed before send completed")
		}
		f.c.CompleteProbe(p.Key, false)
		if f.status().State != "normal" || f.c.tracked != 0 {
			t.Fatal("failed write became network latency/loss")
		}
	})
	t.Run("early reply followed by successful write", func(t *testing.T) {
		f := newFixture(t, "1ms")
		p := f.begin(f.start, time.Second)
		f.reply(p, 10*time.Millisecond)
		f.c.CompleteProbe(p.Key, true)
		if f.status().State != "obsessing" {
			t.Fatal("successful early reply was lost")
		}
	})
	t.Run("writing probe cannot expire", func(t *testing.T) {
		f := newFixture(t, "50%")
		p := f.begin(f.start, time.Second)
		f.expire(f.start.Add(10 * time.Second))
		if f.status().State != "warming" || len(f.c.deadlines) != 0 {
			t.Fatal("writing probe expired before write succeeded")
		}
		f.c.CompleteProbe(p.Key, true)
		f.expire(f.clock.Now())
		if f.status().State != "obsessing" {
			t.Fatal("successful overdue send did not expire")
		}
	})
}

func TestRecoveryCannotBridgeSilence(t *testing.T) {
	f := newFixture(t, "50%")
	start := f.obsess()
	f.success(start, 10*time.Millisecond)
	f.expire(start.Add(2 * time.Minute))
	if f.status().State != "obsessing" {
		t.Fatal("timer alone recovered")
	}
	f.success(start.Add(2*time.Minute), 10*time.Millisecond)
	if status := f.status(); status.State != "obsessing" || status.HealthyForMS != 0 {
		t.Fatalf("sparse samples bridged silence: %+v", status)
	}
	f.success(start.Add(2*time.Minute+200*time.Millisecond), 10*time.Millisecond)
	if f.status().HealthyForMS != 200 {
		t.Fatal("twice-cadence jitter allowance was not inclusive")
	}
	f.success(start.Add(2*time.Minute+400*time.Millisecond+time.Nanosecond), 10*time.Millisecond)
	if f.status().HealthyForMS != 0 {
		t.Fatal("gap above twice cadence did not restart recovery")
	}
}

func TestRecoveryRequiresEveryEarlierProbeToSettle(t *testing.T) {
	f := newFixture(t, "50%")
	start := f.obsess()
	for i := 0; i < 600; i++ {
		f.success(start.Add(time.Duration(i)*100*time.Millisecond), 10*time.Millisecond)
	}
	missing := f.sent(start.Add(time.Minute), time.Second)
	f.success(start.Add(time.Minute+100*time.Millisecond), 10*time.Millisecond)
	if status := f.status(); status.State != "obsessing" || status.HealthyForMS != 59900 {
		t.Fatalf("later reply hid unresolved earlier probe: %+v", status)
	}
	f.expire(missing.SentAt.Add(missing.Timeout))
	if status := f.status(); status.State != "obsessing" || status.HealthyForMS != 0 {
		t.Fatalf("older loss did not invalidate recovery: %+v", status)
	}
	if f.c.tracked != 0 {
		t.Fatal("settled FIFO backlog retained")
	}
}

func TestBufferedSuccessMustMeetNewlyFrozenThreshold(t *testing.T) {
	f := newFixture(t, "50%")
	f.success(f.start, 0)
	for i := 0; i < 3; i++ {
		f.success(f.start.Add(58*time.Second+time.Duration(i)*100*time.Millisecond), 10*time.Millisecond)
	}
	// This probe captures a 7.5ms baseline while the oldest zero RTT sample
	// is still in the window. Its eventual loss must freeze an 11.25ms limit.
	f.sent(f.start.Add(59500*time.Millisecond), time.Second)
	// The zero sample ages out before this probe starts. Its 14ms reply is
	// acceptable under the new normal 15ms limit and waits in the FIFO.
	f.success(f.start.Add(60600*time.Millisecond), 14*time.Millisecond)
	if f.status().State != "normal" {
		t.Fatal("later probe was not initially a normal success")
	}
	f.expire(f.clock.Now())
	status := f.status()
	requireMS(t, status.ThresholdMS, 11.25)
	if status.State != "obsessing" || status.HealthyForMS != 0 || f.c.samples != 0 {
		t.Fatalf("buffered result bypassed frozen threshold: %+v", status)
	}
	f.success(f.start.Add(60700*time.Millisecond), 10*time.Millisecond)
	if f.status().HealthyForMS != 0 {
		t.Fatal("recovery counted the earlier degraded buffered reply")
	}
}

func TestRecoveryCanFinishWithNewerProbesStillInFlight(t *testing.T) {
	f := newFixture(t, "500ms")
	f.c.targets[1].target.Config.RecoverAfter = config.Duration(time.Second)
	f.success(f.start, 600*time.Millisecond)
	start := f.start.Add(time.Second)
	var probes []model.ProbeEvent
	for i := 0; i <= 12; i++ {
		probes = append(probes, f.sent(start.Add(time.Duration(i)*100*time.Millisecond), time.Second))
		if i >= 2 {
			f.reply(probes[i-2], 200*time.Millisecond)
		}
		if i < 12 && f.status().State != "obsessing" {
			t.Fatalf("pipeline recovered early at probe %d", i)
		}
	}
	if status := f.status(); status.State != "normal" || len(f.c.probes) != 2 || f.c.tracked != 2 {
		t.Fatalf("newer in-flight probes prevented recovery: status=%+v pending=%d", status, len(f.c.probes))
	}
}

func TestGapAndSendErrorInterruptRecoveryWithoutTriggeringNormalTarget(t *testing.T) {
	f := newFixture(t, "50%")
	f.c.ObserveGap(model.SchedulerGap{TargetID: 1, MissedCount: 1})
	p := f.begin(f.start, time.Second)
	f.c.CompleteProbe(p.Key, false)
	if f.status().State != "warming" {
		t.Fatal("local failure started obsessing")
	}
	start := f.obsess()
	f.success(start, 10*time.Millisecond)
	f.success(start.Add(100*time.Millisecond), 10*time.Millisecond)
	f.c.ObserveGap(model.SchedulerGap{TargetID: 1, FirstScheduled: start.Add(200 * time.Millisecond), Interval: 100 * time.Millisecond, MissedCount: 1})
	if f.status().HealthyForMS != 0 {
		t.Fatal("gap did not reset healthy progress")
	}
	f.success(start.Add(300*time.Millisecond), 10*time.Millisecond)
	f.success(start.Add(400*time.Millisecond), 10*time.Millisecond)
	p = f.begin(start.Add(500*time.Millisecond), time.Second)
	f.c.CompleteProbe(p.Key, false)
	if status := f.status(); status.State != "obsessing" || status.HealthyForMS != 0 {
		t.Fatalf("send error did not reset recovery=%+v", status)
	}
}

func TestProbeCapturesPreEventBaselineBeforeOverlappingResults(t *testing.T) {
	f := newFixture(t, "50%")
	f.warm()
	bad := f.sent(f.start.Add(15*time.Second), 2*time.Minute)
	later := f.sent(f.start.Add(16*time.Second), 2*time.Minute)
	f.reply(later, 14*time.Millisecond)
	// Even after the rolling history ages out, the event uses the mean that
	// preceded its send, never a later observation or an empty current window.
	f.move(f.start.Add(90 * time.Second))
	if f.status().BaselineSamples != 0 {
		t.Fatal("old history did not age out")
	}
	f.reply(bad, 80*time.Second)
	status := f.status()
	if status.State != "obsessing" || status.BaselineSamples != 3 || status.HealthyForMS != 0 {
		t.Fatalf("overlapping event baseline=%+v", status)
	}
	requireMS(t, status.BaselineMS, 10)
	requireMS(t, status.ThresholdMS, 15)
}

func TestSourceMismatchCannotHideLossAndIPv6ZonesMatch(t *testing.T) {
	f := newFixture(t, "50%")
	p := f.sent(f.start, time.Second)
	wrong := p
	wrong.Responder, wrong.ReplyAt, wrong.RTT = netip.MustParseAddr("192.0.2.2"), f.start.Add(time.Millisecond), time.Millisecond
	f.c.ObserveReply(wrong)
	f.expire(f.start.Add(time.Second))
	if f.status().Reason != "loss" {
		t.Fatal("mismatched source was accepted")
	}
	f.endpoint = netip.MustParseAddr("fe80::1%eth0")
	f.c.UpdateEndpoint(1, f.endpoint)
	p = f.sent(f.start.Add(2*time.Second), time.Second)
	p.Responder, p.ReplyAt, p.RTT = netip.MustParseAddr("fe80::1"), p.SentAt.Add(time.Millisecond), time.Millisecond
	f.c.ObserveReply(p)
	if status := f.status(); status.State != "warming" || status.BaselineSamples != 1 {
		t.Fatalf("IPv6 zone normalization=%+v", status)
	}
}

func TestEndpointChangeClearsIncidentAndRejectsOldQueuedProbes(t *testing.T) {
	f := newFixture(t, "50%")
	start := f.obsess()
	old := f.sent(start, time.Second)
	newEndpoint := netip.MustParseAddr("192.0.2.2")
	f.c.UpdateEndpoint(1, newEndpoint)
	f.reply(old, 10*time.Millisecond)
	f.sent(start.Add(100*time.Millisecond), time.Second) // Still the old endpoint.
	f.expire(start.Add(2 * time.Second))
	status := f.status()
	if status.State != "warming" || status.BaselineSamples != 0 || status.IntervalMS != 5000 || f.c.tracked != 0 {
		t.Fatalf("old endpoint contaminated new state=%+v", status)
	}
	f.endpoint = newEndpoint
	f.success(start.Add(3*time.Second), 20*time.Millisecond)
	if f.status().BaselineSamples != 1 {
		t.Fatal("new endpoint probe was ignored")
	}
}

func TestControllerDeadlineTimerAndMonitoringLifecycle(t *testing.T) {
	f := newFixture(t, "50%")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	eventually(t, func() bool { return f.status().Monitoring })
	f.sent(f.start, time.Second)
	f.clock.Add(time.Second)
	eventually(t, func() bool { return f.status().State == "obsessing" })
	cancel()
	eventually(t, func() bool { return !f.status().Monitoring })
}

func TestTrackingGuardBoundsWritingAndSettledProbes(t *testing.T) {
	f := newFixture(t, "50%")
	for i := 0; i <= maxTrackedSamples; i++ {
		f.begin(f.start, time.Second)
	}
	if f.c.tracked != maxTrackedSamples || len(f.c.probes) != maxTrackedSamples || !f.status().TrackingLimited {
		t.Fatalf("tracking guard: tracked=%d probes=%d status=%+v", f.c.tracked, len(f.c.probes), f.status())
	}
	f.c.UpdateEndpoint(1, netip.MustParseAddr("192.0.2.2"))
	if f.c.tracked != 0 || len(f.c.probes) != 0 || f.status().TrackingLimited {
		t.Fatal("endpoint reset did not release tracking guard state")
	}
}

func TestTrackingGuardReportsHistoryEvictionAndInterruptsRecovery(t *testing.T) {
	f := newFixture(t, "50%")
	f.c.targets[1].target.Config.RecoverAfter = config.Duration(time.Second)
	start := f.obsess()
	f.success(start, 10*time.Millisecond)
	f.success(start.Add(100*time.Millisecond), 10*time.Millisecond)
	for i := 0; i < maxTrackedSamples; i++ {
		f.begin(start.Add(200*time.Millisecond), time.Second)
	}
	if !f.status().TrackingLimited || f.status().HealthyForMS != 0 || f.c.tracked+f.c.samples > maxTrackedSamples {
		t.Fatalf("history eviction was invisible or retained progress: %+v", f.status())
	}
	// A probe admitted after the hard cap cannot be tracked. Its potentially
	// unanswered lifetime remains an observation gap after capacity frees.
	f.begin(start.Add(200*time.Millisecond), 3*time.Second)
	for key := range f.c.probes {
		f.c.CompleteProbe(key, false)
	}
	for i := 0; i <= 10; i++ {
		f.success(start.Add(400*time.Millisecond+time.Duration(i)*100*time.Millisecond), 10*time.Millisecond)
	}
	if status := f.status(); status.State != "obsessing" || status.HealthyForMS != 0 || !status.TrackingLimited {
		t.Fatalf("recovered before an untracked probe's deadline: %+v", status)
	}
	for i := 0; i <= 10; i++ {
		f.success(start.Add(4*time.Second+time.Duration(i)*100*time.Millisecond), 10*time.Millisecond)
	}
	if status := f.status(); status.State != "normal" || status.TrackingLimited {
		t.Fatalf("fresh complete recovery did not clear capacity incident: %+v", status)
	}
}

func TestDisabledTargetHasNoControllerState(t *testing.T) {
	c, err := New(nil, []Target{{ID: 1, Interval: time.Second}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.BeginProbe(model.ProbeEvent{TargetID: 1, Key: model.ProbeKey{Sequence: 1}, Endpoint: netip.MustParseAddr("192.0.2.1"), Timeout: time.Second})
	status, ok := c.Snapshot(1)
	if !ok || status.Enabled || status.State != "disabled" || c.tracked != 0 {
		t.Fatalf("disabled target=%+v", status)
	}
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
