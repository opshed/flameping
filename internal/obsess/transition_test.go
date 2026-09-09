package obsess

import (
	"net/netip"
	"reflect"
	"testing"
	"time"

	"flameping/internal/config"
	"flameping/internal/model"
)

func transitionProbe(f *fixture, at time.Time) model.ProbeEvent {
	f.move(at)
	f.sequence++
	probe := model.ProbeEvent{
		Key:      model.ProbeKey{RunID: model.RunID{1, 2, 3, 4}, Sequence: f.sequence},
		TargetID: 1, EndpointID: 17, Endpoint: f.endpoint,
		ScheduledAt: at.Add(-5 * time.Millisecond), SentAt: at, Timeout: time.Second,
	}
	f.c.BeginProbe(probe)
	f.c.CompleteProbe(probe.Key, true)
	return probe
}

func transitionRawReply(probe model.ProbeEvent, rtt time.Duration) model.ProbeEvent {
	class := model.ReplyOnTime
	if rtt > probe.Timeout {
		class = model.ReplyLate
	}
	// Echo receive events have no target or endpoint identity. Their send time
	// is reconstructed from the payload; retain the original send's metadata.
	return model.ProbeEvent{
		Key: probe.Key, SentAt: probe.SentAt.Add(time.Nanosecond), Timeout: probe.Timeout,
		ReplyAt: probe.SentAt.Add(rtt), RTT: rtt, ReplyClass: class,
		Responder: probe.Endpoint, ICMPType: 0, ICMPCode: 0,
	}
}

func TestTransitionCapturesTimeoutLateAndLatencyEvidenceOnce(t *testing.T) {
	for _, test := range []struct {
		name, threshold, reason string
		warm                    bool
		rtt                     time.Duration
	}{
		{name: "deadline without baseline", threshold: "50%", reason: "loss"},
		{name: "late reply before deadline worker", threshold: "50%", reason: "loss", rtt: 1250 * time.Millisecond},
		{name: "relative spike", threshold: "50%", reason: "latency", warm: true, rtt: 40 * time.Millisecond},
		{name: "absolute spike before warmup", threshold: "100ms", reason: "latency", rtt: 150 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, test.threshold)
			var changes []Transition
			f.c.transition = func(change Transition) { changes = append(changes, change) }
			if test.warm {
				f.warm()
			}
			probe := transitionProbe(f, f.start.Add(15*time.Second))
			raw := transitionRawReply(probe, test.rtt)
			wantAt := probe.SentAt.Add(probe.Timeout)
			if test.rtt == 0 {
				f.expire(wantAt)
			} else {
				f.move(raw.ReplyAt)
				f.c.ObserveReply(raw)
				if test.reason == "latency" {
					wantAt = raw.ReplyAt
				}
			}
			if len(changes) != 1 {
				t.Fatalf("entry transitions=%d, want one", len(changes))
			}
			change := changes[0]
			if change.TargetID != 1 || change.Endpoint != probe.Endpoint || change.Reason != test.reason || !change.At.Equal(wantAt) || change.Status.State != "obsessing" || change.Status.IntervalMS != 100 || change.Status.Reason != test.reason {
				t.Fatalf("entry transition=%+v", change)
			}
			if change.Trigger == nil {
				t.Fatal("entry has no trigger evidence")
			}
			got := *change.Trigger
			if got.Key != probe.Key || got.TargetID != probe.TargetID || got.EndpointID != probe.EndpointID || got.Endpoint != probe.Endpoint || !got.ScheduledAt.Equal(probe.ScheduledAt) || !got.SentAt.Equal(probe.SentAt) || got.Timeout != probe.Timeout {
				t.Fatalf("original send metadata was lost: got=%+v want=%+v", got, probe)
			}
			if test.rtt == 0 {
				if !got.ReplyAt.IsZero() || got.RTT != 0 || got.ReplyClass != "" || got.Responder.IsValid() || change.Status.ThresholdMS != nil {
					t.Fatalf("timeout invented reply or baseline evidence: %+v", change)
				}
			} else if got.RTT != raw.RTT || got.ReplyClass != raw.ReplyClass || got.Responder != raw.Responder || !got.ReplyAt.Equal(raw.ReplyAt) {
				t.Fatalf("reply evidence not merged: got=%+v raw=%+v", got, raw)
			}
			if test.warm {
				requireMS(t, change.Status.BaselineMS, 10)
				requireMS(t, change.Status.ThresholdMS, 15)
			} else if test.reason == "latency" {
				requireMS(t, change.Status.ThresholdMS, 100)
				if change.Status.BaselineMS != nil {
					t.Fatal("absolute startup spike invented a baseline")
				}
			}

			f.c.ObserveReply(transitionRawReply(probe, 2*time.Second))
			f.expire(f.start.Add(19 * time.Second))
			f.success(f.start.Add(20*time.Second), 900*time.Millisecond)
			f.c.ObserveGap(model.SchedulerGap{TargetID: 1, MissedCount: 1})
			if len(changes) != 1 {
				t.Fatalf("later faults or duplicates emitted another entry: %+v", changes)
			}
			if !reflect.DeepEqual(*changes[0].Trigger, got) {
				t.Fatal("later faults changed the captured trigger")
			}
		})
	}
}

func TestTransitionSnapshotSurvivesDNSResetBeforeCallback(t *testing.T) {
	f := newFixture(t, "50%")
	f.warm()
	probe := transitionProbe(f, f.start.Add(15*time.Second))
	retainedProbe := f.c.probes[probe.Key]
	var emitted []Transition
	f.c.transition = func(change Transition) {
		// Callback consumers are allowed to read the current state, but it may
		// already belong to a different endpoint than the captured transition.
		if _, ok := f.c.Snapshot(change.TargetID); !ok {
			t.Fatal("callback could not read current state")
		}
		emitted = append(emitted, change)
	}
	f.move(probe.SentAt.Add(probe.Timeout))
	f.c.mu.Lock()
	pending := f.c.expire(f.clock.Now())
	f.c.mu.Unlock()
	if len(pending) != 1 {
		t.Fatalf("pending transitions=%d", len(pending))
	}
	newEndpoint := netip.MustParseAddr("192.0.2.2")
	f.c.UpdateEndpoint(1, newEndpoint)
	retainedProbe.event.Endpoint = newEndpoint
	retainedProbe.event.RTT = 99 * time.Second
	retainedProbe.event.TargetID = 999
	f.c.emit(pending)
	if len(emitted) != 2 {
		t.Fatalf("transitions=%+v", emitted)
	}
	var entered, reset *Transition
	for i := range emitted {
		switch emitted[i].Reason {
		case "loss":
			entered = &emitted[i]
		case "endpoint_changed":
			reset = &emitted[i]
		}
	}
	if entered == nil || entered.Endpoint != probe.Endpoint || entered.Trigger == nil || !reflect.DeepEqual(*entered.Trigger, probe) {
		t.Fatalf("deferred entry snapshot changed: %+v", entered)
	}
	requireMS(t, entered.Status.BaselineMS, 10)
	requireMS(t, entered.Status.ThresholdMS, 15)
	if reset == nil || reset.Endpoint != newEndpoint || reset.Trigger != nil || reset.Status.Reason != "" || reset.Status.SinceMS != nil || reset.Status.State != "warming" || reset.Status.BaselineMS != nil || reset.Status.ThresholdMS != nil {
		t.Fatalf("DNS reset retained old incident evidence: %+v", reset)
	}
	if *entered.Status.SinceMS != probe.SentAt.Add(probe.Timeout).UnixMilli() {
		t.Fatal("DNS reset changed the entry timestamp")
	}
}

func TestRecoveryTransitionHasExplicitCauseAndNoOldTrigger(t *testing.T) {
	f := newFixture(t, "50%")
	f.c.targets[1].target.Config.RecoverAfter = config.Duration(300 * time.Millisecond)
	var changes []Transition
	f.c.transition = func(change Transition) { changes = append(changes, change) }
	start := f.obsess()
	for i := 0; i <= 3; i++ {
		f.success(start.Add(time.Duration(i)*100*time.Millisecond), 7*time.Millisecond)
	}
	if len(changes) != 2 {
		t.Fatalf("transitions=%+v", changes)
	}
	entry, exit := changes[0], changes[1]
	if exit.Reason != "recovered" || exit.Trigger != nil || exit.Status.State != "normal" || exit.Status.IntervalMS != 5000 || exit.Status.Reason != "" || exit.Status.SinceMS != nil || !exit.At.Equal(f.clock.Now()) {
		t.Fatalf("recovery transition retained stale cause: %+v", exit)
	}
	requireMS(t, entry.Status.BaselineMS, 10)
	requireMS(t, entry.Status.ThresholdMS, 15)
	requireMS(t, exit.Status.BaselineMS, 7)
	requireMS(t, exit.Status.ThresholdMS, 10.5)
	if entry.Trigger == nil || entry.Trigger.RTT != 40*time.Millisecond {
		t.Fatalf("recovery changed entering spike: %+v", entry.Trigger)
	}
}
