package app

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/opshed/flameping/internal/config"
	"github.com/opshed/flameping/internal/model"
	"github.com/opshed/flameping/internal/obsess"
	"github.com/opshed/flameping/internal/store/sqlite"
)

func obsessLogFixture(threshold string) (sqlite.Target, *config.ObsessConfig, obsess.Transition) {
	target := sqlite.Target{ID: 73, StableID: "wan-check", DisplayName: "WAN uplink", ConfiguredAddress: "example.test", Interval: 5 * time.Second, Timeout: time.Second}
	policy := &config.ObsessConfig{Interval: config.Duration(100 * time.Millisecond), LatencyThreshold: threshold, BaselineWindow: config.Duration(time.Minute), MinSamples: 3, RecoverAfter: config.Duration(time.Minute)}
	sentAt := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	endpoint := netip.MustParseAddr("192.0.2.1")
	change := obsess.Transition{
		TargetID: target.ID, Endpoint: endpoint, Reason: "loss", At: sentAt.Add(time.Second),
		Status:  obsess.Status{Enabled: true, Monitoring: true, State: "obsessing", Reason: "loss", IntervalMS: 100, RecoverAfterMS: 60000},
		Trigger: &model.ProbeEvent{Key: model.ProbeKey{RunID: model.RunID{1, 2}, Sequence: 42}, TargetID: target.ID, Endpoint: endpoint, ScheduledAt: sentAt, SentAt: sentAt, Timeout: time.Second},
	}
	return target, policy, change
}

func obsessJSONLog(t *testing.T, target sqlite.Target, policy *config.ObsessConfig, change obsess.Transition) map[string]any {
	t.Helper()
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	logObsessTransition(logger, target, policy, change)
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatalf("invalid JSON log: %s: %v", output.String(), err)
	}
	for key, value := range record {
		if value == nil {
			t.Errorf("log emitted null field %q", key)
		}
	}
	if record["target_id"] != "wan-check" || record["target_name"] != "WAN uplink" || record["target_address"] != "example.test" || record["target_ip"] != change.Endpoint.String() {
		t.Fatalf("target identity is not human-readable or loses endpoint: %+v", record)
	}
	return record
}

func requireAbsentObsessLogFields(t *testing.T, record map[string]any, fields ...string) {
	t.Helper()
	for _, field := range fields {
		if value, exists := record[field]; exists {
			t.Errorf("unexpected %s=%v", field, value)
		}
	}
}

func TestObsessLogTimeoutIdentifiesTargetAndMissingBaseline(t *testing.T) {
	target, policy, change := obsessLogFixture("50%")
	record := obsessJSONLog(t, target, policy, change)
	if record["msg"] != "target started obsessing" || record["reason"] != "loss" || record["trigger"] != "timeout" || record["timeout_ms"] != float64(1000) || record["interval_ms"] != float64(100) || record["normal_interval_ms"] != float64(5000) || record["probe_sequence"] != float64(42) {
		t.Fatalf("timeout log omitted actionable evidence: %+v", record)
	}
	if record["latency_baseline_ready"] != false || record["baseline_samples_required"] != float64(3) || record["baseline_samples"] != float64(0) {
		t.Fatalf("baseline availability was not explained: %+v", record)
	}
	explanation, _ := record["baseline_status"].(string)
	if !strings.Contains(explanation, "Need 3") || !strings.Contains(explanation, "have 0") || !strings.Contains(explanation, "Loss detection is active") {
		t.Fatalf("unhelpful missing-baseline explanation: %q", explanation)
	}
	if record["event_at"] != change.At.Format(time.RFC3339Nano) || record["probe_sent_at"] != change.Trigger.SentAt.Format(time.RFC3339Nano) || record["probe_deadline"] != change.Trigger.SentAt.Add(change.Trigger.Timeout).Format(time.RFC3339Nano) {
		t.Fatalf("timeout timestamps=%+v", record)
	}
	requireAbsentObsessLogFields(t, record, "threshold_ms", "baseline_ms", "rtt_ms", "reply_at", "increase_percent")
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("timeout sample: %s", encoded)
}

func TestObsessLogSpikeShowsActualRTTAndFrozenComparison(t *testing.T) {
	for _, test := range []struct {
		name, rule               string
		baseline, threshold, rtt float64
	}{
		{name: "relative", rule: "50%", baseline: 10, threshold: 15, rtt: 40},
		{name: "absolute before baseline", rule: "100ms", threshold: 100, rtt: 150},
		{name: "small absolute spike", rule: "100ns", threshold: 0.0001, rtt: 0.0002},
	} {
		t.Run(test.name, func(t *testing.T) {
			target, policy, change := obsessLogFixture(test.rule)
			change.Reason, change.Status.Reason = "latency", "latency"
			change.Status.ThresholdMS = &test.threshold
			if test.baseline > 0 {
				change.Status.BaselineMS, change.Status.BaselineSamples = &test.baseline, 12
			}
			change.Trigger.RTT = time.Duration(test.rtt * float64(time.Millisecond))
			change.Trigger.ReplyAt, change.Trigger.ReplyClass, change.Trigger.Responder = change.Trigger.SentAt.Add(change.Trigger.RTT), model.ReplyOnTime, change.Endpoint
			change.At = change.Trigger.ReplyAt
			record := obsessJSONLog(t, target, policy, change)
			if record["trigger"] != "latency_spike" || record["reason"] != "latency" || record["rtt_ms"] != test.rtt || record["threshold_ms"] != test.threshold || record["latency_rule"] != test.rule || record["reply_at"] != change.At.Format(time.RFC3339Nano) {
				t.Fatalf("spike comparison=%+v", record)
			}
			if test.baseline > 0 {
				if record["baseline_ms"] != float64(10) || record["increase_percent"] != float64(300) || record["latency_baseline_ready"] != true {
					t.Fatalf("frozen relative comparison=%+v", record)
				}
			} else {
				requireAbsentObsessLogFields(t, record, "baseline_ms", "increase_percent", "baseline_status", "latency_baseline_ready")
			}
			if test.name == "small absolute spike" {
				detail, _ := record["detail"].(string)
				if !strings.Contains(detail, "0.0002 ms") || !strings.Contains(detail, "0.0001 ms") {
					t.Fatalf("small spike comparison rounded distinct RTTs together: %q", detail)
				}
			}
		})
	}
}

func TestObsessLogLateReplyReportsMeasuredRTTAndDeadline(t *testing.T) {
	target, policy, change := obsessLogFixture("50%")
	change.Trigger.RTT = 1250 * time.Millisecond
	change.Trigger.ReplyAt = change.Trigger.SentAt.Add(change.Trigger.RTT)
	change.Trigger.ReplyClass, change.Trigger.Responder = model.ReplyLate, change.Endpoint
	record := obsessJSONLog(t, target, policy, change)
	if record["trigger"] != "late_reply" || record["reason"] != "loss" || record["rtt_ms"] != float64(1250) || record["timeout_ms"] != float64(1000) || record["reply_at"] != change.Trigger.ReplyAt.Format(time.RFC3339Nano) || record["event_at"] != change.At.Format(time.RFC3339Nano) {
		t.Fatalf("late reply lacks RTT versus deadline evidence: %+v", record)
	}
	detail, _ := record["detail"].(string)
	if !strings.Contains(detail, "after its deadline") || !strings.Contains(detail, "1250") || !strings.Contains(detail, "1000") {
		t.Fatalf("late reply explanation=%q", detail)
	}
}

func TestObsessLogExitUsesCauseWithoutClaimingNewBaselineWasPreEvent(t *testing.T) {
	for _, reason := range []string{"recovered", "endpoint_changed"} {
		t.Run(reason, func(t *testing.T) {
			target, policy, change := obsessLogFixture("50%")
			change.Reason, change.Status.State, change.Status.Reason, change.Status.IntervalMS = reason, "normal", "", 5000
			baseline, threshold := 90.0, 135.0
			change.Status.BaselineMS, change.Status.ThresholdMS, change.Status.BaselineSamples = &baseline, &threshold, 600
			if reason == "endpoint_changed" {
				change.Endpoint = netip.MustParseAddr("192.0.2.2")
				change.Status.State = "warming"
			}
			// Even if a stale triggering pointer reached the formatter, the
			// explicit exit cause must prevent it becoming an entering log.
			record := obsessJSONLog(t, target, policy, change)
			if record["msg"] != "target stopped obsessing" || record["reason"] != reason || record["interval_ms"] != float64(5000) {
				t.Fatalf("exit message lost its cause: %+v", record)
			}
			detail, _ := record["detail"].(string)
			if detail == "" || reason == "recovered" && !strings.Contains(detail, "recovery period") || reason == "endpoint_changed" && !strings.Contains(detail, "baseline was reset") {
				t.Fatalf("exit explanation=%q", detail)
			}
			requireAbsentObsessLogFields(t, record, "trigger", "probe_sequence", "probe_sent_at", "probe_deadline", "timeout_ms", "rtt_ms", "reply_at", "baseline_ms", "threshold_ms", "increase_percent", "baseline_status", "baseline_samples")
		})
	}
}

func TestObsessTextLogFormatsThresholdAsNumber(t *testing.T) {
	target, policy, change := obsessLogFixture("50%")
	baseline, threshold := 10.0, 15.0
	change.Reason, change.Status.Reason = "latency", "latency"
	change.Status.BaselineMS, change.Status.ThresholdMS, change.Status.BaselineSamples = &baseline, &threshold, 3
	change.Trigger.RTT, change.Trigger.ReplyClass = 40*time.Millisecond, model.ReplyOnTime
	change.Trigger.ReplyAt = change.Trigger.SentAt.Add(change.Trigger.RTT)
	var output bytes.Buffer
	logObsessTransition(slog.New(slog.NewTextHandler(&output, nil)), target, policy, change)
	text := output.String()
	if !strings.Contains(text, "target_id=wan-check") || !strings.Contains(text, `target_name="WAN uplink"`) || !strings.Contains(text, "threshold_ms=15") || !strings.Contains(text, "baseline_ms=10") || !strings.Contains(text, "rtt_ms=40") || strings.Contains(text, "threshold_ms=0x") {
		t.Fatalf("text log lost numeric evidence: %s", text)
	}
}
