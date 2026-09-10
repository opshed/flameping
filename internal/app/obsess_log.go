package app

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/opshed/flameping/internal/config"
	"github.com/opshed/flameping/internal/model"
	"github.com/opshed/flameping/internal/obsess"
	"github.com/opshed/flameping/internal/store/sqlite"
)

func logObsessTransition(logger *slog.Logger, target sqlite.Target, policy *config.ObsessConfig, change obsess.Transition) {
	state := change.Status
	attrs := []any{
		"target_id", target.StableID, "target_name", target.DisplayName,
		"target_address", target.ConfiguredAddress, "state", state.State,
		"reason", change.Reason, "event_at", change.At,
		"interval_ms", state.IntervalMS, "normal_interval_ms", durationMS(target.Interval),
	}
	if change.Endpoint.IsValid() {
		attrs = append(attrs, "target_ip", change.Endpoint.String())
	}
	message := "target stopped obsessing"
	switch change.Reason {
	case "recovered":
		attrs = append(attrs, "detail", "Healthy probe observations satisfied the recovery period.", "recover_after_ms", state.RecoverAfterMS)
	case "endpoint_changed":
		attrs = append(attrs, "detail", "Resolved IP changed; normal probing resumed and the latency baseline was reset.")
	default:
		message = "target started obsessing"
		if policy != nil {
			attrs = append(attrs, "latency_rule", policy.LatencyThreshold,
				"baseline_window_ms", durationMS(policy.BaselineWindow.Value()), "recover_after_ms", state.RecoverAfterMS)
			_, relative, _ := policy.Threshold()
			if relative > 0 {
				attrs = append(attrs, "baseline_samples_required", policy.MinSamples, "latency_baseline_ready", state.ThresholdMS != nil)
				if state.ThresholdMS == nil {
					attrs = append(attrs, "baseline_status", fmt.Sprintf("Need %d preceding on-time replies; have %d. Loss detection is active.", policy.MinSamples, state.BaselineSamples))
				}
			}
		}
		attrs = append(attrs, "baseline_samples", state.BaselineSamples)
		if state.BaselineMS != nil {
			attrs = append(attrs, "baseline_ms", *state.BaselineMS)
		}
		if state.ThresholdMS != nil {
			attrs = append(attrs, "threshold_ms", *state.ThresholdMS)
		}
		if probe := change.Trigger; probe != nil {
			attrs = append(attrs, "probe_sequence", probe.Key.Sequence, "probe_sent_at", probe.SentAt,
				"probe_deadline", probe.SentAt.Add(probe.Timeout), "timeout_ms", durationMS(probe.Timeout))
			if !probe.ReplyAt.IsZero() {
				attrs = append(attrs, "reply_at", probe.ReplyAt, "rtt_ms", durationMS(probe.RTT))
			}
			switch {
			case change.Reason == "latency":
				attrs = append(attrs, "trigger", "latency_spike")
				if state.ThresholdMS != nil {
					attrs = append(attrs, "detail", fmt.Sprintf("Ping RTT %g ms exceeded the %g ms latency threshold.", durationMS(probe.RTT), *state.ThresholdMS))
				}
				if state.BaselineMS != nil && *state.BaselineMS > 0 {
					attrs = append(attrs, "increase_percent", (durationMS(probe.RTT) / *state.BaselineMS - 1)*100)
				}
			case probe.ReplyClass == model.ReplyLate || !probe.ReplyAt.IsZero():
				attrs = append(attrs, "trigger", "late_reply", "detail", fmt.Sprintf("Ping reply arrived after its deadline: RTT %g ms, timeout %g ms.", durationMS(probe.RTT), durationMS(probe.Timeout)))
			default:
				attrs = append(attrs, "trigger", "timeout", "detail", fmt.Sprintf("No ping reply was observed before the %g ms deadline.", durationMS(probe.Timeout)))
			}
		}
	}
	logger.Info(message, attrs...)
}

func durationMS(value time.Duration) float64 { return float64(value) / float64(time.Millisecond) }
