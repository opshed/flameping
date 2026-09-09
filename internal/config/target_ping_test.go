package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func loadTargetPing(t *testing.T, global, target string) (Config, error) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(file, []byte(global+"\ntargets:\n  - id: gateway\n    address: 127.0.0.1\n"+target), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(file)
}

func TestTargetPingInheritsEachSetting(t *testing.T) {
	global := "ping: {interval: 5s, timeout: 2s, min_interval: 250ms}"
	for _, test := range []struct {
		name, fields               string
		interval, timeout, minimum time.Duration
	}{
		{"omitted", "", 5 * time.Second, 2 * time.Second, 250 * time.Millisecond},
		{"empty", "    ping: {}\n", 5 * time.Second, 2 * time.Second, 250 * time.Millisecond},
		{"null", "    ping: null\n", 5 * time.Second, 2 * time.Second, 250 * time.Millisecond},
		{"interval", "    ping: {interval: 1s}\n", time.Second, 2 * time.Second, 250 * time.Millisecond},
		{"timeout", "    ping: {timeout: 500ms}\n", 5 * time.Second, 500 * time.Millisecond, 250 * time.Millisecond},
		{"minimum", "    ping: {min_interval: 500ms}\n", 5 * time.Second, 2 * time.Second, 500 * time.Millisecond},
		{"all", "    ping: {interval: 300ms, timeout: 100ms, min_interval: 100ms}\n", 300 * time.Millisecond, 100 * time.Millisecond, 100 * time.Millisecond},
		{"legacy", "    interval: 1s\n    timeout: 500ms\n", time.Second, 500 * time.Millisecond, 250 * time.Millisecond},
		{"mixed fields", "    interval: 1s\n    ping: {timeout: 500ms, min_interval: 100ms}\n", time.Second, 500 * time.Millisecond, 100 * time.Millisecond},
		{"null field", "    interval: 1s\n    ping: {interval: null, timeout: 500ms}\n", time.Second, 500 * time.Millisecond, 250 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := loadTargetPing(t, global, test.fields)
			if err != nil {
				t.Fatal(err)
			}
			target := cfg.Targets[0]
			if target.EffectiveInterval(cfg) != test.interval || target.EffectiveTimeout(cfg) != test.timeout || target.EffectiveMinInterval(cfg) != test.minimum {
				t.Fatalf("resolved interval=%v timeout=%v minimum=%v", target.EffectiveInterval(cfg), target.EffectiveTimeout(cfg), target.EffectiveMinInterval(cfg))
			}
			if cfg.Ping.Interval.Value() != 5*time.Second || cfg.Ping.Timeout.Value() != 2*time.Second || cfg.Ping.MinInterval.Value() != 250*time.Millisecond {
				t.Fatal("target override mutated global defaults")
			}
		})
	}
}

func TestTargetPingRejectsInvalidOrAmbiguousOverrides(t *testing.T) {
	for _, test := range []struct{ fields, want string }{
		{"    ping: {interval: 0s}\n", "interval must be positive"},
		{"    ping: {interval: -1s}\n", "interval must be positive"},
		{"    ping: {timeout: 0s}\n", "timeout must be positive"},
		{"    ping: {timeout: -1s}\n", "timeout must be positive"},
		{"    ping: {min_interval: 0s}\n", "min_interval must be at least 100ms"},
		{"    ping: {min_interval: 50ms}\n", "min_interval must be at least 100ms"},
		{"    ping: {min_interval: -1s}\n", "min_interval must be at least 100ms"},
		{"    ping: {interval: 100ms}\n", "at least 250ms"},
		{"    ping: {min_interval: 6s}\n", "at least 6s"},
		{"    interval: 1s\n    ping: {interval: 1s}\n", "not both"},
		{"    timeout: 1s\n    ping: {timeout: 2s}\n", "not both"},
		{"    interval: -1s\n", "cannot be negative"},
		{"    timeout: -1s\n", "cannot be negative"},
		{"    ping: {event_queue: 64}\n", "field event_queue not found"},
		{"    ping: {send_queue: 4}\n", "field send_queue not found"},
		{"    ping: {max_probes_per_second: 20}\n", "field max_probes_per_second not found"},
		{"    ping: {typo: 1s}\n", "field typo not found"},
		{"    ping: false\n", "cannot unmarshal"},
	} {
		t.Run(test.fields, func(t *testing.T) {
			_, err := loadTargetPing(t, "ping: {min_interval: 250ms}", test.fields)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v want %q", err, test.want)
			}
		})
	}
}

func TestTargetMinimumControlsObsessIndependently(t *testing.T) {
	for _, test := range []struct{ minimum, want string }{{"100ms", "100ms"}, {"1s", "1s"}} {
		cfg, err := loadTargetPing(t, "ping: {min_interval: 500ms}", "    ping: {min_interval: "+test.minimum+"}\n    obsess: {}\n")
		if err != nil {
			t.Fatal(err)
		}
		if got := cfg.Targets[0].EffectiveObsess(cfg).Interval.String(); got != test.want {
			t.Fatalf("fast interval=%s want %s", got, test.want)
		}
		other := TargetConfig{Obsess: &ObsessConfig{}}
		if other.EffectiveObsess(cfg).Interval.Value() != 500*time.Millisecond {
			t.Fatal("one target changed another target's fast interval")
		}
	}
	_, err := loadTargetPing(t, "", "    ping: {min_interval: 500ms}\n    obsess: {interval: 100ms}\n")
	if err == nil || !strings.Contains(err.Error(), "obsess.interval must be at least 500ms") {
		t.Fatalf("fast interval bypassed target minimum: %v", err)
	}
}

func TestTargetPingOverridesParticipateInBudgets(t *testing.T) {
	for _, test := range []struct{ global, target, want string }{
		{"ping: {max_probes_per_second: 5}", "    ping: {interval: 100ms}\n", "rate 10.00/s"},
		{"ping: {min_interval: 500ms, max_probes_per_second: 5}", "    ping: {min_interval: 100ms}\n    obsess: {}\n", "rate 10.00/s"},
		{"storage: {raw_retention: 24h}", "    ping: {timeout: 23h59m30s}\n", "maximum target timeout"},
		{"", "    ping: {timeout: 10000s}\n    obsess: {}\n", "probe/sample slots"},
		{"", "    ping: {interval: 30s}\n    obsess: {}\n", "min_samples cannot fit"},
	} {
		_, err := loadTargetPing(t, test.global, test.target)
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("budget error=%v want %q", err, test.want)
		}
	}
}
