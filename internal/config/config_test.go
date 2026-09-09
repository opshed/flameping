package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaultsAndFractionalInterval(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := []byte("targets:\n  - id: gateway\n    name: Gateway\n    address: 127.0.0.1\n    interval: 100ms\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Targets[0].EffectiveInterval(cfg); got != 100*time.Millisecond {
		t.Fatalf("interval = %v", got)
	}
	if cfg.Storage.MaxBytes.Int64() != 5<<30 {
		t.Fatalf("max bytes = %d", cfg.Storage.MaxBytes)
	}
}

func TestLoadRejectsUnknownField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("mystery: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "field mystery not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidatePublicAndRate(t *testing.T) {
	cfg := Defaults()
	cfg.Server.Listen = "0.0.0.0:8080"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected public bind error")
	}
	cfg.Server.AllowPublic = true
	cfg.Ping.MaxProbesPerSecond = 1
	cfg.Targets = []TargetConfig{{ID: "a", Address: "127.0.0.1", Interval: Duration(100 * time.Millisecond)}}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("unexpected rate error: %v", err)
	}
}

func TestBytes(t *testing.T) {
	for input, want := range map[string]int64{"1GiB": 1 << 30, "1.5MiB": 1572864, "64KiB": 65536} {
		got, err := parseBytes(input)
		if err != nil || got != want {
			t.Fatalf("parseBytes(%q) = %d, %v; want %d", input, got, err, want)
		}
	}
}

func loadObsessYAML(t *testing.T, fields string) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := "targets:\n  - id: gateway\n    address: 127.0.0.1\n" + fields
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(path)
}

func TestObsessOptInAndDefaults(t *testing.T) {
	for _, fields := range []string{"", "    obsess: null\n", "    obsess: {enabled: false}\n"} {
		cfg, err := loadObsessYAML(t, fields)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Targets[0].EffectiveObsess(cfg) != nil {
			t.Fatalf("obsess unexpectedly enabled for %q", fields)
		}
	}
	cfg, err := loadObsessYAML(t, "    obsess: {}\n")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Ping.MinInterval = Duration(250 * time.Millisecond)
	o := cfg.Targets[0].EffectiveObsess(cfg)
	if o == nil || o.Interval.Value() != 250*time.Millisecond || o.LatencyThreshold != "50%" || o.BaselineWindow.Value() != time.Minute || o.RecoverAfter.Value() != time.Minute || o.MinSamples != 3 {
		t.Fatalf("unexpected defaults: %+v", o)
	}
	o.MinSamples = 10
	if cfg.Targets[0].Obsess.MinSamples != 0 {
		t.Fatal("effective defaults mutated the source config")
	}
}

func TestObsessRejectsExplicitInvalidFields(t *testing.T) {
	for _, test := range []struct{ field, want string }{
		{"interval: 0s", "obsess.interval"},
		{"interval: 50ms", "obsess.interval"},
		{"interval: 5s", "faster"},
		{"baseline_window: 0s", "must be positive"},
		{"recover_after: 0s", "must be positive"},
		{"min_samples: 0", "must be positive"},
		{"min_samples: 100", "cannot fit"},
		{"latency_threshold: ''", "latency_threshold"},
		{"latency_threshold: 0ms", "latency_threshold"},
		{"latency_threshold: NaN%", "finite"},
		{"latency_threshold: +Inf%", "finite"},
		{"latency_threshold: -50%", "positive"},
		{"latency_threshold: 1e300%", "duration range"},
		{"unknown: true", "field unknown not found"},
	} {
		t.Run(test.field, func(t *testing.T) {
			_, err := loadObsessYAML(t, "    obsess:\n      "+test.field+"\n")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
	if _, err := loadObsessYAML(t, "    obsess: true\n"); err == nil {
		t.Fatal("accepted non-mapping obsess config")
	}
}

func TestObsessThresholds(t *testing.T) {
	for _, test := range []struct {
		raw      string
		absolute time.Duration
		percent  float64
	}{{"100ms", 100 * time.Millisecond, 0}, {"50%", 0, 50}, {"12.5%", 0, 12.5}} {
		absolute, percent, err := (ObsessConfig{LatencyThreshold: test.raw}).Threshold()
		if err != nil || absolute != test.absolute || percent != test.percent {
			t.Fatalf("Threshold(%q) = %v, %v, %v", test.raw, absolute, percent, err)
		}
	}
	// Absolute RTT thresholds remain usable at normal intervals too slow to
	// gather the relative threshold's default minimum baseline samples.
	if _, err := loadObsessYAML(t, "    interval: 2m\n    obsess:\n      latency_threshold: 100ms\n"); err != nil {
		t.Fatal(err)
	}
}

func TestRelativeBaselineMustFitPrecedingSamples(t *testing.T) {
	if _, err := loadObsessYAML(t, "    interval: 5s\n    obsess:\n      baseline_window: 10s\n      min_samples: 3\n"); err == nil || !strings.Contains(err.Error(), "cannot fit") {
		t.Fatalf("accepted an impossible preceding baseline: %v", err)
	}
	if _, err := loadObsessYAML(t, "    interval: 5s\n    obsess:\n      baseline_window: 15s\n      min_samples: 3\n"); err != nil {
		t.Fatalf("preceding samples fit the window: %v", err)
	}
}

func TestObsessWorstCaseRateAndMemoryBudget(t *testing.T) {
	cfg := Defaults()
	cfg.Targets = []TargetConfig{
		{ID: "a", Address: "127.0.0.1", Obsess: &ObsessConfig{}},
		{ID: "b", Address: "127.0.0.2", Obsess: &ObsessConfig{}},
	}
	cfg.Ping.MaxProbesPerSecond = 15
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "rate 20.00/s") {
		t.Fatalf("fast rate was not enforced: %v", err)
	}
	disabled := false
	cfg.Targets[1].Obsess.Enabled = &disabled
	if err := cfg.Validate(); err != nil {
		t.Fatalf("disabled obsess affected fast rate: %v", err)
	}
	cfg.Ping.MaxProbesPerSecond = 5000
	cfg.Targets[1].Obsess.Enabled = nil
	for i := range cfg.Targets {
		cfg.Targets[i].Timeout = Duration(4900 * time.Second)
	}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "probe/sample slots") {
		t.Fatalf("combined state budget was not enforced: %v", err)
	}
	cfg.Targets = cfg.Targets[:1]
	if err := cfg.Validate(); err != nil {
		t.Fatalf("individual target should fit: %v", err)
	}
	cfg.Targets[0].Timeout = 0
	cfg.Targets[0].Obsess.RecoverAfter = Duration(24 * time.Hour)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("recovery duration need not retain its entire history: %v", err)
	}
	cfg.Targets[0].Obsess.BaselineWindow = Duration(24 * time.Hour)
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "probe/sample slots") {
		t.Fatalf("history budget was not enforced: %v", err)
	}
}
