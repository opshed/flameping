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
