package sqlite

import (
	"context"
	"testing"
	"time"

	"flameping/internal/config"
)

func TestSyncTargetsResolvesPingOverridesAndRestoresInheritance(t *testing.T) {
	db, cfg, _, _ := openTestDB(t)
	interval, timeout, minimum := config.Duration(750*time.Millisecond), config.Duration(250*time.Millisecond), config.Duration(500*time.Millisecond)
	cfg.Targets = []config.TargetConfig{
		{ID: "loopback", Address: "127.0.0.1", Ping: &config.TargetPingConfig{Interval: &interval, Timeout: &timeout, MinInterval: &minimum}},
		{ID: "default", Address: "127.0.0.2"},
		{ID: "legacy", Address: "127.0.0.3", Interval: config.Duration(2 * time.Second), Timeout: config.Duration(500 * time.Millisecond)},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	targets, err := db.SyncTargets(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][2]time.Duration{"loopback": {750 * time.Millisecond, 250 * time.Millisecond}, "default": {5 * time.Second, time.Second}, "legacy": {2 * time.Second, 500 * time.Millisecond}}
	for _, target := range targets {
		if got := [2]time.Duration{target.Interval, target.Timeout}; got != want[target.StableID] {
			t.Fatalf("runtime target %s timings=%v want %v", target.StableID, got, want[target.StableID])
		}
	}
	// Removing just the interval override must restore the global interval
	// without losing the target's timeout override on the next startup.
	cfg.Targets[0].Ping.Interval = nil
	targets, err = db.SyncTargets(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if targets[0].Interval != 5*time.Second || targets[0].Timeout != 250*time.Millisecond {
		t.Fatalf("resynced runtime target=%+v", targets[0])
	}
	want["loopback"] = [2]time.Duration{5 * time.Second, 250 * time.Millisecond}
	summaries, err := db.Targets(context.Background(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != len(want) {
		t.Fatalf("target count=%d", len(summaries))
	}
	for _, target := range summaries {
		values := want[target.StableID]
		if target.IntervalMS != float64(values[0])/float64(time.Millisecond) || target.TimeoutMS != float64(values[1])/float64(time.Millisecond) {
			t.Fatalf("persisted target %s timings=%v/%v", target.StableID, target.IntervalMS, target.TimeoutMS)
		}
	}
}
