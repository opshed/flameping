package app

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/opshed/flameping/internal/config"
)

func TestRunLifecycleWithoutCollectors(t *testing.T) {
	cfg := config.Defaults()
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.Storage.Path = filepath.Join(t.TempDir(), "lifecycle.db")
	cfg.Storage.MaxBytes = config.Bytes(128 << 20)
	cfg.Storage.FreeSpaceReserve = 0
	cfg.Storage.FreeSpaceReservePercent = 0
	cfg.Traceroute.Enabled = false
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	time.Sleep(time.Second)
	cancel()
	select {
	case err := <-done:
		if err != nil && strings.Contains(err.Error(), "operation not permitted") {
			t.Skip("sandbox does not permit a loopback listener")
		}
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("daemon did not complete staged shutdown")
	}
}
