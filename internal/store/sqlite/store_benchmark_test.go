package sqlite

import (
	"context"
	"crypto/rand"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"flameping/internal/config"
	"flameping/internal/model"
)

func openBenchmarkDB(b *testing.B) (*DB, Target, model.RunID) {
	b.Helper()
	cfg := config.Defaults()
	cfg.Storage.Path = filepath.Join(b.TempDir(), "benchmark.db")
	cfg.Storage.MaxBytes = config.Bytes(5 << 30)
	cfg.Storage.FreeSpaceReserve = 0
	cfg.Storage.FreeSpaceReservePercent = 0
	db, err := Open(context.Background(), cfg.Storage)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	cfg.Targets = []config.TargetConfig{{ID: "benchmark", Address: "127.0.0.1", Interval: config.Duration(100 * time.Millisecond)}}
	targets, err := db.SyncTargets(context.Background(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	var runID model.RunID
	if _, err := rand.Read(runID[:]); err != nil {
		b.Fatal(err)
	}
	if err := db.StartRun(context.Background(), runID, "benchmark", "benchmark"); err != nil {
		b.Fatal(err)
	}
	return db, targets[0], runID
}

func BenchmarkApplyProbeBatch512(b *testing.B) {
	db, target, runID := openBenchmarkDB(b)
	ctx := context.Background()
	address := netip.MustParseAddr("127.0.0.1")
	sequence := uint64(0)
	b.ReportAllocs()
	b.SetBytes(512)
	b.ResetTimer()
	for range b.N {
		events := make([]model.Event, 512)
		for index := range events {
			sequence++
			at := time.Now()
			events[index] = model.Event{Kind: model.EventProbeSent, Probe: model.ProbeEvent{
				Key: model.ProbeKey{RunID: runID, Sequence: sequence}, TargetID: target.ID,
				Endpoint: address, ScheduledAt: at, SentAt: at, Timeout: time.Second,
			}}
		}
		if err := db.applyBatch(ctx, events, make(map[model.ProbeKey]orphanReply)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLatestTargetSeekAt100kRows(b *testing.B) {
	db, target, runID := openBenchmarkDB(b)
	ctx := context.Background()
	tx, err := db.writer.BeginTx(ctx, nil)
	if err != nil {
		b.Fatal(err)
	}
	endpoint, err := ensureEndpoint(ctx, tx, target.ID, "127.0.0.1", 4, "", time.Now().UnixMicro())
	if err != nil {
		b.Fatal(err)
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO ping_samples(run_id,sequence,target_id,endpoint_id,scheduled_at_us,sent_at_us,timeout_ns) VALUES(?,?,?,?,?,?,?)`)
	if err != nil {
		b.Fatal(err)
	}
	base := time.Now().Add(-time.Hour).UnixMicro()
	for sequence := int64(1); sequence <= 100_000; sequence++ {
		if _, err := stmt.ExecContext(ctx, runID.Bytes(), sequence, target.ID, endpoint, base+sequence, base+sequence, int64(time.Second)); err != nil {
			b.Fatal(err)
		}
	}
	if err := stmt.Close(); err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := db.Targets(ctx, time.Now()); err != nil {
			b.Fatal(err)
		}
	}
}
