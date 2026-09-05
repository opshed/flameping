package sqlite

import (
	"context"
	"testing"
	"time"

	"flameping/internal/model"
)

func TestInterfaceDiagnosticsAndResetOnlyIntervals(t *testing.T) {
	db, _, _, _ := openTestDB(t)
	ctx := context.Background()
	at := time.Now().Add(-time.Hour).Truncate(time.Minute)
	base := model.InterfaceEvent{Name: "eth0", BootID: "boot", IfIndex: 2, MAC: "mac", SampledAt: at}
	next := base
	next.SampledAt = at.Add(5 * time.Second)
	next.Counters = model.InterfaceCounters{RXBytes: 5_000_000, TXBytes: 10_000_000, RXErrors: 10, TXErrors: 11,
		RXDropped: 12, TXDropped: 13, RXMissed: 14, RXFIFO: 1, TXFIFO: 2, RXCRC: 3, RXFrame: 4, TXCarrier: 5, Collisions: 6}
	missing := base
	missing.SampledAt = at.Add(15 * time.Second)
	missing.ResetReason = "interface_missing"
	events := []model.Event{{Kind: model.EventInterfaceSnapshot, Interface: base}, {Kind: model.EventInterfaceSnapshot, Interface: next}, {Kind: model.EventInterfaceReset, Interface: missing}}
	if err := db.applyBatch(ctx, events, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}
	series, err := db.InterfaceSeries(ctx, "eth0", at, at.Add(20*time.Second), 21)
	if err != nil {
		t.Fatal(err)
	}
	if series.BucketMS != 1000 || series.SourceResolutionMS != 0 || len(series.Points) != 2 {
		t.Fatalf("unexpected raw buckets: %+v", series)
	}
	p := series.Points[0]
	if !p.HasDeltas || p.RXMbps != 8 || p.TXMbps != 16 || p.RXFIFO != 1 || p.TXFIFO != 2 || p.RXCRC != 3 || p.RXFrame != 4 || p.TXCarrier != 5 || p.Collisions != 6 || p.RXErrors != 10 || p.RXMissed != 14 {
		t.Fatalf("lost diagnostic increments: %+v", p)
	}
	p = series.Points[1]
	if p.HasDeltas || !p.Reset || p.ResetCount != 1 || len(series.Resets) != 1 || series.Resets[0].Reason != "interface_missing" || series.Resets[0].AtMS != missing.SampledAt.UnixMilli() {
		t.Fatalf("reset-only bucket must not imply observed zero counters: %+v", series)
	}
	combined, err := db.InterfaceSeries(ctx, "eth0", at, at.Add(20*time.Second), 1)
	if err != nil || len(combined.Points) != 1 || combined.BucketMS != 20_000 || combined.Points[0].TimeMS != at.UnixMilli() || combined.Points[0].RXCRC != 3 || !combined.Points[0].HasDeltas || combined.Points[0].ResetCount != 1 {
		t.Fatalf("single point aggregation lost metadata: %+v, %v", combined, err)
	}
	initial, err := db.InterfaceSeries(ctx, "eth0", at, at.Add(time.Second), 10)
	if err != nil || len(initial.Points) != 0 {
		t.Fatalf("initial baseline is not a counter interval: %+v, %v", initial, err)
	}
}

func TestInterfaceRollupDiagnosticsAndBoundedResetDetails(t *testing.T) {
	db, _, _, _ := openTestDB(t)
	ctx := context.Background()
	at := time.Now().Add(-10 * 24 * time.Hour).Truncate(time.Minute)
	result, err := db.writer.ExecContext(ctx, `INSERT INTO interface_generations(boot_id,ifindex,name,display_name,mac,started_at_us) VALUES('boot',2,'eth0','Ethernet','mac',?)`, at.Add(-time.Hour).UnixMicro())
	if err != nil {
		t.Fatal(err)
	}
	id, _ := result.LastInsertId()
	// sample_count includes baselines. elapsed_ns alone tells us whether deltas exist.
	for i, elapsed := range []int64{60_000_000_000, 0} {
		_, err = db.writer.ExecContext(ctx, `INSERT INTO interface_rollups(generation_id,resolution_s,bucket_start_us,elapsed_ns,sample_count,reset_count,rx_bytes_delta,tx_bytes_delta,rx_packets_delta,tx_packets_delta,rx_errors_delta,tx_errors_delta,rx_dropped_delta,tx_dropped_delta,rx_missed_delta,rx_fifo_delta,tx_fifo_delta,rx_crc_delta,rx_frame_delta,tx_carrier_delta,collisions_delta,peak_rx_bytes_per_s,peak_tx_bytes_per_s,updated_at_us) VALUES(?,60,?,?,1,101,0,0,0,0,0,0,0,0,0,?,?,?,?,?,?,1000000,500000,?)`, id, at.Add(time.Duration(i)*time.Minute).UnixMicro(), elapsed, 1-i, 2*(1-i), 3*(1-i), 4*(1-i), 5*(1-i), 6*(1-i), time.Now().UnixMicro())
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 101; i++ {
		if _, err := db.writer.ExecContext(ctx, `INSERT INTO interface_resets(generation_id,name,at_us,reason) VALUES(?,'eth0',?,'counter_decreased')`, id, at.Add(10*time.Second+time.Duration(i)*time.Microsecond).UnixMicro()); err != nil {
			t.Fatal(err)
		}
	}
	// The endpoint is half-open and must not contribute an extra reset.
	if _, err := db.writer.ExecContext(ctx, `INSERT INTO interface_resets(name,at_us,reason) VALUES('eth0',?,'outside')`, at.Add(2*time.Minute).UnixMicro()); err != nil {
		t.Fatal(err)
	}
	series, err := db.InterfaceSeries(ctx, "eth0", at.Add(time.Second), at.Add(2*time.Minute), 120)
	if err != nil {
		t.Fatal(err)
	}
	if series.SourceResolutionMS != 60_000 || series.BucketMS != 60_000 || len(series.Points) != 2 {
		t.Fatalf("unexpected historical tier: %+v", series)
	}
	p := series.Points[0]
	if !p.Partial || !p.HasDeltas || p.RXMbps != 8 || p.RXCRC != 3 || p.RXFIFO != 1 || p.TXFIFO != 2 || p.RXFrame != 4 || p.TXCarrier != 5 || p.Collisions != 6 || p.ResetCount != 101 {
		t.Fatalf("rollup diagnostics or authoritative reset count lost: %+v", p)
	}
	if series.Points[1].HasDeltas || series.Points[1].Reset || series.Points[1].ResetCount != 0 {
		t.Fatalf("baseline-only rollup or duplicated rollup resets: %+v", series.Points[1])
	}
	if !series.ResetsTruncated || len(series.Resets) != 100 || series.Resets[0].Reason != "counter_decreased" {
		t.Fatalf("reset detail cap must not affect marker counts: %+v", series)
	}
	// A narrow zoom still uses the whole retained historical bucket, explicitly marked partial.
	zoom, err := db.InterfaceSeries(ctx, "eth0", at.Add(9*time.Second), at.Add(11*time.Second), 10)
	if err != nil || len(zoom.Points) != 1 || !zoom.Points[0].Partial || zoom.Points[0].RXCRC != 3 || zoom.Points[0].ResetCount != 101 {
		t.Fatalf("historical zoom lost clipped bucket or exact resets: %+v, %v", zoom, err)
	}
}
