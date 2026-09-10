package sqlite

import (
	"context"
	"fmt"
	"math"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"flameping/internal/config"
	"flameping/internal/model"
)

func TestOverviewExactWindowDirtyRollupAndPending(t *testing.T) {
	db, _, target, runID := openTestDB(t)
	ctx := context.Background()
	to := time.Now().Add(-2 * time.Minute).Truncate(time.Minute).Add(37 * time.Second)
	from := to.Add(-15 * time.Minute)
	minute := from.Truncate(time.Minute).Add(time.Minute)
	var sequence uint64
	probe := func(at time.Time, rtt time.Duration, local bool) model.ProbeEvent {
		t.Helper()
		sequence++
		p := model.ProbeEvent{Key: model.ProbeKey{RunID: runID, Sequence: sequence}, TargetID: target.ID,
			Endpoint: netip.MustParseAddr("127.0.0.1"), ScheduledAt: at, SentAt: at, Timeout: time.Second}
		kind := model.EventProbeSent
		if local {
			kind = model.EventProbeSendError
			p.SendErrorCode = "local"
		}
		events := []model.Event{{Kind: kind, Probe: p}}
		if rtt > 0 {
			p.ReplyAt, p.RTT, p.Responder, p.ReplyClass = at.Add(rtt), rtt, p.Endpoint, model.ReplyOnTime
			if rtt > p.Timeout {
				p.ReplyClass = model.ReplyLate
			}
			events = append(events, model.Event{Kind: model.EventProbeReply, Probe: p})
		}
		if err := db.applyBatch(ctx, events, make(map[model.ProbeKey]orphanReply)); err != nil {
			t.Fatal(err)
		}
		return p
	}
	probe(from.Add(-time.Microsecond), 900*time.Millisecond, false)
	probe(from, 10*time.Millisecond, false)
	probe(minute.Add(time.Second), 20*time.Millisecond, false)
	probe(minute.Add(2*time.Second), 30*time.Millisecond, false)
	late := probe(minute.Add(time.Minute), 0, false)
	probe(minute.Add(2*time.Minute), 0, false)
	probe(minute.Add(3*time.Minute), 0, true)
	probe(to.Add(-100*time.Millisecond), 0, false)
	probe(to, 900*time.Millisecond, false)
	if err := db.applyBatch(ctx, []model.Event{{Kind: model.EventSchedulerGap, Gap: model.SchedulerGap{
		TargetID: target.ID, FirstScheduled: from.Add(-3 * time.Second), Interval: 2 * time.Second, MissedCount: 4,
	}}}, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}
	if err := db.recomputeDirty(ctx, 1000, to); err != nil {
		t.Fatal(err)
	}
	late.ReplyAt, late.RTT, late.ReplyClass, late.Responder = late.SentAt.Add(1500*time.Millisecond), 1500*time.Millisecond, model.ReplyLate, late.Endpoint
	if err := db.applyBatch(ctx, []model.Event{{Kind: model.EventProbeReply, Probe: late}}, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}
	result, err := db.Overview(ctx, to, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	p := result.Targets[0].Recent
	if result.FromMS != from.UnixMilli() || result.ToMS != to.UnixMilli() || len(result.Targets[0].Trend) != 16 ||
		p.Scheduled != 9 || p.Attempted != 7 || p.Sent != 6 || p.Settled != 5 || p.Pending != 1 ||
		p.OnTime != 3 || p.Late != 1 || p.Unanswered != 1 || p.SendErrors != 1 || p.SchedulerMissed != 2 || p.RTTCount != 4 || !p.Partial {
		t.Fatalf("overview lost exact bounds, outcomes, or pending: %+v", result)
	}
	if p.DeadlineMissPct == nil || math.Abs(*p.DeadlineMissPct-100.0/3) > .001 || p.NoReplyPct == nil || math.Abs(*p.NoReplyPct-100.0/6) > .001 || p.P95MS == nil || math.Abs(*p.P95MS-1500) > 15 {
		t.Fatalf("overview denominators or histogram: %+v", p)
	}
	// The hybrid path must match a direct exact raw read even after a persisted
	// unanswered observation becomes a late reply and invalidates its rollup.
	tx, err := db.readers.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	raw, err := queryRawAggregates(ctx, tx, target.ID, from, to, to, time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	combined := make(map[int64]*queryAggregate)
	for _, value := range raw {
		mergeQueryAggregate(combined, 0, value.pingAggregate, value.partial)
	}
	if expected := overviewPingValue(combined[0]); !reflect.DeepEqual(expected, p) {
		t.Fatalf("hybrid=%+v raw=%+v", p, expected)
	}
	var trendSent, trendGaps int64
	for _, bucket := range result.Targets[0].Trend {
		trendSent += bucket.Sent
		trendGaps += bucket.SchedulerMissed
		if bucket.FromMS < result.FromMS || bucket.ToMS > result.ToMS || bucket.FromMS >= bucket.ToMS {
			t.Fatalf("trend escapes exact window: %+v", bucket)
		}
	}
	if trendSent != p.Sent || trendGaps != p.SchedulerMissed {
		t.Fatal("trend does not sum to window evidence")
	}
}

func TestOverviewEmptyActiveTargetsAndStaleGap(t *testing.T) {
	db, cfg, old, _ := openTestDB(t)
	ctx := context.Background()
	cfg.Targets = []config.TargetConfig{{ID: "z", Name: "Zulu", Address: "127.0.0.1"}, {ID: "a", Name: "Alpha", Address: "127.0.0.2"}}
	targets, err := db.SyncTargets(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	to := time.Now().Truncate(time.Millisecond)
	events := []model.Event{{Kind: model.EventSchedulerGap, Gap: model.SchedulerGap{TargetID: targets[0].ID, FirstScheduled: to.Add(-time.Minute), Interval: time.Second, MissedCount: 1}},
		{Kind: model.EventSchedulerGap, Gap: model.SchedulerGap{TargetID: old.ID, FirstScheduled: to.Add(-time.Second), Interval: time.Second, MissedCount: 1}}}
	if err := db.applyBatch(ctx, events, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}
	result, err := db.Overview(ctx, to, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Targets) != 2 || result.Targets[0].StableID != "a" || result.Targets[1].StableID != "z" || result.Targets[1].State != "stale" {
		t.Fatalf("active order or gap freshness: %+v", result.Targets)
	}
	empty := result.Targets[0].Recent
	if empty.Scheduled != 0 || empty.P50MS != nil || empty.DeadlineMissPct != nil || empty.NoReplyPct != nil || empty.Partial {
		t.Fatalf("empty data became success: %+v", empty)
	}
	if _, err := db.Overview(ctx, to, 24*time.Hour); err == nil {
		t.Fatal("unbounded overview window accepted")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := db.Overview(canceled, to, time.Hour); err == nil {
		t.Fatal("canceled overview query succeeded")
	}
}

func TestOverviewGapSpansRawEdgesAndCleanMinutes(t *testing.T) {
	db, _, target, _ := openTestDB(t)
	ctx := context.Background()
	to := time.Now().Add(-time.Minute).Truncate(time.Minute).Add(37 * time.Second)
	from := to.Add(-5 * time.Minute)
	if err := db.applyBatch(ctx, []model.Event{{Kind: model.EventSchedulerGap, Gap: model.SchedulerGap{
		TargetID: target.ID, FirstScheduled: from.Add(-30 * time.Second), Interval: time.Second, MissedCount: 345,
	}}}, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}
	raw, err := db.Overview(ctx, to, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.recomputeDirty(ctx, 1000, to); err != nil {
		t.Fatal(err)
	}
	hybrid, err := db.Overview(ctx, to, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if hybrid.Targets[0].Recent.SchedulerMissed != 300 || hybrid.Targets[0].Recent.Scheduled != 300 || hybrid.Targets[0].Recent.Sent != 0 ||
		!reflect.DeepEqual(raw.Targets[0].Recent, hybrid.Targets[0].Recent) || !reflect.DeepEqual(raw.Targets[0].Trend, hybrid.Targets[0].Trend) {
		t.Fatalf("spanning gap raw=%+v hybrid=%+v", raw.Targets[0], hybrid.Targets[0])
	}
}

func TestOverviewInterfaceSignalsAndRetainedRoutes(t *testing.T) {
	db, cfg, target, _ := openTestDB(t)
	ctx := context.Background()
	cfg.Interfaces = []config.InterfaceConfig{{Name: "eth0", DisplayName: "Ethernet"}, {Name: "missing0"}}
	if _, err := db.SyncTargets(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	to := time.Now().Add(-time.Minute).Truncate(time.Minute)
	from := to.Add(-5 * time.Minute)
	base := model.InterfaceEvent{Name: "eth0", BootID: "boot", IfIndex: 2, MAC: "mac", SampledAt: from.Add(-5 * time.Second)}
	next := base
	next.SampledAt = from.Add(5 * time.Second)
	next.Counters = model.InterfaceCounters{RXBytes: 10_000_000, TXBytes: 20_000_000, RXErrors: 2, TXDropped: 3, RXCRC: 4, RXMissed: 5}
	reset := next
	reset.SampledAt, reset.ResetReason = from.Add(10*time.Second), "interface_missing"
	events := []model.Event{{Kind: model.EventInterfaceSnapshot, Interface: base}, {Kind: model.EventInterfaceSnapshot, Interface: next}, {Kind: model.EventInterfaceReset, Interface: reset}}
	for i, state := range []string{"completed", "completed", "error", "timed_out"} {
		events = append(events, model.Event{Kind: model.EventTraceCompleted, Trace: model.TraceResult{TargetID: target.ID, Endpoint: netip.MustParseAddr("127.0.0.1"),
			StartedAt: from.Add(time.Duration(i) * time.Second), EndedAt: from.Add(time.Duration(i+1) * time.Second), Status: state, Reached: i == 0, Method: "test"}})
	}
	if err := db.applyBatch(ctx, events, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}
	var endpoint int64
	if err := db.readers.QueryRow(`SELECT id FROM endpoints WHERE target_id=? LIMIT 1`, target.ID).Scan(&endpoint); err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Time{from.Add(-time.Microsecond), from, to} {
		if _, err := db.writer.Exec(`INSERT INTO route_changes(target_id,endpoint_id,old_signature,new_signature,first_seen_us,confirmed_at_us) VALUES(?,?,'a','b',?,?)`, target.ID, endpoint, at.UnixMicro(), at.UnixMicro()); err != nil {
			t.Fatal(err)
		}
	}
	result, err := db.Overview(ctx, to, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	i := result.Interfaces[0]
	if len(result.Interfaces) != 2 || i.Present || !i.HasDeltas || !i.HasErrors || !i.HasDrops || !i.Partial || i.PeakRXMbps != 8 || i.PeakTXMbps != 16 || i.RXErrors != 2 || i.TXDropped != 3 || i.RXCRC != 4 || i.RXMissed != 5 || i.Resets != 1 || result.Interfaces[1].HasDeltas {
		t.Fatalf("interface evidence or missing state lost: %+v", result.Interfaces)
	}
	route := result.Targets[0].Route
	if route.RouteHistoryCounts != (RouteHistoryCounts{Traces: 4, Reached: 1, Unreached: 1, Errors: 2, Changes: 1}) || route.LastChangeAtMS == nil || *route.LastChangeAtMS != from.UnixMilli() {
		t.Fatalf("route observations or confirmation bounds: %+v", route)
	}
}

// This represents 200 targets probing at one-second intervals for an hour,
// with clean minute rollups and a recent raw edge. Historical depth is absent
// from the query's cost. Setup is excluded from timing and allocation results.
func BenchmarkOverview200TargetsOneHour(b *testing.B) {
	db, _, runID := openBenchmarkDB(b)
	ctx := context.Background()
	cfg := config.Defaults()
	for i := range 200 {
		cfg.Targets = append(cfg.Targets, config.TargetConfig{ID: fmt.Sprintf("target-%03d", i), Address: "127.0.0.1", Interval: config.Duration(time.Second)})
	}
	targets, err := db.SyncTargets(ctx, cfg)
	if err != nil {
		b.Fatal(err)
	}
	to := time.Now().Truncate(time.Minute).Add(30 * time.Second)
	from := to.Add(-time.Hour)
	tx, err := db.writer.BeginTx(ctx, nil)
	if err != nil {
		b.Fatal(err)
	}
	for index, target := range targets {
		endpoint, err := ensureEndpoint(ctx, tx, target.ID, "127.0.0.1", 4, "", from.UnixMicro())
		if err != nil {
			b.Fatal(err)
		}
		// Bulk SQL keeps representative raw setup practical without timing it.
		if _, err := tx.ExecContext(ctx, `WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<3599)
			INSERT INTO ping_samples(run_id,sequence,target_id,endpoint_id,scheduled_at_us,sent_at_us,timeout_ns,reply_at_us,rtt_ns,reply_class)
			SELECT ?,?+i,?,?,?+i*1000000,?+i*1000000,1000000000,?+i*1000000+10000,10000000,'on_time' FROM n`,
			runID.Bytes(), index*3600, target.ID, endpoint, from.UnixMicro(), from.UnixMicro(), from.UnixMicro()); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	for _, target := range targets {
		for at := from.Truncate(time.Minute).Add(time.Minute); at.Before(to.Truncate(time.Minute)); at = at.Add(time.Minute) {
			if err := db.recomputePingMinute(ctx, dirtyKey{kind: "ping", entityID: target.ID, resolution: 60, bucketUS: at.UnixMicro()}, to); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		result, err := db.Overview(queryCtx, to, time.Hour)
		cancel()
		if err != nil || len(result.Targets) != 200 || result.Targets[0].Recent.Sent != 3600 {
			b.Fatalf("overview: targets=%d err=%v", len(result.Targets), err)
		}
	}
}
