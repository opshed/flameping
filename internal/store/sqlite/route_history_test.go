package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/netip"
	"testing"
	"time"

	"flameping/internal/model"
)

func TestRouteHistoryBoundsCountsAndDrilldown(t *testing.T) {
	db, cfg, target, _ := openTestDB(t)
	ctx := context.Background()
	from := time.Unix(1_750_000_000, 0)
	to := from.Add(5 * time.Second)
	routeA := `[{"ttl":1,"responders":["192.0.2.1"]}]`
	routeB := `[{"ttl":1,"responders":["192.0.2.2"]}]`
	insert := func(start, end time.Duration, status, signature string, reached bool) int64 {
		t.Helper()
		hop := 0
		if reached {
			hop = 3
		}
		trace := model.TraceResult{TargetID: target.ID, Endpoint: netip.MustParseAddr("127.0.0.1"),
			Method: "paris-udp", FlowID: "test-flow", StartedAt: from.Add(start), EndedAt: from.Add(end),
			Status: status, Reached: reached, ReachedHop: hop, Signature: signature}
		if status == "error" {
			trace.ErrorDetail = "send failed"
		}
		if err := db.applyBatch(ctx, []model.Event{{Kind: model.EventTraceCompleted, Trace: trace}}, make(map[model.ProbeKey]orphanReply)); err != nil {
			t.Fatal(err)
		}
		var id int64
		if err := db.readers.QueryRowContext(ctx, `SELECT MAX(id) FROM trace_runs`).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	// A confirmation at the left boundary belongs in the range even though its
	// linked trace began outside it. At the right boundary it belongs only to the
	// next range. Sub-millisecond observation boundaries remain exact in SQL.
	insert(-2*time.Second, -1900*time.Millisecond, "completed", routeA, true)
	insert(-time.Second, -900*time.Millisecond, "completed", routeB, true)
	oldID := insert(-100*time.Millisecond, 0, "completed", routeB, true)
	insert(-time.Microsecond, time.Millisecond, "completed", routeB, true)
	insert(0, 100*time.Millisecond, "completed", routeB, true)
	candidateID := insert(1500*time.Millisecond, 1600*time.Millisecond, "completed", routeB, false)
	insert(2*time.Second, 2100*time.Millisecond, "timed_out", routeB, false)
	insert(2200*time.Millisecond, 2300*time.Millisecond, "error", "", false)
	confirmingID := insert(3*time.Second, 3100*time.Millisecond, "completed", routeB, false)
	insert(3999*time.Millisecond, 4100*time.Millisecond, "completed", routeB, true)
	lastID := insert(5*time.Second-time.Microsecond, 5*time.Second, "completed", routeB, true)
	insert(5*time.Second, 5100*time.Millisecond, "completed", routeB, true)

	// Other targets must not contribute to the selected target's aggregates.
	cfg.Targets[0].ID = "other"
	others, err := db.SyncTargets(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.applyBatch(ctx, []model.Event{{Kind: model.EventTraceCompleted, Trace: model.TraceResult{
		TargetID: others[0].ID, Endpoint: netip.MustParseAddr("127.0.0.1"), Method: "classic-udp",
		StartedAt: from, EndedAt: from.Add(time.Second), Status: "error",
	}}}, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}

	history, err := db.RouteHistory(ctx, target.StableID, from, to, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := RouteHistoryCounts{Traces: 7, Reached: 3, Unreached: 2, Errors: 2, Changes: 2}
	if history.Totals != want || history.FromMS != from.UnixMilli() || history.ToMS != to.UnixMilli() || history.BucketMS != 1667 {
		t.Fatalf("history totals or bounds = %+v", history)
	}
	wantBuckets := []RouteHistoryCounts{
		{Traces: 2, Reached: 1, Unreached: 1, Changes: 1},
		{Traces: 3, Unreached: 1, Errors: 2, Changes: 1},
		{Traces: 2, Reached: 2},
	}
	if len(history.Buckets) != len(wantBuckets) {
		t.Fatalf("buckets = %+v", history.Buckets)
	}
	for i, want := range wantBuckets {
		bucket := history.Buckets[i]
		if bucket.RouteHistoryCounts != want || bucket.FromMS != from.UnixMilli()+int64(i)*1667 ||
			bucket.ToMS != min(from.UnixMilli()+int64(i+1)*1667, to.UnixMilli()) {
			t.Fatalf("bucket %d = %+v, want counts %+v", i, bucket, want)
		}
	}
	if !history.TracesTruncated || !history.ChangesTruncated || len(history.Traces) != 1 || len(history.Changes) != 1 {
		t.Fatalf("detail caps = %+v", history)
	}
	if trace := history.Traces[0]; trace.ID != lastID || trace.Endpoint != "127.0.0.1" || trace.FlowID != "test-flow" {
		t.Fatalf("latest trace = %+v", trace)
	}
	change := history.Changes[0]
	if change.FirstSeenMS != from.Add(1500*time.Millisecond).UnixMilli() || change.ConfirmedMS != from.Add(3100*time.Millisecond).UnixMilli() ||
		change.Endpoint != "127.0.0.1" || change.Method != "paris-udp" || change.FlowID != "test-flow" ||
		change.OldTraceID == nil || *change.OldTraceID != oldID || change.CandidateTraceID == nil || *change.CandidateTraceID != candidateID ||
		change.ConfirmingTraceID == nil || *change.ConfirmingTraceID != confirmingID || change.OldReachedHop != 3 || change.NewReachedHop != 0 {
		t.Fatalf("change detail = %+v", change)
	}
	if string(change.OldRoute) != routeB || string(change.NewRoute) != routeB {
		t.Fatalf("route snapshots = %+v", change)
	}
	detail, err := db.Trace(ctx, *change.CandidateTraceID)
	if err != nil || detail.ID != candidateID || detail.Endpoint != "127.0.0.1" || detail.FlowID != "test-flow" {
		t.Fatalf("linked detail = %+v, error %v", detail, err)
	}

	selected := history.Buckets[1]
	drilldown, err := db.RouteHistory(ctx, target.StableID, time.UnixMilli(selected.FromMS), time.UnixMilli(selected.ToMS), 3, 500)
	if err != nil {
		t.Fatal(err)
	}
	if drilldown.Totals != selected.RouteHistoryCounts || drilldown.TracesTruncated || drilldown.ChangesTruncated ||
		len(drilldown.Traces) != 3 || len(drilldown.Changes) != 1 || drilldown.Changes[0].FirstSeenMS >= selected.FromMS {
		t.Fatalf("bucket drilldown = %+v", drilldown)
	}
	next, err := db.RouteHistory(ctx, target.StableID, to, to.Add(time.Second), 1, 500)
	if err != nil || next.Totals.Traces != 1 || next.Totals.Changes != 1 {
		t.Fatalf("adjacent range = %+v, error %v", next, err)
	}
}

func TestRouteHistoryEmptyBinsAndValidation(t *testing.T) {
	db, _, target, _ := openTestDB(t)
	ctx := context.Background()
	from := time.Unix(1_750_000_000, 0)
	history, err := db.RouteHistory(ctx, target.StableID, from, from.Add(2*time.Millisecond), 300, 500)
	if err != nil {
		t.Fatal(err)
	}
	if len(history.Buckets) != 2 || history.BucketMS != 1 || history.Totals != (RouteHistoryCounts{}) ||
		history.Traces == nil || history.Changes == nil || history.TracesTruncated || history.ChangesTruncated {
		t.Fatalf("empty history = %+v", history)
	}
	for _, bucket := range history.Buckets {
		if bucket.RouteHistoryCounts != (RouteHistoryCounts{}) || bucket.ToMS-bucket.FromMS != 1 {
			t.Fatalf("empty bucket = %+v", bucket)
		}
	}
	encoded, err := json.Marshal(history)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatal(err)
	}
	if _, ok := value["traces"].([]any); !ok {
		t.Fatalf("empty traces do not encode as an array: %s", encoded)
	}
	if _, ok := value["changes"].([]any); !ok {
		t.Fatalf("empty changes do not encode as an array: %s", encoded)
	}
	for _, args := range []struct {
		span             time.Duration
		maxPoints, limit int
	}{
		{time.Second, 0, 1}, {time.Second, 301, 1}, {time.Second, 1, 0}, {time.Second, 1, 501},
		{0, 1, 1}, {-time.Second, 1, 1}, {time.Microsecond, 1, 1}, {11 * 365 * 24 * time.Hour, 1, 1},
	} {
		if _, err := db.RouteHistory(ctx, target.StableID, from, from.Add(args.span), args.maxPoints, args.limit); err == nil {
			t.Errorf("accepted invalid arguments %+v", args)
		}
	}
	if _, err := db.RouteHistory(ctx, "missing", from, from.Add(time.Second), 1, 1); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unknown target error = %v", err)
	}
}

func TestRouteHistoryPreservesEventSnapshotsWithoutRawTraces(t *testing.T) {
	db, _, target, _ := openTestDB(t)
	ctx := context.Background()
	from := time.Unix(1_750_000_000, 0)
	for i, route := range []string{
		`[{"ttl":2,"responders":["192.0.2.1"]}]`,
		`[{"ttl":2,"responders":["192.0.2.2","192.0.2.3"]}]`,
		`[{"ttl":2,"responders":["192.0.2.2","192.0.2.3"]}]`,
	} {
		trace := model.TraceResult{TargetID: target.ID, Endpoint: netip.MustParseAddr("127.0.0.1"), Method: "classic-udp", FlowID: "flow",
			StartedAt: from.Add(time.Duration(i) * time.Second), EndedAt: from.Add(time.Duration(i)*time.Second + time.Millisecond), Status: "completed", Signature: route}
		if err := db.applyBatch(ctx, []model.Event{{Kind: model.EventTraceCompleted, Trace: trace}}, make(map[model.ProbeKey]orphanReply)); err != nil {
			t.Fatal(err)
		}
	}
	// Snapshot signatures remain useful if references have been cleared. Normal
	// retention protects linked runs; this also exercises legacy/missing links.
	if _, err := db.writer.ExecContext(ctx, `DELETE FROM trace_runs`); err != nil {
		t.Fatal(err)
	}
	history, err := db.RouteHistory(ctx, target.StableID, from, from.Add(3*time.Second), 3, 10)
	if err != nil {
		t.Fatal(err)
	}
	if history.Totals.Traces != 0 || history.Totals.Changes != 1 || len(history.Changes) != 1 || len(history.Traces) != 0 {
		t.Fatalf("history with missing raw traces = %+v", history)
	}
	change := history.Changes[0]
	if change.OldTraceID != nil || change.CandidateTraceID != nil || change.ConfirmingTraceID != nil || change.Method != "" || change.FlowID != "" ||
		change.Endpoint != "127.0.0.1" || string(change.NewRoute) != `[{"ttl":2,"responders":["192.0.2.2","192.0.2.3"]}]` {
		t.Fatalf("snapshot with missing traces = %+v", change)
	}
}
