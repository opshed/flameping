package sqlite

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/opshed/flameping/internal/model"
)

func TestObsessRawHistoryPreservesFastProbesOnNormalIntervalTarget(t *testing.T) {
	db, _, target, runID := openTestDB(t)
	if target.Interval != 5*time.Second {
		t.Fatalf("test requires a normal 5s interval, got %v", target.Interval)
	}
	ctx := context.Background()
	minute := time.Now().Add(-2 * time.Minute).Truncate(time.Minute)
	start := minute.Add(10 * time.Second)
	endpoint := netip.MustParseAddr("127.0.0.1")
	events := make([]model.Event, 0, 19)
	for i := 0; i < 10; i++ {
		sentAt := start.Add(time.Duration(i) * 100 * time.Millisecond)
		probe := model.ProbeEvent{
			Key: model.ProbeKey{RunID: runID, Sequence: uint64(i + 1)}, TargetID: target.ID,
			Endpoint: endpoint, ScheduledAt: sentAt, SentAt: sentAt, Timeout: time.Second,
		}
		kind := model.EventProbeSent
		if i == 9 {
			kind = model.EventProbeSendError
			probe.SendErrorCode, probe.SendErrorMessage = "socket_write", "injected send error"
		}
		events = append(events, model.Event{Kind: kind, Probe: probe})
		if i < 8 {
			probe.RTT, probe.ReplyClass = 10*time.Millisecond, model.ReplyOnTime
			if i == 7 {
				probe.RTT, probe.ReplyClass = 1100*time.Millisecond, model.ReplyLate
			}
			probe.ReplyAt, probe.Responder = sentAt.Add(probe.RTT), endpoint
			events = append(events, model.Event{Kind: model.EventProbeReply, Probe: probe})
		}
	}
	events = append(events, model.Event{Kind: model.EventSchedulerGap, Gap: model.SchedulerGap{
		TargetID: target.ID, FirstScheduled: start.Add(time.Second), Interval: 100 * time.Millisecond, MissedCount: 1,
	}})
	if err := db.applyBatch(ctx, events, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}

	series, err := db.PingSeries(ctx, target.StableID, start, start.Add(1100*time.Millisecond), 20)
	if err != nil {
		t.Fatal(err)
	}
	if series.BucketMS != 100 || series.ResolutionS != 1 || len(series.Points) != 11 {
		t.Fatalf("fast detail was collapsed to normal interval: bucket=%vms resolution=%ds points=%d", series.BucketMS, series.ResolutionS, len(series.Points))
	}
	for i, point := range series.Points {
		if point.TimeMS != start.Add(time.Duration(i)*100*time.Millisecond).UnixMilli() || point.Scheduled != 1 {
			t.Fatalf("fast point %d = %+v", i, point)
		}
	}
	assertObsessHistoryCounts(t, series)

	capped, err := db.PingSeries(ctx, target.StableID, start, start.Add(1100*time.Millisecond), 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(capped.Points) > 3 {
		t.Fatalf("fast history exceeded requested point cap: %d", len(capped.Points))
	}
	assertObsessHistoryCounts(t, capped)

	if err := db.recomputeDirty(ctx, 32, time.Now()); err != nil {
		t.Fatal(err)
	}
	broader, err := db.PingSeries(ctx, target.StableID, minute, minute.Add(time.Minute), 2)
	if err != nil {
		t.Fatal(err)
	}
	if broader.BucketMS != 60000 || broader.ResolutionS != 60 || len(broader.Points) != 1 {
		t.Fatalf("minute rollup changed: bucket=%vms resolution=%ds points=%d", broader.BucketMS, broader.ResolutionS, len(broader.Points))
	}
	assertObsessHistoryCounts(t, broader)

	targets, err := db.Targets(ctx, time.Now())
	if err != nil || len(targets) != 1 || targets[0].IntervalMS != 5000 {
		t.Fatalf("recording fast history changed configured interval: targets=%+v error=%v", targets, err)
	}
}

func assertObsessHistoryCounts(t *testing.T, series PingSeries) {
	t.Helper()
	var scheduled, attempted, sent, onTime, late, unanswered, sendErrors, missed, rtt int64
	for _, point := range series.Points {
		scheduled += point.Scheduled
		attempted += point.Attempted
		sent += point.Sent
		onTime += point.OnTime
		late += point.Late
		unanswered += point.Unanswered
		sendErrors += point.SendErrors
		missed += point.SchedulerMissed
		rtt += point.RTTCount
	}
	if scheduled != 11 || attempted != 10 || sent != 9 || onTime != 7 || late != 1 || unanswered != 1 || sendErrors != 1 || missed != 1 || rtt != 8 {
		t.Fatalf("counts: scheduled=%d attempted=%d sent=%d on_time=%d late=%d unanswered=%d send_errors=%d missed=%d rtt=%d", scheduled, attempted, sent, onTime, late, unanswered, sendErrors, missed, rtt)
	}
}

func TestObsessHistoryRoundsBucketWidthBeforeEnforcingPointCap(t *testing.T) {
	db, _, target, runID := openTestDB(t)
	ctx := context.Background()
	// A 1s span with seven requested points needs 166666.667us buckets.
	// Truncating that width to 166666us puts these queue-burst observations
	// in eight buckets. Round upward to storage precision before bucketing.
	const truncatedWidthUS int64 = 166666
	baseUS := time.Now().Add(-time.Minute).UnixMicro()
	startUS := baseUS - baseUS%truncatedWidthUS + truncatedWidthUS - 1
	start := time.UnixMicro(startUS)
	offsetsUS := []int64{0, 1, 166667, 333333, 499999, 666665, 833331, 999997}
	endpoint := netip.MustParseAddr("127.0.0.1")
	events := make([]model.Event, 0, 2*len(offsetsUS))
	for i, offset := range offsetsUS {
		sentAt := start.Add(time.Duration(offset) * time.Microsecond)
		probe := model.ProbeEvent{
			Key: model.ProbeKey{RunID: runID, Sequence: uint64(i + 1)}, TargetID: target.ID,
			Endpoint: endpoint, ScheduledAt: sentAt, SentAt: sentAt, Timeout: time.Second,
		}
		events = append(events, model.Event{Kind: model.EventProbeSent, Probe: probe})
		probe.ReplyAt, probe.RTT, probe.ReplyClass, probe.Responder = sentAt.Add(time.Microsecond), time.Microsecond, model.ReplyOnTime, endpoint
		events = append(events, model.Event{Kind: model.EventProbeReply, Probe: probe})
	}
	if err := db.applyBatch(ctx, events, make(map[model.ProbeKey]orphanReply)); err != nil {
		t.Fatal(err)
	}
	series, err := db.PingSeries(ctx, target.StableID, start, start.Add(time.Second), 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(series.Points) > 7 || series.BucketMS != 166.667 {
		t.Fatalf("bucket=%vms points=%d, want 166.667ms and at most 7 points", series.BucketMS, len(series.Points))
	}
	var scheduled, sent, onTime int64
	for _, point := range series.Points {
		scheduled += point.Scheduled
		sent += point.Sent
		onTime += point.OnTime
	}
	if scheduled != 8 || sent != 8 || onTime != 8 {
		t.Fatalf("rounded buckets lost samples: scheduled=%d sent=%d on_time=%d", scheduled, sent, onTime)
	}
}
