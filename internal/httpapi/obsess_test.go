package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"flameping/internal/eventbus"
	"flameping/internal/model"
	"flameping/internal/obsess"
	"flameping/internal/store/sqlite"
)

type liveObsessProvider struct {
	states map[int64]obsess.Status
}

func (p *liveObsessProvider) Snapshot(id int64) (obsess.Status, bool) {
	state, ok := p.states[id]
	return state, ok
}

func obsessTargetResponse(t *testing.T, server *Server) (sqlite.TargetSummary, *obsess.Status, map[string]json.RawMessage) {
	t.Helper()
	response := httptest.NewRecorder()
	server.HTTP.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/targets", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var targets []json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &targets); err != nil || len(targets) != 1 {
		t.Fatalf("targets = %s, error = %v", response.Body.String(), err)
	}
	var target struct {
		sqlite.TargetSummary
		Obsess *obsess.Status `json:"obsess"`
	}
	if err := json.Unmarshal(targets[0], &target); err != nil {
		t.Fatalf("invalid target JSON: %s: %v", targets[0], err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(targets[0], &fields); err != nil {
		t.Fatal(err)
	}
	return target.TargetSummary, target.Obsess, fields
}

func TestObsessTargetAPIPreservesMeasurementsAndRefreshesLiveStatus(t *testing.T) {
	server := testServer(t)
	ctx := context.Background()
	targets, err := server.db.Targets(ctx, time.Now())
	if err != nil || len(targets) != 1 {
		t.Fatalf("targets=%+v, error=%v", targets, err)
	}
	targetID := targets[0].ID
	runID := model.RunID{42}
	if err := server.db.StartRun(ctx, runID, "obsess-api-test", "test"); err != nil {
		t.Fatal(err)
	}
	sentAt := time.Now().Add(-time.Second)
	probe := model.ProbeEvent{
		Key: model.ProbeKey{RunID: runID, Sequence: 1}, TargetID: targetID,
		Endpoint: netip.MustParseAddr("127.0.0.1"), ScheduledAt: sentAt, SentAt: sentAt, Timeout: time.Second,
	}
	bus := eventbus.New(8)
	if err := bus.Publish(ctx, model.Event{Kind: model.EventProbeSent, Probe: probe}); err != nil {
		t.Fatal(err)
	}
	probe.ReplyAt, probe.RTT, probe.ReplyClass, probe.Responder = sentAt.Add(12*time.Millisecond), 12*time.Millisecond, model.ReplyOnTime, probe.Endpoint
	if err := bus.Publish(ctx, model.Event{Kind: model.EventProbeReply, Probe: probe}); err != nil {
		t.Fatal(err)
	}
	bus.Close()
	if err := server.db.RunWriter(ctx, bus); err != nil {
		t.Fatal(err)
	}

	baseline, threshold := 20.0, 30.0
	since := sentAt.Add(-10 * time.Second).UnixMilli()
	provider := &liveObsessProvider{states: map[int64]obsess.Status{
		targetID: {Enabled: true, Monitoring: true, State: "obsessing", IntervalMS: 100, Reason: "latency", SinceMS: &since,
			BaselineMS: &baseline, ThresholdMS: &threshold, BaselineSamples: 12, HealthyForMS: 12500, RecoverAfterMS: 60000},
	}}
	server.SetObsessProvider(provider)
	target, status, _ := obsessTargetResponse(t, server)
	if target.IntervalMS != 5000 || target.State != "up" || target.LastRTTMS == nil || *target.LastRTTMS != 12 {
		t.Fatalf("live status changed persisted measurement fields: %+v", target)
	}
	if status == nil || !status.Enabled || status.State != "obsessing" || status.IntervalMS != 100 || status.BaselineMS == nil || *status.BaselineMS != 20 || status.ThresholdMS == nil || *status.ThresholdMS != 30 || status.SinceMS == nil || *status.SinceMS != since || status.HealthyForMS != 12500 {
		t.Fatalf("active status did not serialize: %+v", status)
	}

	provider.states[targetID] = obsess.Status{Enabled: true, Monitoring: true, State: "normal", IntervalMS: 5000, RecoverAfterMS: 60000}
	target, status, _ = obsessTargetResponse(t, server)
	if status == nil || status.State != "normal" || status.IntervalMS != 5000 || status.SinceMS != nil || status.ThresholdMS != nil || target.State != "up" {
		t.Fatalf("next request retained stale active status: target=%+v status=%+v", target, status)
	}

	provider.states[targetID] = obsess.Status{Enabled: false, State: "disabled"}
	_, status, fields := obsessTargetResponse(t, server)
	if _, exists := fields["obsess"]; exists || status != nil {
		t.Fatal("disabled status must be omitted from target JSON")
	}
	delete(provider.states, targetID)
	_, status, fields = obsessTargetResponse(t, server)
	if _, exists := fields["obsess"]; exists || status != nil {
		t.Fatal("missing status must be omitted from target JSON")
	}
}

func TestObsessTargetAPIOmitsStatusWithoutProvider(t *testing.T) {
	server := testServer(t)
	target, status, fields := obsessTargetResponse(t, server)
	if _, exists := fields["obsess"]; exists || status != nil || target.IntervalMS != 5000 || target.State != "unknown" {
		t.Fatalf("unexpected default target response: target=%+v status=%+v", target, status)
	}
}
