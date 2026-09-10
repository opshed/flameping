package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"flameping/internal/obsess"
	"flameping/internal/store/sqlite"
)

func TestOverviewAPIWindowsAndLiveObsess(t *testing.T) {
	s := testServer(t)
	targets, err := s.db.Targets(context.Background(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	s.SetObsessProvider(&liveObsessProvider{states: map[int64]obsess.Status{targets[0].ID: {Enabled: true, State: "obsessing", IntervalMS: 100}}})
	for query, window := range map[string]int64{"": 15 * 60_000, "?window=5m": 5 * 60_000, "?window=15m": 15 * 60_000, "?window=1h": 60 * 60_000} {
		r := httptest.NewRecorder()
		s.HTTP.Handler.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/api/v1/overview"+query, nil))
		if r.Code != http.StatusOK || r.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s: %d %s", query, r.Code, r.Body.String())
		}
		var result struct {
			sqlite.Overview
			Targets []struct {
				sqlite.OverviewTarget
				Obsess *obsess.Status `json:"obsess"`
			} `json:"targets"`
		}
		if err := json.Unmarshal(r.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result.WindowMS != window || result.ToMS-result.FromMS != window || result.AsOfMS != result.ToMS || len(result.Targets) != 1 || result.Targets[0].Obsess == nil || result.Targets[0].Obsess.State != "obsessing" || result.Targets[0].Recent.DeadlineMissPct != nil || result.Interfaces == nil {
			t.Fatalf("overview contract: %+v", result)
		}
	}
}

func TestOverviewAPIRejectsUnboundedWindow(t *testing.T) {
	s := testServer(t)
	for _, query := range []string{"24h", "0", "1m", "garbage"} {
		r := httptest.NewRecorder()
		s.HTTP.Handler.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/api/v1/overview?window="+query, nil))
		if r.Code != http.StatusBadRequest {
			t.Fatalf("%s: %d %s", query, r.Code, r.Body.String())
		}
	}
}
