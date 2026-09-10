package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/opshed/flameping/internal/config"
	"github.com/opshed/flameping/internal/store/sqlite"
)

func testServer(t *testing.T) *Server {
	t.Helper()
	cfg := config.Defaults()
	cfg.Storage.Path = filepath.Join(t.TempDir(), "api.db")
	cfg.Storage.MaxBytes = config.Bytes(128 << 20)
	db, err := sqlite.Open(context.Background(), cfg.Storage)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cfg.Targets = []config.TargetConfig{{ID: "loopback", Address: "127.0.0.1"}}
	cfg.Interfaces = []config.InterfaceConfig{{Name: "eth0", DisplayName: "Ethernet 0"}}
	if _, err := db.SyncTargets(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	server, err := New("127.0.0.1:0", db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func TestInterfaceAPIExposesMissingState(t *testing.T) {
	server := testServer(t)
	response := httptest.NewRecorder()
	server.HTTP.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/interfaces", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var value []struct {
		Name    string `json:"name"`
		Present bool   `json:"present"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	if len(value) != 1 || value[0].Name != "eth0" || value[0].Present {
		t.Fatalf("interfaces=%+v", value)
	}
}

func TestHealthAPIAndSecurityHeaders(t *testing.T) {
	server := testServer(t)
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()
	server.HTTP.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Content-Security-Policy") == "" {
		t.Fatal("missing CSP")
	}
}

func TestTargetAndSeriesAPI(t *testing.T) {
	server := testServer(t)
	for _, path := range []string{"/api/v1/targets", "/api/v1/targets/loopback/ping?max_points=100"} {
		response := httptest.NewRecorder()
		server.HTTP.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s status = %d: %s", path, response.Code, response.Body.String())
		}
		var value any
		if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
			t.Fatalf("%s invalid JSON: %v", path, err)
		}
	}
}

func TestSeriesRejectsInvalidRange(t *testing.T) {
	server := testServer(t)
	response := httptest.NewRecorder()
	server.HTTP.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/targets/loopback/ping?from=20&to=10", nil))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestRouteHistoryAPI(t *testing.T) {
	server := testServer(t)
	response := httptest.NewRecorder()
	server.HTTP.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		"/api/v1/targets/loopback/route-history?from=1000&to=1100&max_points=3&limit=1", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var history sqlite.RouteHistory
	if err := json.Unmarshal(response.Body.Bytes(), &history); err != nil {
		t.Fatal(err)
	}
	if history.FromMS != 1000 || history.ToMS != 1100 || history.BucketMS != 34 || len(history.Buckets) != 3 ||
		history.Totals != (sqlite.RouteHistoryCounts{}) || history.Traces == nil || history.Changes == nil {
		t.Fatalf("history = %+v", history)
	}
	response = httptest.NewRecorder()
	server.HTTP.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/targets/loopback/route-history", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("default range status = %d: %s", response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	server.HTTP.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/targets/missing/route-history", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing target status = %d: %s", response.Code, response.Body.String())
	}
}

func TestRouteHistoryAPIRejectsInvalidParameters(t *testing.T) {
	server := testServer(t)
	for _, query := range []string{
		"from=20&to=10", "from=20&to=20", "from=bad", "to=bad", "max_points=0", "max_points=301",
		"max_points=bad", "limit=0", "limit=501", "limit=bad", "from=0&to=999999999999",
		"from=9223372036854775806&to=9223372036854775807",
		"from=2025-01-01T00:00:00.0001Z&to=2025-01-01T00:00:00.0002Z",
	} {
		response := httptest.NewRecorder()
		server.HTTP.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/targets/loopback/route-history?"+query, nil))
		if response.Code != http.StatusBadRequest {
			t.Errorf("%s status = %d: %s", query, response.Code, response.Body.String())
		}
	}
}
