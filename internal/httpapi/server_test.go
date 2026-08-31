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

	"flameping/internal/config"
	"flameping/internal/store/sqlite"
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
