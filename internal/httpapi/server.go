package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"flameping/internal/store/sqlite"
	"flameping/internal/webui"
)

type Server struct {
	HTTP *http.Server
	db   *sqlite.DB
}

func New(address string, db *sqlite.DB, logger *slog.Logger) (*Server, error) {
	mux := http.NewServeMux()
	s := &Server{db: db}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		jsonResponse(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("GET /api/v1/status", s.status)
	mux.HandleFunc("GET /api/v1/targets", s.targets)
	mux.HandleFunc("GET /api/v1/targets/{target}/ping", s.ping)
	mux.HandleFunc("GET /api/v1/interfaces", s.interfaces)
	mux.HandleFunc("GET /api/v1/interfaces/{name}/series", s.interfaceSeries)
	mux.HandleFunc("GET /api/v1/targets/{target}/traces", s.traces)
	mux.HandleFunc("GET /api/v1/traces/{id}", s.trace)
	mux.HandleFunc("GET /api/v1/route-changes", s.routeChanges)
	dist, err := fs.Sub(webui.Dist, "dist")
	if err != nil {
		return nil, err
	}
	mux.Handle("GET /", http.FileServerFS(dist))
	s.HTTP = &http.Server{Addr: address, Handler: middleware(mux, logger), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	return s, nil
}

func (s *Server) Shutdown(ctx context.Context) error { return s.HTTP.Shutdown(ctx) }

func middleware(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self'; script-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
		started := time.Now()
		if strings.HasPrefix(r.URL.Path, "/api/") {
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			defer cancel()
			r = r.WithContext(ctx)
		}
		next.ServeHTTP(w, r)
		logger.Debug("http request", "method", r.Method, "path", r.URL.Path, "duration", time.Since(started))
	})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if !s.db.Ready() {
		jsonError(w, http.StatusServiceUnavailable, "storage writer is not ready")
		return
	}
	jsonResponse(w, http.StatusOK, map[string]string{"status": "ready"})
}
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	value, err := s.db.Status(r.Context())
	respond(w, value, err)
}
func (s *Server) targets(w http.ResponseWriter, r *http.Request) {
	value, err := s.db.Targets(r.Context(), time.Now())
	respond(w, value, err)
}
func (s *Server) interfaces(w http.ResponseWriter, r *http.Request) {
	value, err := s.db.Interfaces(r.Context())
	respond(w, value, err)
}

func (s *Server) ping(w http.ResponseWriter, r *http.Request) {
	from, to, maxPoints, err := seriesParams(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, err.Error())
		return
	}
	value, err := s.db.PingSeries(r.Context(), r.PathValue("target"), from, to, maxPoints)
	respond(w, value, err)
}
func (s *Server) interfaceSeries(w http.ResponseWriter, r *http.Request) {
	from, to, maxPoints, err := seriesParams(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, err.Error())
		return
	}
	value, err := s.db.InterfaceSeries(r.Context(), r.PathValue("name"), from, to, maxPoints)
	respond(w, value, err)
}
func (s *Server) traces(w http.ResponseWriter, r *http.Request) {
	limit, err := intParam(r, "limit", 50, 1, 500)
	if err != nil {
		jsonError(w, 400, err.Error())
		return
	}
	value, err := s.db.Traces(r.Context(), r.PathValue("target"), limit)
	respond(w, value, err)
}
func (s *Server) trace(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		jsonError(w, 400, "invalid trace id")
		return
	}
	value, err := s.db.Trace(r.Context(), id)
	respond(w, value, err)
}
func (s *Server) routeChanges(w http.ResponseWriter, r *http.Request) {
	limit, err := intParam(r, "limit", 100, 1, 500)
	if err != nil {
		jsonError(w, 400, err.Error())
		return
	}
	value, err := s.db.RouteChanges(r.Context(), r.URL.Query().Get("target"), limit)
	respond(w, value, err)
}

func seriesParams(r *http.Request) (time.Time, time.Time, int, error) {
	to := time.Now()
	from := to.Add(-6 * time.Hour)
	var err error
	if raw := r.URL.Query().Get("to"); raw != "" {
		to, err = parseTime(raw)
		if err != nil {
			return from, to, 0, errors.New("invalid to time")
		}
	}
	if raw := r.URL.Query().Get("from"); raw != "" {
		from, err = parseTime(raw)
		if err != nil {
			return from, to, 0, errors.New("invalid from time")
		}
	}
	maxPoints, err := intParam(r, "max_points", 1000, 1, 20000)
	if err != nil || !from.Before(to) || to.Sub(from) > 10*365*24*time.Hour {
		return from, to, 0, errors.New("invalid query range")
	}
	return from, to, maxPoints, nil
}
func parseTime(raw string) (time.Time, error) {
	if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.UnixMilli(ms), nil
	}
	return time.Parse(time.RFC3339, raw)
}
func intParam(r *http.Request, name string, fallback, low, high int) (int, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < low || value > high {
		return 0, errors.New("invalid " + name)
	}
	return value, nil
}
func respond(w http.ResponseWriter, value any, err error) {
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			jsonError(w, http.StatusGatewayTimeout, "query deadline exceeded")
		} else if errors.Is(err, sql.ErrNoRows) {
			jsonError(w, 404, "not found")
		} else {
			jsonError(w, 500, "internal error")
		}
		return
	}
	jsonResponse(w, 200, value)
}
func jsonResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func jsonError(w http.ResponseWriter, status int, message string) {
	jsonResponse(w, status, map[string]string{"error": strings.TrimSpace(message)})
}
