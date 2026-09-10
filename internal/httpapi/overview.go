package httpapi

import (
	"net/http"
	"time"

	"flameping/internal/obsess"
	"flameping/internal/store/sqlite"
)

func (s *Server) overview(w http.ResponseWriter, r *http.Request) {
	window := 15 * time.Minute
	switch r.URL.Query().Get("window") {
	case "", "15m":
	case "5m":
		window = 5 * time.Minute
	case "1h":
		window = time.Hour
	default:
		jsonError(w, http.StatusBadRequest, "invalid overview window; use 5m, 15m, or 1h")
		return
	}
	value, err := s.db.Overview(r.Context(), time.Now(), window)
	if err != nil {
		respond(w, nil, err)
		return
	}
	type target struct {
		sqlite.OverviewTarget
		Obsess *obsess.Status `json:"obsess,omitempty"`
	}
	result := struct {
		sqlite.Overview
		Targets []target `json:"targets"`
	}{Overview: value, Targets: make([]target, 0, len(value.Targets))}
	for _, item := range value.Targets {
		t := target{OverviewTarget: item}
		if s.obsess != nil {
			if state, ok := s.obsess.Snapshot(item.ID); ok && state.Enabled {
				t.Obsess = &state
			}
		}
		result.Targets = append(result.Targets, t)
	}
	// The browser explicitly retains prior observations when refresh fails.
	w.Header().Set("Cache-Control", "no-store")
	respond(w, result, nil)
}
