package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/thorOdinson16/multiplayer-infra/services/replay-service/internal/archive"
	"github.com/thorOdinson16/multiplayer-infra/services/replay-service/internal/checkpoint"
)

// Handler handles replay API requests
type Handler struct {
	archiver   *archive.Archiver
	checkpoint *checkpoint.Writer
}

// NewHandler creates a new API handler
func NewHandler(archiver *archive.Archiver, cp *checkpoint.Writer) *Handler {
	return &Handler{
		archiver:   archiver,
		checkpoint: cp,
	}
}

// GetReplay handles GET /replay/{matchId} (FR-RP-05)
func (h *Handler) GetReplay(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	matchID := vars["matchId"]

	speed := 1.0
	if speedStr := r.URL.Query().Get("speed"); speedStr != "" {
		if s, err := strconv.ParseFloat(speedStr, 64); err == nil {
			speed = s
		}
	}

	events, err := h.archiver.GetReplay(r.Context(), matchID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{
			"error":   "replay_not_found",
			"message": "Replay not found for this match",
		})
		return
	}

	response := map[string]interface{}{
		"matchId":    matchID,
		"events":     events,
		"speed":      speed,
		"totalTicks": len(events),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// SeekReplay handles GET /replay/{matchId}/seek?tick={n} (FR-RP-02)
func (h *Handler) SeekReplay(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	matchID := vars["matchId"]

	tickStr := r.URL.Query().Get("tick")
	if tickStr == "" {
		http.Error(w, "tick parameter required", http.StatusBadRequest)
		return
	}

	tick, err := strconv.ParseUint(tickStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid tick value", http.StatusBadRequest)
		return
	}

	cp, err := h.checkpoint.GetCheckpoint(matchID, tick)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{
			"error":   "checkpoint_not_found",
			"message": "No checkpoint available for this tick",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cp)
}