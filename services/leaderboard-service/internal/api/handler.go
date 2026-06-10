package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/thorOdinson16/multiplayer-infra/services/leaderboard-service/internal/store"
)

// Handler handles leaderboard API requests
type Handler struct {
	store *store.CouchbaseStore
}

// NewHandler creates a new API handler
func NewHandler(st *store.CouchbaseStore) *Handler {
	return &Handler{store: st}
}

// GetLeaderboard handles GET /leaderboard?window=daily|weekly|all (FR-LB-02)
func (h *Handler) GetLeaderboard(w http.ResponseWriter, r *http.Request) {
	window := r.URL.Query().Get("window")
	if window == "" {
		window = "all"
	}

	limitStr := r.URL.Query().Get("limit")
	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	entries, err := h.store.GetLeaderboard(window, limit)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "Failed to fetch leaderboard"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"window":  window,
		"entries": entries,
		"count":   len(entries),
	})
}

// GetPlayerStats handles GET /leaderboard/player/{id} (FR-LB-05)
func (h *Handler) GetPlayerStats(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	playerID := vars["id"]

	stats, err := h.store.GetPlayerStats(playerID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{
			"error":   "player_not_found",
			"message": "Player not found",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}
