package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thorOdinson16/multiplayer-infra/services/reconnect-handler/internal/delta"
	"github.com/thorOdinson16/multiplayer-infra/services/reconnect-handler/internal/store"
)

func main() {
	redisAddr := getEnv("REDIS_ADDR", "redis.infra.svc.cluster.local:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "")
	serverPort := getEnv("SERVER_PORT", "8080")

	// Initialize Redis
	redisStore, err := store.NewRedisStore(redisAddr, redisPassword)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redisStore.Close()

	// HTTP handlers
	mux := http.NewServeMux()

	// Reconnect endpoint (FR-FT-05)
	mux.HandleFunc("/reconnect/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			MatchID  string `json:"matchId"`
			PlayerID string `json:"playerId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		if req.MatchID == "" || req.PlayerID == "" {
			http.Error(w, "matchId and playerId required", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		// Check if player is in the match (FR-FT-04)
		inMatch, err := redisStore.IsPlayerInMatch(ctx, req.MatchID, req.PlayerID)
		if err != nil || !inMatch {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{
				"error":   "player_not_found",
				"message": "Player not found in match or hold window expired",
			})
			return
		}

		// Get current match state
		currentState, err := redisStore.GetMatchState(ctx, req.MatchID)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{
				"error":   "match_not_found",
				"message": "Match state not available",
			})
			return
		}

		// Compute delta (FR-FT-05, FR-FT-06)
		payload, err := delta.ComputeDelta(currentState, req.PlayerID)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{
				"error":   "delta_error",
				"message": err.Error(),
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(payload)
	})

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("READY"))
	})

	server := &http.Server{
		Addr:    ":" + serverPort,
		Handler: mux,
	}

	go func() {
		log.Printf("Reconnect Handler starting on port %s", serverPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Reconnect Handler...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
