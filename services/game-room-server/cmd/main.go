package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/game"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/kafka"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/raft"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/redis"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/spectator"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/ws"
)

func main() {
	// Configuration
	matchID := getEnv("MATCH_ID", uuid.New().String())
	nodeID := getEnv("NODE_ID", "game-room-0")
	bindAddr := getEnv("RAFT_BIND", "0.0.0.0:7000")
	dataDir := getEnv("RAFT_DATA_DIR", "/var/lib/raft")
	bootstrap := getEnvBool("RAFT_BOOTSTRAP", true)
	kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka.infra.svc.cluster.local:9092")
	redisAddr := getEnv("REDIS_ADDR", "redis.infra.svc.cluster.local:6379")
	maxPlayers := getEnvInt("MAX_PLAYERS", 8)
	tickRate := getEnvInt("TICK_RATE", 20)
	serverPort := getEnv("SERVER_PORT", "8080")

	// Initialize game state
	gameState := game.NewGameState(matchID, maxPlayers)

	// Initialize Raft
	raftNode, err := raft.NewRaftNode(raft.Config{
		NodeID:    nodeID,
		BindAddr:  bindAddr,
		DataDir:   dataDir,
		Bootstrap: bootstrap,
	}, gameState)
	if err != nil {
		log.Fatalf("Failed to start Raft: %v", err)
	}
	defer raftNode.Shutdown()

	// Initialize Kafka publisher
	kafkaPub := kafka.NewPublisher([]string{kafkaBrokers})
	defer kafkaPub.Close()

	// Initialize Redis
	redisStore, err := redis.NewStateStore(redisAddr, "")
	if err != nil {
		log.Printf("Warning: Failed to connect to Redis: %v", err)
	}
	defer redisStore.Close()

	// Publish match start
	ctx := context.Background()
	kafkaPub.PublishMatchStart(ctx, matchID)
	gameState.SetStatus("running")

	// Initialize spectator ring buffer
	specBuffer := spectator.NewRingBuffer(30, tickRate)
	spectators := make(map[string]*websocket.Conn)
	stopSpectator := make(chan struct{})
	go specBuffer.FlushLoop(spectators, stopSpectator)

	// Initialize broadcaster
	broadcaster := game.NewBroadcaster()

	// WebSocket handler
	wsHandler := ws.NewHandler(gameState, func(event *game.InputEvent) {
		// Process input through Raft if leader
		if raftNode.IsLeader() {
			raftNode.ApplyInput(event)
		}
	})

	// Tick loop with broadcast and Kafka publish
	tickLoop := game.NewTickLoop(gameState, tickRate, func(tick uint64, state *game.GameState) {
		// Broadcast state to players (FR-GR-04)
		broadcaster.BroadcastToPlayers(state)

		// Enqueue for spectators (ADR-07)
		specBuffer.Enqueue(tick, state)

		// Publish to Kafka every 5 ticks to reduce load
		if tick%5 == 0 {
			kafkaPub.PublishTickEvent(ctx, matchID, tick, state)
		}

		// Save state to Redis every 10 ticks
		if tick%10 == 0 && redisStore != nil {
			redisStore.SaveState(ctx, matchID, state, 10*time.Minute)
		}
	})
	tickLoop.Start()
	defer tickLoop.Stop()

	// HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/game/", func(w http.ResponseWriter, r *http.Request) {
		playerID := r.URL.Query().Get("playerId")
		username := r.URL.Query().Get("username")
		if playerID == "" || username == "" {
			http.Error(w, "playerId and username required", http.StatusBadRequest)
			return
		}
		wsHandler.HandleConnection(w, r, playerID, username)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if raftNode.IsLeader() {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("READY"))
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("FOLLOWER"))
		}
	})

	server := &http.Server{
		Addr:    ":" + serverPort,
		Handler: mux,
	}

	go func() {
		log.Printf("Game Room Server starting on port %s (match: %s)", serverPort, matchID)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Game Room Server...")
	stopSpectator <- struct{}{}
	kafkaPub.PublishMatchEnd(ctx, matchID)
	gameState.SetStatus("finished")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server.Shutdown(shutdownCtx)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return defaultValue
}