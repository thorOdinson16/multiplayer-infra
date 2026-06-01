package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
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
	kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka.infra.svc.cluster.local:9092")
	redisAddr := getEnv("REDIS_ADDR", "redis.infra.svc.cluster.local:6379")
	maxPlayers := getEnvInt("MAX_PLAYERS", 8)
	tickRate := getEnvInt("TICK_RATE", 20)
	serverPort := getEnv("SERVER_PORT", "8080")

	// Initialize game state
	gameState := game.NewGameState(matchID, maxPlayers)

	// Initialize Raft
	raftService := getEnv("RAFT_SERVICE", "game-room-server-headless")
	raftNamespace := getEnv("RAFT_NAMESPACE", "game-platform")
	
	// Determine if this pod should bootstrap (only pod-0)
	podName := getEnv("POD_NAME", nodeID)
	isPodZero := strings.HasSuffix(podName, "-0")
	
	log.Printf("Raft configuration: nodeID=%s, isPodZero=%v, podName=%s", nodeID, isPodZero, podName)
	
	raftNode, err := raft.NewRaftNode(raft.Config{
		NodeID:    nodeID,
		BindAddr:  bindAddr,
		DataDir:   dataDir,
		Bootstrap: isPodZero,  // Only pod-0 bootstraps
		Service:   raftService,
		Namespace: raftNamespace,
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
	if err := kafkaPub.PublishMatchStart(ctx, matchID); err != nil {
		log.Printf("Warning: Failed to publish match start event: %v", err)
	}
	gameState.SetStatus("running")

	// Initialize spectator ring buffer
	specBuffer := spectator.NewRingBuffer(30, tickRate)
	spectators := make(map[string]*websocket.Conn)
	stopSpectator := make(chan struct{})
	go specBuffer.FlushLoop(spectators, stopSpectator)

	// Initialize broadcaster
	broadcaster := game.NewBroadcaster()

	// WebSocket handler - PASS THE BROADCASTER
	wsHandler := ws.NewHandler(gameState, func(event *game.InputEvent) {
		// Process input through Raft if leader
		if raftNode.IsLeader() {
			if err := raftNode.ApplyInput(event); err != nil {
				log.Printf("Failed to apply input to Raft: %v", err)
			}
		} else {
			log.Printf("Ignoring input - not leader (current leader: %s)", raftNode.LeaderAddress())
		}
	}, broadcaster)

	// Tick loop with broadcast and Kafka publish
	tickLoop := game.NewTickLoop(gameState, tickRate, func(tick uint64, state *game.GameState) {
		// Broadcast state to players (FR-GR-04)
		broadcaster.BroadcastToPlayers(state)

		// Enqueue for spectators (ADR-07)
		specBuffer.Enqueue(tick, state)

		// Publish to Kafka every 5 ticks to reduce load
		if tick%5 == 0 {
			if err := kafkaPub.PublishTickEvent(ctx, matchID, tick, state); err != nil {
				log.Printf("Failed to publish tick event: %v", err)
			}
		}

		// Save state to Redis every 10 ticks
		if tick%10 == 0 && redisStore != nil {
			if err := redisStore.SaveState(ctx, matchID, state, 10*time.Minute); err != nil {
				log.Printf("Failed to save state to Redis: %v", err)
			}
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
	
	// Add spectator endpoint
	mux.HandleFunc("/spectate/", func(w http.ResponseWriter, r *http.Request) {
		spectatorID := r.URL.Query().Get("spectatorId")
		matchIDParam := r.URL.Query().Get("matchId")
		if spectatorID == "" || matchIDParam == "" {
			http.Error(w, "spectatorId and matchId required", http.StatusBadRequest)
			return
		}
		wsHandler.AddSpectator(w, r, spectatorID, matchIDParam)
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
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{
			"matchId": "` + matchID + `",
			"nodeId": "` + nodeID + `",
			"isLeader": ` + strconv.FormatBool(raftNode.IsLeader()) + `,
			"playerCount": ` + strconv.Itoa(broadcaster.PlayerCount()) + `,
			"tickRate": ` + strconv.Itoa(tickRate) + `
		}`))
	})

	server := &http.Server{
		Addr:    ":" + serverPort,
		Handler: mux,
	}

	go func() {
		log.Printf("Game Room Server starting on port %s (match: %s)", serverPort, matchID)
		log.Printf("Node ID: %s, Leader: %v", nodeID, raftNode.IsLeader())
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
	if err := kafkaPub.PublishMatchEnd(ctx, matchID); err != nil {
		log.Printf("Warning: Failed to publish match end event: %v", err)
	}
	gameState.SetStatus("finished")
	broadcaster.CloseAll()

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