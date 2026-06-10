// services/game-room-server/cmd/main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/game"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/kafka"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/observability"
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
	etcdEndpoints := getEnv("ETCD_ENDPOINTS", "etcd.infra.svc.cluster.local:2379")
	maxPlayers := getEnvInt("MAX_PLAYERS", 8)
	tickRate := getEnvInt("TICK_RATE", 20)
	raftClusterSize := getEnvInt("RAFT_CLUSTER_SIZE", 3)
	serverPort := getEnv("SERVER_PORT", "8080")
	matchDuration := getEnvInt("MATCH_DURATION", 300) // 5 minutes default
	otelEndpoint := getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	tracingShutdown, err := observability.InitTracing(context.Background(), "game-room-server", otelEndpoint)
	if err != nil {
		log.Printf("Warning: Failed to initialize tracing: %v", err)
	} else {
		defer tracingShutdown(context.Background())
	}

	// Initialize game state
	gameState := game.NewGameState(matchID, maxPlayers)
	gameState.Duration = matchDuration

	// Initialize Raft
	raftService := getEnv("RAFT_SERVICE", "game-room-server-headless")
	raftNamespace := getEnv("RAFT_NAMESPACE", "game-platform")

	// Determine if this pod should bootstrap (only pod-0)
	podName := getEnv("POD_NAME", nodeID)
	isPodZero := strings.HasSuffix(podName, "-0")

	log.Printf("Raft configuration: nodeID=%s, isPodZero=%v, podName=%s", nodeID, isPodZero, podName)

	raftNode, err := raft.NewRaftNode(raft.Config{
		NodeID:        nodeID,
		BindAddr:      bindAddr,
		DataDir:       dataDir,
		Bootstrap:     isPodZero,
		Service:       raftService,
		Namespace:     raftNamespace,
		EtcdEndpoints: []string{etcdEndpoints},
		MatchID:       matchID,
		ClusterSize:   raftClusterSize,
	}, gameState)
	if err != nil {
		log.Fatalf("Failed to start Raft: %v", err)
	}
	defer raftNode.Shutdown()

	// Initialize Kafka publisher
	log.Printf("Initializing Kafka publisher with brokers: %v", kafkaBrokers)
	kafkaPub := kafka.NewPublisher([]string{kafkaBrokers})
	defer kafkaPub.Close()
	log.Printf("Kafka publisher initialized")

	// Initialize Redis
	redisStore, err := redis.NewStateStore(redisAddr, "")
	if err != nil {
		log.Printf("Warning: Failed to connect to Redis: %v", err)
	}
	if redisStore != nil {
		defer redisStore.Close()
	}

	// Publish match start
	ctx := context.Background()
	if err := kafkaPub.PublishMatchStart(ctx, matchID); err != nil {
		log.Printf("Warning: Failed to publish match start event: %v", err)
	} else {
		log.Printf("Published match start event for match %s", matchID)
	}
	gameState.SetStatus("running")

	// Track last activity time for empty room detection
	var emptyRoomTimer *time.Timer
	var emptyRoomTimerActive bool
	var tickMu sync.Mutex

	// Define room empty handler - finishes the room when no players
	roomEmptyHandler := func() {
		log.Printf("🏁 Room empty - all players left, finishing match %s", matchID)
		if gameState.GetStatus() == "running" {
			gameState.SetStatus("finished")
			// Publish match end
			if err := kafkaPub.PublishMatchEnd(ctx, matchID); err != nil {
				log.Printf("Warning: Failed to publish match end event: %v", err)
			}
		}
	}

	// Initialize broadcaster with empty room callback
	broadcaster := game.NewBroadcasterWithCallback(roomEmptyHandler)

	// Initialize spectator ring buffer
	specBuffer := spectator.NewRingBuffer(30, tickRate)
	stopSpectator := make(chan struct{})
	go specBuffer.FlushLoop(broadcaster, stopSpectator)

	// WebSocket handler
	matchTTL := time.Duration(matchDuration+60) * time.Second
	wsHandler := ws.NewHandler(gameState, func(event *game.InputEvent) {
		// Only process input if there are connected players
		if gameState.GetConnectedPlayerCount() == 0 {
			log.Printf("Skipping input - no players connected")
			return
		}
		// Process input through Raft if leader
		if raftNode.IsLeader() {
			if err := raftNode.ApplyInput(event); err != nil {
				log.Printf("Failed to apply input to Raft: %v", err)
			}
		} else {
			log.Printf("Ignoring input - not leader (current leader: %s)", raftNode.LeaderAddress())
		}
	}, broadcaster, func(playerID string) {
		if redisStore == nil {
			return
		}
		if err := redisStore.AddPlayerToMatch(ctx, matchID, playerID, matchTTL); err != nil {
			log.Printf("Failed to add player %s to Redis match membership: %v", playerID, err)
		}
	})

	// Track if tick loop is running
	var tickLoop *game.TickLoop
	var tickLoopRunning bool

	// Function to stop tick loop if running
	stopTickLoop := func() {
		tickMu.Lock()
		defer tickMu.Unlock()
		if tickLoopRunning && tickLoop != nil {
			log.Printf("Stopping tick loop")
			tickLoop.Stop()
			tickLoopRunning = false
		}
	}

	// Function to start tick loop if not running and there are players
	startTickLoopIfNeeded := func() {
		tickMu.Lock()
		shouldStart := !tickLoopRunning && gameState.GetConnectedPlayerCount() > 0 && gameState.GetStatus() == "running"
		if shouldStart {
			log.Printf("Starting tick loop - players connected")
			tickLoop = game.NewTickLoop(gameState, tickRate, func(tick uint64, state *game.GameState) {
				// Check if match should finish (duration reached)
				if state.IsMatchFinished() && state.GetStatus() == "running" {
					log.Printf("🏁 Match %s finished by leader (duration reached)", matchID)
					state.SetStatus("finished")
					stopTickLoop()
					return
				}

				// Check for empty room
				if state.GetConnectedPlayerCount() == 0 {
					tickMu.Lock()
					if !emptyRoomTimerActive {
						emptyRoomTimerActive = true
						emptyRoomTimer = time.AfterFunc(30*time.Second, func() {
							log.Printf("🏁 Room empty for 30 seconds - shutting down match %s", matchID)
							if gameState.GetStatus() == "running" {
								gameState.SetStatus("finished")
								if err := kafkaPub.PublishMatchEnd(ctx, matchID); err != nil {
									log.Printf("Warning: Failed to publish match end: %v", err)
								}
							}
							tickMu.Unlock() // unlock before calling stopTickLoop which will re-lock
							stopTickLoop()
							return
						})
					}
					tickMu.Unlock()
					return
				}

				// Reset empty room timer if we have players
				tickMu.Lock()
				if emptyRoomTimerActive {
					emptyRoomTimer.Stop()
					emptyRoomTimerActive = false
				}
				tickMu.Unlock()

				// Broadcast state to players (FR-GR-04)
				broadcaster.BroadcastToPlayers(state)

				// Enqueue for spectators (ADR-07)
				specBuffer.Enqueue(tick, state)

				// Debug: Log every 20 ticks
				if tick%20 == 0 {
					log.Printf("🔔 Tick %d - Players: %d (connected: %d)", tick, len(state.Players), state.GetConnectedPlayerCount())
				}

				// Publish to Kafka every 5 ticks to reduce load
				if tick%5 == 0 {
					if err := kafkaPub.PublishTickEvent(ctx, matchID, tick, state); err != nil {
						log.Printf("Failed to publish tick event: %v", err)
					}
				}

				// Publish telemetry every 100 ticks (5 seconds at 20 TPS)
				if tick%100 == 0 && state.GetConnectedPlayerCount() > 0 {
					log.Printf("📊 Publishing telemetry at tick %d, players: %d", tick, state.GetConnectedPlayerCount())
					for _, player := range state.Players {
						if player.Connected {
							if err := kafkaPub.PublishTelemetry(ctx, matchID, player.PlayerID, player.X, player.Y, "position_update"); err != nil {
								log.Printf("❌ Failed to publish telemetry for player %s: %v", player.PlayerID, err)
							}
						}
					}
				}

				// Save state to Redis every 10 ticks
				if tick%10 == 0 && redisStore != nil {
					if err := redisStore.SaveState(ctx, matchID, state, matchTTL); err != nil {
						log.Printf("Failed to save state to Redis: %v", err)
					}
				}
			})
			tickLoop.Start()
			tickLoopRunning = true
		}
		tickMu.Unlock()
	}

	// Start tick loop initially if there are players
	startTickLoopIfNeeded()

	// HTTP server
	mux := http.NewServeMux()

	// Leader endpoint for discovery
	mux.HandleFunc("/leader", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if raftNode.IsLeader() {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"isLeader": true,
				"nodeId":   nodeID,
				"address":  fmt.Sprintf("%s.%s.%s.svc.cluster.local:8080", nodeID, raftService, raftNamespace),
			})
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"isLeader": false,
				"leader":   raftNode.LeaderAddress(),
			})
		}
	})

	mux.HandleFunc("/game/", func(w http.ResponseWriter, r *http.Request) {
		playerID := r.URL.Query().Get("playerId")
		username := r.URL.Query().Get("username")
		if playerID == "" || username == "" {
			http.Error(w, "playerId and username required", http.StatusBadRequest)
			return
		}
		wsHandler.HandleConnection(w, r, playerID, username)
		// After connection, try to start tick loop
		startTickLoopIfNeeded()
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

	// Match status endpoint - allows checking if room is finished
	mux.HandleFunc("/match/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		tickMu.Lock()
		tlr := tickLoopRunning
		tickMu.Unlock()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"matchId":         matchID,
			"status":          gameState.GetStatus(),
			"playerCount":     gameState.GetConnectedPlayerCount(),
			"totalPlayers":    len(gameState.GetSnapshot().Players),
			"tick":            gameState.Tick,
			"tickLoopRunning": tlr,
		})
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
		tickMu.Lock()
		tlr := tickLoopRunning
		tickMu.Unlock()
		w.Write([]byte(`{
            "matchId": "` + matchID + `",
            "nodeId": "` + nodeID + `",
            "isLeader": ` + strconv.FormatBool(raftNode.IsLeader()) + `,
            "playerCount": ` + strconv.Itoa(broadcaster.PlayerCount()) + `,
            "tickRate": ` + strconv.Itoa(tickRate) + `,
            "status": "` + gameState.GetStatus() + `",
            "tickLoopRunning": ` + strconv.FormatBool(tlr) + `
        }`))
	})

	server := &http.Server{
		Addr:    ":" + serverPort,
		Handler: mux,
	}

	go func() {
		log.Printf("Game Room Server starting on port %s (match: %s)", serverPort, matchID)
		log.Printf("Node ID: %s, Leader: %v", nodeID, raftNode.IsLeader())
		log.Printf("Match duration: %d seconds", matchDuration)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Game Room Server...")

	// Publish match end if not already finished
	if !gameState.IsMatchFinished() {
		log.Printf("Match ending due to shutdown - publishing match end event")
		if err := kafkaPub.PublishMatchEnd(ctx, matchID); err != nil {
			log.Printf("Warning: Failed to publish match end event: %v", err)
		}
		gameState.SetStatus("finished")
	} else {
		log.Printf("Match already finished, skipping end event")
	}

	stopTickLoop()
	if emptyRoomTimerActive {
		emptyRoomTimer.Stop()
	}
	close(stopSpectator)
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
