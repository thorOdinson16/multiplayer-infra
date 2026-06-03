package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thorOdinson16/multiplayer-infra/services/matchmaking-service/internal/consumer"
	"github.com/thorOdinson16/multiplayer-infra/services/matchmaking-service/internal/deadletter"
	"github.com/thorOdinson16/multiplayer-infra/services/matchmaking-service/internal/matcher"
	"github.com/thorOdinson16/multiplayer-infra/services/matchmaking-service/internal/provisioner"
	"github.com/thorOdinson16/multiplayer-infra/services/matchmaking-service/internal/registry"
)

func main() {
	// Configuration
	rabbitmqURL := getEnv("RABBITMQ_URL", "amqp://admin:password@rabbitmq.infra.svc.cluster.local:5672/")
	etcdEndpoint := getEnv("ETCD_ENDPOINT", "etcd.infra.svc.cluster.local:2379")
	namespace := getEnv("NAMESPACE", "game-platform")
	serverPort := getEnv("SERVER_PORT", "8080")

	// Initialize matchmaker
	config := matcher.DefaultConfig()
	matchmaker := matcher.NewMatcher(config)

	// Initialize RabbitMQ consumer
	rmqConsumer, err := consumer.NewRabbitMQConsumer(rabbitmqURL, "matchmaking.requests", matchmaker)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer rmqConsumer.Close()

	if err := rmqConsumer.Start(); err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	// Initialize dead-letter handler
	expiryHandler, err := deadletter.NewExpiryHandler(rabbitmqURL)
	if err != nil {
		log.Printf("Warning: Failed to connect expiry handler: %v", err)
	} else {
		defer expiryHandler.Close()
		expiryHandler.ProcessExpired()
	}

	// Initialize etcd registry
	etcdReg, err := registry.NewEtcdRegistry([]string{etcdEndpoint})
	if err != nil {
		log.Printf("Warning: Failed to connect to etcd: %v", err)
	} else {
		defer etcdReg.Close()
		etcdReg.StartRoomWatcher()
	}

	// Initialize K8s provisioner
	k8sProv, err := provisioner.NewK8sProvisioner(namespace)
	if err != nil {
		log.Printf("Warning: Failed to create K8s provisioner: %v (running outside cluster?)", err)
	}

	// Process assembled lobbies
	go processLobbies(matchmaker, etcdReg, k8sProv)

	// Start matchmaking loop
	go matchmaker.Run()
	defer matchmaker.Stop()

	// HTTP server for metrics and health
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if rmqConsumer.IsBrokerAvailable() {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("DEGRADED: RabbitMQ unavailable"))
		}
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("READY"))
	})
	queueHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if !rmqConsumer.IsBrokerAvailable() {
			http.Error(w, "RabbitMQ unavailable", http.StatusServiceUnavailable)
			return
		}

		var req consumer.MatchmakingMessage
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if req.PlayerID == "" || req.Username == "" {
			http.Error(w, "playerId and username required", http.StatusBadRequest)
			return
		}
		if req.Timestamp == "" {
			req.Timestamp = time.Now().UTC().Format(time.RFC3339)
		}

		if err := rmqConsumer.PublishRequest(req); err != nil {
			if errors.Is(err, consumer.ErrBrokerUnavailable) {
				http.Error(w, "RabbitMQ unavailable", http.StatusServiceUnavailable)
				return
			}
			http.Error(w, "failed to enqueue matchmaking request", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"status": "queued"})
	}
	mux.HandleFunc("/matchmaking/queue", queueHandler)
	mux.HandleFunc("/queue", queueHandler)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("# HELP matchmaking_queue_length Current queue length\n"))
		w.Write([]byte("# TYPE matchmaking_queue_length gauge\n"))
		w.Write([]byte(fmt.Sprintf("matchmaking_queue_length %d\n", matchmaker.QueueLength())))
		w.Write([]byte("# HELP matchmaking_broker_unavailable RabbitMQ broker unavailable status\n"))
		w.Write([]byte("# TYPE matchmaking_broker_unavailable gauge\n"))
		brokerUnavailable := 0
		if !rmqConsumer.IsBrokerAvailable() {
			brokerUnavailable = 1
		}
		w.Write([]byte(fmt.Sprintf("matchmaking_broker_unavailable %d\n", brokerUnavailable)))
	})

	go func() {
		log.Printf("Matchmaking Service starting on port %s", serverPort)
		if err := http.ListenAndServe(":"+serverPort, mux); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down Matchmaking Service...")
}

func processLobbies(m *matcher.Matcher, reg *registry.EtcdRegistry, prov *provisioner.K8sProvisioner) {
	for lobby := range m.Lobbies() {
		log.Printf("Lobby assembled: %d players", len(lobby))

		// Check for available room in etcd
		var room *registry.RoomInfo
		var err error
		if reg != nil {
			room, err = reg.GetAvailableRoom()
		}
		if err != nil || room == nil {
			// No available room — provision new one (FR-MM-05)
			matchID := generateMatchID()
			if prov != nil {
				var playerIDs []string
				for _, p := range lobby {
					playerIDs = append(playerIDs, p.PlayerID)
				}
				if err := prov.CreateGameRoom(matchID, playerIDs); err != nil {
					log.Printf("Failed to create game room: %v", err)
					continue
				}
			}
			if reg != nil {
				reg.RegisterRoom(matchID, "provisioning")
			}
		}

		// Notify players (will be handled by Notification Service)
		for _, player := range lobby {
			log.Printf("Player %s matched", player.PlayerID)
		}
	}
}

func generateMatchID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
