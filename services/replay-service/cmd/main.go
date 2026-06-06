package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorilla/mux"
	"github.com/thorOdinson16/multiplayer-infra/services/replay-service/internal/api"
	"github.com/thorOdinson16/multiplayer-infra/services/replay-service/internal/archive"
	"github.com/thorOdinson16/multiplayer-infra/services/replay-service/internal/checkpoint"
	"github.com/thorOdinson16/multiplayer-infra/services/replay-service/internal/consumer"
	"github.com/thorOdinson16/multiplayer-infra/services/replay-service/internal/observability"
)

func main() {
	kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka.infra.svc.cluster.local:9092")
	couchbaseHost := getEnv("COUCHBASE_HOST", "couchbase.infra.svc.cluster.local")
	couchbaseUser := getEnv("COUCHBASE_USER", "admin")
	couchbasePass := getEnv("COUCHBASE_PASSWORD", "password")
	minioEndpoint := getEnv("MINIO_ENDPOINT", "minio.infra.svc.cluster.local:9000")
	minioUser := getEnv("MINIO_USER", "minioadmin")
	minioPass := getEnv("MINIO_PASSWORD", "minioadmin")
	serverPort := getEnv("SERVER_PORT", "8080")
	otelEndpoint := getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	tracingShutdown, err := observability.InitTracing(context.Background(), "replay-service", otelEndpoint)
	if err != nil {
		log.Printf("Warning: Failed to initialize tracing: %v", err)
	} else {
		defer tracingShutdown(context.Background())
	}

	// Initialize checkpoint writer
	connStr := "couchbase://" + couchbaseHost
	cpWriter, err := checkpoint.NewWriter(connStr, couchbaseUser, couchbasePass)
	if err != nil {
		log.Printf("Warning: Failed to connect to Couchbase: %v", err)
	} else {
		defer cpWriter.Close()
	}

	// Initialize MinIO archiver
	arch, err := archive.NewArchiver(minioEndpoint, minioUser, minioPass, "replays")
	if err != nil {
		log.Printf("Warning: Failed to connect to MinIO: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consuming match.events
	eventConsumer := consumer.NewEventConsumer([]string{kafkaBrokers})
	eventConsumer.SetEventHandler(func(event *consumer.MatchEvent) {
		if cpWriter != nil {
			cpWriter.AppendEvent(event)
			// Write checkpoint every 300 ticks (FR-RP-03)
			if event.Tick%300 == 0 {
				cpWriter.WriteCheckpoint(event.MatchID, event.Tick)
			}
		}
	})
	eventConsumer.Start(ctx)
	defer eventConsumer.Close()

	// Start consuming match.lifecycle
	lifecycleConsumer := consumer.NewLifecycleConsumer([]string{kafkaBrokers})
	lifecycleConsumer.SetMatchEndHandler(func(ctx context.Context, matchID string) {
		log.Printf("Match ended: %s - triggering replay finalization", matchID)
		if arch != nil && cpWriter != nil {
			events := cpWriter.DrainEvents(matchID)
			if len(events) == 0 {
				log.Printf("No replay events buffered for match %s", matchID)
				return
			}
			if err := arch.ArchiveMatch(ctx, matchID, events); err != nil {
				log.Printf("Failed to archive replay for match %s: %v", matchID, err)
				return
			}
			log.Printf("Replay finalized for match %s", matchID)
		}
	})
	lifecycleConsumer.Start(ctx)
	defer lifecycleConsumer.Close()

	// HTTP API
	handler := api.NewHandler(arch, cpWriter)
	router := mux.NewRouter()
	router.HandleFunc("/replay/{matchId}", handler.GetReplay).Methods("GET")
	router.HandleFunc("/replay/{matchId}/seek", handler.SeekReplay).Methods("GET")
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	router.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("READY"))
	})

	server := &http.Server{
		Addr:    ":" + serverPort,
		Handler: router,
	}

	go func() {
		log.Printf("Replay Service starting on port %s", serverPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down Replay Service...")
	cancel()
	server.Shutdown(context.Background())
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
