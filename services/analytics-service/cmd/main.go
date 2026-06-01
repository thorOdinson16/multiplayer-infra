package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/thorOdinson16/multiplayer-infra/services/analytics-service/internal/consumer"
	"github.com/thorOdinson16/multiplayer-infra/services/analytics-service/internal/metrics"
)

func main() {
	kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka.infra.svc.cluster.local:9092")
	serverPort := getEnv("SERVER_PORT", "8080")

	m := consumer.NewMetrics()
	tc := consumer.NewTelemetryConsumer([]string{kafkaBrokers}, m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc.Start(ctx)
	defer tc.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", metrics.Handler(m))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("READY"))
	})

	server := &http.Server{Addr: ":" + serverPort, Handler: mux}
	go func() {
		log.Printf("Analytics Service starting on port %s", serverPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down Analytics Service...")
	cancel()
	server.Shutdown(context.Background())
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" { return value }
	return defaultValue
}