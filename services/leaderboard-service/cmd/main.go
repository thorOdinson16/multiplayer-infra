package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorilla/mux"
	"github.com/thorOdinson16/multiplayer-infra/services/leaderboard-service/internal/api"
	"github.com/thorOdinson16/multiplayer-infra/services/leaderboard-service/internal/consumer"
	"github.com/thorOdinson16/multiplayer-infra/services/leaderboard-service/internal/store"
)

func main() {
	couchbaseHost := getEnv("COUCHBASE_HOST", "couchbase.infra.svc.cluster.local")
	couchbaseUser := getEnv("COUCHBASE_USER", "admin")
	couchbasePass := getEnv("COUCHBASE_PASSWORD", "password")
	kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka.infra.svc.cluster.local:9092")
	serverPort := getEnv("SERVER_PORT", "8080")

	connStr := "couchbase://" + couchbaseHost
	st, err := store.NewCouchbaseStore(connStr, couchbaseUser, couchbasePass)
	if err != nil {
		log.Fatalf("Failed to connect to Couchbase: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lifecycleConsumer := consumer.NewLifecycleConsumer([]string{kafkaBrokers}, st)
	lifecycleConsumer.Start(ctx)
	defer lifecycleConsumer.Close()

	handler := api.NewHandler(st)
	router := mux.NewRouter()
	router.HandleFunc("/leaderboard", handler.GetLeaderboard).Methods("GET")
	router.HandleFunc("/leaderboard/player/{id}", handler.GetPlayerStats).Methods("GET")
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	router.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("READY"))
	})

	server := &http.Server{Addr: ":" + serverPort, Handler: router}

	go func() {
		log.Printf("Leaderboard Service starting on port %s", serverPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down Leaderboard Service...")
	cancel()
	server.Shutdown(context.Background())
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}