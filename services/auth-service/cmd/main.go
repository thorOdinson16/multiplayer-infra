package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/thorOdinson16/multiplayer-infra/services/auth-service/internal/handler"
	"github.com/thorOdinson16/multiplayer-infra/services/auth-service/internal/jwt"
	"github.com/thorOdinson16/multiplayer-infra/services/auth-service/internal/store"
)

func main() {
	// Configuration from environment variables (NFR-M-02)
	couchbaseHost := getEnv("COUCHBASE_HOST", "couchbase.infra.svc.cluster.local")
	couchbaseUser := getEnv("COUCHBASE_USER", "admin")
	couchbasePass := getEnv("COUCHBASE_PASSWORD", "password")
	jwtSecret := getEnv("JWT_SECRET", "")
	jwtExpiryHours := getEnvInt("JWT_EXPIRY_HOURS", 24)
	serverPort := getEnv("SERVER_PORT", "8080")

	// Generate JWT secret if not provided (first run)
	if jwtSecret == "" {
		var err error
		jwtSecret, err = jwt.GenerateSecret()
		if err != nil {
			log.Fatalf("Failed to generate JWT secret: %v", err)
		}
		log.Println("Generated new JWT secret")
	}

	// Connect to Couchbase
	connStr := fmt.Sprintf("couchbase://%s", couchbaseHost)
	couchbaseStore, err := store.NewCouchbaseStore(connStr, couchbaseUser, couchbasePass)
	if err != nil {
		log.Fatalf("Failed to connect to Couchbase: %v", err)
	}
	defer couchbaseStore.Close()

	// Initialize JWT manager
	jwtManager := jwt.NewManager(jwtSecret, jwtExpiryHours)

	// Initialize handler
	authHandler := handler.NewAuthHandler(couchbaseStore, jwtManager, jwtExpiryHours)

	// Set up HTTP routes
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/login", authHandler.Login)
	mux.HandleFunc("/auth/refresh", authHandler.Refresh)
	mux.HandleFunc("/auth/logout", authHandler.Logout)
	mux.HandleFunc("/auth/validate", authHandler.Validate)

	// Health check endpoints (NFR-M-01)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("READY"))
	})

	// Start server
	server := &http.Server{
		Addr:         ":" + serverPort,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	go func() {
		log.Printf("Auth Service starting on port %s", serverPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Auth Service...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Auth Service stopped")
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