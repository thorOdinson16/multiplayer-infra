package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/thorOdinson16/multiplayer-infra/services/notification-service/internal/consumer"
	"github.com/thorOdinson16/multiplayer-infra/services/notification-service/internal/dispatcher"
)

func main() {
	rmqURL := getEnv("RABBITMQ_URL", "amqp://admin:password@rabbitmq.infra.svc.cluster.local:5672/")
	serverPort := getEnv("SERVER_PORT", "8080")

	rmq, err := consumer.NewRabbitMQConsumer(rmqURL)
	if err != nil { log.Fatalf("Failed to connect to RabbitMQ: %v", err) }
	defer rmq.Close()

	d := dispatcher.NewDispatcher()
	rmq.SetHandler(d.Dispatch)
	rmq.Start()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK); w.Write([]byte("OK"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK); w.Write([]byte("READY"))
	})

	server := &http.Server{Addr: ":" + serverPort, Handler: mux}
	go func() {
		log.Printf("Notification Service starting on port %s", serverPort)
		server.ListenAndServe()
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down Notification Service...")
	server.Shutdown(nil)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" { return value }
	return defaultValue
}