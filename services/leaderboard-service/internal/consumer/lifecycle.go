package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/thorOdinson16/multiplayer-infra/services/leaderboard-service/internal/store"
)

// LifecycleConsumer consumes match.lifecycle events (FR-LB-01)
type LifecycleConsumer struct {
	reader *kafka.Reader
	store  *store.CouchbaseStore
}

// NewLifecycleConsumer creates a new lifecycle consumer
func NewLifecycleConsumer(brokers []string, st *store.CouchbaseStore) *LifecycleConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       "match.lifecycle",
		GroupID:     "leaderboard-service",
		StartOffset: kafka.FirstOffset,
	})

	return &LifecycleConsumer{
		reader: reader,
		store:  st,
	}
}

// Start begins consuming lifecycle events
func (c *LifecycleConsumer) Start(ctx context.Context) {
	log.Println("Starting match.lifecycle consumer (group: leaderboard-service)")

	go func() {
		for {
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Error reading message: %v", err)
				time.Sleep(time.Second)
				continue
			}

			var event struct {
				Type    string `json:"type"`
				MatchID string `json:"matchId"`
				Outcome struct {
					Winner string         `json:"winner"`
					Scores map[string]int `json:"scores"`
				} `json:"outcome"`
			}

			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Failed to unmarshal event: %v", err)
				continue
			}

			if event.Type == "match_end" {
				log.Printf("Match ended: %s, winner: %s", event.MatchID, event.Outcome.Winner)

				// Extract players from scores map
				var players []string
				for playerID := range event.Outcome.Scores {
					players = append(players, playerID)
				}

				// Update player stats
				outcome := &store.MatchOutcome{
					MatchID: event.MatchID,
					Players: players,
					Winner:  event.Outcome.Winner,
					Scores:  event.Outcome.Scores,
					EndedAt: time.Now().UTC().Format(time.RFC3339),
				}

				if err := c.store.UpdatePlayerStats(outcome); err != nil {
					log.Printf("Failed to update player stats: %v", err)
				} else {
					log.Printf("Successfully updated leaderboard for match %s", event.MatchID)
				}
			}
		}
	}()
}

// Close closes the consumer
func (c *LifecycleConsumer) Close() error {
	return c.reader.Close()
}
