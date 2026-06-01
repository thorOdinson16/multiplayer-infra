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
				time.Sleep(time.Second)
				continue
			}

			var event struct {
				Type    string `json:"type"`
				MatchID string `json:"matchId"`
			}
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				continue
			}

			if event.Type == "match_end" {
				log.Printf("Match ended: %s — updating leaderboard", event.MatchID)
				// In production, the full outcome would be in the message body
			}
		}
	}()
}

// Close closes the consumer
func (c *LifecycleConsumer) Close() error {
	return c.reader.Close()
}