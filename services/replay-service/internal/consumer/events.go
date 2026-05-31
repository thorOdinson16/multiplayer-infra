package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// MatchEvent is a tick event from match.events topic
type MatchEvent struct {
	Type    string          `json:"type"`
	MatchID string          `json:"matchId"`
	Tick    uint64          `json:"tick"`
	State   json.RawMessage `json:"state"`
	Time    string          `json:"time"`
}

// EventConsumer consumes match events from Kafka (FR-RP-01)
type EventConsumer struct {
	reader  *kafka.Reader
	onEvent func(event *MatchEvent)
}

// NewEventConsumer creates a new event consumer
func NewEventConsumer(brokers []string) *EventConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       "match.events",
		GroupID:     "replay-service",
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})

	return &EventConsumer{
		reader: reader,
	}
}

// SetEventHandler sets the callback for received events
func (c *EventConsumer) SetEventHandler(handler func(event *MatchEvent)) {
	c.onEvent = handler
}

// Start begins consuming events
func (c *EventConsumer) Start(ctx context.Context) {
	log.Println("Starting match.events consumer (group: replay-service)")

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

			var event MatchEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Failed to unmarshal event: %v", err)
				continue
			}

			if c.onEvent != nil {
				c.onEvent(&event)
			}
		}
	}()
}

// Close closes the consumer
func (c *EventConsumer) Close() error {
	return c.reader.Close()
}