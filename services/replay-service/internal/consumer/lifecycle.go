package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
)

// LifecycleEvent is a match start/end event from match.lifecycle topic
type LifecycleEvent struct {
	Type    string `json:"type"` // "match_start", "match_end"
	MatchID string `json:"matchId"`
	Time    string `json:"time"`
}

// LifecycleConsumer consumes match lifecycle events (FR-RP-07)
type LifecycleConsumer struct {
	reader *kafka.Reader
	onEnd  func(ctx context.Context, matchID string)
}

// NewLifecycleConsumer creates a new lifecycle consumer
func NewLifecycleConsumer(brokers []string) *LifecycleConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       "match.lifecycle",
		GroupID:     "replay-lifecycle",
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})

	return &LifecycleConsumer{
		reader: reader,
	}
}

// SetMatchEndHandler sets the callback for match end events
func (c *LifecycleConsumer) SetMatchEndHandler(handler func(ctx context.Context, matchID string)) {
	c.onEnd = handler
}

// Start begins consuming lifecycle events
func (c *LifecycleConsumer) Start(ctx context.Context) {
	log.Println("Starting match.lifecycle consumer (group: replay-lifecycle)")

	go func() {
		for {
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Error reading lifecycle message: %v", err)
				time.Sleep(time.Second)
				continue
			}

			var event LifecycleEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Failed to unmarshal lifecycle event: %v", err)
				continue
			}

			if event.Type == "match_end" && c.onEnd != nil {
				msgCtx := extractLifecycleTraceContext(ctx, msg.Headers)
				_, span := otel.Tracer("replay-service").Start(msgCtx, "kafka.consume match.lifecycle")
				span.SetAttributes(attribute.String("match.id", event.MatchID), attribute.String("match.lifecycle", event.Type))
				c.onEnd(msgCtx, event.MatchID)
				span.End()
			}
		}
	}()
}

func extractLifecycleTraceContext(ctx context.Context, headers []kafka.Header) context.Context {
	carrier := propagation.MapCarrier{}
	for _, header := range headers {
		carrier.Set(header.Key, string(header.Value))
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// Close closes the consumer
func (c *LifecycleConsumer) Close() error {
	return c.reader.Close()
}
