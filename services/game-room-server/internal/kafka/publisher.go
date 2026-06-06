package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/game"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
)

// Publisher publishes game events to Kafka topics
type Publisher struct {
	eventsWriter    *kafka.Writer
	lifecycleWriter *kafka.Writer
	telemetryWriter *kafka.Writer // ADDED for telemetry
}

// MatchEvent represents a game tick event for Kafka
type MatchEvent struct {
	Type    string          `json:"type"`
	MatchID string          `json:"matchId"`
	Tick    uint64          `json:"tick"`
	State   *game.GameState `json:"state"`
	Time    string          `json:"time"`
}

// LifecycleEvent represents a match lifecycle event
type LifecycleEvent struct {
	Type    string `json:"type"` // "match_start", "match_end"
	MatchID string `json:"matchId"`
	Time    string `json:"time"`
}

// TelemetryEvent represents a telemetry event for analytics
type TelemetryEvent struct {
	Type     string  `json:"type"`
	MatchID  string  `json:"matchId"`
	PlayerID string  `json:"playerId"`
	X        float64 `json:"x"`
	Y        float64 `json:"y"`
	Time     string  `json:"time"`
}

// NewPublisher creates a new Kafka publisher
func NewPublisher(brokers []string) *Publisher {
	return &Publisher{
		eventsWriter: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        "match.events",
			Balancer:     &kafka.Hash{},
			BatchSize:    1,
			BatchTimeout: 5 * time.Millisecond,
		},
		lifecycleWriter: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    "match.lifecycle",
			Balancer: &kafka.Hash{},
		},
		telemetryWriter: &kafka.Writer{ // ADDED
			Addr:     kafka.TCP(brokers...),
			Topic:    "match.telemetry",
			Balancer: &kafka.Hash{},
		},
	}
}

// PublishTickEvent publishes a game tick to match.events (FR-GR-05)
func (p *Publisher) PublishTickEvent(ctx context.Context, matchID string, tick uint64, state *game.GameState) error {
	ctx, span := otel.Tracer("game-room-server").Start(ctx, "kafka.publish match.events")
	defer span.End()
	span.SetAttributes(attribute.String("match.id", matchID), attribute.Int64("match.tick", int64(tick)))

	event := MatchEvent{
		Type:    "tick",
		MatchID: matchID,
		Tick:    tick,
		State:   state,
		Time:    time.Now().UTC().Format(time.RFC3339),
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := kafka.Message{
		Key:     []byte(matchID),
		Value:   data,
		Time:    time.Now(),
		Headers: traceHeaders(ctx),
	}

	if err := p.eventsWriter.WriteMessages(ctx, msg); err != nil {
		log.Printf("Failed to publish tick event: %v", err)
		return err
	}

	return nil
}

// PublishMatchStart publishes a match start event (FR-GR-09)
func (p *Publisher) PublishMatchStart(ctx context.Context, matchID string) error {
	ctx, span := otel.Tracer("game-room-server").Start(ctx, "kafka.publish match.lifecycle")
	defer span.End()
	span.SetAttributes(attribute.String("match.id", matchID), attribute.String("match.lifecycle", "match_start"))

	event := LifecycleEvent{
		Type:    "match_start",
		MatchID: matchID,
		Time:    time.Now().UTC().Format(time.RFC3339),
	}

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return p.lifecycleWriter.WriteMessages(ctx, kafka.Message{
		Key:     []byte(matchID),
		Value:   data,
		Headers: traceHeaders(ctx),
	})
}

// PublishMatchEnd publishes a match end event (FR-GR-09)
func (p *Publisher) PublishMatchEnd(ctx context.Context, matchID string) error {
	ctx, span := otel.Tracer("game-room-server").Start(ctx, "kafka.publish match.lifecycle")
	defer span.End()
	span.SetAttributes(attribute.String("match.id", matchID), attribute.String("match.lifecycle", "match_end"))

	event := LifecycleEvent{
		Type:    "match_end",
		MatchID: matchID,
		Time:    time.Now().UTC().Format(time.RFC3339),
	}

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return p.lifecycleWriter.WriteMessages(ctx, kafka.Message{
		Key:     []byte(matchID),
		Value:   data,
		Headers: traceHeaders(ctx),
	})
}

// PublishTelemetry publishes telemetry events for analytics (FR-AN-01)
func (p *Publisher) PublishTelemetry(ctx context.Context, matchID, playerID string, x, y float64, eventType string) error {
	ctx, span := otel.Tracer("game-room-server").Start(ctx, "kafka.publish match.telemetry")
	defer span.End()
	span.SetAttributes(attribute.String("match.id", matchID), attribute.String("player.id", playerID), attribute.String("telemetry.type", eventType))

	event := TelemetryEvent{
		Type:     eventType,
		MatchID:  matchID,
		PlayerID: playerID,
		X:        x,
		Y:        y,
		Time:     time.Now().UTC().Format(time.RFC3339),
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal telemetry event: %w", err)
	}

	msg := kafka.Message{
		Key:     []byte(matchID),
		Value:   data,
		Time:    time.Now(),
		Headers: traceHeaders(ctx),
	}

	if err := p.telemetryWriter.WriteMessages(ctx, msg); err != nil {
		log.Printf("Failed to publish telemetry event: %v", err)
		return err
	}

	return nil
}

func traceHeaders(ctx context.Context) []kafka.Header {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	headers := make([]kafka.Header, 0, len(carrier))
	for key, value := range carrier {
		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	return headers
}

// Close closes the Kafka writers
func (p *Publisher) Close() error {
	if err := p.eventsWriter.Close(); err != nil {
		return err
	}
	if err := p.lifecycleWriter.Close(); err != nil {
		return err
	}
	if err := p.telemetryWriter.Close(); err != nil { // ADDED
		return err
	}
	return nil
}
