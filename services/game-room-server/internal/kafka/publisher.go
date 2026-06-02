package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/game"
)

// Publisher publishes game events to Kafka topics
type Publisher struct {
	eventsWriter    *kafka.Writer
	lifecycleWriter *kafka.Writer
	telemetryWriter *kafka.Writer // ADDED for telemetry
}

// MatchEvent represents a game tick event for Kafka
type MatchEvent struct {
	Type    string           `json:"type"`
	MatchID string           `json:"matchId"`
	Tick    uint64           `json:"tick"`
	State   *game.GameState  `json:"state"`
	Time    string           `json:"time"`
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
		Key:   []byte(matchID),
		Value: data,
		Time:  time.Now(),
	}

	if err := p.eventsWriter.WriteMessages(ctx, msg); err != nil {
		log.Printf("Failed to publish tick event: %v", err)
		return err
	}

	return nil
}

// PublishMatchStart publishes a match start event (FR-GR-09)
func (p *Publisher) PublishMatchStart(ctx context.Context, matchID string) error {
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
		Key:   []byte(matchID),
		Value: data,
	})
}

// PublishMatchEnd publishes a match end event (FR-GR-09)
func (p *Publisher) PublishMatchEnd(ctx context.Context, matchID string) error {
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
		Key:   []byte(matchID),
		Value: data,
	})
}

// PublishTelemetry publishes telemetry events for analytics (FR-AN-01)
func (p *Publisher) PublishTelemetry(ctx context.Context, matchID, playerID string, x, y float64, eventType string) error {
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
		Key:   []byte(matchID),
		Value: data,
		Time:  time.Now(),
	}

	if err := p.telemetryWriter.WriteMessages(ctx, msg); err != nil {
		log.Printf("Failed to publish telemetry event: %v", err)
		return err
	}

	return nil
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