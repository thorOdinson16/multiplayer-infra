package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type TelemetryEvent struct {
	Type     string  `json:"type"`
	MatchID  string  `json:"matchId"`
	PlayerID string  `json:"playerId"`
	X        float64 `json:"x"`
	Y        float64 `json:"y"`
	Duration float64 `json:"duration"`
	Time     string  `json:"time"`
}

type TelemetryConsumer struct {
	reader  *kafka.Reader
	metrics *Metrics
}

func NewTelemetryConsumer(brokers []string, m *Metrics) *TelemetryConsumer {
	return &TelemetryConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   "match.telemetry",
			GroupID: "analytics-service",
		}),
		metrics: m,
	}
}

func (c *TelemetryConsumer) Start(ctx context.Context) {
	log.Println("Starting match.telemetry consumer (group: analytics-service)")
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
			var event TelemetryEvent
			if json.Unmarshal(msg.Value, &event) == nil {
				c.metrics.RecordEvent(&event)
			}
		}
	}()
}

func (c *TelemetryConsumer) Close() error { return c.reader.Close() }
