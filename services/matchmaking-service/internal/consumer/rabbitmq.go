package consumer

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/thorOdinson16/multiplayer-infra/services/matchmaking-service/internal/matcher"
)

// MatchmakingMessage is the message format from the matchmaking queue
type MatchmakingMessage struct {
	PlayerID  string `json:"playerId"`
	Username  string `json:"username"`
	EloRating int    `json:"eloRating"`
	Timestamp string `json:"timestamp"`
}

// RabbitMQConsumer consumes matchmaking requests from RabbitMQ
type RabbitMQConsumer struct {
	conn            *amqp091.Connection
	channel         *amqp091.Channel
	matcher         *matcher.Matcher
	queue           string
	uri             string
	brokerAvailable bool
}

// NewRabbitMQConsumer creates a new RabbitMQ consumer
func NewRabbitMQConsumer(url, queueName string, m *matcher.Matcher) (*RabbitMQConsumer, error) {
	conn, err := amqp091.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	return &RabbitMQConsumer{
		conn:            conn,
		channel:         channel,
		matcher:         m,
		queue:           queueName,
		uri:             url,
		brokerAvailable: true,
	}, nil
}

// IsBrokerAvailable returns whether RabbitMQ is reachable (FR-MM-07)
func (c *RabbitMQConsumer) IsBrokerAvailable() bool {
	return c.brokerAvailable
}

// Start begins consuming messages from the queue
func (c *RabbitMQConsumer) Start() error {
	msgs, err := c.channel.Consume(
		c.queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	log.Printf("Consuming from queue: %s", c.queue)

	go func() {
		for msg := range msgs {
			c.handleMessage(msg)
		}
		c.brokerAvailable = false
	}()

	go c.monitorBroker()

	return nil
}

func (c *RabbitMQConsumer) handleMessage(msg amqp091.Delivery) {
	var matchMsg MatchmakingMessage
	if err := json.Unmarshal(msg.Body, &matchMsg); err != nil {
		log.Printf("Invalid message: %v", err)
		msg.Nack(false, false)
		return
	}

	msgTime, err := time.Parse(time.RFC3339, matchMsg.Timestamp)
	if err == nil && time.Since(msgTime) > 60*time.Second {
		log.Printf("Expired matchmaking request for player %s", matchMsg.PlayerID)
		msg.Nack(false, false)
		return
	}

	c.matcher.AddPlayer(&matcher.MatchmakingRequest{
		PlayerID:  matchMsg.PlayerID,
		Username:  matchMsg.Username,
		EloRating: matchMsg.EloRating,
	})

	msg.Ack(false)
}

func (c *RabbitMQConsumer) monitorBroker() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if c.conn.IsClosed() {
			c.brokerAvailable = false
			conn, err := amqp091.Dial(c.uri)
			if err == nil {
				c.conn = conn
				channel, err := conn.Channel()
				if err == nil {
					c.channel = channel
					c.brokerAvailable = true
					c.Start()
					return
				}
			}
		}
	}
}

// Close closes the RabbitMQ connection
func (c *RabbitMQConsumer) Close() {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}