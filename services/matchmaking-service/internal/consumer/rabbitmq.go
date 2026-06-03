package consumer

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
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
	mu              sync.RWMutex
	conn            *amqp091.Connection
	channel         *amqp091.Channel
	matcher         *matcher.Matcher
	queue           string
	uri             string
	brokerAvailable bool
}

var ErrBrokerUnavailable = errors.New("rabbitmq broker unavailable")

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
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.brokerAvailable
}

func (c *RabbitMQConsumer) setBrokerAvailable(available bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.brokerAvailable = available
}

// PublishRequest publishes a new matchmaking request to RabbitMQ.
func (c *RabbitMQConsumer) PublishRequest(msg MatchmakingMessage) error {
	if !c.IsBrokerAvailable() {
		return ErrBrokerUnavailable
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal matchmaking request: %w", err)
	}

	c.mu.RLock()
	channel := c.channel
	c.mu.RUnlock()
	if channel == nil {
		c.setBrokerAvailable(false)
		return ErrBrokerUnavailable
	}

	if err := channel.Publish(
		"",
		c.queue,
		false,
		false,
		amqp091.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp091.Persistent,
			Body:         body,
		},
	); err != nil {
		c.setBrokerAvailable(false)
		return ErrBrokerUnavailable
	}

	return nil
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
		c.setBrokerAvailable(false)
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
		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil {
			c.setBrokerAvailable(false)
			continue
		}

		if conn.IsClosed() {
			c.setBrokerAvailable(false)
			conn, err := amqp091.Dial(c.uri)
			if err == nil {
				channel, err := conn.Channel()
				if err == nil {
					c.mu.Lock()
					c.conn = conn
					c.channel = channel
					c.brokerAvailable = true
					c.mu.Unlock()
					c.Start()
					return
				}
				conn.Close()
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
