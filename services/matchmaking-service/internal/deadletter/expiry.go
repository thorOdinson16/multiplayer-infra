package deadletter

import (
	"encoding/json"
	"log"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// ExpiredRequest represents an expired matchmaking request
type ExpiredRequest struct {
	PlayerID  string `json:"playerId"`
	Username  string `json:"username"`
	EloRating int    `json:"eloRating"`
	ExpiredAt string `json:"expiredAt"`
}

// ExpiryHandler handles expired matchmaking requests (FR-MM-08)
type ExpiryHandler struct {
	conn    *amqp091.Connection
	channel *amqp091.Channel
}

// NewExpiryHandler creates a new expiry handler
func NewExpiryHandler(url string) (*ExpiryHandler, error) {
	conn, err := amqp091.Dial(url)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &ExpiryHandler{
		conn:    conn,
		channel: channel,
	}, nil
}

// PublishExpired publishes an expired request to the dead-letter queue
func (h *ExpiryHandler) PublishExpired(playerID, username string, eloRating int) error {
	req := ExpiredRequest{
		PlayerID:  playerID,
		Username:  username,
		EloRating: eloRating,
		ExpiredAt: time.Now().UTC().Format(time.RFC3339),
	}

	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	return h.channel.Publish(
		"deadletter.exchange",
		"",
		false,
		false,
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        body,
			Timestamp:   time.Now(),
		},
	)
}

// Close closes the connection
func (h *ExpiryHandler) Close() {
	if h.channel != nil {
		h.channel.Close()
	}
	if h.conn != nil {
		h.conn.Close()
	}
}

// ProcessExpired processes expired messages from the dead-letter queue
func (h *ExpiryHandler) ProcessExpired() error {
	msgs, err := h.channel.Consume(
		"matchmaking.expired",
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			var req ExpiredRequest
			if err := json.Unmarshal(msg.Body, &req); err != nil {
				continue
			}
			log.Printf("Player %s matchmaking expired — notifying via Notification Service", req.PlayerID)
			// Notification dispatch will be handled by the Notification Service
		}
	}()

	return nil
}