package consumer

import (
	"encoding/json"
	"log"

	"github.com/rabbitmq/amqp091-go"
)

type Notification struct {
	Type     string `json:"type"`
	PlayerID string `json:"playerId"`
	MatchID  string `json:"matchId"`
	Message  string `json:"message"`
}

type RabbitMQConsumer struct {
	conn    *amqp091.Connection
	channel *amqp091.Channel
	onNotify func(*Notification)
}

func NewRabbitMQConsumer(url string) (*RabbitMQConsumer, error) {
	conn, err := amqp091.Dial(url)
	if err != nil { return nil, err }
	ch, err := conn.Channel()
	if err != nil { conn.Close(); return nil, err }
	return &RabbitMQConsumer{conn: conn, channel: ch}, nil
}

func (c *RabbitMQConsumer) SetHandler(h func(*Notification)) { c.onNotify = h }

func (c *RabbitMQConsumer) Start() {
	queues := []string{"notifications.match.found", "notifications.match.ended",
		"notifications.match.expired", "notifications.player.joined", "notifications.system.alert"}
	for _, q := range queues {
		msgs, _ := c.channel.Consume(q, "", true, false, false, false, nil)
		go func(q string, msgs <-chan amqp091.Delivery) {
			for msg := range msgs {
				var n Notification
				if json.Unmarshal(msg.Body, &n) == nil && c.onNotify != nil {
					c.onNotify(&n)
				}
			}
		}(q, msgs)
	}
	log.Println("Notification Service consuming from 5 queues")
}

func (c *RabbitMQConsumer) Close() {
	c.channel.Close()
	c.conn.Close()
}