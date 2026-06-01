package dispatcher

import (
	"log"

	"github.com/thorOdinson16/multiplayer-infra/services/notification-service/internal/consumer"
)

type Dispatcher struct{}

func NewDispatcher() *Dispatcher { return &Dispatcher{} }

func (d *Dispatcher) Dispatch(n *consumer.Notification) {
	log.Printf("Dispatching notification [%s] to player %s: %s", n.Type, n.PlayerID, n.Message)
	// In production: push to WebSocket connection via NGINX
}