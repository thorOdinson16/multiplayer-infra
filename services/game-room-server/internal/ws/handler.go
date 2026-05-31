package ws

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/game"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins in dev
	},
}

// Handler manages WebSocket connections for a game room
type Handler struct {
	mu       sync.RWMutex
	game     *game.GameState
	onInput  func(event *game.InputEvent)
}

// NewHandler creates a new WebSocket handler
func NewHandler(gs *game.GameState, onInput func(event *game.InputEvent)) *Handler {
	return &Handler{
		game:    gs,
		onInput: onInput,
	}
}

// HandleConnection handles a new WebSocket connection
func (h *Handler) HandleConnection(w http.ResponseWriter, r *http.Request, playerID, username string) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade WebSocket: %v", err)
		return
	}

	h.mu.Lock()
	h.game.AddPlayer(playerID, username)
	h.mu.Unlock()

	log.Printf("Player %s (%s) connected", username, playerID)

	// Read messages from the client
	go h.readLoop(conn, playerID)
}

func (h *Handler) readLoop(conn *websocket.Conn, playerID string) {
	defer func() {
		conn.Close()
		h.mu.Lock()
		h.game.RemovePlayer(playerID)
		h.mu.Unlock()
		log.Printf("Player %s disconnected", playerID)
	}()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			return
		}

		var event game.InputEvent
		if err := json.Unmarshal(message, &event); err != nil {
			log.Printf("Invalid message from %s: %v", playerID, err)
			continue
		}

		event.PlayerID = playerID

		if h.onInput != nil {
			h.onInput(&event)
		}
	}
}