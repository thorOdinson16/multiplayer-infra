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

type Handler struct {
	mu          sync.RWMutex
	game        *game.GameState
	onInput     func(event *game.InputEvent)
	broadcaster *game.Broadcaster
}

func NewHandler(gs *game.GameState, onInput func(event *game.InputEvent), broadcaster *game.Broadcaster) *Handler {
	return &Handler{
		game:        gs,
		onInput:     onInput,
		broadcaster: broadcaster,
	}
}

func (h *Handler) HandleConnection(w http.ResponseWriter, r *http.Request, playerID, username string) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade WebSocket: %v", err)
		return
	}

	// Register connection with broadcaster for state updates
	if h.broadcaster != nil {
		h.broadcaster.AddConnection(playerID, conn)
		log.Printf("Player %s registered with broadcaster", playerID)
	}

	h.mu.Lock()
	h.game.AddPlayer(playerID, username)
	h.mu.Unlock()

	// CRITICAL FIX: Send initial state immediately to prevent client timeout
	initialState := h.game.GetSnapshot()
	initialData, err := json.Marshal(initialState)
	if err != nil {
		log.Printf("Failed to marshal initial state: %v", err)
	} else {
		if err := conn.WriteMessage(websocket.TextMessage, initialData); err != nil {
			log.Printf("Failed to send initial state to player %s: %v", playerID, err)
		} else {
			log.Printf("Sent initial state to player %s (tick: %d, players: %d)", playerID, initialState.Tick, len(initialState.Players))
		}
	}

	log.Printf("Player %s (%s) connected", username, playerID)
	go h.readLoop(conn, playerID)
}

func (h *Handler) readLoop(conn *websocket.Conn, playerID string) {
	defer func() {
		conn.Close()
		if h.broadcaster != nil {
			h.broadcaster.RemoveConnection(playerID)
		}
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

// AddSpectator handles spectator WebSocket connections
func (h *Handler) AddSpectator(w http.ResponseWriter, r *http.Request, spectatorID, matchID string) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade spectator WebSocket: %v", err)
		return
	}

	if h.broadcaster != nil {
		h.broadcaster.AddSpectator(spectatorID, conn)
		log.Printf("Spectator %s registered with broadcaster", spectatorID)
	}

	log.Printf("Spectator %s connected to match %s", spectatorID, matchID)
}