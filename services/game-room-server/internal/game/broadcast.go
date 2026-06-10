// services/game-room-server/internal/game/broadcast.go
package game

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/gorilla/websocket"
)

// Broadcaster sends state snapshots to connected clients
type Broadcaster struct {
	mu            sync.RWMutex
	connections   map[string]*websocket.Conn // playerID -> conn
	spectators    map[string]*websocket.Conn // spectatorID -> conn
	emptyCallback func()                     // Callback when all players disconnect
}

// NewBroadcaster creates a new broadcaster
func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		connections: make(map[string]*websocket.Conn),
		spectators:  make(map[string]*websocket.Conn),
	}
}

// NewBroadcasterWithCallback creates a broadcaster with empty room callback
func NewBroadcasterWithCallback(callback func()) *Broadcaster {
	return &Broadcaster{
		connections:   make(map[string]*websocket.Conn),
		spectators:    make(map[string]*websocket.Conn),
		emptyCallback: callback,
	}
}

// AddConnection registers a player connection
func (b *Broadcaster) AddConnection(playerID string, conn *websocket.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.connections[playerID] = conn
	log.Printf("Broadcaster: added player %s (total players: %d)", playerID, len(b.connections))
}

// RemoveConnection removes a player connection
func (b *Broadcaster) RemoveConnection(playerID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if conn, ok := b.connections[playerID]; ok {
		conn.Close()
		delete(b.connections, playerID)
		log.Printf("Broadcaster: removed player %s (remaining: %d)", playerID, len(b.connections))

		// Check if room is now empty
		if len(b.connections) == 0 && b.emptyCallback != nil {
			log.Printf("Room is now empty - triggering room finish")
			go b.emptyCallback()
		}
	}
}

// AddSpectator registers a spectator connection
func (b *Broadcaster) AddSpectator(spectatorID string, conn *websocket.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.spectators[spectatorID] = conn
	log.Printf("Broadcaster: added spectator %s (total spectators: %d)", spectatorID, len(b.spectators))
}

// RemoveSpectator removes a spectator connection
func (b *Broadcaster) RemoveSpectator(spectatorID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if conn, ok := b.spectators[spectatorID]; ok {
		conn.Close()
		delete(b.spectators, spectatorID)
	}
}

// BroadcastToPlayers sends the state snapshot to all connected players (FR-GR-04)
func (b *Broadcaster) BroadcastToPlayers(state *GameState) {
	snapshot := state.GetSnapshot()
	data, err := json.Marshal(snapshot)
	if err != nil {
		log.Printf("Failed to marshal state: %v", err)
		return
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	for playerID, conn := range b.connections {
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Printf("Failed to send to player %s: %v", playerID, err)
		}
	}
}

// BroadcastToSpectators sends the state snapshot to all connected spectators
func (b *Broadcaster) BroadcastToSpectators(state *GameState) {
	snapshot := state.GetSnapshot()
	data, err := json.Marshal(snapshot)
	if err != nil {
		log.Printf("Failed to marshal state: %v", err)
		return
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	for spectatorID, conn := range b.spectators {
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Printf("Failed to send to spectator %s: %v", spectatorID, err)
		}
	}
}

// BroadcastSpectatorPayload sends an already delayed payload to connected spectators.
func (b *Broadcaster) BroadcastSpectatorPayload(data []byte) {
	// Write to all spectators. Collect failed connections and close them outside
	// the critical section to avoid blocking other operations while performing
	// network IO under lock.
	b.mu.RLock()
	toClose := make([]string, 0)
	for spectatorID, conn := range b.spectators {
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Printf("Failed to send to spectator %s: %v", spectatorID, err)
			toClose = append(toClose, spectatorID)
		}
	}
	b.mu.RUnlock()

	if len(toClose) == 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	for _, spectatorID := range toClose {
		if conn, ok := b.spectators[spectatorID]; ok {
			conn.Close()
			delete(b.spectators, spectatorID)
		}
	}
}

// PlayerCount returns the number of connected players
func (b *Broadcaster) PlayerCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.connections)
}

// CloseAll closes all connections
func (b *Broadcaster) CloseAll() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, conn := range b.connections {
		conn.Close()
	}
	for _, conn := range b.spectators {
		conn.Close()
	}
}
