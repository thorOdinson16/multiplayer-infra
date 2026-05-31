package game

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/gorilla/websocket"
)

// Broadcaster sends state snapshots to connected clients
type Broadcaster struct {
	mu          sync.RWMutex
	connections map[string]*websocket.Conn // playerID -> conn
	spectators  map[string]*websocket.Conn // spectatorID -> conn
}

// NewBroadcaster creates a new broadcaster
func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		connections: make(map[string]*websocket.Conn),
		spectators:  make(map[string]*websocket.Conn),
	}
}

// AddConnection registers a player connection
func (b *Broadcaster) AddConnection(playerID string, conn *websocket.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.connections[playerID] = conn
}

// RemoveConnection removes a player connection
func (b *Broadcaster) RemoveConnection(playerID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if conn, ok := b.connections[playerID]; ok {
		conn.Close()
		delete(b.connections, playerID)
	}
}

// AddSpectator registers a spectator connection
func (b *Broadcaster) AddSpectator(spectatorID string, conn *websocket.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.spectators[spectatorID] = conn
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