package game

import (
	"sync"
	"time"
)

// PlayerState represents a player's position and stats in-game
type PlayerState struct {
	PlayerID  string  `json:"playerId"`
	Username  string  `json:"username"`
	X         float64 `json:"x"`
	Y         float64 `json:"y"`
	Health    int     `json:"health"`
	Score     int     `json:"score"`
	Connected bool    `json:"connected"`
}

// GameState is the authoritative state of a single game room
type GameState struct {
	mu         sync.RWMutex
	MatchID    string                  `json:"matchId"`
	Tick       uint64                  `json:"tick"`
	Players    map[string]*PlayerState `json:"players"`
	StartTime  time.Time               `json:"startTime"`
	MaxPlayers int                     `json:"maxPlayers"`
	Status     string                  `json:"status"` // "waiting", "running", "finished"
}

// NewGameState creates a new game state
func NewGameState(matchID string, maxPlayers int) *GameState {
	return &GameState{
		MatchID:    matchID,
		Tick:       0,
		Players:    make(map[string]*PlayerState),
		StartTime:  time.Now().UTC(),
		MaxPlayers: maxPlayers,
		Status:     "waiting",
	}
}

// AddPlayer adds a player to the game
func (gs *GameState) AddPlayer(playerID, username string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	if len(gs.Players) >= gs.MaxPlayers {
		return
	}

	gs.Players[playerID] = &PlayerState{
		PlayerID:  playerID,
		Username:  username,
		X:         float64(len(gs.Players) * 100),
		Y:         float64(len(gs.Players) * 100),
		Health:    100,
		Score:     0,
		Connected: true,
	}
}

// RemovePlayer marks a player as disconnected
func (gs *GameState) RemovePlayer(playerID string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	if p, ok := gs.Players[playerID]; ok {
		p.Connected = false
	}
}

// ReconnectPlayer marks a player as reconnected
func (gs *GameState) ReconnectPlayer(playerID string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	if p, ok := gs.Players[playerID]; ok {
		p.Connected = true
	}
}

// UpdatePlayerPosition updates a player's position
func (gs *GameState) UpdatePlayerPosition(playerID string, x, y float64) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	if p, ok := gs.Players[playerID]; ok && p.Connected {
		p.X = clamp(x, 0, 1000)
		p.Y = clamp(y, 0, 1000)
	}
}

// GetSnapshot returns a read-only copy of the current state
func (gs *GameState) GetSnapshot() *GameState {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	snapshot := &GameState{
		MatchID:    gs.MatchID,
		Tick:       gs.Tick,
		Players:    make(map[string]*PlayerState),
		StartTime:  gs.StartTime,
		MaxPlayers: gs.MaxPlayers,
		Status:     gs.Status,
	}

	for id, p := range gs.Players {
		pc := *p
		snapshot.Players[id] = &pc
	}

	return snapshot
}

// SetStatus updates the game status
func (gs *GameState) SetStatus(status string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.Status = status
}

// IncrementTick advances the tick counter
func (gs *GameState) IncrementTick() uint64 {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.Tick++
	return gs.Tick
}

func clamp(val, min, max float64) float64 {
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}