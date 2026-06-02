// services/game-room-server/internal/game/state.go
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
	Status     string                  `json:"status"`    // "waiting", "running", "finished"
	Duration   int                     `json:"duration"`  // Match duration in seconds (default 300 = 5 minutes)
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
		Duration:   300, // 5 minutes default match duration
	}
}

// AddPlayer adds a player to the game
func (gs *GameState) AddPlayer(playerID, username string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	if len(gs.Players) >= gs.MaxPlayers {
		return
	}

	// Position players at different starting spots (100,100), (200,100), etc.
	startX := 100.0 + float64(len(gs.Players))*100.0
	startY := 100.0

	gs.Players[playerID] = &PlayerState{
		PlayerID:  playerID,
		Username:  username,
		X:         startX,
		Y:         startY,
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

// UpdatePlayerPosition updates a player's position (absolute)
func (gs *GameState) UpdatePlayerPosition(playerID string, x, y float64) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	if p, ok := gs.Players[playerID]; ok && p.Connected {
		p.X = clamp(x, 0, 1000)
		p.Y = clamp(y, 0, 1000)
	}
}

// MovePlayer adds delta to player's position (relative movement)
func (gs *GameState) MovePlayer(playerID string, deltaX, deltaY float64) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	if p, ok := gs.Players[playerID]; ok && p.Connected {
		newX := p.X + deltaX
		newY := p.Y + deltaY
		p.X = clamp(newX, 0, 1000)
		p.Y = clamp(newY, 0, 1000)
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
		Duration:   gs.Duration,
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

// GetStatus returns the current game status
func (gs *GameState) GetStatus() string {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.Status
}

// IncrementTick advances the tick counter
func (gs *GameState) IncrementTick() uint64 {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.Tick++
	return gs.Tick
}

// IsMatchFinished returns true if the match has ended
func (gs *GameState) IsMatchFinished() bool {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.Status == "finished"
}

// GetConnectedPlayerCount returns number of connected players
func (gs *GameState) GetConnectedPlayerCount() int {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	count := 0
	for _, p := range gs.Players {
		if p.Connected {
			count++
		}
	}
	return count
}

// HasActivePlayers returns true if there is at least one connected player
func (gs *GameState) HasActivePlayers() bool {
	return gs.GetConnectedPlayerCount() > 0
}

// IsActive returns true if there are connected players or match is running
func (gs *GameState) IsActive() bool {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.Status == "running" && gs.GetConnectedPlayerCount() > 0
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