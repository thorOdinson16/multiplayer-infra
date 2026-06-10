package matcher

import (
	"math"
	"sync"
	"time"
)

// MatchmakingRequest represents a player waiting to be matched
type MatchmakingRequest struct {
	PlayerID  string
	Username  string
	EloRating int
	QueuedAt  time.Time
}

// MatchmakingConfig configures matchmaking behavior
type MatchmakingConfig struct {
	MinPlayers         int
	MaxPlayers         int
	WindowSeconds      int
	InitialEloRange    int
	MaxEloRange        int
	RangeExpansionRate int
}

// DefaultConfig returns the default matchmaking configuration
func DefaultConfig() MatchmakingConfig {
	return MatchmakingConfig{
		MinPlayers:         2,
		MaxPlayers:         8,
		WindowSeconds:      30,
		InitialEloRange:    100,
		MaxEloRange:        500,
		RangeExpansionRate: 50,
	}
}

// Matcher handles lobby assembly using Elo-based matching
type Matcher struct {
	mu      sync.Mutex
	config  MatchmakingConfig
	queue   []*MatchmakingRequest
	lobbies chan []*MatchmakingRequest
	stopCh  chan struct{}
}

// NewMatcher creates a new matchmaker
func NewMatcher(config MatchmakingConfig) *Matcher {
	return &Matcher{
		config:  config,
		queue:   make([]*MatchmakingRequest, 0),
		lobbies: make(chan []*MatchmakingRequest, 10),
		stopCh:  make(chan struct{}),
	}
}

// AddPlayer adds a player to the matchmaking queue
func (m *Matcher) AddPlayer(req *MatchmakingRequest) {
	m.mu.Lock()
	defer m.mu.Unlock()
	req.QueuedAt = time.Now()
	m.queue = append(m.queue, req)
}

// RemovePlayer removes a player from the queue
func (m *Matcher) RemovePlayer(playerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, req := range m.queue {
		if req.PlayerID == playerID {
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			return
		}
	}
}

// QueueLength returns the current number of players waiting
func (m *Matcher) QueueLength() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.queue)
}

// Lobbies returns the channel where assembled lobbies are sent
func (m *Matcher) Lobbies() <-chan []*MatchmakingRequest {
	return m.lobbies
}

// Run starts the matchmaking loop
func (m *Matcher) Run() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.tryMatch()
		case <-m.stopCh:
			return
		}
	}
}

// Stop stops the matchmaking loop
func (m *Matcher) Stop() {
	close(m.stopCh)
}

// tryMatch attempts to assemble a lobby from the current queue
func (m *Matcher) tryMatch() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.queue) < m.config.MinPlayers {
		return
	}

	now := time.Now()
	eloRange := m.config.InitialEloRange

	// Group players by time window (FR-MM-03)
	var candidates []*MatchmakingRequest
	for _, req := range m.queue {
		if now.Sub(req.QueuedAt).Seconds() <= float64(m.config.WindowSeconds) {
			candidates = append(candidates, req)
		}
	}

	if len(candidates) < m.config.MinPlayers {
		return
	}

	// Try to find a match within Elo range, expanding over time (FR-MM-04)
	for len(candidates) >= m.config.MinPlayers && eloRange <= m.config.MaxEloRange {
		lobby := m.findEloMatch(candidates, eloRange)
		if lobby != nil && len(lobby) >= m.config.MinPlayers {
			// Remove matched players from queue
			for _, p := range lobby {
				m.removeFromQueueLocked(p.PlayerID)
			}
			m.lobbies <- lobby
			return
		}
		eloRange += m.config.RangeExpansionRate
	}
}

// findEloMatch finds players within an Elo range
func (m *Matcher) findEloMatch(candidates []*MatchmakingRequest, eloRange int) []*MatchmakingRequest {
	if len(candidates) == 0 {
		return nil
	}

	// Find average Elo of top candidate
	pivot := candidates[0]
	var lobby []*MatchmakingRequest
	lobby = append(lobby, pivot)

	for _, req := range candidates[1:] {
		if math.Abs(float64(req.EloRating-pivot.EloRating)) <= float64(eloRange) {
			lobby = append(lobby, req)
			if len(lobby) >= m.config.MaxPlayers {
				break
			}
		}
	}

	if len(lobby) >= m.config.MinPlayers {
		return lobby
	}
	return nil
}

// removeFromQueueLocked removes a player (must hold lock)
func (m *Matcher) removeFromQueueLocked(playerID string) {
	for i, req := range m.queue {
		if req.PlayerID == playerID {
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			return
		}
	}
}
