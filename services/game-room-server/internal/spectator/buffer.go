package spectator

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/game"
)

// RingBuffer implements a fixed-size circular buffer for spectator delay (ADR-07)
type RingBuffer struct {
	mu     sync.RWMutex
	buffer []*TickSnapshot
	size   int
	head   int
	tail   int
	count  int
	delay  time.Duration
}

// TickSnapshot is a timestamped game state snapshot
type SpectatorState struct {
	MatchID    string                      `json:"matchId"`
	Tick       uint64                      `json:"tick"`
	Players    map[string]game.PlayerState `json:"players"`
	StartTime  time.Time                   `json:"startTime"`
	MaxPlayers int                         `json:"maxPlayers"`
	Status     string                      `json:"status"`
	Duration   int                         `json:"duration"`
}

// TickSnapshot is a timestamped lightweight game state snapshot for spectators
type TickSnapshot struct {
	Tick      uint64          `json:"tick"`
	Timestamp time.Time       `json:"timestamp"`
	State     *SpectatorState `json:"state"`
}

// NewRingBuffer creates a new ring buffer
// maxDelaySeconds: maximum delay in seconds
// tickRate: ticks per second
func NewRingBuffer(maxDelaySeconds, tickRate int) *RingBuffer {
	size := maxDelaySeconds * tickRate // e.g., 30 * 20 = 600
	return &RingBuffer{
		buffer: make([]*TickSnapshot, size),
		size:   size,
		delay:  time.Duration(maxDelaySeconds) * time.Second,
	}
}

// Enqueue adds a tick snapshot to the buffer (non-blocking)
func (rb *RingBuffer) Enqueue(tick uint64, state *game.GameState) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Build a lightweight spectator view to avoid copying sync.RWMutex and other runtime state.
	gs := state.GetSnapshot()
	sp := &SpectatorState{
		MatchID:    gs.MatchID,
		Tick:       gs.Tick,
		Players:    make(map[string]game.PlayerState, len(gs.Players)),
		StartTime:  gs.StartTime,
		MaxPlayers: gs.MaxPlayers,
		Status:     gs.Status,
		Duration:   gs.Duration,
	}
	for id, p := range gs.Players {
		sp.Players[id] = *p
	}

	snapshot := &TickSnapshot{
		Tick:      tick,
		Timestamp: time.Now(),
		State:     sp,
	}

	rb.buffer[rb.head] = snapshot
	rb.head = (rb.head + 1) % rb.size

	if rb.count < rb.size {
		rb.count++
	} else {
		rb.tail = (rb.tail + 1) % rb.size
	}
}

// GetSnapshot returns a state snapshot delayed by the configured amount
func (rb *RingBuffer) GetSnapshot() *TickSnapshot {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return nil
	}

	now := time.Now()
	targetTime := now.Add(-rb.delay)

	// Find the snapshot closest to the target time
	var best *TickSnapshot
	idx := rb.tail
	for i := 0; i < rb.count; i++ {
		snap := rb.buffer[idx]
		if snap.Timestamp.Before(targetTime) || snap.Timestamp.Equal(targetTime) {
			best = snap
		} else {
			break
		}
		idx = (idx + 1) % rb.size
	}

	return best
}

// FlushLoop continuously sends delayed state to spectator connections.
func (rb *RingBuffer) FlushLoop(broadcaster *game.Broadcaster, stopCh <-chan struct{}) {
	ticker := time.NewTicker(50 * time.Millisecond) // 20 TPS flush rate
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			snapshot := rb.GetSnapshot()
			if snapshot == nil {
				continue
			}

			data, err := json.Marshal(snapshot)
			if err != nil {
				log.Printf("Failed to marshal spectator snapshot: %v", err)
				continue
			}

			broadcaster.BroadcastSpectatorPayload(data)
		case <-stopCh:
			return
		}
	}
}
