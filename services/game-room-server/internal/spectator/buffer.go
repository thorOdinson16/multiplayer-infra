package spectator

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/game"
)

// RingBuffer implements a fixed-size circular buffer for spectator delay (ADR-07)
type RingBuffer struct {
	mu       sync.RWMutex
	buffer   []*TickSnapshot
	size     int
	head     int
	tail     int
	count    int
	delay    time.Duration
}

// TickSnapshot is a timestamped game state snapshot
type TickSnapshot struct {
	Tick      uint64          `json:"tick"`
	Timestamp time.Time       `json:"timestamp"`
	State     *game.GameState `json:"state"`
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

	snapshot := &TickSnapshot{
		Tick:      tick,
		Timestamp: time.Now(),
		State:     state.GetSnapshot(),
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

// FlushLoop continuously sends delayed state to spectator connections
func (rb *RingBuffer) FlushLoop(spectators map[string]*websocket.Conn, stopCh chan struct{}) {
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

			for id, conn := range spectators {
				if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
					log.Printf("Failed to send to spectator %s: %v", id, err)
				}
			}
		case <-stopCh:
			return
		}
	}
}