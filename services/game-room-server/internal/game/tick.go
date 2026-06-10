// services/game-room-server/internal/game/tick.go
package game

import (
	"log"
	"time"
)

// TickRate defines the game simulation rate (FR-GR-03)
const DefaultTickRate = 20 // 20 TPS = 50ms per tick

// TickLoop runs the game simulation at a fixed rate
type TickLoop struct {
	state    *GameState
	tickRate int
	stopCh   chan struct{}
	onTick   func(tick uint64, state *GameState)
	running  bool
}

// NewTickLoop creates a new tick loop
func NewTickLoop(state *GameState, tickRate int, onTick func(tick uint64, state *GameState)) *TickLoop {
	if tickRate <= 0 {
		tickRate = DefaultTickRate
	}
	return &TickLoop{
		state:    state,
		tickRate: tickRate,
		stopCh:   make(chan struct{}),
		onTick:   onTick,
		running:  false,
	}
}

// Start begins the tick loop
func (tl *TickLoop) Start() {
	if tl.running {
		log.Println("Tick loop already running")
		return
	}

	interval := time.Second / time.Duration(tl.tickRate)
	ticker := time.NewTicker(interval)
	tl.running = true

	log.Printf("Tick loop started: %d TPS (interval: %v)", tl.tickRate, interval)

	go func() {
		for {
			select {
			case <-ticker.C:
				// Check if state is finished before ticking
				if tl.state.IsMatchFinished() {
					log.Printf("Match is finished, stopping tick loop")
					ticker.Stop()
					tl.running = false
					return
				}

				tick := tl.state.IncrementTick()
				if tl.onTick != nil {
					tl.onTick(tick, tl.state)
				}
			case <-tl.stopCh:
				ticker.Stop()
				tl.running = false
				return
			}
		}
	}()
}

// Stop stops the tick loop
func (tl *TickLoop) Stop() {
	if !tl.running {
		return
	}
	close(tl.stopCh)
	log.Println("Tick loop stopped")
}

// IsRunning returns whether the tick loop is running
func (tl *TickLoop) IsRunning() bool {
	return tl.running
}
