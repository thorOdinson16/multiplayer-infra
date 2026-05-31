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
	}
}

// Start begins the tick loop
func (tl *TickLoop) Start() {
	interval := time.Second / time.Duration(tl.tickRate)
	ticker := time.NewTicker(interval)

	log.Printf("Tick loop started: %d TPS (interval: %v)", tl.tickRate, interval)

	go func() {
		for {
			select {
			case <-ticker.C:
				tick := tl.state.IncrementTick()
				if tl.onTick != nil {
					tl.onTick(tick, tl.state)
				}
			case <-tl.stopCh:
				ticker.Stop()
				return
			}
		}
	}()
}

// Stop stops the tick loop
func (tl *TickLoop) Stop() {
	close(tl.stopCh)
}