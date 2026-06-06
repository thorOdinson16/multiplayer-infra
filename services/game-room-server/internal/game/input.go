package game

import (
	"context"
	"log"
)

// InputEvent represents a player input from a WebSocket message
type InputEvent struct {
	Ctx      context.Context `json:"-"`
	Type     string          `json:"type"`
	PlayerID string          `json:"playerId"`
	DeltaX   float64         `json:"deltaX,omitempty"`
	DeltaY   float64         `json:"deltaY,omitempty"`
	X        float64         `json:"x,omitempty"`
	Y        float64         `json:"y,omitempty"`
}

// ProcessInput applies a player input to the game state
func ProcessInput(state *GameState, event *InputEvent) {
	log.Printf("📥 Processing input: type=%s, player=%s, deltaX=%.1f, deltaY=%.1f",
		event.Type, event.PlayerID, event.DeltaX, event.DeltaY)

	switch event.Type {
	case "move":
		if event.DeltaX != 0 || event.DeltaY != 0 {
			// Relative movement - use the new MovePlayer method
			state.MovePlayer(event.PlayerID, event.DeltaX, event.DeltaY)
		} else if event.X != 0 || event.Y != 0 {
			// Absolute position
			state.UpdatePlayerPosition(event.PlayerID, event.X, event.Y)
			log.Printf("📍 Player %s set to absolute position (%.1f,%.1f)", event.PlayerID, event.X, event.Y)
		}
	default:
		log.Printf("❓ Unknown input type: %s", event.Type)
	}
}
