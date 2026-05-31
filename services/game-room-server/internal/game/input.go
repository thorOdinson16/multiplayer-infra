package game

// InputEvent represents a player input from a WebSocket message
type InputEvent struct {
	Type     string  `json:"type"`     // "move", "shoot", "interact"
	PlayerID string  `json:"playerId"`
	X        float64 `json:"x"`
	Y        float64 `json:"y"`
	Delta    float64 `json:"delta,omitempty"`
}

// ProcessInput applies a player input to the game state
func ProcessInput(state *GameState, event *InputEvent) {
	switch event.Type {
	case "move":
		state.UpdatePlayerPosition(event.PlayerID, event.X, event.Y)
	default:
		// Other input types handled in future iterations
	}
}