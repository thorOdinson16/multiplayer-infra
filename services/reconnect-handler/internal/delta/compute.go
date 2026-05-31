package delta

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
)

const MaxDeltaSize = 64 * 1024 // 64KB per FR-FT-06

// ReconnectPayload is the response sent to a reconnecting player
type ReconnectPayload struct {
	MatchID    string          `json:"matchId"`
	PlayerID   string          `json:"playerId"`
	Tick       uint64          `json:"tick"`
	StateDelta json.RawMessage `json:"stateDelta"`
	Compressed bool            `json:"compressed"`
}

// ComputeDelta computes a compressed state delta for a reconnecting player
// Returns the delta and whether compression was applied
func ComputeDelta(currentState []byte, playerID string) (*ReconnectPayload, error) {
	// Parse current state to extract player info
	var state map[string]interface{}
	if err := json.Unmarshal(currentState, &state); err != nil {
		return nil, fmt.Errorf("failed to parse state: %w", err)
	}

	// Extract tick from state
	tick, _ := state["tick"].(float64)
	matchID, _ := state["matchId"].(string)

	payload := &ReconnectPayload{
		MatchID:    matchID,
		PlayerID:   playerID,
		Tick:       uint64(tick),
		StateDelta: currentState,
		Compressed: false,
	}

	// Compress if payload exceeds threshold
	if len(currentState) > MaxDeltaSize/2 {
		compressed, err := compressData(currentState)
		if err == nil && len(compressed) <= MaxDeltaSize {
			payload.StateDelta = compressed
			payload.Compressed = true
		}
	}

	// Verify final size (FR-FT-06)
	if len(payload.StateDelta) > MaxDeltaSize {
		return nil, fmt.Errorf("delta payload exceeds 64KB limit: %d bytes", len(payload.StateDelta))
	}

	return payload, nil
}

// compressData compresses data using gzip
func compressData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := writer.Write(data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}