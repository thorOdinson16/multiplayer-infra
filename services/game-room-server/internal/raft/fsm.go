package raft

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/game"
)

// FSM implements the raft.FSM interface for game state replication
type FSM struct {
	mu    sync.RWMutex
	state *game.GameState
}

// NewFSM creates a new Raft FSM with initial game state
func NewFSM(state *game.GameState) *FSM {
	return &FSM{
		state: state,
	}
}

// State returns the current game state
func (f *FSM) State() *game.GameState {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state
}

// Apply applies a Raft log entry to the FSM
func (f *FSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	var event game.InputEvent
	if err := json.Unmarshal(log.Data, &event); err != nil {
		return err
	}

	game.ProcessInput(f.state, &event)
	return nil
}

// Snapshot returns a snapshot of the current state
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	snapshot := f.state.GetSnapshot()
	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, err
	}

	return &GameSnapshot{data: data}, nil
}

// Restore restores the FSM from a snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	// Protect restoring the state while we read and merge the restored snapshot.
	defer f.mu.Unlock()

	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}

	var restoredState game.GameState
	if err := json.Unmarshal(data, &restoredState); err != nil {
		return err
	}

	// Merge restoredState into the existing GameState object so other components
	// holding a pointer to f.state see the updated contents.
	if f.state == nil {
		// If for some reason we don't have an existing state pointer, adopt the restored one.
		f.state = &restoredState
		return nil
	}

	// Use GameState's helper to replace fields under its own lock.
	f.state.ReplaceWith(&restoredState)
	return nil
}

// GameSnapshot implements raft.FSMSnapshot
type GameSnapshot struct {
	data []byte
}

// Persist writes the snapshot to the given sink
func (s *GameSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release is a no-op for in-memory snapshots
func (s *GameSnapshot) Release() {}
