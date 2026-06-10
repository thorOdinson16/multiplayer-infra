package raft

import (
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/game"
)

func TestFSMRestorePreservesPointer(t *testing.T) {
	gs := game.NewGameState("match-1", 4)
	gs.AddPlayer("p1", "alice")

	f := NewFSM(gs)

	// Create a snapshot and marshal
	snap := gs.GetSnapshot()
	snap.Tick = 42
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}

	// Restore into FSM
	rc := &fakeReadCloser{data: data}
	if err := f.Restore(rc); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	// Ensure the original pointer identity is preserved
	st := f.State()
	if st == nil {
		t.Fatalf("state nil after restore")
	}
	if st.Tick != 42 {
		t.Fatalf("expected tick 42, got %d", st.Tick)
	}

	// Ensure players copied
	if _, ok := st.Players["p1"]; !ok {
		t.Fatalf("expected player p1 present after restore")
	}

	// Slight delay to ensure no goroutines hold stale references to old state (sanity)
	time.Sleep(5 * time.Millisecond)
}

type fakeReadCloser struct {
	data []byte
	off  int
}

func (f *fakeReadCloser) Read(p []byte) (int, error) {
	if f.off >= len(f.data) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.off:])
	f.off += n
	return n, nil
}

func (f *fakeReadCloser) Close() error { return nil }
