package spectator

import (
	"testing"
	"time"

	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/game"
)

func TestRingBufferEnqueueAndGet(t *testing.T) {
	rb := NewRingBuffer(1, 20)
	gs := game.NewGameState("m1", 4)
	gs.AddPlayer("p1", "alice")
	rb.Enqueue(1, gs)
	time.Sleep(10 * time.Millisecond)
	snap := rb.GetSnapshot()
	if snap == nil {
		t.Fatalf("expected snapshot, got nil")
	}
	if snap.State == nil {
		t.Fatalf("expected spectator state")
	}
	if _, ok := snap.State.Players["p1"]; !ok {
		t.Fatalf("expected player p1 in snapshot")
	}
}
