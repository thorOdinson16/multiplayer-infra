package consumer

import (
	"sync"
	"testing"
)

func TestMetricsConcurrency(t *testing.T) {
	m := NewMetrics()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.RecordEvent(&TelemetryEvent{Type: "move", MatchID: "m1"})
		}()
	}
	wg.Wait()
	if m.TotalEvents != 100 {
		t.Fatalf("expected 100 events, got %d", m.TotalEvents)
	}
}
