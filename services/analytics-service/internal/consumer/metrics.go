package consumer

import "sync"

type Metrics struct {
	mu               sync.RWMutex
	TotalEvents      int64
	TotalMatches     int64
	ActiveMatches    map[string]bool
	MovementCount    int64
	AvgMatchDuration float64
}

func NewMetrics() *Metrics {
	return &Metrics{ActiveMatches: make(map[string]bool)}
}

func (m *Metrics) RecordEvent(event *TelemetryEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.TotalEvents++
	if event.Type == "match_start" {
		m.ActiveMatches[event.MatchID] = true
		m.TotalMatches++
	}
	if event.Type == "match_end" {
		delete(m.ActiveMatches, event.MatchID)
	}
	if event.Type == "move" {
		m.MovementCount++
	}
}

// Lock/Unlock expose the write lock semantics for callers that need exclusive access.
func (m *Metrics) Lock()   { m.mu.Lock() }
func (m *Metrics) Unlock() { m.mu.Unlock() }

// RLock/RUnlock expose read-lock semantics when callers only need shared access.
func (m *Metrics) RLock()   { m.mu.RLock() }
func (m *Metrics) RUnlock() { m.mu.RUnlock() }
