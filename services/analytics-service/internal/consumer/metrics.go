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

func (m *Metrics) Lock()    { m.mu.RLock() }
func (m *Metrics) Unlock()  { m.mu.RUnlock() }