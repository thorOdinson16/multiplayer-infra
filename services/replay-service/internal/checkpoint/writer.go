package checkpoint

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/thorOdinson16/multiplayer-infra/services/replay-service/internal/consumer"
)

// CheckpointRecord represents a replay checkpoint (§8.1)
type CheckpointRecord struct {
	Type           string          `json:"type"`
	MatchID        string          `json:"matchId"`
	Tick           uint64          `json:"tick"`
	SnapshotState  json.RawMessage `json:"snapshotState"`
	KafkaOffset    int64           `json:"kafkaOffset"`
	CreatedAt      string          `json:"createdAt"`
}

// Writer manages checkpoint persistence to Couchbase (FR-RP-03)
type Writer struct {
	cluster    *gocb.Cluster
	coll       *gocb.Collection
	mu         sync.Mutex
	events     map[string][]*consumer.MatchEvent // matchID -> events
}

// NewWriter creates a new checkpoint writer
func NewWriter(connStr, username, password string) (*Writer, error) {
	cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
		Username: username,
		Password: password,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Couchbase: %w", err)
	}

	bucket := cluster.Bucket("replays")
	coll := bucket.Scope("_default").Collection("_default")

	return &Writer{
		cluster: cluster,
		coll:    coll,
		events:  make(map[string][]*consumer.MatchEvent),
	}, nil
}

// AppendEvent stores an event in memory
func (w *Writer) AppendEvent(event *consumer.MatchEvent) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events[event.MatchID] = append(w.events[event.MatchID], event)
}

// WriteCheckpoint persists a checkpoint every 300 ticks (FR-RP-03)
func (w *Writer) WriteCheckpoint(matchID string, tick uint64) error {
	w.mu.Lock()
	events := w.events[matchID]
	w.mu.Unlock()

	if len(events) == 0 {
		return nil
	}

	lastEvent := events[len(events)-1]
	record := CheckpointRecord{
		Type:          "replay_checkpoint",
		MatchID:       matchID,
		Tick:          tick,
		SnapshotState: lastEvent.State,
		KafkaOffset:   0,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
	}

	key := fmt.Sprintf("%s:%d", matchID, tick)
	_, err := w.coll.Upsert(key, record, nil)
	if err != nil {
		return fmt.Errorf("failed to write checkpoint: %w", err)
	}

	log.Printf("Checkpoint written: match=%s tick=%d", matchID, tick)
	return nil
}

// GetCheckpoint retrieves the nearest checkpoint for seeking (FR-RP-02)
func (w *Writer) GetCheckpoint(matchID string, tick uint64) (*CheckpointRecord, error) {
	// Find the nearest checkpoint at or before the requested tick
	key := fmt.Sprintf("%s:%d", matchID, tick)
	result, err := w.coll.Get(key, nil)
	if err == nil {
		var record CheckpointRecord
		if err := result.Content(&record); err == nil {
			return &record, nil
		}
	}

	// Try the most recent checkpoint
	query := fmt.Sprintf(
		"SELECT r.* FROM replays r WHERE r.type = 'replay_checkpoint' AND r.matchId = '%s' AND r.tick <= %d ORDER BY r.tick DESC LIMIT 1",
		matchID, tick,
	)
	rows, err := w.cluster.Query(query, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var record CheckpointRecord
	for rows.Next() {
		if err := rows.Row(&record); err != nil {
			return nil, err
		}
		return &record, nil
	}

	return nil, fmt.Errorf("no checkpoint found for match %s tick %d", matchID, tick)
}

// Close closes the Couchbase connection
func (w *Writer) Close() error {
	return w.cluster.Close(nil)
}