package store

import (
	"time"

	"github.com/couchbase/gocb/v2"
)

// MatchRecord represents a completed match in Couchbase (§8.1)
type MatchRecord struct {
	Type        string           `json:"type"`
	MatchID     string           `json:"matchId"`
	Players     []string         `json:"players"`
	StartedAt   string           `json:"startedAt"`
	EndedAt     string           `json:"endedAt"`
	DurationSec int              `json:"durationSeconds"`
	Outcome     MatchOutcome     `json:"outcome"`
}

// MatchOutcome represents match results
type MatchOutcome struct {
	Winner string         `json:"winner"`
	Scores map[string]int `json:"scores"`
}

// CouchbaseStore handles Couchbase operations for game rooms
type CouchbaseStore struct {
	cluster    *gocb.Cluster
	matchColl  *gocb.Collection
}

// NewCouchbaseStore creates a new Couchbase store connection
func NewCouchbaseStore(connStr, username, password string) (*CouchbaseStore, error) {
	cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
		Username: username,
		Password: password,
	})
	if err != nil {
		return nil, err
	}

	bucket := cluster.Bucket("matches")
	coll := bucket.Scope("_default").Collection("_default")

	return &CouchbaseStore{
		cluster:   cluster,
		matchColl: coll,
	}, nil
}

// SaveMatch records a completed match
func (s *CouchbaseStore) SaveMatch(record *MatchRecord) error {
	record.Type = "match"
	_, err := s.matchColl.Insert(record.MatchID, record, nil)
	return err
}

// GetMatch retrieves a match record
func (s *CouchbaseStore) GetMatch(matchID string) (*MatchRecord, error) {
	result, err := s.matchColl.Get(matchID, nil)
	if err != nil {
		return nil, err
	}

	var record MatchRecord
	if err := result.Content(&record); err != nil {
		return nil, err
	}

	return &record, nil
}

// Close closes the Couchbase connection
func (s *CouchbaseStore) Close() error {
	return s.cluster.Close(nil)
}