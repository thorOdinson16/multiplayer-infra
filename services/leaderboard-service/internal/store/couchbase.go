package store

import (
	"github.com/couchbase/gocb/v2"
)

// LeaderboardEntry represents a player's ranking data
type LeaderboardEntry struct {
	PlayerID     string  `json:"playerId"`
	Username     string  `json:"username"`
	EloRating    int     `json:"eloRating"`
	Wins         int     `json:"wins"`
	Losses       int     `json:"losses"`
	TotalMatches int     `json:"totalMatches"`
	WinRate      float64 `json:"winRate"`
	AverageScore float64 `json:"averageScore"`
	Rank         int     `json:"rank"`
}

// MatchOutcome represents a match result to write
type MatchOutcome struct {
	MatchID   string         `json:"matchId"`
	Players   []string       `json:"players"`
	Winner    string         `json:"winner"`
	Scores    map[string]int `json:"scores"`
	StartedAt string         `json:"startedAt"`
	EndedAt   string         `json:"endedAt"`
}

// CouchbaseStore handles leaderboard operations
type CouchbaseStore struct {
	cluster *gocb.Cluster
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

	return &CouchbaseStore{cluster: cluster}, nil
}

// UpdatePlayerStats updates player stats after a match (FR-LB-01)
func (s *CouchbaseStore) UpdatePlayerStats(outcome *MatchOutcome) error {
	for _, playerID := range outcome.Players {
		isWinner := playerID == outcome.Winner
		score := outcome.Scores[playerID]

		var ops []gocb.MutateInSpec
		ops = append(ops, gocb.IncrementSpec("totalMatches", int64(1), nil))
		if isWinner {
			ops = append(ops, gocb.IncrementSpec("wins", int64(1), nil))
		} else {
			ops = append(ops, gocb.IncrementSpec("losses", int64(1), nil))
		}
		ops = append(ops, gocb.IncrementSpec("averageScore", int64(score), nil))

		bucket := s.cluster.Bucket("players")
		coll := bucket.Scope("_default").Collection("_default")
		_, err := coll.MutateIn(playerID, ops, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetLeaderboard returns ranked players by time window (FR-LB-02, FR-LB-03)
func (s *CouchbaseStore) GetLeaderboard(window string, limit int) ([]LeaderboardEntry, error) {
	query := "SELECT p.playerId, p.username, p.eloRating, p.wins, p.losses, " +
		"p.totalMatches, p.averageScore, " +
		"ROUND((p.wins * 100.0) / GREATEST(p.totalMatches, 1), 2) AS winRate " +
		"FROM players p " +
		"WHERE p.type = 'player' " +
		"ORDER BY p.wins DESC, p.eloRating DESC " +
		"LIMIT $1"

	rows, err := s.cluster.Query(query, &gocb.QueryOptions{
		PositionalParameters: []interface{}{limit},
	})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []LeaderboardEntry
	rank := 1
	for rows.Next() {
		var entry LeaderboardEntry
		if err := rows.Row(&entry); err != nil {
			return nil, err
		}
		entry.Rank = rank
		entries = append(entries, entry)
		rank++
	}

	return entries, nil
}

// GetPlayerStats returns a single player's stats (FR-LB-05)
func (s *CouchbaseStore) GetPlayerStats(playerID string) (*LeaderboardEntry, error) {
	bucket := s.cluster.Bucket("players")
	coll := bucket.Scope("_default").Collection("_default")

	result, err := coll.Get(playerID, nil)
	if err != nil {
		return nil, err
	}

	var player struct {
		PlayerID     string  `json:"playerId"`
		Username     string  `json:"username"`
		EloRating    int     `json:"eloRating"`
		Wins         int     `json:"wins"`
		Losses       int     `json:"losses"`
		TotalMatches int     `json:"totalMatches"`
		AverageScore float64 `json:"averageScore"`
	}
	if err := result.Content(&player); err != nil {
		return nil, err
	}

	winRate := 0.0
	if player.TotalMatches > 0 {
		winRate = float64(player.Wins) * 100.0 / float64(player.TotalMatches)
	}

	return &LeaderboardEntry{
		PlayerID:     player.PlayerID,
		Username:     player.Username,
		EloRating:    player.EloRating,
		Wins:         player.Wins,
		Losses:       player.Losses,
		TotalMatches: player.TotalMatches,
		WinRate:      winRate,
		AverageScore: player.AverageScore,
	}, nil
}

// Close closes the Couchbase connection
func (s *CouchbaseStore) Close() error {
	return s.cluster.Close(nil)
}