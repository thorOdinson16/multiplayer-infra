package store

import (
	"fmt"
	"time"

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

type PlayerMatchStat struct {
	Type      string `json:"type"`
	MatchID   string `json:"matchId"`
	PlayerID  string `json:"playerId"`
	Score     int    `json:"score"`
	Win       int    `json:"win"`
	Loss      int    `json:"loss"`
	Timestamp string `json:"timestamp"`
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
	bucket := s.cluster.Bucket("players")
	coll := bucket.Scope("_default").Collection("_default")
	timestamp := outcome.EndedAt
	if timestamp == "" {
		timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	for _, playerID := range outcome.Players {
		isWinner := playerID == outcome.Winner
		score := outcome.Scores[playerID]
		win := 0
		loss := 1
		if isWinner {
			win = 1
			loss = 0
		}

		query := "UPDATE players AS p USE KEYS $playerID " +
			"SET p.totalMatches = IFMISSINGORNULL(p.totalMatches, 0) + 1, " +
			"p.wins = IFMISSINGORNULL(p.wins, 0) + $win, " +
			"p.losses = IFMISSINGORNULL(p.losses, 0) + $loss, " +
			"p.scoreSum = IFMISSINGORNULL(p.scoreSum, 0) + $score, " +
			"p.averageScore = ROUND((IFMISSINGORNULL(p.scoreSum, 0) + $score) / (IFMISSINGORNULL(p.totalMatches, 0) + 1), 2) " +
			"RETURNING p.playerId"
		rows, err := s.cluster.Query(query, &gocb.QueryOptions{
			NamedParameters: map[string]interface{}{
				"playerID": playerID,
				"win":      win,
				"loss":     loss,
				"score":    score,
			},
		})
		if err != nil {
			return err
		}
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()

		stat := PlayerMatchStat{
			Type:      "player_match_stat",
			MatchID:   outcome.MatchID,
			PlayerID:  playerID,
			Score:     score,
			Win:       win,
			Loss:      loss,
			Timestamp: timestamp,
		}
		docID := fmt.Sprintf("player_match_stat::%s::%s", outcome.MatchID, playerID)
		if _, err := coll.Upsert(docID, stat, nil); err != nil {
			return err
		}
	}
	return nil
}

// GetLeaderboard returns ranked players by time window (FR-LB-02, FR-LB-03)
func (s *CouchbaseStore) GetLeaderboard(window string, limit int) ([]LeaderboardEntry, error) {
	query := "SELECT p.playerId, p.username, p.eloRating, p.wins, p.losses, " +
		"p.totalMatches, IFMISSINGORNULL(p.averageScore, 0) AS averageScore, " +
		"ROUND((p.wins * 100.0) / GREATEST(p.totalMatches, 1), 2) AS winRate " +
		"FROM players p " +
		"WHERE p.type = 'player' " +
		"ORDER BY p.wins DESC, p.eloRating DESC " +
		"LIMIT $limit"
	params := map[string]interface{}{"limit": limit}

	if window == "daily" || window == "weekly" {
		cutoff := time.Now().UTC().Add(-24 * time.Hour)
		if window == "weekly" {
			cutoff = time.Now().UTC().Add(-7 * 24 * time.Hour)
		}
		query = "SELECT p.playerId, p.username, p.eloRating, " +
			"SUM(s.win) AS wins, SUM(s.loss) AS losses, COUNT(1) AS totalMatches, " +
			"ROUND(AVG(s.score), 2) AS averageScore, " +
			"ROUND((SUM(s.win) * 100.0) / GREATEST(COUNT(1), 1), 2) AS winRate " +
			"FROM players s JOIN players p ON KEYS s.playerId " +
			"WHERE s.type = 'player_match_stat' AND s.timestamp >= $cutoff " +
			"GROUP BY p.playerId, p.username, p.eloRating " +
			"ORDER BY wins DESC, p.eloRating DESC " +
			"LIMIT $limit"
		params["cutoff"] = cutoff.Format(time.RFC3339)
	}

	rows, err := s.cluster.Query(query, &gocb.QueryOptions{NamedParameters: params})
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
