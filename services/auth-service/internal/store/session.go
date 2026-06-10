package store

import (
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
)

// Player represents a player document in Couchbase (§8.1)
type Player struct {
	Type         string  `json:"type"`
	PlayerID     string  `json:"playerId"`
	Username     string  `json:"username"`
	PasswordHash string  `json:"passwordHash"`
	EloRating    int     `json:"eloRating"`
	Wins         int     `json:"wins"`
	Losses       int     `json:"losses"`
	TotalMatches int     `json:"totalMatches"`
	AverageScore float64 `json:"averageScore"`
	CreatedAt    string  `json:"createdAt"`
	LastSeen     string  `json:"lastSeen"`
}

// Session represents a session document in Couchbase (§8.1)
type Session struct {
	Type      string `json:"type"`
	SessionID string `json:"sessionId"`
	PlayerID  string `json:"playerId"`
	Token     string `json:"token"`
	ExpiresAt string `json:"expiresAt"`
	IPAddress string `json:"ipAddress"`
}

// CouchbaseStore handles Couchbase operations for auth
type CouchbaseStore struct {
	cluster     *gocb.Cluster
	bucket      *gocb.Bucket
	playerColl  *gocb.Collection
	sessionColl *gocb.Collection
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

	bucket := cluster.Bucket("players")
	if err := bucket.WaitUntilReady(30*time.Second, nil); err != nil {
		cluster.Close(nil)
		return nil, err
	}
	playerColl := bucket.Scope("_default").Collection("_default")

	sessionBucket := cluster.Bucket("sessions")
	if err := sessionBucket.WaitUntilReady(30*time.Second, nil); err != nil {
		cluster.Close(nil)
		return nil, err
	}
	sessionColl := sessionBucket.Scope("_default").Collection("_default")

	return &CouchbaseStore{
		cluster:     cluster,
		bucket:      bucket,
		playerColl:  playerColl,
		sessionColl: sessionColl,
	}, nil
}

// GetPlayerByUsername retrieves a player by username
func (s *CouchbaseStore) GetPlayerByUsername(username string) (*Player, error) {
	query := "SELECT p.* FROM players p WHERE p.type = 'player' AND p.username = $1"
	rows, err := s.cluster.Query(query, &gocb.QueryOptions{
		PositionalParameters: []interface{}{username},
	})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var player Player
	for rows.Next() {
		if err := rows.Row(&player); err != nil {
			return nil, err
		}
		return &player, nil
	}

	return nil, gocb.ErrDocumentNotFound
}

// CreatePlayer inserts a new player document
func (s *CouchbaseStore) CreatePlayer(player *Player) error {
	player.Type = "player"
	player.EloRating = 1200
	player.CreatedAt = time.Now().UTC().Format(time.RFC3339)
	player.LastSeen = player.CreatedAt

	// Create a username -> playerID mapping document to enforce uniqueness.
	// Key format: username::<username>
	mappingKey := "username::" + player.Username
    _, err := s.playerColl.Insert(mappingKey, map[string]string{"playerId": player.PlayerID}, nil)
	if err != nil {
		return err
	}

	// Insert player document using the playerID key. If this fails, attempt to remove the mapping.
	_, err = s.playerColl.Insert(player.PlayerID, player, nil)
	if err != nil {
		// Best-effort cleanup of mapping
        _ = s.playerColl.Remove(mappingKey, nil)
		return err
	}
	return nil
}

// CreateSession stores a session document with TTL matching JWT expiry
func (s *CouchbaseStore) CreateSession(session *Session, ttlHours int) error {
	session.Type = "session"
	session.SessionID = uuid.New().String()

	_, err := s.sessionColl.Insert(session.SessionID, session, &gocb.InsertOptions{
		Expiry: time.Duration(ttlHours) * time.Hour,
	})
	return err
}

// GetSession retrieves a session by session ID
func (s *CouchbaseStore) GetSession(sessionID string) (*Session, error) {
	result, err := s.sessionColl.Get(sessionID, nil)
	if err != nil {
		return nil, err
	}

	var session Session
	if err := result.Content(&session); err != nil {
		return nil, err
	}

	return &session, nil
}

// DeleteSession removes a session (logout)
func (s *CouchbaseStore) DeleteSession(sessionID string) error {
	_, err := s.sessionColl.Remove(sessionID, nil)
	return err
}

// UpdateLastSeen updates the lastSeen timestamp on a player
func (s *CouchbaseStore) UpdateLastSeen(playerID string) error {
	_, err := s.playerColl.MutateIn(playerID, []gocb.MutateInSpec{
		gocb.UpsertSpec("lastSeen", time.Now().UTC().Format(time.RFC3339), nil),
	}, nil)
	return err
}

// Close closes the Couchbase cluster connection
func (s *CouchbaseStore) Close() error {
	return s.cluster.Close(nil)
}
