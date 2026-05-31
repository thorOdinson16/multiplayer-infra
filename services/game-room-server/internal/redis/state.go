package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/game"
)

// StateStore manages per-match state in Redis (§8.3)
type StateStore struct {
	client *goredis.Client
}

// NewStateStore creates a new Redis state store
func NewStateStore(addr, password string) (*StateStore, error) {
	client := goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &StateStore{client: client}, nil
}

// SaveState persists the current game state to Redis
func (s *StateStore) SaveState(ctx context.Context, matchID string, state *game.GameState, ttl time.Duration) error {
	key := fmt.Sprintf("match:%s:state", matchID)
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return s.client.Set(ctx, key, data, ttl).Err()
}

// GetState retrieves game state from Redis
func (s *StateStore) GetState(ctx context.Context, matchID string) (*game.GameState, error) {
	key := fmt.Sprintf("match:%s:state", matchID)
	data, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		return nil, err
	}

	var state game.GameState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}

	return &state, nil
}

// SavePlayerDelta stores a player's last state delta for reconnection
func (s *StateStore) SavePlayerDelta(ctx context.Context, playerID string, delta []byte) error {
	key := fmt.Sprintf("player:%s:delta", playerID)
	return s.client.Set(ctx, key, delta, 30*time.Second).Err()
}

// GetPlayerDelta retrieves a player's last state delta
func (s *StateStore) GetPlayerDelta(ctx context.Context, playerID string) ([]byte, error) {
	key := fmt.Sprintf("player:%s:delta", playerID)
	return s.client.Get(ctx, key).Bytes()
}

// SaveSpectatorBuffer persists the spectator buffer
func (s *StateStore) SaveSpectatorBuffer(ctx context.Context, matchID string, entries []string, ttl time.Duration) error {
	key := fmt.Sprintf("match:%s:spectator_buffer", matchID)
	pipe := s.client.Pipeline()
	pipe.Del(ctx, key)
	for _, entry := range entries {
		pipe.RPush(ctx, key, entry)
	}
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// Close closes the Redis connection
func (s *StateStore) Close() error {
	return s.client.Close()
}