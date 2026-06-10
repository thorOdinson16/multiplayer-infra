package store

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// RedisStore handles Redis operations for reconnection state
type RedisStore struct {
	client *goredis.Client
}

// NewRedisStore creates a new Redis store connection
func NewRedisStore(addr, password string) (*RedisStore, error) {
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

	return &RedisStore{client: client}, nil
}

// GetPlayerDelta retrieves a player's last state delta (§8.3)
func (s *RedisStore) GetPlayerDelta(ctx context.Context, playerID string) ([]byte, error) {
	key := fmt.Sprintf("player:%s:delta", playerID)
	return s.client.Get(ctx, key).Bytes()
}

// IsPlayerInMatch checks if a player is still in an active match
func (s *RedisStore) IsPlayerInMatch(ctx context.Context, matchID, playerID string) (bool, error) {
	key := fmt.Sprintf("match:%s:players", matchID)
	return s.client.SIsMember(ctx, key, playerID).Result()
}

// GetMatchState retrieves the current match state
func (s *RedisStore) GetMatchState(ctx context.Context, matchID string) ([]byte, error) {
	key := fmt.Sprintf("match:%s:state", matchID)
	return s.client.Get(ctx, key).Bytes()
}

// Close closes the Redis connection
func (s *RedisStore) Close() error {
	return s.client.Close()
}
