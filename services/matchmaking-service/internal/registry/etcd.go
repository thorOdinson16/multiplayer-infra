package registry

import (
	"context"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// RoomInfo represents a registered game room
type RoomInfo struct {
	MatchID string
	Address string
	Status  string // "available", "active", "terminating"
}

// EtcdRegistry manages game room registration in etcd
type EtcdRegistry struct {
	client *clientv3.Client
	prefix string
}

// NewEtcdRegistry creates a new etcd registry
func NewEtcdRegistry(endpoints []string) (*EtcdRegistry, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	return &EtcdRegistry{
		client: client,
		prefix: "/game-rooms/",
	}, nil
}

// GetAvailableRoom finds an available game room
func (r *EtcdRegistry) GetAvailableRoom() (*RoomInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := r.client.Get(ctx, r.prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	for _, kv := range resp.Kvs {
		// Check if room is available
		value := string(kv.Value)
		if value == "available" {
			return &RoomInfo{
				MatchID: string(kv.Key[len(r.prefix):]),
				Address: "",
				Status:  "available",
			}, nil
		}
	}

	return nil, fmt.Errorf("no available rooms")
}

// RegisterRoom registers a new game room
func (r *EtcdRegistry) RegisterRoom(matchID, status string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	key := fmt.Sprintf("%s%s", r.prefix, matchID)
	_, err := r.client.Put(ctx, key, status)
	return err
}

// SetRoomStatus updates a room's status
func (r *EtcdRegistry) SetRoomStatus(matchID, status string) error {
	return r.RegisterRoom(matchID, status)
}

// WatchRooms watches for room changes
func (r *EtcdRegistry) WatchRooms() clientv3.WatchChan {
	return r.client.Watch(context.Background(), r.prefix, clientv3.WithPrefix())
}

// Close closes the etcd connection
func (r *EtcdRegistry) Close() error {
	return r.client.Close()
}

// DeregisterRoom removes a room from the registry
func (r *EtcdRegistry) DeregisterRoom(matchID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	key := fmt.Sprintf("%s%s", r.prefix, matchID)
	_, err := r.client.Delete(ctx, key)
	return err
}

// StartRoomWatcher watches for room status changes
func (r *EtcdRegistry) StartRoomWatcher() {
	watchChan := r.WatchRooms()
	go func() {
		for resp := range watchChan {
			for _, ev := range resp.Events {
				log.Printf("Room event: %s %s -> %s", ev.Type, ev.Kv.Key, ev.Kv.Value)
			}
		}
	}()
}