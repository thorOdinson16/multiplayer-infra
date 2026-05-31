package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/game"
)

// RaftNode wraps a Raft consensus node
type RaftNode struct {
	raft    *raft.Raft
	fsm     *FSM
	dataDir string
}

// Config holds Raft node configuration
type Config struct {
	NodeID    string
	BindAddr  string // address to bind AND advertise (pod IP:port from K8s downward API)
	DataDir   string
	Bootstrap bool // true for first node in cluster
}

// NewRaftNode creates and starts a new Raft node
func NewRaftNode(cfg Config, gameState *game.GameState) (*RaftNode, error) {
	// Create FSM
	fsm := NewFSM(gameState)

	// Create Raft config
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.HeartbeatTimeout = 500 * time.Millisecond
	raftConfig.ElectionTimeout = 500 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 250 * time.Millisecond
	raftConfig.CommitTimeout = 25 * time.Millisecond

	// Create directories
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	// BoltDB for stable log storage
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-log.bolt"))
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %w", err)
	}

	// BoltDB for stable KV storage
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-stable.bolt"))
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store: %w", err)
	}

	// In-memory snapshot store
	snapshotStore := raft.NewInmemSnapshotStore()

	// Resolve the bind address for TCP transport
	tcpAddr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve bind address: %w", err)
	}

	// TCP transport — uses the pod's real IP which is advertisable
	transport, err := raft.NewTCPTransport(
		cfg.BindAddr,
		tcpAddr,
		3,
		10*time.Second,
		os.Stderr,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	// Create Raft instance
	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}

	// Bootstrap cluster if this is the first node
	if cfg.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(cfg.NodeID),
					Address: raft.ServerAddress(cfg.BindAddr),
				},
			},
		}
		r.BootstrapCluster(configuration)
	}

	rn := &RaftNode{
		raft:    r,
		fsm:     fsm,
		dataDir: cfg.DataDir,
	}

	log.Printf("Raft node %s started on %s", cfg.NodeID, cfg.BindAddr)
	return rn, nil
}

// IsLeader returns true if this node is the Raft leader
func (rn *RaftNode) IsLeader() bool {
	return rn.raft.State() == raft.Leader
}

// ApplyInput applies a player input through Raft consensus
func (rn *RaftNode) ApplyInput(event *game.InputEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	future := rn.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to apply to raft: %w", err)
	}

	return nil
}

// LeaderAddress returns the address of the current leader
func (rn *RaftNode) LeaderAddress() raft.ServerAddress {
	return rn.raft.Leader()
}

// GetState returns the current game state from the FSM
func (rn *RaftNode) GetState() *game.GameState {
	return rn.fsm.State()
}

// Shutdown gracefully shuts down the Raft node
func (rn *RaftNode) Shutdown() error {
	future := rn.raft.Shutdown()
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to shutdown raft: %w", err)
	}
	log.Println("Raft node shutdown complete")
	return nil
}