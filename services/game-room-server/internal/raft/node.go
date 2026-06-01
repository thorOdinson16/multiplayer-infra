package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/game"
)

type RaftNode struct {
	raft    *raft.Raft
	fsm     *FSM
	dataDir string
	nodeID  string
}

type Config struct {
	NodeID    string
	BindAddr  string
	DataDir   string
	Bootstrap bool
	Service   string
	Namespace string
}

func NewRaftNode(cfg Config, gameState *game.GameState) (*RaftNode, error) {
	fsm := NewFSM(gameState)

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.HeartbeatTimeout = 500 * time.Millisecond
	raftConfig.ElectionTimeout = 500 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 250 * time.Millisecond
	raftConfig.CommitTimeout = 25 * time.Millisecond

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-log.bolt"))
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %w", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-stable.bolt"))
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store: %w", err)
	}

	snapshotStore := raft.NewInmemSnapshotStore()

	tcpAddr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve bind address: %w", err)
	}

	transport, err := raft.NewTCPTransport(cfg.BindAddr, tcpAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}

	rn := &RaftNode{
		raft:    r,
		fsm:     fsm,
		dataDir: cfg.DataDir,
		nodeID:  cfg.NodeID,
	}

	// Bootstrap only if this is the first node AND bootstrap flag is true
	if cfg.Bootstrap {
		// Wait a bit for other pods to be ready
		time.Sleep(2 * time.Second)
		
		// Discover all peers in the StatefulSet
		peers, err := rn.discoverPeers(cfg)
		if err != nil {
			log.Printf("Warning: Failed to discover peers: %v", err)
			// Bootstrap with just this node as fallback
			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raft.ServerID(cfg.NodeID),
						Address: raft.ServerAddress(cfg.BindAddr),
					},
				},
			}
			r.BootstrapCluster(configuration)
		} else if len(peers) > 0 {
			configuration := raft.Configuration{
				Servers: peers,
			}
			r.BootstrapCluster(configuration)
			log.Printf("Bootstrapped cluster with %d peers", len(peers))
		}
	} else {
		// Non-bootstrap nodes: try to join the existing cluster
		go rn.joinCluster(cfg)
	}

	log.Printf("Raft node %s started on %s", cfg.NodeID, cfg.BindAddr)
	return rn, nil
}

func (rn *RaftNode) discoverPeers(cfg Config) ([]raft.Server, error) {
	// Parse ordinal from pod name (e.g., game-room-server-0 -> 0)
	parts := strings.Split(cfg.NodeID, "-")
	if len(parts) < 2 {
		return nil, fmt.Errorf("cannot parse ordinal from node ID: %s", cfg.NodeID)
	}
	
	currentOrdinal, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return nil, fmt.Errorf("invalid ordinal in node ID: %s", cfg.NodeID)
	}

	var servers []raft.Server
	
	// Add all peers with lower ordinal as potential voters
	for i := 0; i < currentOrdinal; i++ {
		peerID := fmt.Sprintf("game-room-server-%d", i)
		peerAddr := fmt.Sprintf("%s.%s.%s.svc.cluster.local:7000", peerID, cfg.Service, cfg.Namespace)
		
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(peerID),
			Address: raft.ServerAddress(peerAddr),
		})
	}
	
	// Add current node
	servers = append(servers, raft.Server{
		ID:      raft.ServerID(cfg.NodeID),
		Address: raft.ServerAddress(cfg.BindAddr),
	})
	
	return servers, nil
}

func (rn *RaftNode) joinCluster(cfg Config) {
	// Wait for leader to be elected
	time.Sleep(5 * time.Second)
	
	// Try to add this node to the cluster via the leader
	for i := 0; i < 30; i++ {
		leader := rn.raft.Leader()
		if leader != "" {
			log.Printf("Found leader: %s, attempting to join", leader)
			// In a real implementation, you'd make an API call to the leader
			// For now, just wait for the leader to discover this node via DNS
			return
		}
		time.Sleep(2 * time.Second)
	}
	
	log.Printf("Could not find leader after 60 seconds, running as standalone")
}

func (rn *RaftNode) IsLeader() bool {
	return rn.raft.State() == raft.Leader
}

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

func (rn *RaftNode) LeaderAddress() raft.ServerAddress {
	return rn.raft.Leader()
}

func (rn *RaftNode) GetState() *game.GameState {
	return rn.fsm.State()
}

func (rn *RaftNode) Shutdown() error {
	future := rn.raft.Shutdown()
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to shutdown raft: %w", err)
	}
	log.Println("Raft node shutdown complete")
	return nil
}