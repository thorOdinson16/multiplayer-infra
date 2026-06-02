package raft

import (
	"context"
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
	clientv3 "go.etcd.io/etcd/client/v3"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/game"
)

type RaftNode struct {
	raft         *raft.Raft
	fsm          *FSM
	dataDir      string
	nodeID       string
	etcdClient   *clientv3.Client
	matchID      string
	leaderKey    string
	stopCh       chan struct{}
}

type Config struct {
	NodeID         string
	BindAddr       string
	DataDir        string
	Bootstrap      bool
	Service        string
	Namespace      string
	EtcdEndpoints  []string
	MatchID        string
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
		raft:      r,
		fsm:       fsm,
		dataDir:   cfg.DataDir,
		nodeID:    cfg.NodeID,
		matchID:   cfg.MatchID,
		leaderKey: fmt.Sprintf("/game-rooms/%s/leader", cfg.MatchID),
		stopCh:    make(chan struct{}),
	}

	// Connect to etcd
	if len(cfg.EtcdEndpoints) > 0 {
		etcdClient, err := clientv3.New(clientv3.Config{
			Endpoints:   cfg.EtcdEndpoints,
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			log.Printf("Warning: Failed to connect to etcd: %v", err)
		} else {
			rn.etcdClient = etcdClient
			go rn.watchLeadership()
		}
	}

	// Bootstrap only if this is the first node
	if cfg.Bootstrap {
		time.Sleep(2 * time.Second)
		peers, err := rn.discoverPeers(cfg)
		if err != nil {
			log.Printf("Warning: Failed to discover peers: %v", err)
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
		go rn.joinCluster(cfg)
	}

	log.Printf("Raft node %s started on %s", cfg.NodeID, cfg.BindAddr)
	return rn, nil
}

func (rn *RaftNode) watchLeadership() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	wasLeader := false

	for {
		select {
		case <-ticker.C:
			isLeader := rn.IsLeader()
			if isLeader && !wasLeader {
				rn.registerLeader()
				log.Printf("🎯 Registered as leader in etcd: %s = %s", rn.leaderKey, rn.nodeID)
			} else if !isLeader && wasLeader {
				rn.unregisterLeader()
				log.Printf("👋 Unregistered as leader from etcd")
			}
			wasLeader = isLeader
		case <-rn.stopCh:
			if wasLeader {
				rn.unregisterLeader()
			}
			return
		}
	}
}

func (rn *RaftNode) registerLeader() {
	if rn.etcdClient == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	lease, err := rn.etcdClient.Grant(ctx, 10)
	if err != nil {
		log.Printf("Failed to create lease: %v", err)
		return
	}
	_, err = rn.etcdClient.Put(ctx, rn.leaderKey, rn.nodeID, clientv3.WithLease(lease.ID))
	if err != nil {
		log.Printf("Failed to register leader in etcd: %v", err)
	}
	go rn.keepLeaderAlive(lease.ID)
}

func (rn *RaftNode) keepLeaderAlive(leaseID clientv3.LeaseID) {
	ctx := context.Background()
	for {
		select {
		case <-time.After(5 * time.Second):
			if rn.etcdClient != nil && rn.IsLeader() {
				_, err := rn.etcdClient.KeepAliveOnce(ctx, leaseID)
				if err != nil {
					log.Printf("Failed to keep leader lease alive: %v", err)
					return
				}
			} else {
				return
			}
		case <-rn.stopCh:
			return
		}
	}
}

func (rn *RaftNode) unregisterLeader() {
	if rn.etcdClient == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rn.etcdClient.Delete(ctx, rn.leaderKey)
}

func (rn *RaftNode) discoverPeers(cfg Config) ([]raft.Server, error) {
	parts := strings.Split(cfg.NodeID, "-")
	if len(parts) < 2 {
		return nil, fmt.Errorf("cannot parse ordinal from node ID: %s", cfg.NodeID)
	}
	currentOrdinal, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return nil, fmt.Errorf("invalid ordinal in node ID: %s", cfg.NodeID)
	}
	var servers []raft.Server
	for i := 0; i < currentOrdinal; i++ {
		peerID := fmt.Sprintf("game-room-server-%d", i)
		peerAddr := fmt.Sprintf("%s.%s.%s.svc.cluster.local:7000", peerID, cfg.Service, cfg.Namespace)
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(peerID),
			Address: raft.ServerAddress(peerAddr),
		})
	}
	servers = append(servers, raft.Server{
		ID:      raft.ServerID(cfg.NodeID),
		Address: raft.ServerAddress(cfg.BindAddr),
	})
	return servers, nil
}

func (rn *RaftNode) joinCluster(cfg Config) {
	time.Sleep(5 * time.Second)
	for i := 0; i < 30; i++ {
		leader := rn.raft.Leader()
		if leader != "" {
			log.Printf("Found leader: %s, attempting to join", leader)
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
	// Skip if no players connected (prevents processing flood when room is empty)
	if rn.fsm.State().GetConnectedPlayerCount() == 0 {
		log.Printf("Skipping input - no players connected")
		return nil
	}
	
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
	close(rn.stopCh)
	if rn.etcdClient != nil {
		rn.unregisterLeader()
		rn.etcdClient.Close()
	}
	future := rn.raft.Shutdown()
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to shutdown raft: %w", err)
	}
	log.Println("Raft node shutdown complete")
	return nil
}