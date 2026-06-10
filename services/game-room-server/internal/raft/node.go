package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/thorOdinson16/multiplayer-infra/services/game-room-server/internal/game"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type RaftNode struct {
	raft       *raft.Raft
	fsm        *FSM
	dataDir    string
	nodeID     string
	etcdClient *clientv3.Client
	matchID    string
	leaderKey  string
	stopCh     chan struct{}
}

type Config struct {
	NodeID        string
	BindAddr      string
	DataDir       string
	Bootstrap     bool
	Service       string
	Namespace     string
	EtcdEndpoints []string
	MatchID       string
	ClusterSize   int
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

	if cfg.ClusterSize <= 0 {
		cfg.ClusterSize = 3
	}

	// Bootstrap pod-0, then have the leader add the remaining voters as they become reachable.
	if cfg.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(cfg.NodeID),
					Address: raft.ServerAddress(cfg.BindAddr),
				},
			},
		}
		future := r.BootstrapCluster(configuration)
		if err := future.Error(); err != nil && !strings.Contains(err.Error(), "already") {
			log.Printf("Warning: Failed to bootstrap cluster: %v", err)
		}
		go rn.addConfiguredVoters(cfg)
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

func (rn *RaftNode) desiredPeers(cfg Config) ([]raft.Server, error) {
	baseName, _, err := parseNodeOrdinal(cfg.NodeID)
	if err != nil {
		return nil, err
	}

	servers := make([]raft.Server, 0, cfg.ClusterSize)
	for i := 0; i < cfg.ClusterSize; i++ {
		peerID := fmt.Sprintf("%s-%d", baseName, i)
		peerAddr := fmt.Sprintf("%s.%s.%s.svc.cluster.local:7000", peerID, cfg.Service, cfg.Namespace)
		if peerID == cfg.NodeID {
			peerAddr = cfg.BindAddr
		}
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(peerID),
			Address: raft.ServerAddress(peerAddr),
		})
	}
	return servers, nil
}

func parseNodeOrdinal(nodeID string) (string, int, error) {
	// Match a trailing dash followed by digits as the ordinal, e.g. "game-room-123-0" -> base "game-room-123", ordinal 0
	re := regexp.MustCompile(`-(\d+)$`)
	matches := re.FindStringSubmatch(nodeID)
	if len(matches) != 2 {
		return "", 0, fmt.Errorf("cannot parse ordinal from node ID: %s", nodeID)
	}
	ordinalStr := matches[1]
	ordinal, err := strconv.Atoi(ordinalStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid ordinal in node ID: %s", nodeID)
	}
	base := strings.TrimSuffix(nodeID, "-"+ordinalStr)
	if base == "" {
		return "", 0, fmt.Errorf("cannot parse base from node ID: %s", nodeID)
	}
	return base, ordinal, nil
}

func (rn *RaftNode) addConfiguredVoters(cfg Config) {
	desired, err := rn.desiredPeers(cfg)
	if err != nil {
		log.Printf("Warning: Failed to compute Raft peers: %v", err)
		return
	}

	for attempt := 0; attempt < 60; attempt++ {
		if !rn.IsLeader() {
			time.Sleep(time.Second)
			continue
		}

		configFuture := rn.raft.GetConfiguration()
		if err := configFuture.Error(); err != nil {
			log.Printf("Failed to read Raft configuration: %v", err)
			time.Sleep(time.Second)
			continue
		}

		existing := make(map[raft.ServerID]bool)
		for _, server := range configFuture.Configuration().Servers {
			existing[server.ID] = true
		}

		missing := 0
		for _, server := range desired {
			if server.ID == raft.ServerID(cfg.NodeID) || existing[server.ID] {
				continue
			}

			missing++
			future := rn.raft.AddVoter(server.ID, server.Address, 0, 5*time.Second)
			if err := future.Error(); err != nil {
				log.Printf("Failed to add Raft voter %s at %s: %v", server.ID, server.Address, err)
				continue
			}
			log.Printf("Added Raft voter %s at %s", server.ID, server.Address)
		}

		if missing == 0 {
			return
		}
		time.Sleep(time.Second)
	}
	log.Printf("Stopped trying to add Raft voters after timeout")
}

func (rn *RaftNode) joinCluster(cfg Config) {
	for i := 0; i < 60; i++ {
		configFuture := rn.raft.GetConfiguration()
		if err := configFuture.Error(); err == nil {
			for _, server := range configFuture.Configuration().Servers {
				if server.ID == raft.ServerID(cfg.NodeID) {
					log.Printf("Joined Raft cluster as voter %s", cfg.NodeID)
					return
				}
			}
		}
		if leader := rn.raft.Leader(); leader != "" {
			log.Printf("Waiting to be added to Raft cluster by leader %s", leader)
		}
		time.Sleep(time.Second)
	}
	log.Printf("Could not join Raft cluster after 60 seconds")
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
