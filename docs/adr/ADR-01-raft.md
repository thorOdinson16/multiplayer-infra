# ADR-01: Raft Consensus over Custom Replication for Game Room State

---

## Context

Each game room must replicate authoritative game state across multiple nodes so that a single pod failure does not end an active match. The system needs:

- A defined leader that processes all writes
- Followers that maintain a consistent replica of committed state
- Automatic leader election when the current leader dies
- Guarantee that no committed state is lost during failover

The question is how to implement this replication.

## Decision

Use the **Hashicorp Raft library** (`github.com/hashicorp/raft`) to implement a three-node consensus group per game room — one leader and two followers.

## Rationale

Raft is a well-specified consensus algorithm with formal correctness proofs. The Hashicorp implementation is production-proven, written in Go (matching our primary language), and provides:

- Leader election with configurable heartbeat and election timeouts
- Log replication with commit quorum (2 of 3 nodes must acknowledge before a tick is committed)
- Snapshotting and log compaction via the FSM interface
- Stable node identity compatible with Kubernetes StatefulSets

The alternative would be a custom primary-backup replication scheme. Custom replication has no formal guarantees, is easy to get subtly wrong under network partition scenarios, and would require significant engineering effort to reach the correctness level that Raft provides out of the box.

## Consequences

- Game room pods must be deployed as StatefulSets with stable DNS (see ADR-05)
- Each game room consumes 3× the pod resources of a single-replica design
- Raft inter-node communication uses gRPC, adding a gRPC dependency to the game room server
- Failover is automatic and completes within 5 seconds under normal conditions
- The FSM interface requires defining `Apply`, `Snapshot`, and `Restore` — these map cleanly to tick application, state snapshot, and reconnect restore respectively

## Alternatives Rejected

| Alternative | Reason rejected |
|-------------|----------------|
| Custom primary-backup | No formal guarantees; split-brain risk under partition |
| Single replica (no replication) | Pod failure ends the match — unacceptable |
| Etcd as the state store | etcd is not designed for high-frequency write workloads like a 20 TPS game tick loop |
| Paxos | Harder to implement correctly than Raft; no mature Go library comparable to Hashicorp Raft |