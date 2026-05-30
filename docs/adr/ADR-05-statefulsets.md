# ADR-05: Kubernetes StatefulSets for Game Room Raft Nodes

---

## Context

Game room servers run as three-node Raft consensus groups. Raft has a hard requirement: each node must have a stable, persistent identity across restarts. Specifically:

- A node's address must be consistent across pod restarts so that peers can reconnect to it
- A node must be able to rejoin its Raft group with the same identity after recovery, not as a brand new member
- Log storage (the Raft persistent log) must survive pod restarts

Kubernetes offers two primary workload types for stateful applications: Deployments and StatefulSets.

## Decision

Deploy game room Raft groups as **Kubernetes StatefulSets** with stable DNS names and PersistentVolumeClaims.

Each game room StatefulSet has 3 replicas. Kubernetes assigns them stable ordinal names:
```
game-room-{matchId}-0   ← initial leader candidate
game-room-{matchId}-1   ← follower
game-room-{matchId}-2   ← follower
```

A headless Service provides stable DNS:
```
game-room-{matchId}-0.game-room-{matchId}.game-platform.svc.cluster.local
```

These DNS names are stable across pod restarts — if pod `game-room-{matchId}-1` is killed, it comes back with the same DNS name and the same PersistentVolumeClaim.

## Rationale

**Deployments cannot satisfy Raft's identity requirement.** A Deployment pod that is killed and restarted receives a new random name and a new ephemeral volume. Its peers in the Raft group have no way to know the new pod is the same logical node. The Raft group would treat it as an unknown peer and reject its vote attempts, causing election failures or split states.

**StatefulSets were designed for exactly this problem.** The Kubernetes StatefulSet controller guarantees:
- Stable, persistent network identity (ordinal-based pod name)
- Ordered startup and shutdown (pods start 0→1→2, terminate 2→1→0)
- PersistentVolumeClaims are bound to the pod identity and reattached on restart

The Hashicorp Raft library stores its persistent log and stable storage on disk. The PersistentVolumeClaim ensures this log survives pod restarts, allowing the restarted node to rejoin with its full log intact rather than starting from an empty state.

## Consequences

- One StatefulSet is created per active match group — the matchmaking service calls the Kubernetes API to create it on lobby assembly
- The StatefulSet is terminated when the match ends and pods are returned to idle or deleted
- Helm chart for `game-room-server` uses `kind: StatefulSet`, a headless Service, and a VolumeClaimTemplate
- The headless Service name must be stable and match the Raft peer address configuration
- Pod startup ordering (0 before 1 before 2) means the first election cannot begin until all three pods are Running — this is acceptable given the 30-second pod readiness budget (NFR-S-03)

## Alternatives Rejected

| Alternative | Reason rejected |
|-------------|----------------|
| Deployment | Random pod names on restart; ephemeral volumes; Raft peers cannot track restarted nodes |
| Deployment + external service registry (etcd) for peer tracking | Complex; requires custom peer reassignment logic; defeats the purpose of using Raft's built-in peer management |
| Single-replica Deployment (no Raft) | No fault tolerance; pod failure ends the match |