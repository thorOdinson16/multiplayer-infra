# ADR-04: etcd over ZooKeeper for Service Coordination

---

## Context

The platform requires a distributed coordination layer for:

- Game room pod self-registration on startup (making new rooms discoverable by the matchmaking service)
- Deregistration on pod termination
- Distributed configuration storage accessible by all services
- Leader discovery for Raft nodes (stable DNS names need to be resolvable at election time)

The question is which coordination system to use.

## Decision

Use **etcd 3.5+** for all distributed coordination and configuration storage.

## Rationale

etcd is the correct choice for three reasons:

**It is what Kubernetes itself uses.** Kubernetes stores all cluster state in etcd. Running etcd as an explicit application-level coordination layer alongside the Kubernetes-internal etcd instance means the team operates one technology with two scopes, not two different coordination systems. The operational model, client libraries, and failure characteristics are already understood.

**ZooKeeper's era has passed.** ZooKeeper was the pre-Kafka-KRaft coordination layer and predates the modern container ecosystem. It requires a JVM, has a more complex operational model (ensemble sizing, session management, ZNode hierarchy), and is not a dependency of any other component in this stack. Kafka in KRaft mode no longer requires ZooKeeper at all — which was ZooKeeper's last strong foothold in a Go-native microservices stack.

**etcd 3.5 fixed critical data consistency bugs.** etcd 3.4 had known issues with data inconsistency under certain failure scenarios (the etcd maintainers disclosed this in their 3.5 release notes). The requirement specifies etcd 3.5+ explicitly because of this — 3.4 is insufficient for a system where coordination correctness matters.

**Watch-based configuration propagation** is a first-class etcd primitive. Services can watch a key prefix and receive push notifications when configuration changes, without polling. This maps directly to the requirement that service-level configuration changes propagate to all pods.

## Consequences

- etcd runs as a StatefulSet with a PersistentVolume (2GB) to survive pod restarts
- The etcd client library (`go.etcd.io/etcd/client/v3`) is a dependency of matchmaking-service and game-room-server
- Game room pods must call etcd on startup to register and on shutdown to deregister; a deregistration hook must be wired into the pod lifecycle (preStop hook or graceful shutdown handler)
- etcd 3.4 (the version present on the development machine at project start) must be upgraded to 3.5+ before any coordination-dependent code is tested

## Alternatives Rejected

| Alternative | Reason rejected |
|-------------|----------------|
| ZooKeeper | JVM dependency; dated operational model; no longer required by Kafka (KRaft mode); not used by any other component in the stack |
| Consul | Capable, but adds a fourth data store alongside Couchbase, Redis, and etcd; service mesh features are out of scope; Kubernetes DNS already handles most service discovery |
| Kubernetes ConfigMaps only | ConfigMaps support watch-based updates but are not designed for high-frequency writes (pod registration events); not suitable as a coordination primitive |
| In-memory service registry | Not durable across pod restarts; not shared across service replicas |