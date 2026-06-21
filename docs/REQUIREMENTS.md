# Distributed Real-Time Multiplayer Infrastructure Platform
## Software Requirements Specification (SRS)

**Version:** 1.3.0  
**Author:** Abhi  
**Date:** June 2026  
**Status:** Active Development (Python rewrite)  
**Changelog:**  
- v1.3.0 — Post-architecture review hardening. Added ADR-08 (atomic commit order), ADR-09 (client leader-discovery), ADR-10 (cold-start replay). Clarified game room pool management, spectator buffer loss behaviour, etcd cluster size, JWT algorithm (RS256), health probe definitions, graceful shutdown, and expiry queue consumption. Updated Redis schema, service specs, and acceptance criteria.  
- v1.2.0 — Complete rewrite to Python + etcd-based leader election. Updated ADR-01, tech stack, language allocation, and constraints.  
- v1.1.0 — Added §16 Open Questions; added FR-MM-07/08; added FR-SP-05; added FR-RP-07; added NFR-S-05; updated §11 with Jaeger backend clarification.  

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Goals and Objectives](#2-goals-and-objectives)
3. [System Architecture Overview](#3-system-architecture-overview)
4. [Technology Stack](#4-technology-stack)
5. [Functional Requirements](#5-functional-requirements)
6. [Non-Functional Requirements](#6-non-functional-requirements)
7. [Service Specifications](#7-service-specifications)
8. [Data Requirements](#8-data-requirements)
9. [Infrastructure Requirements](#9-infrastructure-requirements)
10. [Security Requirements](#10-security-requirements)
11. [Observability Requirements](#11-observability-requirements)
12. [CI/CD Requirements](#12-cicd-requirements)
13. [Constraints and Assumptions](#13-constraints-and-assumptions)
14. [Acceptance Criteria](#14-acceptance-criteria)
15. [Glossary](#15-glossary)
16. [Open Questions](#16-open-questions)

---

## 1. Project Overview

### 1.1 Purpose
This document specifies the requirements for a **Distributed Real-Time Multiplayer Infrastructure Platform**—a production-grade backend system built around a simple top-down arena game. The game serves as a realistic, observable load generator for the infrastructure. The primary subject of the project is the infrastructure itself: distributed state management, event streaming, consensus-based fault tolerance, and autonomous scaling.

### 1.2 Scope
The platform encompasses the full lifecycle of a multiplayer session—from player authentication and matchmaking through real-time game execution, fault recovery, replay archival, and analytics aggregation. All backend services are independently containerized, orchestrated via Kubernetes, and observable end-to-end through distributed tracing.

### 1.3 Background and Motivation
Modern multiplayer game backends are among the most demanding distributed systems in production. They require sub-100ms state synchronization, zero-downtime failover, dynamic scaling under unpredictable load, and long-term event archival—all simultaneously. This project reconstructs that class of system from first principles, deliberately selecting tools that represent current industry practice rather than legacy defaults.

The secondary motivation is demonstrating deep familiarity with Couchbase's core capabilities—memory-first document storage, N1QL query semantics, and cross-datacenter replication—within a realistic, high-throughput data access pattern.

### 1.4 Intended Audience
- The author, as a technical reference throughout development
- Engineering interviewers at Couchbase and similar systems-focused organizations
- Open-source contributors and reviewers evaluating the project on GitHub

---

## 2. Goals and Objectives

### 2.1 Primary Goals

| ID | Goal |
|----|------|
| G-01 | Demonstrate a complete, working distributed backend system with real observable behavior |
| G-02 | Implement authoritative game state replication and leader election using etcd leases (Raft under the hood) |
| G-03 | Showcase Couchbase as a primary data store with N1QL queries, XDCR, and memory-first access |
| G-04 | Separate event streaming (Kafka) from task queuing (RabbitMQ) with clearly justified boundaries |
| G-05 | Achieve autonomous pod scaling in response to matchmaking demand via Kubernetes HPA |
| G-06 | Provide end-to-end distributed tracing across all services via OpenTelemetry |

### 2.2 Non-Goals
- Building a commercially polished or visually sophisticated game client
- Supporting mobile platforms or native desktop clients
- Implementing monetization, anti-cheat, or DRM systems
- Achieving geo-distributed multi-region deployment in the initial version

---

## 3. System Architecture Overview

### 3.1 Architectural Pattern
The platform follows an **event-driven microservices architecture** with the following structural layers:

```
Clients (WebSocket)
       │
  NGINX Gateway  ←── TLS termination, rate limiting, WS proxy
       │
Core Services   ←── Auth, Matchmaking, Game Rooms, Replay, Leaderboard,
       │               Analytics, Notification, Reconnect Handler
       │
Messaging Layer ←── Kafka (event log) + RabbitMQ (task queues)
       │
Data Layer      ←── Couchbase (primary) + Redis (ephemeral) + Object Storage (archives)
       │
Infrastructure  ←── Kubernetes + etcd + Docker + GitHub Actions + ArgoCD
       │
Observability   ←── OpenTelemetry + Prometheus + Grafana + Jaeger
```

### 3.2 Key Architectural Decisions

**ADR-01: etcd-based leader election over embedded Raft**  
Game room leader election is delegated to etcd leases rather than embedding a Raft library in the application. etcd's underlying Raft consensus guarantees exactly-one leader at any time. State durability is provided by Kafka's immutable event log and Redis snapshots, not by an in-memory Raft log. This separation of concerns—election vs. state persistence—makes the system easier to debug and allows followers to be stateless watchers that only awake on leader failure.

**ADR-02: Kafka and RabbitMQ as complementary, not competing brokers**  
Kafka is used exclusively for immutable, ordered, high-throughput event logs (movement, telemetry, replay). RabbitMQ is used exclusively for task queues requiring competing consumers and explicit acknowledgement (matchmaking, notifications, async jobs). These are fundamentally different messaging primitives and must not be conflated.

**ADR-03: Couchbase as primary data store**  
Couchbase is selected over PostgreSQL or MongoDB for its memory-first bucket architecture, native N1QL support, and XDCR capability. These features map directly to the access patterns required: hot player session data served from RAM, flexible leaderboard queries without schema migrations, and future multi-region replication.

**ADR-04: etcd over ZooKeeper**  
etcd replaces ZooKeeper for service coordination and distributed configuration. ZooKeeper is a pre-Kafka-KRaft dependency with a dated operational model. etcd is what Kubernetes itself uses internally and represents current industry practice for distributed key-value coordination.

**ADR-05: StatefulSets for stable node identity**  
Game room replicas require stable DNS names and persistent volumes to ensure etcd lease ownership can be reliably tied to a specific pod. Kubernetes ephemeral pods cannot provide this. Game room replicas are deployed as StatefulSets with stable network identities.

**ADR-06: CPU-based HPA as a v1 scaling proxy**  
Kubernetes HPA is configured on CPU utilization (70% threshold) as a pragmatic proxy for matchmaking demand in v1. CPU is a lagging indicator for game room load; a future iteration will augment or replace this with a custom metrics signal derived from matchmaking queue depth (see §16, OQ-04).

**ADR-07: Spectator state delay via server-side ring buffer**  
The configurable spectator broadcast delay (FR-SP-02) is implemented as a server-side ring buffer on the game room leader. Each committed tick snapshot is enqueued with a timestamp; the leader flushes entries to spectator connections after the configured delay elapses. This approach isolates spectator delivery from the player broadcast path entirely and adds no latency to authoritative state propagation.

**ADR-08: Atomic commit order for state durability**  
The game room leader commits state in the following order to guarantee that the Kafka event log remains the durable source of truth:
1. Publish the state transition event to Kafka (`match.events`) and wait for acknowledgement.
2. On successful publish, apply the new state to Redis (hot cache) and update the internal memory state.
3. Persist the Kafka offset of the event alongside the Redis state (`match:{matchId}:last_offset`).
On failover, the new leader loads Redis state and the last offset, then replays any Kafka events beyond that offset before accepting new inputs. This ensures that Redis never outpaces Kafka and that no committed state is lost.

**ADR-09: Client-to-leader routing via etcd registration**  
After winning the etcd lease, the game room leader writes its reachable pod address (stable DNS name) to the key `/match/{matchId}/leader-address`. The Reconnect Handler reads this key to direct reconnecting players to the correct leader endpoint. NGINX is dynamically reconfigured via a small sidecar that watches etcd for active match routes, ensuring WebSocket connections are proxied to the correct leader without manual reloads.

**ADR-10: Cold-start replay path for game rooms**  
In the event of a complete data‑plane loss (Redis pod crash, node restart), the game room leader can reconstruct the latest game state by replaying the entire `match.events` Kafka partition from offset zero (or from the nearest Couchbase checkpoint). This recovery path is invoked whenever Redis holds no state for an active match. Normal fast‑failover (etcd lease loss) uses the Redis snapshot + incremental Kafka replay.

---

## 4. Technology Stack

### 4.1 Full Stack Reference

| Layer | Technology | Version Target | Role |
|-------|-----------|---------------|------|
| Gateway | NGINX | 1.24+ | WebSocket proxy, TLS, rate limiting, load balancing |
| Containerization | Docker | 29+ | Service packaging and image management |
| Orchestration | Kubernetes | 1.36+ | Pod scheduling, scaling, service discovery, StatefulSets |
| Package Management | Helm | 3.x | Kubernetes manifest templating and release management |
| Consensus / Election | etcd leases (via `etcd3-py`) | 3.5+ | Leader election; Raft delegated to etcd itself |
| Event Streaming | Apache Kafka | 3.7+ (KRaft mode) | Immutable event log: movement, telemetry, replay |
| Task Queue | RabbitMQ | 3.13+ | Matchmaking queues, notifications, async job dispatch |
| Coordination | etcd | 3.5+ | Distributed config, service coordination, leader discovery |
| Primary Database | Couchbase Server | 7.6+ | Player profiles, sessions, match history, leaderboard |
| Cache / Pub-Sub | Redis | 7.x | Ephemeral per-match state, real-time pub/sub |
| Object Storage | MinIO (S3-compatible) | latest | Replay archive storage |
| Observability | OpenTelemetry Collector | latest | Distributed tracing pipeline across all services |
| Trace Backend | Jaeger | latest (self-hosted in-cluster) | Trace storage and query UI |
| Metrics | Prometheus + Grafana | latest | Service metrics, dashboards, alerting |
| CI | GitHub Actions | — | Automated test, build, image push on merge |
| CD | ArgoCD | 3.x | GitOps-based Kubernetes deployment sync |
| Primary Language | Python 3.11+ with FastAPI + Uvicorn | latest | Core backend services (async WebSocket & REST) |
| Secondary Language | Node.js | 20 LTS | Game client, lightweight auxiliary services |
| Local Kubernetes | minikube | 1.38+ | Local development cluster |

### 4.2 Language Allocation by Service

| Service | Language | Justification |
|---------|----------|---------------|
| Auth Service | Python (FastAPI) | Async REST, native JWT libraries, rapid iteration |
| Matchmaking Service | Python (aio-pika) | Async RabbitMQ consumer, easy to implement competing consumers |
| Game Room Server | Python (FastAPI + asyncio) | Async WebSocket handling; CPU-bound tick loop offloaded via `run_in_executor` |
| Replay Service | Python (confluent-kafka) | Kafka consumer with high-level consumer groups |
| Leaderboard Service | Python (FastAPI) | Couchbase Python SDK v4, N1QL query execution |
| Analytics Service | Python (confluent-kafka) | Kafka consumer, Prometheus metrics exposure |
| Notification Service | Python (aio-pika) | Async RabbitMQ dispatch |
| Reconnect Handler | Python (redis.asyncio) | Async Redis client, state delta computation |
| Game Client | Node.js | WebSocket client, browser-compatible rendering |

---

## 5. Functional Requirements

### 5.1 Authentication

| ID | Requirement |
|----|-------------|
| FR-AUTH-01 | The system shall authenticate players using username/password credentials and issue a signed JWT on success |
| FR-AUTH-02 | JWTs shall have a configurable expiry (default 24 hours) and be validated on every WebSocket connection upgrade |
| FR-AUTH-03 | Session tokens shall be stored in Couchbase with a TTL matching the JWT expiry |
| FR-AUTH-04 | Expired or invalid tokens shall result in immediate connection rejection with a descriptive error code |
| FR-AUTH-05 | The Auth Service shall support token refresh without requiring full re-authentication |

### 5.2 Matchmaking

| ID | Requirement |
|----|-------------|
| FR-MM-01 | The system shall accept matchmaking requests from authenticated players and place them in a RabbitMQ queue |
| FR-MM-02 | Players shall be matched based on skill rating using an Elo-adjacent scoring model |
| FR-MM-03 | Matchmaking shall assemble lobbies of 2–8 players within a configurable time window (default 30 seconds) |
| FR-MM-04 | If no suitable match is found within the time window, the skill range shall expand incrementally |
| FR-MM-05 | On lobby assembly, Matchmaking shall assign an available game room from the room pool. If no idle room exists, it shall trigger Kubernetes to create a new game room StatefulSet. |
| FR-MM-06 | Players shall receive a match-found notification via the Notification Service within 500ms of lobby assembly |
| FR-MM-07 | If the RabbitMQ broker is unavailable, the Matchmaking Service shall reject new matchmaking requests with a `503 Service Unavailable` response and expose a degraded-state metric; it shall not silently drop requests or block indefinitely |
| FR-MM-08 | Matchmaking requests that remain unprocessed for longer than 2× the configured time window shall be expired from the queue. Expired messages are routed to the `matchmaking.expired` queue; the Notification Service consumes this queue and sends a `match.expired` alert to the affected player. |

### 5.3 Game Room and Real-Time Gameplay

| ID | Requirement |
|----|-------------|
| FR-GR-01 | Each game room shall run as a logical cluster of three replicas with leader election delegated to etcd (which itself is Raft-based) |
| FR-GR-02 | The leader shall be the sole processor of player inputs; followers shall remain passive watchers awaiting lease expiration |
| FR-GR-03 | The game simulation shall advance at a fixed tick rate of 20 ticks per second (50ms per tick) |
| FR-GR-04 | Authoritative state snapshots shall be broadcast to all connected clients after each committed tick |
| FR-GR-05 | Each committed state transition shall be published to Kafka first, and only after acknowledgement shall the state be applied to Redis and internal memory (see ADR-08) |
| FR-GR-06 | Player input latency from client send to state broadcast shall be under 100ms on a local network |
| FR-GR-07 | The game room shall support a minimum of 8 simultaneous player connections |
| FR-GR-08 | Spectator connections shall receive read-only state broadcasts without participating in input processing |
| FR-GR-09 | On match completion, the game room leader shall publish a match-end event to the `match.lifecycle` Kafka topic, write the final outcome to Couchbase, and then release the room (release etcd lease and unregister from pool). |

### 5.4 Fault Tolerance and Reconnection

| ID | Requirement |
|----|-------------|
| FR-FT-01 | On leader pod failure, a follower shall detect etcd lease expiry and acquire the lease within 5 seconds |
| FR-FT-02 | No committed game state shall be lost during a leader failover (state is durable in Kafka and Redis per ADR-08) |
| FR-FT-03 | Players shall experience a visible pause of no more than 5 seconds during a failover event |
| FR-FT-04 | On player WebSocket disconnection, the server shall hold their slot for 30 seconds before removing them |
| FR-FT-05 | On reconnection within the hold window, the Reconnect Handler shall deliver a compressed state delta and the current leader’s WebSocket endpoint (read from etcd) enabling the client to re-enter the match |
| FR-FT-06 | The state delta payload shall not exceed 64KB for a match with up to 8 players |
| FR-FT-07 | On Redis full‑data loss (e.g., pod crash), the game room leader shall be able to replay the Kafka event log from the beginning to rebuild hot state (cold‑start recovery) |

### 5.5 Replay System

| ID | Requirement |
|----|-------------|
| FR-RP-01 | The Replay Service shall consume all movement events from the `match.events` Kafka topic and persist them as a structured, seekable event log per match |
| FR-RP-02 | A replay shall be reconstructable to any tick by replaying events from the beginning or from the nearest checkpoint |
| FR-RP-03 | Checkpoints shall be written every 300 ticks (15 seconds at 20 TPS) |
| FR-RP-04 | Completed match replays shall be archived to MinIO object storage within 60 seconds of match end |
| FR-RP-05 | The Replay Service shall expose an API to retrieve and stream a replay at configurable playback speeds (0.5×, 1×, 2×) |
| FR-RP-06 | Replays shall be retained in object storage for a minimum of 30 days |
| FR-RP-07 | The Replay Service shall also consume the `match.lifecycle` Kafka topic to detect match-end events and trigger replay finalization; it shall not rely solely on the `match.events` stream for match boundary detection |

### 5.6 Leaderboard

| ID | Requirement |
|----|-------------|
| FR-LB-01 | Match outcomes shall be written to Couchbase immediately on match completion, triggered by consumption of the `match.lifecycle` Kafka topic |
| FR-LB-02 | The Leaderboard Service shall expose a ranked player list queryable by time window (daily, weekly, all-time) |
| FR-LB-03 | Rankings shall be computed using N1QL queries over the player document model in Couchbase |
| FR-LB-04 | Leaderboard queries shall return results within 200ms for datasets up to 100,000 player documents |
| FR-LB-05 | A player's personal rank, win rate, and average score shall be retrievable in a single API call |

### 5.7 Analytics

| ID | Requirement |
|----|-------------|
| FR-AN-01 | The Analytics Service shall consume telemetry events from the `match.telemetry` Kafka topic |
| FR-AN-02 | The service shall aggregate movement heatmaps, kill positions, session durations, and match lengths |
| FR-AN-03 | Aggregated metrics shall be exposed to Grafana via a Prometheus-compatible endpoint |
| FR-AN-04 | Raw telemetry events shall be retained in Kafka for 7 days before expiry |

### 5.8 Spectator Mode

| ID | Requirement |
|----|-------------|
| FR-SP-01 | Spectators shall be able to join an active match via a shareable match ID |
| FR-SP-02 | Spectators shall receive the same state broadcast as players but with a configurable delay of 0–30 seconds |
| FR-SP-03 | Spectator connections shall not count toward the player slot limit |
| FR-SP-04 | Spectator connections shall be automatically terminated when the match ends |
| FR-SP-05 | The spectator broadcast delay (FR-SP-02) shall be implemented as a server-side ring buffer on the game room leader. Committed tick snapshots are timestamped and enqueued; the leader flushes entries to spectator connections after the configured delay. This implementation must not introduce any latency on the player broadcast path (see ADR-07). |
| FR-SP-06 | Spectator ring buffer state is not preserved across leader failover; spectators may experience a discontinuity and must re-subscribe. This is accepted as a non-critical limitation. |

---

## 6. Non-Functional Requirements

### 6.1 Performance

| ID | Requirement |
|----|-------------|
| NFR-P-01 | WebSocket connection establishment (including auth) shall complete within 200ms under normal load |
| NFR-P-02 | Matchmaking queue processing shall handle 100 concurrent matchmaking requests without degradation |
| NFR-P-03 | Kafka event publishing from the game room shall add no more than 5ms of latency to the tick cycle |
| NFR-P-04 | Couchbase reads for player profile data shall complete within 5ms for documents resident in the memory-first bucket |
| NFR-P-05 | Redis reads for per-match state shall complete within 2ms |
| NFR-P-06 | The system shall support a minimum of 10 concurrent active matches on a single-node minikube deployment |

### 6.2 Reliability

| ID | Requirement |
|----|-------------|
| NFR-R-01 | The system shall tolerate the loss of any single service pod without data loss or unrecoverable state corruption |
| NFR-R-02 | Game room state shall survive the failure of one out of three replicas; etcd lease expiration triggers a new leader election |
| NFR-R-03 | The matchmaking queue shall be durable—RabbitMQ messages shall persist across broker restarts |
| NFR-R-04 | Kafka topics shall be configured with a replication factor of 3 in any multi-broker deployment; in single-node local deployment, replication factor 1 is acceptable but documented |
| NFR-R-05 | The etcd cluster used for service coordination shall consist of at least 3 nodes in any environment intended to demonstrate high availability (including local development). |

### 6.3 Scalability

| ID | Requirement |
|----|-------------|
| NFR-S-01 | Game room pods shall scale horizontally in response to matchmaking demand via Kubernetes HPA |
| NFR-S-02 | The HPA shall trigger scale-up when average CPU utilization across game room pods exceeds 70% |
| NFR-S-03 | New game room pods shall be schedulable and ready to accept connections within 30 seconds of scale-up trigger |
| NFR-S-04 | The architecture shall support horizontal scaling of all stateless services without configuration changes |
| NFR-S-05 | CPU utilization (NFR-S-02) is a v1 approximation for matchmaking demand. This is a known simplification: CPU is a lagging indicator and may not scale-up proactively under a sudden surge of matchmaking requests. A future iteration should expose matchmaking queue depth as a custom metric and configure HPA to scale on it directly (see §16, OQ-04) |

### 6.4 Maintainability

| ID | Requirement |
|----|-------------|
| NFR-M-01 | Each service shall expose a `/health` (liveness) and `/ready` (readiness) endpoint conforming to Kubernetes probe conventions. The game room’s `/ready` probe shall verify connectivity to etcd, Redis, and Kafka. |
| NFR-M-02 | All service configuration shall be externalized via environment variables or Kubernetes ConfigMaps—no hardcoded values |
| NFR-M-03 | All Docker images shall be tagged with the Git commit SHA that produced them |
| NFR-M-04 | Kubernetes manifests shall be managed as Helm charts versioned in a dedicated GitOps repository |
| NFR-M-05 | The game room leader shall gracefully release its etcd lease on match completion or voluntary shutdown to avoid unnecessary re‑elections. |

---

## 7. Service Specifications

### 7.1 Gateway Service (NGINX)
- **Role:** Single ingress point for all client traffic
- **Protocol:** HTTP/1.1, HTTP/2, WebSocket upgrade
- **Responsibilities:** TLS termination, WebSocket proxying, upstream load balancing, per-IP rate limiting (100 req/s default), health check routing
- **Dynamic upstream:** An etcd‑watcher sidecar container updates an in‑memory upstream list based on active `/match/{matchId}/leader-address` keys, enabling NGINX to route WebSocket connections to the correct game room leader without full reloads (using `http` lua‑module or NGINX Plus `zone` if available; open‑source fallback uses graceful reload every few seconds).
- **Upstream targets:** Auth Service, Game Room pods (by match ID header routing)

### 7.2 Auth Service
- **Role:** Credential validation and session lifecycle management
- **API:** REST over HTTP
- **Endpoints:** `POST /auth/login`, `POST /auth/refresh`, `POST /auth/logout`, `GET /auth/validate`
- **Dependencies:** Couchbase (session store), NGINX (inbound)
- **Token format:** JWT, RS256 (asymmetric), configurable expiry. Public key is distributed via a well‑known endpoint for inter‑service validation.
- **Scalability:** Stateless, horizontally scalable

### 7.3 Matchmaking Service
- **Role:** Skill-based player grouping and game room assignment
- **Queue:** RabbitMQ, durable queue, competing consumers
- **Algorithm:** Elo-range expansion with configurable time decay
- **Failure mode:** On RabbitMQ unavailability, returns `503` immediately; exposes `matchmaking_broker_unavailable` metric; does not block or silently drop (see FR-MM-07)
- **Request expiry:** Expired requests are dead‑lettered to `matchmaking.expired`; Notification Service consumes this queue and sends `match.expired` notifications.
- **Pool management:** Idle game rooms register themselves in etcd under `/rooms/available/{roomId}`. The matchmaking service queries this directory to select an available room; if none exists, it calls the Kubernetes API to create a new StatefulSet.
- **Dependencies:** RabbitMQ (inbound queue), Kubernetes API (room provisioning), Notification Service (outbound), etcd (room pool registry)
- **Scalability:** Stateless consumers, horizontally scalable

### 7.4 Game Room Server
- **Role:** Authoritative game simulation with replicated state
- **Protocol:** WebSocket (player I/O), HTTP (etcd lease management)
- **Deployment:** Kubernetes StatefulSet, 3 replicas per match group
- **Consensus:** etcd lease-based leader election. Followers are passive watchers—they do not replicate state via a Raft log. State is persisted to Redis (hot cache) and Kafka (immutable event log) with atomic order: Kafka first, then Redis + offset (ADR-08). On leader failure, etcd lease expires and a follower acquires it.
- **Leader registration:** On winning the lease, writes its stable pod DNS name to `/match/{matchId}/leader-address` in etcd.
- **Tick rate:** 20 TPS (configurable)
- **Spectator buffering:** Server-side ring buffer on the leader; player broadcast path is unaffected (ADR-07). Buffer is **not** persisted; on failover, spectators will see a gap.
- **Cold‑start recovery:** If no Redis state exists, the leader replays the entire Kafka `match.events` partition from offset 0 (or from the latest Couchbase checkpoint). During this replay, it does not accept player connections until caught up.
- **Match-end event:** On match completion, publishes to `match.lifecycle`, writes final outcome to Couchbase, then releases the etcd lease and removes itself from the available pool.
- **Health probes:**
  - Liveness: `GET /health` → 200 if process alive.
  - Readiness: `GET /ready` → 200 only if connections to etcd, Redis, and Kafka are healthy and (for leader) lease is held.
- **Graceful shutdown:** On SIGTERM, the leader finishes processing the current tick, publishes any pending Kafka events, releases the etcd lease, and exits cleanly.
- **Dependencies:** Redis (hot state + offset), Kafka (event publish to `match.events` and `match.lifecycle`), etcd (service registration and leader election), Couchbase (match record write)

### 7.5 Reconnect Handler
- **Role:** State delta computation and delivery for reconnecting players
- **Dependencies:** Redis (last-known state + offset), etcd (to read current leader address), Auth Service (token revalidation)
- **Payload:** Compressed JSON delta + `leaderAddress` field (from `/match/{matchId}/leader-address`), max 64KB
- **Hold window:** 30 seconds (configurable)

### 7.6 Replay Service
- **Role:** Event log persistence, checkpoint management, replay streaming
- **Consumers:**
  - Kafka topic `match.events`, consumer group `replay-service` — event ingestion
  - Kafka topic `match.lifecycle`, consumer group `replay-lifecycle` — match boundary detection and finalization trigger
- **Storage:** Checkpoints in Couchbase, archives in MinIO
- **API:** `GET /replay/{matchId}`, `GET /replay/{matchId}/seek?tick={n}`
- **Dependencies:** Kafka, Couchbase, MinIO

### 7.7 Leaderboard Service
- **Role:** Match outcome recording and ranked query serving
- **Consumer:** Kafka topic `match.lifecycle`, consumer group `leaderboard-service` — triggers outcome write on match-end event
- **Query engine:** Couchbase N1QL
- **API:** `GET /leaderboard?window=daily|weekly|all`, `GET /leaderboard/player/{id}`
- **Indexing:** Couchbase GSI indexes on `score`, `wins`, `playerId`, `timestamp`
- **Dependencies:** Kafka (`match.lifecycle`), Couchbase

### 7.8 Analytics Service
- **Role:** Telemetry aggregation and metrics exposure
- **Consumers:** Kafka topic `match.telemetry`, consumer group `analytics-service`
- **Output:** Prometheus metrics endpoint `/metrics`
- **Dependencies:** Kafka, Prometheus

### 7.9 Notification Service
- **Role:** Async event dispatch to connected clients
- **Queue:** RabbitMQ, topic exchange
- **Event types:** `match.found`, `match.ended`, `match.expired` (consumed from `matchmaking.expired` queue), `player.joined`, `system.alert`
- **Dependencies:** RabbitMQ, NGINX (client push path)

---

## 8. Data Requirements

### 8.1 Couchbase Document Model

**Player document** (`players` bucket)
```json
{
  "type": "player",
  "playerId": "uuid",
  "username": "string",
  "passwordHash": "string",
  "eloRating": 1200,
  "wins": 0,
  "losses": 0,
  "totalMatches": 0,
  "averageScore": 0.0,
  "createdAt": "ISO8601",
  "lastSeen": "ISO8601"
}
```

**Session document** (`sessions` bucket, memory-first, TTL 24h)
```json
{
  "type": "session",
  "sessionId": "uuid",
  "playerId": "uuid",
  "token": "jwt_string",
  "expiresAt": "ISO8601",
  "ipAddress": "string"
}
```

**Match document** (`matches` bucket)
```json
{
  "type": "match",
  "matchId": "uuid",
  "players": ["playerId"],
  "startedAt": "ISO8601",
  "endedAt": "ISO8601",
  "durationSeconds": 0,
  "outcome": { "winner": "playerId", "scores": {} },
  "replayArchiveUrl": "string"
}
```

**Replay checkpoint document** (`replays` bucket)
```json
{
  "type": "replay_checkpoint",
  "matchId": "uuid",
  "tick": 300,
  "snapshotState": {},
  "kafkaOffset": 0,
  "createdAt": "ISO8601"
}
```

### 8.2 Kafka Topics

| Topic | Partitioning | Retention | Consumers |
|-------|-------------|-----------|-----------|
| `match.events` | By `matchId` | 7 days | Replay Service (`replay-service`), Analytics Service (`analytics-service`) |
| `match.telemetry` | By `matchId` | 7 days | Analytics Service (`analytics-service`) |
| `match.lifecycle` | By `matchId` | 30 days (longer retention for administrative replay triggers) | Leaderboard Service (`leaderboard-service`), Replay Service (`replay-lifecycle`) |

### 8.3 Redis Key Schema

| Key Pattern | Type | TTL | Contents |
|-------------|------|-----|----------|
| `match:{matchId}:state` | Hash | Match duration + 60s | Current tick state |
| `match:{matchId}:players` | Set | Match duration + 60s | Connected player IDs |
| `match:{matchId}:spectator_buffer` | List | Match duration + 60s | Ring buffer entries (non‑durable, lost on failover) |
| `match:{matchId}:last_offset` | String | Match duration + 60s | Kafka offset of the last state applied to Redis |
| `player:{playerId}:delta` | String | 30s | Last state delta for reconnect |

### 8.4 RabbitMQ Queues and Exchanges

| Queue / Exchange | Type | Durability | Purpose |
|-----------------|------|-----------|---------|
| `matchmaking.requests` | Queue (direct) | Durable | Inbound matchmaking requests |
| `matchmaking.expired` | Queue (dead-letter) | Durable | Requests expired after 2× window; consumed by Notification Service |
| `notifications.exchange` | Exchange (topic) | Durable | Fan-out routing for all notification event types |
| `notifications.match.expired` (bound to `notifications.exchange`) | Queue | Durable | Queue for `match.expired` events; Notification Service consumes |

---

## 9. Infrastructure Requirements

### 9.1 Kubernetes

- Minimum cluster: 1 node (minikube), 8 CPU cores, 16GB RAM for local development
- Namespaces: `game-platform`, `monitoring`, `infra`
- Resource limits defined on all pods; no unbounded containers
- StatefulSets used for: Game Room replicas, Couchbase, Kafka, etcd (3‑node cluster)
- Deployments used for: all stateless services
- HPA configured on Game Room Deployment with CPU-based scaling (see NFR-S-02, NFR-S-05)
- etcd cluster: 3 nodes with anti‑affinity (simulated via `podAntiAffinity` on hostname) to ensure resilience even in local demonstration.

### 9.2 Networking

- All inter-service communication within the cluster uses Kubernetes DNS (`service.namespace.svc.cluster.local`)
- External traffic enters only through the NGINX ingress
- No service exposes a NodePort except NGINX and the Kubernetes Dashboard
- Network policies restrict cross-namespace traffic to declared rules only

### 9.3 Storage

| Component | Storage Type | Size |
|-----------|-------------|------|
| Couchbase data bucket | PersistentVolume | 20GB |
| Kafka log storage | PersistentVolume | 30GB |
| etcd | PersistentVolume | 2GB (each) |
| MinIO | PersistentVolume | 50GB |
| Redis | EmptyDir (ephemeral) | — |

---

## 10. Security Requirements

| ID | Requirement |
|----|-------------|
| SEC-01 | All external traffic shall be encrypted with TLS 1.2 or higher |
| SEC-02 | JWT signing keys (private key) shall be stored as Kubernetes Secrets; public key shall be accessible via a well‑known HTTP endpoint |
| SEC-03 | No service shall run as root inside its container |
| SEC-04 | Docker images shall use minimal base images (distroless or alpine) to reduce attack surface |
| SEC-05 | Couchbase credentials shall be injected via Kubernetes Secrets |
| SEC-06 | RabbitMQ and Kafka shall require authentication for all producer and consumer connections |
| SEC-07 | NGINX shall enforce a rate limit of 100 requests per second per IP before forwarding to upstream services |
| SEC-08 | Inter-service API calls shall use either JWT validation (for user‑facing actions) or mutual TLS (mTLS) / API keys for system‑to‑system communication in production; for v1, service accounts with shared secrets may be used. |

---

## 11. Observability Requirements

### 11.1 Distributed Tracing
- All services shall be instrumented with the OpenTelemetry Python SDK
- Trace context shall propagate across Kafka message headers and RabbitMQ message properties
- A single player input event shall produce a complete trace spanning: NGINX → Game Room → Kafka publish → Replay Service consume
- Traces shall be collected by the OpenTelemetry Collector and exported to **Jaeger**, deployed as a self-hosted instance within the `monitoring` namespace.

### 11.2 Metrics
The following metrics shall be exposed by each service:

| Service | Key Metrics |
|---------|-------------|
| NGINX | Active connections, request rate, upstream response time |
| Matchmaking | Queue depth, average wait time, lobbies assembled per minute, `matchmaking_broker_unavailable` (bool gauge), `matchmaking_expired_count` |
| Game Room | Active matches, tick processing latency, etcd election count |
| Kafka | Consumer lag per topic and consumer group, publish rate, partition offset |
| Couchbase | Read/write latency, memory utilization, N1QL query duration |
| Redis | Hit rate, memory usage, connected clients |

### 11.3 Alerting
- Alert on etcd election rate exceeding 2 elections per 5 minutes (indicates instability)
- Alert on Kafka consumer lag exceeding 10,000 events on any topic
- Alert on matchmaking queue depth exceeding 50 unprocessed messages
- Alert on any pod restart loop (restartCount > 3 in 10 minutes)
- Alert on `matchmaking_broker_unavailable` gauge being non-zero for more than 30 seconds

### 11.4 Dashboards
A Grafana dashboard shall provide a real-time view of:
- Active match count and player count
- Matchmaking funnel (queued → matched → expired → in-game)
- Full distributed trace for a selected match (linked to Jaeger)
- Couchbase bucket memory utilization
- Kafka topic lag per consumer group

---

## 12. CI/CD Requirements

### 12.1 GitHub Actions (CI)
On every push to `main` and every pull request:
1. Run unit tests for all services
2. Run integration tests against a docker-compose test environment
3. Build Docker images for all services
4. Tag images with the Git commit SHA
5. Push images to the container registry
6. Fail the pipeline on any test failure or build error

### 12.2 ArgoCD (CD)
- ArgoCD shall watch the GitOps manifest repository for changes
- On manifest update, ArgoCD shall automatically sync the Kubernetes cluster to the declared state
- Sync shall use a rolling update strategy with a maximum of 1 unavailable pod per Deployment
- ArgoCD shall notify on sync failure via GitHub commit status

### 12.3 Environment Promotion
| Environment | Trigger | Cluster |
|-------------|---------|---------|
| Development | Every commit to `main` | minikube (local) |
| Staging | Manual tag `v*-rc` | minikube or remote |
| Production | Manual tag `v*` | Remote cluster (future) |

---

## 13. Constraints and Assumptions

### 13.1 Constraints
- Development environment is a single Ubuntu 24.04 machine with minikube; no multi-node cluster in phase 1
- The game client is intentionally minimal—a functional 2D arena, not a polished product
- Kafka runs in KRaft mode (no ZooKeeper dependency)
- All backend services are written in Python (3.11+) unless explicitly specified otherwise
- Leader election is delegated to etcd's built-in Raft mechanism; no application-level consensus library is used
- Jaeger is self-hosted within the cluster; no external managed tracing backend is used in v1
- Open-source NGINX is used; dynamic upstream updates are achieved with an etcd‑watcher sidecar that triggers a graceful reload every few seconds if necessary.

### 13.2 Assumptions
- Network latency between game client and server is under 50ms for local development testing
- Player skill ratings are initialized at 1200 (standard Elo baseline)
- A match consists of a single game room with 2–8 players and a fixed duration of 5 minutes
- Object storage (MinIO) is deployed within the same cluster as all other services
- The Couchbase cluster runs as a single-node instance in development

---

## 14. Acceptance Criteria

The project is considered complete when all of the following are demonstrable:

| ID | Criterion |
|----|-----------|
| AC-01 | Two or more clients can connect, be matched, enter a game room, and play in real time |
| AC-02 | Killing the game room leader pod mid-match results in automatic failover and match resumption within 5 seconds |
| AC-03 | A disconnected player can reconnect within 30 seconds and re-enter the match without match loss, receiving the current leader’s address from the Reconnect Handler |
| AC-04 | A completed match replay is seekable and accurately reconstructs the match from any tick |
| AC-05 | The leaderboard returns correct rankings via N1QL query within 200ms |
| AC-06 | A distributed trace for a single player input is visible end-to-end in Jaeger |
| AC-07 | Matchmaking demand triggers automatic game room pod scale-up via Kubernetes HPA |
| AC-08 | The full CI pipeline (test → build → push) completes successfully on a clean commit |
| AC-09 | ArgoCD syncs a manifest change to the cluster without manual intervention |
| AC-10 | The Grafana dashboard shows live match count, Kafka lag, and Couchbase memory utilization simultaneously |
| AC-11 | Making the RabbitMQ broker unavailable causes the Matchmaking Service to return `503` and surface the `matchmaking_broker_unavailable` metric within 30 seconds |
| AC-12 | A spectator joining with a 10-second delay observes state that is consistently 10 seconds behind the live player broadcast |
| AC-13 | After a Redis pod crash mid‑match, the game room leader recovers by replaying the Kafka log and continues without manual intervention (cold‑start recovery). |

---

## 15. Glossary

| Term | Definition |
|------|------------|
| **Raft** | A consensus algorithm that ensures distributed log replication with a defined leader election process |
| **KRaft** | Kafka's native consensus mode, replacing ZooKeeper as the metadata coordination layer |
| **N1QL** | Couchbase's SQL-compatible query language for JSON documents |
| **XDCR** | Cross-Datacenter Replication—Couchbase's mechanism for asynchronously replicating bucket data across geographically separated clusters |
| **HPA** | Horizontal Pod Autoscaler—Kubernetes controller that adjusts replica count based on observed metrics |
| **StatefulSet** | Kubernetes workload type that provides stable network identity and persistent storage across pod restarts |
| **GitOps** | An operational model where Kubernetes cluster state is declared in a Git repository and automatically reconciled by a CD tool (ArgoCD) |
| **OpenTelemetry** | A vendor-neutral observability framework providing APIs and SDKs for distributed tracing, metrics, and logging |
| **Jaeger** | An open-source distributed tracing backend used to store, query, and visualize OpenTelemetry trace data |
| **Consumer Lag** | The difference between the latest Kafka offset produced and the latest offset consumed—a measure of processing backlog |
| **Consumer Group** | A named group of Kafka consumers that collectively read a topic, with each partition assigned to exactly one member at a time |
| **State Delta** | A compressed representation of the difference between a player's last known game state and the current game state, used for efficient reconnection |
| **Tick** | A single simulation step in the game loop, executed at a fixed rate (20 per second) |
| **GSI** | Global Secondary Index—a Couchbase index type that supports N1QL queries across a bucket |
| **Dead-letter Queue** | A RabbitMQ queue that receives messages which could not be processed or expired before consumption—used here for expired matchmaking requests |
| **Ring Buffer** | A fixed-size circular data structure used to implement the spectator broadcast delay; old entries are overwritten as new ticks are committed |
| **Cold‑start replay** | The process of rebuilding game state from the Kafka event log when no Redis snapshot exists (full data‑plane loss) |

---

## 16. Open Questions

| ID | Question | Status |
|----|----------|--------|
| OQ-01 | Should player documents be denormalized with summary stats or queried dynamically? | Resolved: N1QL provides dynamic ranking; summary stats are derived at query time. |
| OQ-02 | What is the optimal matchmaking queue batching strategy? | Pending benchmark during implementation. |
| OQ-03 | Should spectator replay delay be adjustable mid-game? | Out of scope for v1; fixed at join time. |
| OQ-04 | When to replace CPU-based HPA with custom queue-depth metric? | Planned for v2; documented in ADR-06 and NFR-S-05. |
| OQ-05 | Is self-hosted Jaeger sufficient, or should a managed backend be used? | Self-hosted in v1; may evaluate managed service for demonstration environments. |

---

*This document is version-controlled alongside the project source code. Updates to architecture, service specifications, or requirements shall be reflected here before implementation begins.*