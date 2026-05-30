# Distributed Real-Time Multiplayer Infrastructure Platform
## Software Requirements Specification (SRS)

**Version:** 1.1.0  
**Author:** Abhi  
**Date:** May 2026  
**Status:** Active Development  
**Changelog:** v1.1.0 — Added §16 Open Questions; added FR-MM-07/08 (matchmaking failure modes); added FR-SP-05 (spectator buffer mechanism); added FR-RP-07 (match.lifecycle consumer alignment); added NFR-S-05 (HPA semantic metric note); updated §11 with Jaeger backend clarification; minor cross-reference fixes throughout.

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

This document specifies the requirements for a **Distributed Real-Time Multiplayer Infrastructure Platform** — a production-grade backend system built around a simple top-down arena game. The game serves as a realistic, observable load generator for the infrastructure. The primary subject of the project is the infrastructure itself: distributed state management, event streaming, consensus-based fault tolerance, and autonomous scaling.

### 1.2 Scope

The platform encompasses the full lifecycle of a multiplayer session — from player authentication and matchmaking through real-time game execution, fault recovery, replay archival, and analytics aggregation. All backend services are independently containerized, orchestrated via Kubernetes, and observable end-to-end through distributed tracing.

### 1.3 Background and Motivation

Modern multiplayer game backends are among the most demanding distributed systems in production. They require sub-100ms state synchronization, zero-downtime failover, dynamic scaling under unpredictable load, and long-term event archival — all simultaneously. This project reconstructs that class of system from first principles, deliberately selecting tools that represent current industry practice rather than legacy defaults.

The secondary motivation is demonstrating deep familiarity with Couchbase's core capabilities — memory-first document storage, N1QL query semantics, and cross-datacenter replication — within a realistic, high-throughput data access pattern.

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
| G-02 | Implement authoritative game state replication using the Raft consensus algorithm |
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

**ADR-01: Raft over custom replication**  
Game room state replication uses the Raft consensus algorithm (via Hashicorp Raft library) rather than a custom replication scheme. Raft provides formal guarantees around leader election, log consistency, and failover that ad-hoc replication cannot.

**ADR-02: Kafka and RabbitMQ as complementary, not competing brokers**  
Kafka is used exclusively for immutable, ordered, high-throughput event logs (movement, telemetry, replay). RabbitMQ is used exclusively for task queues requiring competing consumers and explicit acknowledgement (matchmaking, notifications, async jobs). These are fundamentally different messaging primitives and must not be conflated.

**ADR-03: Couchbase as primary data store**  
Couchbase is selected over PostgreSQL or MongoDB for its memory-first bucket architecture, native N1QL support, and XDCR capability. These features map directly to the access patterns required: hot player session data served from RAM, flexible leaderboard queries without schema migrations, and future multi-region replication.

**ADR-04: etcd over ZooKeeper**  
etcd replaces ZooKeeper for service coordination and distributed configuration. ZooKeeper is a pre-Kafka-KRaft dependency with a dated operational model. etcd is what Kubernetes itself uses internally and represents current industry practice for distributed key-value coordination.

**ADR-05: StatefulSets for Raft nodes**  
Raft requires stable node identity across restarts. Kubernetes ephemeral pods cannot provide this. Game room Raft groups are deployed as StatefulSets with stable DNS names and persistent volume claims, ensuring Raft elections function correctly under pod restarts.

**ADR-06: CPU-based HPA as a v1 scaling proxy**  
Kubernetes HPA is configured on CPU utilization (70% threshold) as a pragmatic proxy for matchmaking demand in v1. CPU is a lagging indicator for game room load; a future iteration should replace or augment this with a custom metrics signal derived from matchmaking queue depth (see §16, OQ-04).

**ADR-07: Spectator state delay via server-side ring buffer**  
The configurable spectator broadcast delay (FR-SP-02) is implemented as a server-side ring buffer on the game room leader. Each committed tick snapshot is enqueued with a timestamp; the leader flushes entries to spectator connections after the configured delay elapses. This approach isolates spectator delivery from the player broadcast path entirely and adds no latency to authoritative state propagation.

---

## 4. Technology Stack

### 4.1 Full Stack Reference

| Layer | Technology | Version Target | Role |
|-------|-----------|---------------|------|
| Gateway | NGINX | 1.24+ | WebSocket proxy, TLS, rate limiting, load balancing |
| Containerization | Docker | 29+ | Service packaging and image management |
| Orchestration | Kubernetes | 1.36+ | Pod scheduling, scaling, service discovery, StatefulSets |
| Package Management | Helm | 3.x | Kubernetes manifest templating and release management |
| Consensus | Hashicorp Raft | latest | Game room state replication and leader election |
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
| Primary Language | Go | 1.22+ | Core backend services |
| Secondary Language | Node.js | 20 LTS | Game client, lightweight auxiliary services |
| Local Kubernetes | minikube | 1.38+ | Local development cluster |

### 4.2 Language Allocation by Service

| Service | Language | Justification |
|---------|----------|---------------|
| Auth Service | Go | Low latency, native JWT libraries, compiled binary |
| Matchmaking Service | Go | Concurrent queue processing, goroutine model |
| Game Room Server | Go | Tick-rate loop, Raft library native in Go |
| Replay Service | Go | Kafka consumer, high-throughput event processing |
| Leaderboard Service | Go | Couchbase Go SDK, N1QL query execution |
| Analytics Service | Go | Kafka consumer, aggregation pipelines |
| Notification Service | Go | RabbitMQ consumer, lightweight dispatch |
| Reconnect Handler | Go | Redis client, state delta computation |
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
| FR-MM-05 | On lobby assembly, Matchmaking shall request a game room from the Game Room pool, triggering pod creation if none are available |
| FR-MM-06 | Players shall receive a match-found notification via the Notification Service within 500ms of lobby assembly |
| FR-MM-07 | If the RabbitMQ broker is unavailable, the Matchmaking Service shall reject new matchmaking requests with a `503 Service Unavailable` response and expose a degraded-state metric; it shall not silently drop requests or block indefinitely |
| FR-MM-08 | Matchmaking requests that remain unprocessed for longer than 2× the configured time window shall be expired from the queue and the affected player notified via the Notification Service |

### 5.3 Game Room and Real-Time Gameplay

| ID | Requirement |
|----|-------------|
| FR-GR-01 | Each game room shall run as a Raft consensus group of three replicas (one leader, two followers) |
| FR-GR-02 | The leader shall be the sole processor of player inputs; followers shall maintain replicated state |
| FR-GR-03 | The game simulation shall advance at a fixed tick rate of 20 ticks per second (50ms per tick) |
| FR-GR-04 | Authoritative state snapshots shall be broadcast to all connected clients after each committed tick |
| FR-GR-05 | Each committed state transition shall be published as an immutable event to the `match.events` Kafka topic, partitioned by match ID |
| FR-GR-06 | Player input latency from client send to state broadcast shall be under 100ms on a local network |
| FR-GR-07 | The game room shall support a minimum of 8 simultaneous player connections |
| FR-GR-08 | Spectator connections shall receive read-only state broadcasts without participating in input processing |
| FR-GR-09 | On match completion, the game room leader shall publish a match-end event to the `match.lifecycle` Kafka topic before releasing the room |

### 5.4 Fault Tolerance and Reconnection

| ID | Requirement |
|----|-------------|
| FR-FT-01 | On leader pod failure, a Raft follower shall be elected as the new leader within 5 seconds |
| FR-FT-02 | No committed game state shall be lost during a leader failover |
| FR-FT-03 | Players shall experience a visible pause of no more than 5 seconds during a failover event |
| FR-FT-04 | On player WebSocket disconnection, the server shall hold their slot for 30 seconds before removing them |
| FR-FT-05 | On reconnection within the hold window, the Reconnect Handler shall deliver a compressed state delta enabling the client to re-enter the match |
| FR-FT-06 | The state delta payload shall not exceed 64KB for a match with up to 8 players |

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
| FR-SP-05 | The spectator broadcast delay (FR-SP-02) shall be implemented as a server-side ring buffer on the game room leader. Committed tick snapshots are timestamped and enqueued; the leader flushes entries to spectator connections after the configured delay. This implementation must not introduce any latency on the player broadcast path (see ADR-07) |

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
| NFR-R-02 | Raft-replicated game room state shall survive the simultaneous failure of one out of three replicas |
| NFR-R-03 | The matchmaking queue shall be durable — RabbitMQ messages shall persist across broker restarts |
| NFR-R-04 | Kafka topics shall be configured with a replication factor of 3 in any multi-broker deployment |

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
| NFR-M-01 | Each service shall expose a `/health` and `/ready` endpoint conforming to Kubernetes liveness and readiness probe conventions |
| NFR-M-02 | All service configuration shall be externalized via environment variables or Kubernetes ConfigMaps — no hardcoded values |
| NFR-M-03 | All Docker images shall be tagged with the Git commit SHA that produced them |
| NFR-M-04 | Kubernetes manifests shall be managed as Helm charts versioned in a dedicated GitOps repository |

---

## 7. Service Specifications

### 7.1 Gateway Service (NGINX)

- **Role:** Single ingress point for all client traffic
- **Protocol:** HTTP/1.1, HTTP/2, WebSocket upgrade
- **Responsibilities:** TLS termination, WebSocket proxying, upstream load balancing, per-IP rate limiting (100 req/s default), health check routing
- **Configuration:** Managed via ConfigMap, hot-reloadable without downtime
- **Upstream targets:** Auth Service, Game Room pods (by match ID header routing)

### 7.2 Auth Service

- **Role:** Credential validation and session lifecycle management
- **API:** REST over HTTP
- **Endpoints:** `POST /auth/login`, `POST /auth/refresh`, `POST /auth/logout`, `GET /auth/validate`
- **Dependencies:** Couchbase (session store), NGINX (inbound)
- **Token format:** JWT, HS256, configurable expiry
- **Scalability:** Stateless, horizontally scalable

### 7.3 Matchmaking Service

- **Role:** Skill-based player grouping and game room assignment
- **Queue:** RabbitMQ, durable queue, competing consumers
- **Algorithm:** Elo-range expansion with configurable time decay
- **Failure mode:** On RabbitMQ unavailability, returns `503` immediately; exposes `matchmaking_broker_unavailable` metric; does not block or silently drop (see FR-MM-07)
- **Request expiry:** Requests older than 2× the configured window are dead-lettered to a `matchmaking.expired` queue and the player notified (see FR-MM-08)
- **Dependencies:** RabbitMQ (inbound queue), Kubernetes API (room provisioning), Notification Service (outbound), etcd (room registry)
- **Scalability:** Stateless consumers, horizontally scalable

### 7.4 Game Room Server

- **Role:** Authoritative game simulation with replicated state
- **Protocol:** WebSocket (player I/O), gRPC (Raft inter-node)
- **Deployment:** Kubernetes StatefulSet, 3 replicas per match group
- **Consensus:** Hashicorp Raft, leader handles all writes, followers replicate
- **Tick rate:** 20 TPS (configurable)
- **Spectator buffering:** Server-side ring buffer on the leader; player broadcast path is unaffected (see ADR-07, FR-SP-05)
- **Match-end event:** On match completion, publishes to `match.lifecycle` before releasing the room (see FR-GR-09)
- **Dependencies:** Redis (hot state), Kafka (event publish to `match.events` and `match.lifecycle`), etcd (service registration), Couchbase (match record write)
- **Failover:** Automatic Raft re-election on leader pod loss, <5s recovery

### 7.5 Reconnect Handler

- **Role:** State delta computation and delivery for reconnecting players
- **Dependencies:** Redis (last-known state), Auth Service (token revalidation)
- **Payload:** Compressed JSON delta, max 64KB
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
- **Event types:** `match.found`, `match.ended`, `match.expired`, `player.joined`, `system.alert`
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
| `match.lifecycle` | By `matchId` | 30 days | Leaderboard Service (`leaderboard-service`), Replay Service (`replay-lifecycle`) |

> **Note:** `match.lifecycle` is the authoritative signal for match completion. Both the Leaderboard Service and Replay Service must consume it to trigger their respective end-of-match workflows. Game Room Servers are the sole producers of `match.lifecycle` events (see FR-GR-09).

### 8.3 Redis Key Schema

| Key Pattern | Type | TTL | Contents |
|-------------|------|-----|----------|
| `match:{matchId}:state` | Hash | Match duration + 60s | Current tick state |
| `match:{matchId}:players` | Set | Match duration + 60s | Connected player IDs |
| `match:{matchId}:spectator_buffer` | List | Match duration + 60s | Ring buffer of timestamped tick snapshots for spectator delay |
| `player:{playerId}:delta` | String | 30s | Last state delta for reconnect |

### 8.4 RabbitMQ Queues and Exchanges

| Queue / Exchange | Type | Durability | Purpose |
|-----------------|------|-----------|---------|
| `matchmaking.requests` | Queue (direct) | Durable | Inbound matchmaking requests from authenticated players |
| `matchmaking.expired` | Queue (dead-letter) | Durable | Requests that exceeded 2× the matchmaking window; triggers player notification |
| `notifications.exchange` | Exchange (topic) | Durable | Fan-out routing for all notification event types |
| `notifications.{eventType}` | Queue (topic-bound) | Durable | Per-event-type queues bound to `notifications.exchange` |

---

## 9. Infrastructure Requirements

### 9.1 Kubernetes

- Minimum cluster: 1 node (minikube), 8 CPU cores, 16GB RAM for local development
- Namespaces: `game-platform`, `monitoring`, `infra`
- Resource limits defined on all pods; no unbounded containers
- StatefulSets used for: Game Room Raft groups, Couchbase, Kafka, etcd
- Deployments used for: all stateless services
- HPA configured on Game Room Deployment with CPU-based scaling (see NFR-S-02, NFR-S-05)

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
| etcd | PersistentVolume | 2GB |
| MinIO | PersistentVolume | 50GB |
| Redis | EmptyDir (ephemeral) | — |

---

## 10. Security Requirements

| ID | Requirement |
|----|-------------|
| SEC-01 | All external traffic shall be encrypted with TLS 1.2 or higher |
| SEC-02 | JWT signing keys shall be stored as Kubernetes Secrets, not ConfigMaps |
| SEC-03 | No service shall run as root inside its container |
| SEC-04 | Docker images shall use minimal base images (distroless or alpine) to reduce attack surface |
| SEC-05 | Couchbase credentials shall be injected via Kubernetes Secrets |
| SEC-06 | RabbitMQ and Kafka shall require authentication for all producer and consumer connections |
| SEC-07 | NGINX shall enforce a rate limit of 100 requests per second per IP before forwarding to upstream services |
| SEC-08 | All inter-service API calls shall validate the caller's JWT before processing |

---

## 11. Observability Requirements

### 11.1 Distributed Tracing

- All services shall be instrumented with the OpenTelemetry Go SDK
- Trace context shall propagate across Kafka message headers and RabbitMQ message properties
- A single player input event shall produce a complete trace spanning: NGINX → Game Room → Kafka publish → Replay Service consume
- Traces shall be collected by the OpenTelemetry Collector and exported to **Jaeger**, deployed as a self-hosted instance within the `monitoring` namespace (see §4.1 and §16 OQ-05 for managed backend consideration)

### 11.2 Metrics

The following metrics shall be exposed by each service:

| Service | Key Metrics |
|---------|-------------|
| NGINX | Active connections, request rate, upstream response time |
| Matchmaking | Queue depth, average wait time, lobbies assembled per minute, `matchmaking_broker_unavailable` (bool gauge) |
| Game Room | Active matches, tick processing latency, Raft election count |
| Kafka | Consumer lag per topic and consumer group, publish rate, partition offset |
| Couchbase | Read/write latency, memory utilization, N1QL query duration |
| Redis | Hit rate, memory usage, connected clients |

### 11.3 Alerting

- Alert on Raft election rate exceeding 2 elections per 5 minutes (indicates instability)
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
- The game client is intentionally minimal — a functional 2D arena, not a polished product
- Kafka runs in KRaft mode (no ZooKeeper dependency)
- All services are written in Go unless explicitly specified otherwise
- The Raft implementation uses the Hashicorp Raft library, not a custom implementation
- Jaeger is self-hosted within the cluster; no external managed tracing backend is used in v1

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
| AC-03 | A disconnected player can reconnect within 30 seconds and re-enter the match without match loss |
| AC-04 | A completed match replay is seekable and accurately reconstructs the match from any tick |
| AC-05 | The leaderboard returns correct rankings via N1QL query within 200ms |
| AC-06 | A distributed trace for a single player input is visible end-to-end in Jaeger |
| AC-07 | Matchmaking demand triggers automatic game room pod scale-up via Kubernetes HPA |
| AC-08 | The full CI pipeline (test → build → push) completes successfully on a clean commit |
| AC-09 | ArgoCD syncs a manifest change to the cluster without manual intervention |
| AC-10 | The Grafana dashboard shows live match count, Kafka lag, and Couchbase memory utilization simultaneously |
| AC-11 | Making the RabbitMQ broker unavailable causes the Matchmaking Service to return `503` and surface the `matchmaking_broker_unavailable` metric within 30 seconds |
| AC-12 | A spectator joining with a 10-second delay observes state that is consistently 10 seconds behind the live player broadcast |

---

## 15. Glossary

| Term | Definition |
|------|------------|
| **Raft** | A consensus algorithm that ensures distributed log replication with a defined leader election process |
| **KRaft** | Kafka's native consensus mode, replacing ZooKeeper as the metadata coordination layer |
| **N1QL** | Couchbase's SQL-compatible query language for JSON documents |
| **XDCR** | Cross-Datacenter Replication — Couchbase's mechanism for asynchronously replicating bucket data across geographically separated clusters |
| **HPA** | Horizontal Pod Autoscaler — Kubernetes controller that adjusts replica count based on observed metrics |
| **StatefulSet** | Kubernetes workload type that provides stable network identity and persistent storage across pod restarts |
| **GitOps** | An operational model where Kubernetes cluster state is declared in a Git repository and automatically reconciled by a CD tool (ArgoCD) |
| **OpenTelemetry** | A vendor-neutral observability framework providing APIs and SDKs for distributed tracing, metrics, and logging |
| **Jaeger** | An open-source distributed tracing backend used to store, query, and visualize OpenTelemetry trace data |
| **Consumer Lag** | The difference between the latest Kafka offset produced and the latest offset consumed — a measure of processing backlog |
| **Consumer Group** | A named group of Kafka consumers that collectively read a topic, with each partition assigned to exactly one member at a time |
| **State Delta** | A compressed representation of the difference between a player's last known game state and the current game state, used for efficient reconnection |
| **Tick** | A single simulation step in the game loop, executed at a fixed rate (20 per second) |
| **GSI** | Global Secondary Index — a Couchbase index type that supports N1QL queries across a bucket |
| **Dead-letter Queue** | A RabbitMQ queue that receives messages which could not be processed or expired before consumption — used here for expired matchmaking requests |
| **Ring Buffer** | A fixed-size circular data structure used to implement the spectator broadcast delay; old entries are overwritten as new ticks are committed |

---

## 16. Open Questions

Decisions that are not yet resolved and may affect implementation. Each item should be closed before the affected component is built.

| ID | Question | Affects | Status |
|----|----------|---------|--------|
| OQ-01 | What is the target concurrent match count for the initial performance baseline beyond the stated minimum of 10? This drives resource limit sizing for game room pods. | §9.1, NFR-P-06 | Open |
| OQ-02 | Should the Jaeger instance use in-memory storage (sufficient for demo/interview) or a persistent backend (Cassandra, Elasticsearch)? In-memory loses all traces on pod restart. | §11.1, §13.1 | Open |
| OQ-03 | What container registry will be used for image storage — Docker Hub, GitHub Container Registry (GHCR), or a self-hosted registry within the cluster? | §12.1, §4.1 | Open |
| OQ-04 | The HPA currently scales on CPU (NFR-S-02). Should matchmaking queue depth be exposed as a custom Kubernetes metric and used as the primary HPA signal in a v1.1 iteration? This would require a custom metrics adapter (e.g., KEDA) and RabbitMQ metric scraping. | NFR-S-02, NFR-S-05, ADR-06 | Open — planned for v1.1 |
| OQ-05 | Should JWT signing use HS256 (symmetric, single shared secret) or RS256 (asymmetric, public/private key pair)? RS256 allows services to validate tokens without access to the signing key, which is more correct for a multi-service architecture. | §7.2, SEC-02 | Open |
| OQ-06 | What is the behavior when a game room Raft group loses quorum (both followers die while the leader is alive)? The leader should stop accepting writes. The match outcome in this case — forfeit, pause, or replay from last committed state — is unspecified. | FR-GR-01, FR-FT-02 | Open |
| OQ-07 | Is a Docker Compose local development environment required alongside the minikube cluster? A Compose file would significantly reduce the iteration cycle for individual service development without running the full cluster. | §13.1, §12.1 | Open |

---

*This document is version-controlled alongside the project source code. Updates to architecture, service specifications, or requirements shall be reflected here before implementation begins.*
