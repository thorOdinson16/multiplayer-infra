# Architecture Description

## Overview

A distributed real-time multiplayer infrastructure platform built around a simple top-down arena game. The game itself is intentionally minimal — it exists as a realistic load generator for the backend. The real subject of this project is the infrastructure: how distributed systems coordinate, how state is replicated under failure, how events flow through a pipeline, and how a modern platform scales without human intervention.

---

## System Architecture

### Client Layer

Players and spectators connect over WebSocket from a web or desktop client. The client is thin — it sends input events and renders authoritative state pushed from the server. No client-side prediction that conflicts with the server; the server is always the source of truth. Spectators connect to the same gateway but are routed to a read-only stream of game state rather than a game room.

### Gateway Layer

All inbound traffic — WebSocket connections, REST calls, health checks — enters through NGINX. It handles TLS termination, connection-level rate limiting, and load balancing across backend service pods. NGINX also acts as the WebSocket upgrade proxy, maintaining long-lived connections from clients and routing them to the appropriate upstream service. No business logic lives here. It is purely a traffic layer.

### Core Services

Each service is independently containerized, deployed as a Kubernetes pod, and communicates either through the message brokers or direct HTTP/gRPC depending on whether the interaction is synchronous or event-driven.

**Auth Service** issues and validates JWT tokens on connection. Session tokens are stored in Couchbase with a TTL, allowing the reconnect handler to validate returning players without re-authentication.

**Matchmaking Service** pulls player connection requests off a RabbitMQ queue, groups players by skill rating, and assigns them to an available game room. If no room is available, it signals Kubernetes to spin up a new game room pod via the cluster API. This is where autoscaling originates.

**Game Room Servers** are the most architecturally significant component. Each room runs a Raft consensus group across three replicas — a leader and two followers. The leader processes all player inputs, advances the authoritative game state tick by tick, and replicates committed state to followers. If the leader pod dies, a follower is elected within seconds and the game resumes. Every committed state change is also published to Kafka as an immutable movement event, which serves both the replay pipeline and the analytics pipeline downstream.

**Reconnect Handler** listens for reconnection requests from clients who dropped mid-game. It pulls the player's last known state delta from Redis — which holds ephemeral per-match state — and sends a compressed snapshot to the client so they can re-enter the match without replaying the full event log.

**Replay Service** consumes the movement event stream from Kafka and persists a structured event log per match. Replays are seekable — the service can reconstruct any game state at any tick by replaying events from the beginning or from a checkpoint. Completed match replays are archived to S3-compatible object storage. Active replays are served directly from the Kafka consumer offset.

**Leaderboard Service** writes match outcomes to Couchbase and queries rankings using N1QL — Couchbase's SQL dialect for JSON documents. This is a deliberate architectural choice: rather than maintaining a sorted set in Redis alone, rankings are derived from structured queries over the player document model, giving the system flexible ranking criteria without schema migrations.

**Analytics Service** consumes telemetry events from a dedicated Kafka topic — movement heatmaps, kill positions, session durations, match lengths — and aggregates them into time-series data for the observability dashboard.

**Notification Service** receives triggered jobs from RabbitMQ — match found, game over, friend joined — and dispatches in-game alerts back to the relevant connected clients through the gateway.

### Messaging Layer

Two brokers with distinct responsibilities, intentionally never conflated.

**Apache Kafka** owns the immutable, ordered event log. Movement events, telemetry, and replay data are high-throughput and append-only — they flow through Kafka. Topics are partitioned by match ID so all events for a given game land on the same partition in order. Consumers — replay service, analytics service — read at their own pace without affecting producers.

**RabbitMQ** owns task queues and async jobs. Matchmaking requests, notification dispatches, and background jobs are work items that need to be claimed, processed exactly once, and acknowledged. This is what a message queue is designed for. Using Kafka for this would be the wrong tool — Kafka doesn't model competing consumers and acknowledgement the way RabbitMQ does natively.

### Data Layer

**Couchbase** is the primary data store. Player profiles, authentication sessions, match history, and leaderboard documents all live here. The memory-first bucket architecture means hot documents — active player profiles, ongoing session tokens — are served entirely from RAM with disk as the persistence layer behind it. N1QL queries give the leaderboard service expressive ranking and filtering without a rigid schema. In a multi-region deployment, XDCR replicates player state across datacenters, which is the most directly relevant Couchbase capability to demonstrate.

**Redis** holds ephemeral per-match state — the current positions, health values, and score state for active games. This is not a replacement for Couchbase; it is a complement. Match state is transient and must be low-latency. Redis pub/sub also handles real-time state broadcast within a match before events are flushed to Kafka.

**Object Storage** holds completed replay archives. Once a match ends, the replay log is serialized and written to an S3-compatible store. This enforces correct data tiering — Kafka is not a long-term archive.

### Infrastructure Layer

**Kubernetes** orchestrates all services. Game room pods scale horizontally via Horizontal Pod Autoscaler triggered by matchmaking demand. StatefulSets with stable DNS are used for Raft nodes — Raft requires stable identity and Kubernetes ephemeral pods alone would break leader election.

**etcd** stores distributed configuration and is the coordination backend for leader discovery across services. Kubernetes itself uses etcd internally, and the project uses it explicitly for service-level configuration — reinforcing understanding of why ZooKeeper was the prior art and what replaced it.

**Docker** packages every service into an image. All images are built in CI, tagged by commit SHA, and pushed to a registry. Kubernetes pulls from the registry on deploy.

**GitHub Actions + ArgoCD** form the CI/CD pipeline. GitHub Actions runs tests, builds Docker images, and pushes them on merge to main. ArgoCD watches the Kubernetes manifest repository and syncs the cluster to the declared state — GitOps, not imperative deployment scripts.

### Observability

**OpenTelemetry** instruments every service with distributed tracing. A single player input — pressing move — produces a trace that spans NGINX, the game room, Kafka publish, and replay service consume. This trace is the clearest demonstration that the system behaves correctly as a distributed whole. Metrics flow to Prometheus and are visualized in Grafana. This layer is not optional decoration — it is how you verify and demonstrate the system in an interview setting.

---

## Project Flow

### 1. Player connects

A player opens the client and connects over WebSocket to the NGINX gateway. NGINX upgrades the connection and forwards it to the Auth Service. The Auth Service validates credentials, issues a signed JWT, and returns a session token stored in Couchbase with a TTL. The player is now authenticated and waiting in the matchmaking pool.

### 2. Matchmaking

The player's connection metadata — skill rating, region, preferred game mode — is pushed as a message onto the RabbitMQ matchmaking queue. The Matchmaking Service is a competing consumer on this queue. It pulls requests off the queue, groups players by skill rating within a time window, and when a full lobby is assembled, it assigns a game room. If no idle game room pod exists, Matchmaking calls the Kubernetes API to schedule a new game room StatefulSet. The new pod registers itself with etcd on startup, making it discoverable by other services.

### 3. Game room initializes

The assigned game room boots its Raft group — three replicas negotiate a leader election via etcd-coordinated discovery. The leader initializes the match state, writes the match document to Couchbase, and notifies all matched players of their room assignment via the Notification Service (RabbitMQ job → NGINX → client push). Players' WebSocket connections are re-routed by NGINX to the game room leader's endpoint.

### 4. Match runs

Players send input events — move, shoot, interact — over WebSocket to the game room leader. The leader applies inputs to the game state, advances the simulation tick, and broadcasts the authoritative state snapshot back to all connected clients and spectators. Each committed state transition is published as an immutable event to a Kafka topic partitioned by match ID. Redis holds the live per-match state snapshot for fast access by the reconnect handler. The match runs at a fixed tick rate; the Kafka event log is the ground truth.

### 5. Fault tolerance in-flight

If the game room leader pod crashes mid-match, the Raft followers detect the missing heartbeat, hold a new election, and one follower is promoted to leader within seconds. The new leader resumes from the last committed state. Players experience a brief freeze, then the game continues — no match loss, no data loss. When a player's WebSocket drops, the Reconnect Handler reads their last state delta from Redis and sends a compressed rejoin payload. The player re-enters without replaying the full event history.

### 6. Match ends

The game room leader writes the final match outcome — scores, kill counts, duration — to Couchbase. A match-end event is published to Kafka. The Replay Service finalizes the event log for the match, writes the seekable replay to object storage, and updates its index. The Leaderboard Service consumes the match outcome and updates rankings via an N1QL upsert. The Analytics Service aggregates the match telemetry into the dashboard. RabbitMQ jobs dispatch match-over notifications to all players. The game room pods are returned to the idle pool or terminated, and Kubernetes reschedules capacity accordingly.

### 7. Replay playback

A player requests a replay from the client. The request hits NGINX, routes to the Replay Service, which locates the match event log in object storage, streams events to a replay reader, and pushes state snapshots to the client at playback speed. The client renders the match identically to how it was played — same tick rate, same state transitions — because the Kafka event log is a complete and ordered record.

### 8. Observability end-to-end

Every step above emits OpenTelemetry spans. A single move input produces a trace: NGINX receive → game room process → Kafka publish → replay consume. Trace IDs propagate through message headers across Kafka and RabbitMQ so the full causal chain is visible in one view. Prometheus scrapes metrics from every service. Grafana dashboards show active matches, matchmaking queue depth, Raft election counts, Kafka consumer lag, Couchbase read latency, and Redis hit rates — all the signals that matter for a live distributed system.