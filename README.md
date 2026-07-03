# Distributed Real-Time Multiplayer Infrastructure Platform

Full-stack distributed systems demo: Python microservices, Node.js game client,
etcd leader election, Couchbase, Kafka, RabbitMQ, Redis, Kubernetes-ready.

## Quick Start

```bash
# 1. Generate JWT keys (already done if present)
./scripts/gen_keys.sh

# 2. Start everything
docker-compose up --build -d

# 3. Wait for all services to be healthy (60-120s)
#    Check status:
docker-compose ps

# 4. Run the E2E test
./scripts/test_end_to_end.sh

# 5. Open the game client
#    http://localhost:3000
```

## Services & Ports

| Service | Port | Description |
|---------|------|-------------|
| Game Client | 3000 | Browser-based arena game |
| NGINX Gateway | 8080 | WebSocket proxy & REST gateway |
| Auth | 8001 | JWT login/register/validate |
| Matchmaking | 8002 | Elo-based player matching |
| Game Room | 8003 | Real-time game simulation |
| Replay | 8004 | Event log & replay archive |
| Leaderboard | 8005 | N1QL-powered rankings |
| Analytics | 8006 | Telemetry aggregation |
| Notification | 8007 | Event dispatch to clients |
| Reconnect Handler | 8008 | State delta for reconnecting players |

## Infrastructure

| Component | Port | Notes |
|-----------|------|-------|
| Redis | 6379 | Ephemeral match state |
| RabbitMQ | 5672/15672 | Task queues (guest/guest) |
| etcd | 2379 | Leader election & coordination |
| Kafka | 9092 | Immutable event log |
| Couchbase | 8091 | Primary data store (Administrator/password) |
| MinIO | 9000/9001 | Object storage (minioadmin/minioadmin) |
| Prometheus | 9090 | Metrics collection |
| Grafana | 3001 | Dashboards (admin/admin) |
| Jaeger | 16686 | Distributed tracing |

## Architecture

```ascii
Client (WebSocket)
  |
NGINX Gateway (port 8080)
  |
  +-- Auth Service ------------> Couchbase
  +-- Matchmaking Service -----> RabbitMQ, etcd
  +-- Game Room Server ---------> Redis, Kafka, etcd, Couchbase
  +-- Reconnect Handler --------> Redis, etcd
  +-- Replay Service -----------> Kafka, MinIO, Couchbase
  +-- Leaderboard Service ------> Kafka, Couchbase
  +-- Analytics Service --------> Kafka, Prometheus
  +-- Notification Service -----> RabbitMQ, Redis
```

## Event Flow

1. **Player connects** → Auth validates → JWT issued
2. **Matchmaking** → Request queued in RabbitMQ → Matcher groups by Elo →
   Notification dispatched → Game room assigned
3. **Game starts** → Three replicas elect leader via etcd →
   Leader runs game loop (20 ticks/s) → Events published to Kafka →
   State snapshots to Redis
4. **Failover** → Leader dies → etcd lease expires →
   Follower acquires lease → Reads Kafka/Redis →
   Game continues within 5 seconds
5. **Match ends** → Lifecycle event to Kafka →
   Leaderboard updates via N1QL → Replay archived to MinIO →
   Analytics aggregated

## Key Features

- **etcd-based leader election** (ADR-01): Three game room replicas race for an etcd lease.
  No embedded Raft — etcd handles consensus.
- **Atomic commit order** (ADR-08): Kafka first, then Redis. Ensures no state loss.
- **Cold-start recovery** (ADR-10): Full replay from Kafka if Redis is lost.
- **Spectator buffer** (ADR-07): Configurable server-side delay, no player impact.
- **Separated brokers** (ADR-02): Kafka for event log, RabbitMQ for task queues.
- **OpenTelemetry tracing**: End-to-end traces across all services → Jaeger.
- **Prometheus metrics**: Every service exposes `/metrics`.

## Monitoring

- **Grafana**: http://localhost:3001 (admin/admin)
- **Jaeger**: http://localhost:16686
- **Prometheus**: http://localhost:9090
- **RabbitMQ**: http://localhost:15672 (guest/guest)
- **Couchbase**: http://localhost:8091 (Administrator/password)
- **MinIO**: http://localhost:9001 (minioadmin/minioadmin)

## Testing

```bash
# Full E2E test (requires docker-compose running)
./scripts/test_end_to_end.sh

# Failover test (kills game-room container)
./scripts/test_failover.sh
```

## Manual Test Steps

1. Open http://localhost:3000 in two browser tabs
2. Register "alice" / "bob" with password "pass123"
3. Click "Join Matchmaking" on both → wait for match found notification
4. Use WASD keys to move, click to shoot
5. Open Grafana: http://localhost:3001 → see live match metrics
6. Open Jaeger: http://localhost:16686 → find traces by service name
7. Test failover: `docker kill $(docker ps --filter name=game-room -q)`
8. View leaderboard: http://localhost:8080/leaderboard

## Project Structure

```
├── client/           # Node.js game client (Express + Canvas)
├── docs/             # Architecture & requirements
├── infra/
│   ├── nginx/        # Gateway configuration
│   ├── kubernetes/   # K8s manifests (base + overlays)
│   └── helm/         # Helm charts
├── monitoring/
│   ├── prometheus.yml
│   ├── opentelemetry-collector-config.yaml
│   └── grafana-dashboards/
├── scripts/          # Init & test scripts
├── services/
│   ├── auth/             # JWT + Couchbase sessions
│   ├── matchmaking/      # RabbitMQ + Elo matcher
│   ├── game-room/        # Game loop + leader election
│   ├── reconnect-handler/# Redis state + etcd
│   ├── replay/           # Kafka + MinIO archives
│   ├── leaderboard/      # Couchbase N1QL
│   ├── analytics/        # Kafka + Prometheus
│   └── notification/     # RabbitMQ + WebSocket
└── docker-compose.yml   # Single-command deploy
```

## Requirements

- Docker Engine 24+ & Docker Compose v2
- ~8GB free RAM for all services
- Python 3.11+ (for local scripts)
- OpenSSL (for key generation)
