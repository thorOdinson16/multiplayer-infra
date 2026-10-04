# Architecture

A multiplayer arena backend: nine small services, a replicated game server with leader election, and an
event pipeline behind it. The game is deliberately minimal (players move and shoot in a 2D arena). It
exists to give the infrastructure something real to carry. For a diagram, see the
[README](../README.md#architecture); for measurements and lessons learned, see
[engineering-notes.md](engineering-notes.md).

## Services

| Service | Responsibility |
|---|---|
| **NGINX gateway** | Single entry point for REST and WebSocket. Rate limiting, CORS, TLS listener. No business logic. Its game-room route map is generated (see decision 6). |
| **Auth** | Register and login (bcrypt), RS256 JWT issue and validation, session revocation. Players in PostgreSQL, sessions in Redis. |
| **Matchmaking** | Consumes join requests from RabbitMQ, groups 2 to 8 players by Elo (the allowed range widens the longer someone waits), claims a free room through etcd, and asks Kubernetes for more rooms when none are free. |
| **Game room** | The authoritative game server. Runs as replicas of the same match; one is elected leader and runs the 20 Hz game loop. |
| **Reconnect handler** | Returns a dropped player's state and the current leader's address from Redis and etcd. |
| **Replay** | Consumes `match.events`, archives each finished match to MinIO as JSON, stores periodic checkpoints in PostgreSQL, serves and seeks replays. |
| **Leaderboard** | Applies each finished match to player stats and Elo in one transaction; serves rankings. |
| **Analytics** | Consumes `match.telemetry` and exposes aggregates as Prometheus metrics. |
| **Notification** | Consumes match events from RabbitMQ and pushes them to connected clients over WebSocket. |
| **etcd-watcher** | Watches leader addresses in etcd and rewrites NGINX's upstream map. |

## Where state lives

| Store | Holds | Notes |
|---|---|---|
| **PostgreSQL** | `players`, `matches`, `processed_matches`, `replay_checkpoints` | Source of truth for durable data. Schema: `scripts/postgres_init.sql`. |
| **Redis** | `session:{id}`, `player_session:{player}` (TTL = JWT lifetime); `match:{id}:state`, `:last_offset`, `:players`; `hold:{match}:{player}` (30 s) | Sessions and the latest game state. Append-only persistence is on in the compose setup. |
| **Kafka** | `match.events` (per-tick state), `match.telemetry`, `match.lifecycle` (match end) | Partitioned by match id so one match is totally ordered. The event log is the source of truth for game state. |
| **RabbitMQ** | `matchmaking.requests`, `notifications.match` | Work queues with acknowledgement. |
| **etcd** | `/match/{id}/leader` (leased), `/match/{id}/leader-address`, `/rooms/available/{room}` | Coordination only; nothing durable. |
| **MinIO** | `{match_id}/replay.json` | Completed replay archives. |

## Life of a match

1. **Join.** A player registers or logs in and gets a JWT. The client posts to the matchmaking API, which
   validates the token before queueing the request.
2. **Match.** Matchmaking groups players by Elo, claims an available room from etcd, and publishes a
   notification so each client learns its room.
3. **Lead.** The room's replicas race to create `/match/{id}/leader` bound to a 5-second lease. The winner
   publishes its address, starts the game loop, and refreshes the lease every `ttl/3`. The others wait on the
   key. The generated NGINX route sends clients to the current leader. A replica that is not leader refuses
   WebSocket connections.
4. **Play.** Each tick (50 ms) the leader applies queued inputs, publishes the new state to Kafka and waits for
   the acknowledgement, then advances in-memory state, writes it to Redis and broadcasts it to players. Spectators
   get the same stream delayed by 10 seconds.
5. **End.** After 300 seconds (configurable) the leader publishes a lifecycle event, writes the match record to
   PostgreSQL, and starts the next match in the same room. The Leaderboard applies the result; Replay archives
   the event log to MinIO.
6. **Replay.** `GET /replay/{match_id}` returns the archive; `/replay/{match_id}/seek?tick=N` returns events from a
   tick onward.

## Failure handling

| Failure | What happens |
|---|---|
| **Leader dies** | The lease expires (about 5 s), a follower wins the election, loads state from Redis and continues from the last Kafka offset. Measured: median 5.1 s to a new leader. |
| **Redis loses a match's state** | The new leader rebuilds state from the nearest replay checkpoint plus Kafka. |
| **Player disconnects** | Their slot is held in Redis for 30 seconds; reconnecting restores their position. |
| **Kafka publish fails** | The tick is dropped and state does not advance, so Redis can never be ahead of the log. |
| **`match.end` delivered twice** | `processed_matches` makes the second one a no-op. |
| **Leaderboard crashes mid-update** | The offset was not committed and the transaction rolled back; the event is retried. |
| **A single tick throws** | It is logged and the loop continues; one bad tick does not end the match. |

## Design decisions

### 1. PostgreSQL for durable data, Redis for sessions

**Context.** The first version kept everything in a single document store. The access patterns turned out to be
ordinary: look up a player by username, update win/loss counters, rank by rating, store a match record, expire
sessions.

**Decision.** Players, matches and replay checkpoints live in PostgreSQL. Sessions live in Redis, which expires
keys natively.

**Why.**
- A `UNIQUE` constraint on `username` removes the check-then-insert race in registration. With the document store,
  five parallel sign-ups for one name all succeeded.
- A transaction with `SELECT ... FOR UPDATE` makes the leaderboard update atomic, so concurrent matches no longer
  lose each other's writes.
- Postgres starts in seconds. The document store needed a raised file-descriptor limit and over a minute to become
  healthy, which slowed every CI run.
- That store's SDK calls were blocking and ran inside `async` handlers, stalling the event loop.

**Cost.** Redis sessions are only as durable as its append-only file. A Redis loss logs everyone out; it does not
lose accounts. Data from the earlier store was not migrated.

### 2. Leader election with etcd leases

**Decision.** Replicas race to create `/match/{id}/leader` in a transaction bound to a 5-second lease. The leader
refreshes it every `ttl/3`; a follower takes over when the key disappears.

**Why.** etcd already provides consensus, so no consensus code lives in the game server. A crashed leader's lease
simply expires.

**Cost.** Failover time is bounded below by the TTL: after a hard kill the key lives until the lease runs out. A
shorter TTL recovers faster but risks replacing a healthy leader after a pause (GC, network blip) that outlasts the
lease. A graceful shutdown skips the wait because the leader revokes its lease. Numbers are in
[engineering-notes.md](engineering-notes.md#failover).

### 3. Kafka for the event log, RabbitMQ for work queues

**Decision.** Per-tick state, telemetry and lifecycle events go through Kafka. Matchmaking requests and
notifications go through RabbitMQ.

**Why.** They are different jobs. The event log needs ordering, retention and replay: a new leader can rebuild
state and the replay service can archive a match. A work queue needs competing consumers and per-message
acknowledgement. One tool for both would mean faking one of them.

### 4. Commit order: Kafka first, then Redis

**Decision.** Each tick the leader publishes to Kafka and waits for the acknowledgement. Only then does it advance
state and write Redis. If the publish fails, the tick is dropped.

**Why.** Kafka is the source of truth and Redis is a cache of the latest state for fast recovery. If Redis is lost,
a new leader replays Kafka. The reverse order could leave Redis ahead of the log, which nothing could repair.

### 5. Idempotent leaderboard updates

**Decision.** The leaderboard commits a Kafka offset only after the database transaction succeeds. Each match is
first inserted into `processed_matches`, keyed on `(match_id, started_at)`, inside the same transaction. A
redelivered event hits the conflict and does nothing.

**Why.** Kafka gives at-least-once delivery, so a crash between "update" and "commit offset" replays the event. Room
ids are reused for successive matches, so `match_id` alone is not unique; the start time disambiguates.

### 6. NGINX with a generated upstream map

**Decision.** `etcd-watcher` watches `/match/*/leader-address` and rewrites an NGINX `map` file. NGINX reloads when
the file changes (polled every 3 seconds). Requests without a `match_id` go to a current leader, so a client never
lands on a follower that would reject it.

**Why.** Open-source NGINX cannot change upstreams at runtime. Reloading on file change is a few lines and reloads are
graceful.

**Cost.** A route change takes up to the reload poll interval to apply, so right after a failover a new connection
can briefly reach the wrong replica and be refused.

## Deployment

- **Local:** `docker compose up` runs everything, with two game-room replicas. Every credential has a development
  default.
- **Kubernetes:** a Helm chart and base manifests are provided. Game rooms are a StatefulSet (three replicas) with a
  HorizontalPodAutoscaler, and matchmaking can create rooms through the Kubernetes API. CI renders the chart but does
  not run it on a live cluster.
- **CI:** GitHub Actions runs lint, unit tests, builds every image, renders the Helm chart, then starts the full
  compose stack and runs the end-to-end, failover, hold-slot and telemetry checks. Images are not pushed to a
  registry, and there is no GitOps controller.

## Observability

Every service exposes Prometheus metrics at `/metrics`, and sends OpenTelemetry traces through the collector to
Jaeger. Grafana is provisioned with Prometheus as its data source. Useful signals include active matches, matchmaking
queue depth, election counts, tick latency, Kafka consumer lag and PostgreSQL query time.
