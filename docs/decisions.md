# Design decisions

Short records of the choices that shape this system: what was decided, why, and what it costs.
The original requirements and earlier ADRs are in [REQUIREMENTS.md](REQUIREMENTS.md).

## 1. PostgreSQL for durable data, Redis for sessions (replaced Couchbase)

**Context.** The first version stored everything in Couchbase. In practice the access patterns were
ordinary: look up a player by username, update win/loss counters, rank by rating, store a match
record, and expire sessions.

**Decision.** Players, matches and replay checkpoints live in PostgreSQL. Sessions live in Redis,
which expires keys natively.

**Why.**
- A `UNIQUE` constraint on `username` removes the check-then-insert race in registration. With
  Couchbase, five parallel sign-ups for one name all succeeded.
- A transaction with `SELECT ... FOR UPDATE` makes the leaderboard update atomic, so concurrent
  matches no longer lose each other's writes.
- Postgres starts in seconds. Couchbase needed a raised file-descriptor limit and over a minute to
  become healthy, which slowed every CI run.
- The Couchbase SDK calls were blocking and ran inside `async` handlers, stalling the event loop.

**Cost.** Redis sessions are only as durable as its append-only file. A Redis loss logs everyone out;
it does not lose accounts. Existing Couchbase data was not migrated.

## 2. Leader election with etcd leases

**Decision.** Game-room replicas race to create `/match/{id}/leader` in a transaction bound to a
5-second lease. The leader refreshes the lease every `ttl/3`. A follower becomes leader when the key
disappears.

**Why.** etcd already provides consensus, so no consensus code lives in the game server. A crashed
leader's lease simply expires.

**Cost.** Failover time is bounded below by the lease TTL: after a hard kill the key lives until the
lease runs out. Measured results are in [benchmarks.md](benchmarks.md). Lowering the TTL speeds
failover but makes a slow GC pause or network blip look like a crash.

## 3. Kafka for the event log, RabbitMQ for work queues

**Decision.** Per-tick state events, telemetry and match lifecycle go through Kafka. Matchmaking
requests and notifications go through RabbitMQ.

**Why.** They are different jobs. The event log needs ordering, retention and replay (a new leader can
rebuild state, the replay service can archive a match). A work queue needs competing consumers and
per-message acknowledgement. Using one tool for both would mean faking one of them.

## 4. Commit order: Kafka first, then Redis

**Decision.** On every tick the leader publishes the state event to Kafka and waits for the
acknowledgement. Only then does it advance in-memory state and write Redis. If the publish fails, the
tick is dropped and state does not advance.

**Why.** Kafka is the source of truth. Redis is a cache of the latest state for fast recovery. If Redis
is lost, a new leader replays Kafka. The reverse order could leave Redis ahead of the log, which
nothing could repair.

## 5. Idempotent leaderboard updates

**Decision.** The leaderboard commits a Kafka offset only after the database transaction succeeds.
Each match is first inserted into `processed_matches`, keyed on `(match_id, started_at)`, inside the
same transaction. A redelivered event hits the conflict and does nothing.

**Why.** Kafka gives at-least-once delivery, so a crash between "update" and "commit offset" replays
the event. Room ids are reused for successive matches, so `match_id` alone is not unique; the start
time disambiguates.

## 6. NGINX with a generated upstream map

**Decision.** A small `etcd-watcher` service watches `/match/*/leader-address` and rewrites an NGINX
`map` file. NGINX reloads when the file changes.

**Why.** Open-source NGINX cannot change upstreams at runtime. Reloading on file change is a few lines
and reloads are graceful. Requests without a `match_id` are routed to a current leader, so a client
never lands on a follower that would reject it.

**Cost.** Route changes take up to the reload poll interval (3 seconds) to apply.
