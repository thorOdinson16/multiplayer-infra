# ADR-03: Couchbase as Primary Data Store

---

## Context

The platform requires a primary data store for:

- Player profiles — read on every connection, written infrequently
- Session tokens — read on every WebSocket upgrade, short TTL, high frequency
- Match history — written on match completion, queried for leaderboards and replays
- Leaderboard rankings — flexible ranking criteria (score, wins, win rate) across large player sets, queryable by time window

The access pattern characteristics are:
- Session documents must be served with sub-5ms latency
- Leaderboard queries must return in under 200ms across 100,000 documents
- Schema flexibility is desirable — player stats will accumulate fields over time
- Future multi-region replication is a goal

## Decision

Use **Couchbase Server 7.6+** as the primary data store for all player, session, match, and leaderboard data.

## Rationale

Couchbase maps directly to all four requirements in ways that PostgreSQL and MongoDB do not simultaneously achieve:

**Memory-first bucket architecture:** The `sessions` bucket is configured as a memory-first bucket, meaning hot documents (active session tokens) are served entirely from RAM with disk as the persistence layer behind it. This eliminates the disk I/O penalty for the highest-frequency read path in the system without requiring a separate caching layer for session data.

**N1QL:** Couchbase's SQL-compatible query language allows flexible leaderboard queries (ranked by score, wins, win rate, filtered by time window) without schema migrations. Adding a new ranking dimension requires only a new GSI index and a new query — no ALTER TABLE, no migration script.

**XDCR (Cross-Datacenter Replication):** When multi-region deployment becomes a goal, Couchbase's built-in XDCR replicates bucket data across geographically separated clusters asynchronously. This is a native capability, not a bolt-on.

**Document model:** JSON documents map naturally to the player, session, match, and replay checkpoint data models without an ORM or serialization layer.

## Consequences

- The Couchbase Go SDK (`github.com/couchbase/gocb/v2`) is a dependency of auth-service, leaderboard-service, replay-service, and game-room-server
- GSI indexes must be defined explicitly for all N1QL query paths: `score`, `wins`, `playerId`, `timestamp`
- Development runs a single-node Couchbase instance; this is sufficient for development but does not exercise XDCR
- Couchbase requires more memory headroom than a comparable PostgreSQL instance; the 20GB PersistentVolume allocation reflects this
- The `sessions` bucket TTL must match JWT expiry to avoid orphaned session documents

## Alternatives Rejected

| Alternative | Reason rejected |
|-------------|----------------|
| PostgreSQL | No memory-first serving for hot documents; schema migrations required for evolving player stats; no native XDCR equivalent |
| MongoDB | No native SQL-compatible query layer comparable to N1QL; XDCR equivalent (Atlas Global Clusters) is a managed-only feature; memory-first bucket architecture not available |
| DynamoDB | Managed-only; not self-hostable in minikube; no N1QL; flexible queries require expensive table scans or complex index design |
| Redis as primary store | Redis is used as the ephemeral per-match cache (see data requirements §8.3) — using it as the primary store would conflate two distinct data lifecycle patterns and create durability risk for player profile data |