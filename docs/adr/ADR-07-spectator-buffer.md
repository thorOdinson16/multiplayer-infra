# ADR-07: Spectator Broadcast Delay via Server-Side Ring Buffer

---

## Context

FR-SP-02 requires that spectator connections receive state broadcasts with a configurable delay of 0–30 seconds relative to live game state. This is a standard feature in competitive games to prevent spectators from feeding real-time positional information to active players.

The implementation must satisfy two constraints simultaneously:
1. Spectators must receive state that is consistently N seconds behind the live player broadcast
2. The spectator delivery path must not add any latency to the player broadcast path — the 20 TPS tick loop and player state delivery are latency-critical

## Decision

Implement the spectator delay as a **server-side ring buffer on the game room leader**. On each committed tick, the leader enqueues a timestamped state snapshot into the ring buffer. A separate goroutine reads from the ring buffer and flushes entries to spectator WebSocket connections once the configured delay has elapsed since the snapshot's timestamp.

The player broadcast path is unaffected — it reads directly from committed state and pushes immediately, with no dependency on the ring buffer.

```
Committed tick
     │
     ├──► Player broadcast goroutine   (immediate, latency-critical)
     │
     └──► Ring buffer enqueue          (non-blocking write, O(1))
               │
               └──► Spectator flush goroutine  (reads buffer, delays by N seconds)
                         │
                         └──► Spectator WebSocket connections
```

## Rationale

**Player path isolation is the primary constraint.** Any implementation that puts the spectator delay in the critical path of the tick loop or player broadcast would introduce jitter or blocking risk into the most latency-sensitive part of the system. The ring buffer decouples spectator delivery entirely — it is a non-blocking enqueue from the tick loop's perspective.

**A ring buffer is the natural data structure.** The delay window (0–30 seconds) at 20 TPS means at most 600 snapshots are in flight at once. A fixed-size ring buffer of 600 entries covers the maximum delay with a bounded memory footprint. Old entries are overwritten as the buffer wraps, which is correct: if a spectator connects after the buffer has wrapped, they receive the oldest available snapshot.

**Server-side delay is simpler and more correct than client-side delay.** A client-side delay would require the server to send state in advance and rely on the client to hold it — which leaks future state to the client process and creates a trust problem in a competitive setting. Server-side delay ensures spectators genuinely do not have access to live state.

## Consequences

- The game room server has a `spectator/buffer.go` package implementing the ring buffer and flush goroutine
- The ring buffer size is computed at startup from `maxDelaySeconds × tickRate` (default: 30 × 20 = 600 entries)
- Spectator WebSocket connections are tracked separately from player connections in the game room's connection registry
- A Redis key `match:{matchId}:spectator_buffer` is defined in the data schema for persistence across leader failover — on election, the new leader must reconstruct or flush the buffer (behavior on failover is: flush buffer, spectators experience a brief state jump to near-live, then delay resumes)
- The spectator delay is configurable per-match at room initialization time, not per-spectator

## Alternatives Rejected

| Alternative | Reason rejected |
|-------------|----------------|
| Client-side delay | Leaks future state to client process; trust problem; client implementation complexity |
| Kafka-based delay (spectators consume from a lagged offset) | Introduces Kafka consumer complexity into the spectator path; offset lag is not a precise time-based delay; adds Kafka dependency to the client connection path |
| Separate delayed-broadcast service | Network hop between game room and spectator broadcast service adds latency and a new failure domain; unnecessary for a bounded delay window |
| No delay (spectators see live state) | Violates FR-SP-02; unacceptable for competitive use |