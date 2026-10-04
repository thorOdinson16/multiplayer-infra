# Benchmarks

Measured on one Linux machine (16 cores, 31 GB RAM) running the whole `docker compose` stack. The load
generator runs on the same machine, so absolute numbers are indicative, not production figures.
Both scripts are in `scripts/`, so you can reproduce them.

## Failover

`scripts/benchmark_failover.sh 10` hard-kills the current leader container (`docker kill`, no graceful
shutdown) ten times and polls every ~100 ms.

- **election** is the time from the kill until a different replica holds the etcd leader key.
- **resume** is the time from the kill until the match tick in Redis moves past its pre-kill value.

| metric | min | median | p95 | max |
|---|---|---|---|---|
| election | 4.86 s | 5.10 s | 6.74 s | 6.74 s |
| resume | 5.00 s | 5.23 s | 6.88 s | 6.88 s |

10 of 10 runs recovered; leadership alternated between the two replicas.

**Reading the result.** The leader key is attached to a 5-second etcd lease, so after a hard kill nothing
can take over until the lease expires. The median election time is the lease TTL plus about 100 ms, and
the match resumes about 130 ms after that. The slowest run (6.7 s) is a lease that was refreshed just
before the kill plus a slower follower campaign cycle. The in-CI check (`test_failover_timing.sh`,
8 s limit) reports 6-7 s because it polls once per second; the finer-grained benchmark shows most of
that was measurement granularity.

**Trade-off.** Failover time is set by the TTL. A shorter TTL recovers faster but risks a healthy
leader being replaced after a pause (GC, network blip) that outlasts the lease. Graceful shutdowns skip
the wait because the leader revokes its lease.

## Game server load

`scripts/load_test.py N 20` registers N players, connects them to one match (staggered 50 ms apart),
and has each send a movement input every 250 ms for 20 seconds.

- **broadcast interval** is the gap between consecutive state messages a client receives. The server
  ticks at 20 Hz, so the target is 50 ms.
- **input to state** is the time from sending an input until a broadcast shows the resulting position.

| players | errors | interval p50 | p95 | p99 | input→state p50 | p95 | p99 |
|---|---|---|---|---|---|---|---|
| 10 | 0 | 50.2 ms | 56.5 ms | 105 ms | 50 ms | 94 ms | 139 ms |
| 20 | 0 | 50.4 ms | 94.1 ms | 141 ms | 47 ms | 132 ms | 156 ms |
| 50 | 0 | 50.7 ms | 156 ms | 189 ms | 69 ms | 195 ms | 230 ms |
| 100 | 0 | 63.2 ms | 253 ms | 281 ms | 165 ms | 315 ms | 348 ms |

**Reading the result.**
- The median tick stays on target up to 50 players. Tail latency grows steadily with player count.
- At 100 players in a single match the median interval slips to 63 ms (about 16 Hz), so the server no
  longer holds 20 Hz. That is the practical ceiling for one room on this setup.
- Each tick sends the full state of every player to every client, so bytes per tick grow with the
  square of the player count. I have not profiled the server; sending deltas instead of full
  snapshots, a binary encoding, and interest management (only nearby players) are the obvious next
  steps. Horizontal scale comes from running more rooms, not bigger ones.
- Starting all 100 clients at once makes the gateway answer 49 of them with HTTP 503. That is the
  NGINX `limit_req` rate limit on `/ws` working as configured (30 requests/s, burst 50), not a server
  failure, which is why joins are staggered in the test.

## Reproduce

```bash
docker compose up -d --build        # wait until healthy (about 90 s)
scripts/benchmark_failover.sh 10    # about 7 minutes
scripts/load_test.py 50 20          # needs: pip install websockets
```
