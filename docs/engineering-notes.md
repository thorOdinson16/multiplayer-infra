# Engineering notes

Two things about how this project was built and checked: what the numbers are, and what went wrong on
the way to getting them. Design rationale is in [architecture.md](architecture.md#design-decisions).

## Measurements

Measured on one Linux machine (16 cores, 31 GB RAM) running the whole `docker compose` stack. The load
generator runs on the same machine, so absolute numbers are indicative, not production figures.
Both scripts are in `scripts/`, so you can reproduce them.

### Failover

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

### Game server load

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

### Reproduce

```bash
docker compose up -d --build        # wait until healthy (about 90 s)
scripts/benchmark_failover.sh 10    # about 7 minutes
scripts/load_test.py 50 20          # needs: pip install websockets
```

## Lessons learned

The code looked finished long before the system worked. Everything below was found by actually
running the full stack and checking behaviour, not by reading code or watching unit tests pass.

### Bugs that only showed up end to end

**nginx stripped the request path.** `proxy_pass http://$backend/;` looks harmless, but when the
target contains a variable, nginx replaces the whole URI instead of swapping the matched prefix.
`POST /auth/register` reached the auth service as `/register` and returned 404. The same mistake broke
five other routes. The fix is to leave the URI off (`proxy_pass http://$backend;`) so the original path
is forwarded.

**A missing type hint rejected every WebSocket.** `async def ws_endpoint(websocket)` has no
`WebSocket` annotation, so FastAPI treated `websocket` as a query parameter and refused the upgrade
with HTTP 403. The game server was healthy and every client was turned away.

**An empty `sadd` killed the game loop.** `redis.sadd(key, *players)` with no players raises, and the
exception ended the loop's task on the very first tick when nobody was connected. Nothing crashed
visibly, the container stayed "healthy", and the tests still reported that failover resumed the match.
The loop now skips the call and survives per-tick errors.

**Matches never restarted.** After a match's 300 seconds the room was marked available in etcd, but
nothing started the next match, so the room went silent. It only showed up in a load test that ran past
five minutes.

**Unpinned dependencies.** `etcd3` generates code that breaks on `protobuf` 4.x; one service had the
pin and another did not, and the unpinned one crash-looped. One service imported `prometheus_client`
without declaring it.

**An upstream image disappeared.** `minio/minio` is no longer published on Docker Hub. It worked on my
machine because a copy was cached locally. CI pulls from a clean slate, so it failed. Every image is
now pinned to a version that has been pulled fresh.

### Concurrency bugs

- **Registration race.** Check-then-insert let five simultaneous sign-ups for one username all succeed.
  A `UNIQUE` constraint is the fix; no amount of application-level checking is.
- **Lost leaderboard updates.** Read, add one, write back: two matches finishing together overwrote
  each other, and a redelivered Kafka message counted a match twice. One transaction with row locks and a
  processed-matches table fixes both.
- **Blocking calls in `async` code.** Synchronous database and bcrypt calls inside `async def` handlers
  froze the event loop for every other request. Plain `def` handlers run in FastAPI's thread pool.

### Tests that passed without testing anything

- The hold-slot test printed `INFO: ... may be OK if match is not running` and exited 0. It had never
  verified anything. It now compares the player's position before disconnecting and after reconnecting.
- The end-to-end "health check" requested the same URL once per service name, so it checked one service
  eight times.
- Dependency installs ended in `|| true` and `2>/dev/null`, which hid a missing requirement for as long
  as the broken service was never started in CI.
- A shell script under `set -e` aborted silently when a helper's `python -c` failed to parse a line.

The pattern: a test that can only pass is worse than no test, because it makes a gap look covered.
When fixing one, first make it fail on purpose.

### What I would do the same way

- Run the real stack in CI rather than mocking it. Every bug above was found that way.
- Reproduce CI locally before changing anything: same compose file, a clean data volume, no cached images.
- Measure instead of asserting. See [Measurements](#measurements).
