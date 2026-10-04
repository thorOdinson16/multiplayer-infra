# Lessons from getting this to run for real

The code looked finished long before the system worked. Everything below was found by actually
running the full stack and checking behaviour, not by reading code or watching unit tests pass.

## Bugs that only showed up end to end

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

## Concurrency bugs

- **Registration race.** Check-then-insert let five simultaneous sign-ups for one username all succeed.
  A `UNIQUE` constraint is the fix; no amount of application-level checking is.
- **Lost leaderboard updates.** Read, add one, write back: two matches finishing together overwrote
  each other, and a redelivered Kafka message counted a match twice. One transaction with row locks and a
  processed-matches table fixes both.
- **Blocking calls in `async` code.** Synchronous database and bcrypt calls inside `async def` handlers
  froze the event loop for every other request. Plain `def` handlers run in FastAPI's thread pool.

## Tests that passed without testing anything

- The hold-slot test printed `INFO: ... may be OK if match is not running` and exited 0. It had never
  verified anything. It now compares the player's position before disconnecting and after reconnecting.
- The end-to-end "health check" requested the same URL once per service name, so it checked one service
  eight times.
- Dependency installs ended in `|| true` and `2>/dev/null`, which hid a missing requirement for as long
  as the broken service was never started in CI.
- A shell script under `set -e` aborted silently when a helper's `python -c` failed to parse a line.

The pattern: a test that can only pass is worse than no test, because it makes a gap look covered.
When fixing one, first make it fail on purpose.

## What I would do the same way

- Run the real stack in CI rather than mocking it. Every bug above was found that way.
- Reproduce CI locally before changing anything: same compose file, a clean data volume, no cached images.
- Measure instead of asserting. See [benchmarks.md](benchmarks.md).
