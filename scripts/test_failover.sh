#!/bin/bash
# Failover test: kill the game-room leader and verify another replica takes
# over AND the match resumes (Redis tick keeps advancing).
set -uo pipefail

MATCH_ID="${MATCH_ID:-test-match-001}"
REDIS_KEY="match:${MATCH_ID}:state"
MAX_FAILOVER_SECONDS="${MAX_FAILOVER_SECONDS:-15}"

fail() { echo "FAIL: $1"; exit 1; }
compose_ps() { docker compose ps -q "$1" 2>/dev/null | head -1; }

ETCD_CONTAINER=$(compose_ps etcd)
[ -n "$ETCD_CONTAINER" ] || fail "No etcd container found. Is docker compose running?"
REDIS_CONTAINER=$(compose_ps redis)
[ -n "$REDIS_CONTAINER" ] || fail "No redis container found."

get_leader() {
  docker exec "$ETCD_CONTAINER" etcdctl get "/match/${MATCH_ID}/leader" --print-value-only 2>/dev/null | tr -d '\r\n'
}

get_tick() {
  local raw
  raw=$(docker exec "$REDIS_CONTAINER" redis-cli GET "$REDIS_KEY" 2>/dev/null)
  python3 -c "import sys,json; print(json.loads(sys.argv[1]).get('tick', -1))" "$raw" 2>/dev/null || echo -1
}

echo "=== Failover Test ==="
echo ""

LEADER_INSTANCE=""
for i in $(seq 1 30); do
  LEADER_INSTANCE=$(get_leader)
  if [ -n "$LEADER_INSTANCE" ]; then
    echo "  Leader found after ${i}s: $LEADER_INSTANCE"
    break
  fi
  sleep 1
done
[ -n "$LEADER_INSTANCE" ] || fail "No leader found in etcd after 30s"

ROOM1=$(compose_ps game-room)
ROOM2=$(compose_ps game-room-2)
if [ "$LEADER_INSTANCE" = "room-2" ]; then
  LEADER_CONTAINER="$ROOM2"
else
  LEADER_CONTAINER="$ROOM1"
fi
[ -n "$LEADER_CONTAINER" ] || fail "Could not determine leader container"

restore_leader() {
  echo "--- Restoring killed replica ---"
  docker update --restart=unless-stopped "$LEADER_CONTAINER" >/dev/null 2>&1 || true
  docker start "$LEADER_CONTAINER" >/dev/null 2>&1 || true
}
trap restore_leader EXIT

TICK_BEFORE=$(get_tick)
echo "Leader container: $LEADER_CONTAINER"
echo "Tick before failover: $TICK_BEFORE"
echo ""

echo "--- Killing leader (auto-restart disabled) ---"
docker update --restart=no "$LEADER_CONTAINER" >/dev/null 2>&1 || true
docker kill "$LEADER_CONTAINER" >/dev/null 2>&1 || true

RECOVERED=false
for i in $(seq 1 "$MAX_FAILOVER_SECONDS"); do
  sleep 1
  NEW_LEADER=$(get_leader)
  if [ -n "$NEW_LEADER" ] && [ "$NEW_LEADER" != "$LEADER_INSTANCE" ]; then
    echo "  New leader: $NEW_LEADER (after ${i}s)"
    RECOVERED=true
    break
  fi
  echo "  waiting for new leader... (${i}s)"
done
[ "$RECOVERED" = "true" ] || fail "No new leader elected within ${MAX_FAILOVER_SECONDS}s"

echo "--- Verifying match resumed ---"
RESUMED=false
for i in $(seq 1 "$MAX_FAILOVER_SECONDS"); do
  sleep 1
  TICK_NOW=$(get_tick)
  if [ "$TICK_NOW" -gt "$TICK_BEFORE" ] 2>/dev/null; then
    echo "  Match resumed: tick advanced $TICK_BEFORE -> $TICK_NOW"
    RESUMED=true
    break
  fi
  echo "  waiting for match to resume... (${i}s, tick=$TICK_NOW)"
done
[ "$RESUMED" = "true" ] || fail "Match did not resume after failover (tick stuck at $TICK_BEFORE)"

echo ""
echo "=== Failover Test Complete ==="
