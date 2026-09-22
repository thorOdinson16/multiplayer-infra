#!/bin/bash
# Failover timing test: measure leader election latency (SLA: 8s)
set -uo pipefail

MATCH_ID="${MATCH_ID:-test-match-001}"
MAX_FAILOVER_SECONDS="${MAX_FAILOVER_SECONDS:-8}"
REDIS_KEY="match:${MATCH_ID}:state"

fail() { echo "FAIL: $1"; exit 1; }
compose_ps() { docker compose ps -q "$1" 2>/dev/null | head -1; }

echo "=== Failover Timing Test ==="
echo "SLA: a live replica assumes leadership within ${MAX_FAILOVER_SECONDS}s"
echo ""

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

LEADER_INSTANCE=""
for i in $(seq 1 30); do
  LEADER_INSTANCE=$(get_leader)
  if [ -n "$LEADER_INSTANCE" ]; then
    break
  fi
  sleep 1
done
[ -n "$LEADER_INSTANCE" ] || fail "No leader found in etcd after 30s"

ROOM1=$(compose_ps game-room)
ROOM2=$(compose_ps game-room-2)
if [ "$LEADER_INSTANCE" = "room-2" ]; then LEADER_CONTAINER="$ROOM2"; else LEADER_CONTAINER="$ROOM1"; fi
[ -n "$LEADER_CONTAINER" ] || fail "Could not determine leader container"

restore_leader() {
  echo "--- Restoring killed replica ---"
  docker update --restart=unless-stopped "$LEADER_CONTAINER" >/dev/null 2>&1 || true
  docker start "$LEADER_CONTAINER" >/dev/null 2>&1 || true
}
trap restore_leader EXIT

TICK_BEFORE=$(get_tick)
echo "Leader: $LEADER_CONTAINER ($LEADER_INSTANCE)"
echo "Tick before failover: $TICK_BEFORE"

START=$(date +%s.%N)
echo "Killing leader (auto-restart disabled) at $(date)"
docker update --restart=no "$LEADER_CONTAINER" >/dev/null 2>&1 || true
docker kill "$LEADER_CONTAINER" >/dev/null 2>&1 || true

RECOVERED=false
for i in $(seq 1 "$MAX_FAILOVER_SECONDS"); do
  sleep 1
  NEW_LEADER=$(get_leader)
  if [ -n "$NEW_LEADER" ] && [ "$NEW_LEADER" != "$LEADER_INSTANCE" ]; then
    END=$(date +%s.%N)
    DURATION=$(python3 -c "print(f'{max(0, $END - $START):.2f}')" 2>/dev/null)
    echo "New leader elected: $NEW_LEADER after ${DURATION}s"
    RECOVERED=true
    break
  fi
  echo "  waiting for new leader... (${i}s)"
done

if [ "$RECOVERED" != "true" ]; then
  END=$(date +%s.%N)
  DURATION=$(python3 -c "print(f'{max(0, $END - $START):.2f}')" 2>/dev/null)
  fail "No new leader elected within ${MAX_FAILOVER_SECONDS}s (took ${DURATION}s)"
fi

# Confirm the match actually resumed on the new leader.
RESUMED=false
for i in $(seq 1 "$MAX_FAILOVER_SECONDS"); do
  sleep 1
  TICK_NOW=$(get_tick)
  if [ "$TICK_NOW" -gt "$TICK_BEFORE" ] 2>/dev/null; then
    echo "Match resumed: tick $TICK_BEFORE -> $TICK_NOW"
    RESUMED=true
    break
  fi
done
[ "$RESUMED" = "true" ] || fail "Match did not resume after failover (tick stuck at $TICK_BEFORE)"

SLA_OK=$(python3 -c "print('true' if float('$DURATION') <= $MAX_FAILOVER_SECONDS else 'false')" 2>/dev/null)
[ "$SLA_OK" = "true" ] || fail "Failover took ${DURATION}s which exceeds SLA of ${MAX_FAILOVER_SECONDS}s"

echo ""
echo "PASS: Failover completed in ${DURATION}s (SLA: ${MAX_FAILOVER_SECONDS}s) and match resumed"
