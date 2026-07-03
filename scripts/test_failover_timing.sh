#!/bin/bash
# Failover timing test: measure leader election latency (SLA: 8s)
set -e

BASE_URL="${BASE_URL:-http://localhost:8080}"
MAX_FAILOVER_SECONDS="${MAX_FAILOVER_SECONDS:-8}"
MATCH_ID="${MATCH_ID:-test-match-001}"

echo "=== Failover Timing Test ==="
echo "SLA: a live replica assumes leadership within ${MAX_FAILOVER_SECONDS}s"
echo ""

# 1. Identify the etcd container and leader
ETCD_CONTAINER=$(docker ps --filter "label=com.docker.compose.service=etcd" --format "{{.Names}}" | head -1)
if [ -z "$ETCD_CONTAINER" ]; then
  echo "FAIL: No etcd container found. Is docker-compose running?"
  exit 1
fi
LEADER_INSTANCE=$(docker exec "$ETCD_CONTAINER" etcdctl get /match/test-match-001/leader --print-value-only 2>/dev/null || echo "")
if [ -z "$LEADER_INSTANCE" ]; then
  echo "FAIL: No leader found in etcd. Is the game-room running?"
  exit 1
fi

LEADER_CONTAINER=$(docker ps --filter "label=com.docker.compose.service=game-room" --format "{{.Names}}" | head -1)
if docker ps --filter "name=game-room-2" --format "{{.Names}}" | grep -q .; then
  SECOND_CONTAINER=$(docker ps --filter "name=game-room-2" --format "{{.Names}}" | head -1)
fi

if [ "$LEADER_INSTANCE" = "room-2" ] && [ -n "$SECOND_CONTAINER" ]; then
  LEADER_CONTAINER="$SECOND_CONTAINER"
fi

if [ -z "$LEADER_CONTAINER" ]; then
  echo "FAIL: Could not determine leader container"
  exit 1
fi

FOLLOWER_CONTAINER=""
if [ "$LEADER_CONTAINER" = "$(docker ps --filter "name=game-room-2" --format "{{.Names}}" | head -1)" ]; then
  FOLLOWER_CONTAINER=$(docker ps --filter "label=com.docker.compose.service=game-room" --format "{{.Names}}" | head -1)
else
  FOLLOWER_CONTAINER=$(docker ps --filter "name=game-room-2" --format "{{.Names}}" | head -1)
fi

echo "Leader: $LEADER_CONTAINER ($LEADER_INSTANCE)"
echo "Follower: $FOLLOWER_CONTAINER"

# 2. Verify the follower is healthy
HEALTH=$(curl -sf "$BASE_URL/health" 2>/dev/null || echo "down")
echo "Health before failover: $HEALTH"

# 3. Record start time and kill leader
START=$(date +%s.%N)
echo "Killing leader ($LEADER_CONTAINER) at $(date)"
docker kill "$LEADER_CONTAINER" >/dev/null 2>&1

# 4. Poll etcd until a new leader is elected (must be the follower or a replacement)
RECOVERED=false
for i in $(seq 1 "$MAX_FAILOVER_SECONDS"); do
  sleep 1
  NEW_LEADER_INSTANCE=$(docker exec "$ETCD_CONTAINER" etcdctl get /match/test-match-001/leader --print-value-only 2>/dev/null || echo "")
  if [ -n "$NEW_LEADER_INSTANCE" ] && [ "$NEW_LEADER_INSTANCE" != "$LEADER_INSTANCE" ]; then
    END=$(date +%s.%N)
    RECOVERED=true
    DURATION=$(python3 -c "print(f'{max(0, $END - $START):.2f}')" 2>/dev/null)
    echo "New leader elected: $NEW_LEADER_INSTANCE after ${DURATION}s"
    break
  fi
  echo "  waiting for new leader... (${i}s)"
done

if [ "$RECOVERED" != "true" ]; then
  END=$(date +%s.%N)
  DURATION=$(python3 -c "print(f'{max(0, $END - $START):.2f}')" 2>/dev/null)
  echo "FAIL: No new leader elected within ${MAX_FAILOVER_SECONDS}s (took ${DURATION}s)"
  exit 1
fi

# 5. Verify failover within SLA
SLA_OK=$(python3 -c "print('true' if float('$DURATION') <= $MAX_FAILOVER_SECONDS else 'false')" 2>/dev/null)
if [ "$SLA_OK" != "true" ]; then
  echo "FAIL: Failover took ${DURATION}s which exceeds SLA of ${MAX_FAILOVER_SECONDS}s"
  exit 1
fi

echo ""
echo "PASS: Failover completed in ${DURATION}s (SLA: ${MAX_FAILOVER_SECONDS}s)"
