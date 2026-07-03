#!/bin/bash
# Failover test: kill the game-room leader and verify another replica takes over
set -e

echo "=== Failover Test ==="
echo ""

# 1. Identify the etcd container and leader
ETCD_CONTAINER=$(docker ps --filter "label=com.docker.compose.service=etcd" --format "{{.Names}}" | head -1)
if [ -z "$ETCD_CONTAINER" ]; then
  echo "ERROR: No etcd container found. Is docker-compose running?"
  exit 1
fi
LEADER_INSTANCE=$(docker exec "$ETCD_CONTAINER" etcdctl get /match/test-match-001/leader --print-value-only 2>/dev/null || echo "")
if [ -z "$LEADER_INSTANCE" ]; then
  echo "ERROR: No leader found in etcd. Is the game-room running?"
  exit 1
fi
echo "Leader instance: $LEADER_INSTANCE"

LEADER_CONTAINER=$(docker ps --filter "label=com.docker.compose.service=game-room" --format "{{.Names}}" | head -1)
if docker ps --filter "name=game-room-2" --format "{{.Names}}" | grep -q .; then
  SECOND_CONTAINER=$(docker ps --filter "name=game-room-2" --format "{{.Names}}" | head -1)
fi

# If leader is room-2 (second replica), swap
if [ "$LEADER_INSTANCE" = "room-2" ] && [ -n "$SECOND_CONTAINER" ]; then
  LEADER_CONTAINER="$SECOND_CONTAINER"
elif [ "$LEADER_INSTANCE" = "room-2" ]; then
  echo "WARN: leader is room-2 but no game-room-2 container found"
fi

if [ -z "$LEADER_CONTAINER" ]; then
  echo "ERROR: Could not determine leader container"
  exit 1
fi

echo "Leader container: $LEADER_CONTAINER"
echo ""

# 2. Check match status before failover
echo "--- Before Failover ---"
curl -sf http://localhost:8003/health 2>/dev/null && echo "  Game room healthy"
if [ -n "$SECOND_CONTAINER" ]; then
  curl -sf http://localhost:8009/health 2>/dev/null && echo "  Game room 2 healthy"
fi

# 3. Kill the leader
echo "--- Killing leader ---"
docker kill "$LEADER_CONTAINER" 2>/dev/null || true
echo "  Killed $LEADER_CONTAINER"
echo ""

# 4. Wait for takeover by remaining replica
echo "--- Waiting for failover ---"
for i in $(seq 1 30); do
  NEW_LEADER=$(docker exec "$ETCD_CONTAINER" etcdctl get /match/test-match-001/leader --print-value-only 2>/dev/null || echo "")
  if [ -n "$NEW_LEADER" ] && [ "$NEW_LEADER" != "$LEADER_INSTANCE" ]; then
    echo "  New leader: $NEW_LEADER (after ${i}s, etcd=$ETCD_CONTAINER)"
    break
  fi
  sleep 1
done

# 5. Check health of remaining replica
echo ""
echo "--- After Failover ---"
for i in $(seq 1 10); do
  STATUS=$(curl -sf -o /dev/null -w "%{http_code}" http://localhost:8003/health 2>/dev/null || echo "down")
  if [ "$STATUS" = "200" ]; then
    echo "  Game room recovered (status: $STATUS)"
    break
  fi
  STATUS2=$(curl -sf -o /dev/null -w "%{http_code}" http://localhost:8009/health 2>/dev/null || echo "down")
  if [ "$STATUS2" = "200" ]; then
    echo "  Game room 2 recovered (status: $STATUS2)"
    break
  fi
  echo "  Waiting for recovery... ($STATUS / $STATUS2)"
  sleep 2
done

echo ""
echo "=== Failover Test Complete ==="
