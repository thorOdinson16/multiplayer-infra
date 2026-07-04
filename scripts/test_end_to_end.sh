#!/bin/bash
# End-to-end test script for multiplayer infra
# Assumes docker-compose is running

set -e

BASE_URL="http://localhost:8080"
GAME_URL="http://localhost:3000"

echo "=== Multiplayer Infra E2E Test ==="
echo ""

# 1. Health checks
echo "--- Health Checks ---"
for svc in auth matchmaking game-room replay leaderboard analytics notification reconnect-handler; do
  status=$(curl -sfk "$BASE_URL/health" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','unknown'))" 2>/dev/null || echo "unreachable")
  echo "  $svc: $status"
done
echo ""

# 2. Register a test player
echo "--- Registration ---"
ALICE=$(curl -sfk -X POST "$BASE_URL/auth/register" \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","password":"pass123"}' 2>/dev/null)
ALICE_TOKEN=$(echo "$ALICE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null)

if [ -n "$ALICE_TOKEN" ]; then
  echo "  Alice registered: ${ALICE_TOKEN:0:20}..."
else
  # Try login instead (if already registered)
  ALICE=$(curl -sfk -X POST "$BASE_URL/auth/login" \
    -H "Content-Type: application/json" \
    -d '{"username":"alice","password":"pass123"}' 2>/dev/null)
  ALICE_TOKEN=$(echo "$ALICE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null)
  echo "  Alice logged in: ${ALICE_TOKEN:0:20}..."
fi

BOB=$(curl -sfk -X POST "$BASE_URL/auth/register" \
  -H "Content-Type: application/json" \
  -d '{"username":"bob","password":"pass123"}' 2>/dev/null)
BOB_TOKEN=$(echo "$BOB" | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null)

if [ -n "$BOB_TOKEN" ]; then
  echo "  Bob registered: ${BOB_TOKEN:0:20}..."
else
  BOB=$(curl -sfk -X POST "$BASE_URL/auth/login" \
    -H "Content-Type: application/json" \
    -d '{"username":"bob","password":"pass123"}' 2>/dev/null)
  BOB_TOKEN=$(echo "$BOB" | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null)
  echo "  Bob logged in: ${BOB_TOKEN:0:20}..."
fi
echo ""

# 3. Validate tokens
echo "--- Token Validation ---"
ALICE_VALID=$(curl -sfk "$BASE_URL/auth/validate" \
  -H "Authorization: Bearer $ALICE_TOKEN" 2>/dev/null)
echo "  Alice valid: $(echo $ALICE_VALID | python3 -c "import sys,json; print(json.load(sys.stdin).get('valid',False))" 2>/dev/null)"

BOB_VALID=$(curl -sfk "$BASE_URL/auth/validate" \
  -H "Authorization: Bearer $BOB_TOKEN" 2>/dev/null)
echo "  Bob valid: $(echo $BOB_VALID | python3 -c "import sys,json; print(json.load(sys.stdin).get('valid',False))" 2>/dev/null)"
echo ""

# 4. Get player profiles
echo "--- Player Profiles ---"
ALICE_PROFILE=$(curl -sfk "$BASE_URL/auth/validate" \
  -H "Authorization: Bearer $ALICE_TOKEN" 2>/dev/null)
ALICE_PID=$(echo "$ALICE_PROFILE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('player_id',''))" 2>/dev/null)

if [ -n "$ALICE_PID" ]; then
  PROFILE=$(curl -sfk "$BASE_URL/players/$ALICE_PID" \
    -H "Authorization: Bearer $ALICE_TOKEN" 2>/dev/null)
  echo "  Alice: $(echo $PROFILE)"
fi
echo ""

# 5. Leaderboard
echo "--- Leaderboard ---"
LB=$(curl -sfk "$BASE_URL/leaderboard" 2>/dev/null)
echo "  $LB" | python3 -m json.tool 2>/dev/null || echo "  $LB"
echo ""

# 6. Matchmaking
echo "--- Matchmaking Queue ---"
MM_RESULT=$(curl -sfk -X POST "$BASE_URL/matchmaking/queue" \
  -H "Content-Type: application/json" \
  -d "{\"token\":\"$ALICE_TOKEN\"}" 2>/dev/null)
echo "  Alice queued: $MM_RESULT"

MM_RESULT=$(curl -sfk -X POST "$BASE_URL/matchmaking/queue" \
  -H "Content-Type: application/json" \
  -d "{\"token\":\"$BOB_TOKEN\"}" 2>/dev/null)
echo "  Bob queued: $MM_RESULT"
echo ""

# 7. Reconnect handler
echo "--- Reconnect Handler ---"
RECONNECT=$(curl -sfk -X POST "$BASE_URL/reconnect" \
  -H "Content-Type: application/json" \
  -d "{\"token\":\"$ALICE_TOKEN\",\"match_id\":\"test-match-001\"}" 2>/dev/null)
echo "  Reconnect: $(echo $RECONNECT | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"tick={d.get('current_tick','?')}, leader={d.get('leader_address','?')}\")" 2>/dev/null || echo "unavailable")"
echo ""

# 8. Replay (if available)
echo "--- Replay (if any matches ended) ---"
REPLAY=$(curl -sfk "$BASE_URL/replay/test-match-001" 2>/dev/null || echo "{}")
echo "  Replay: $(echo $REPLAY | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'{d.get(\"event_count\",0)} events, from tick {d.get(\"start_tick\",\"?\")}' )" 2>/dev/null || echo "not found")"
echo ""

# 9. Metrics
echo "--- Prometheus Metrics (sample) ---"
curl -sfk "$BASE_URL/metrics" 2>/dev/null | grep -E "^(auth_|matchmaking_|gameroom_|analytics_|leaderboard_)" | head -20 || echo "  No custom metrics found"
echo ""

# 10. Grafana
echo "--- Grafana ---"
GF_STATUS=$(curl -sf -o /dev/null -w "%{http_code}" http://localhost:3001 2>/dev/null || echo "unreachable")
echo "  Grafana UI: http://localhost:3001 (status: $GF_STATUS)"
echo ""

# 11. Game Client
echo "--- Game Client ---"
GC_STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "$GAME_URL" 2>/dev/null || echo "unreachable")
echo "  Game Client: $GAME_URL (status: $GC_STATUS)"
echo ""

# 12. Jaeger
echo "--- Jaeger ---"
J_STATUS=$(curl -sf -o /dev/null -w "%{http_code}" http://localhost:16686 2>/dev/null || echo "unreachable")
echo "  Jaeger UI: http://localhost:16686 (status: $J_STATUS)"
echo ""

echo "=== Test Complete ==="
echo ""
echo "Open these in your browser:"
echo "  Game Client:    http://localhost:3000"
echo "  Grafana:        http://localhost:3001 (admin/admin)"
echo "  Jaeger:         http://localhost:16686"
echo "  RabbitMQ:       http://localhost:15672 (guest/guest)"
echo "  Couchbase:      http://localhost:8091 (Administrator/password)"
echo "  MinIO Console:  http://localhost:9001 (minioadmin/minioadmin)"
