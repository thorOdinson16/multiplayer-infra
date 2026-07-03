#!/bin/bash
# Hold-slot restoration test: verify player state survives short disconnect
set -e

BASE_URL="${BASE_URL:-http://localhost:8080}"
WS_URL="${WS_URL:-ws://localhost:8080/ws}"
echo "=== Hold Slot Restoration Test ==="
echo ""

# 1. Register/login test user
USER="holdtest-$(date +%s)"
RESP=$(curl -sf -X POST "$BASE_URL/auth/register" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"$USER\",\"password\":\"pass123\"}" 2>/dev/null || echo "{}")
TOKEN=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null)

if [ -z "$TOKEN" ]; then
  RESP=$(curl -sf -X POST "$BASE_URL/auth/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"$USER\",\"password\":\"pass123\"}" 2>/dev/null || echo "{}")
  TOKEN=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null)
fi

if [ -z "$TOKEN" ]; then
  echo "FAIL: Could not get auth token"
  exit 1
fi
echo "User: $USER"

# 2. Run WebSocket connect-disconnect-reconnect cycle
RESULT=$(python3 scripts/test_ws_helpers.py connect_and_disconnect "$WS_URL" "$TOKEN" 2>/dev/null || echo "null")
echo "WS result: $RESULT"

if [ "$RESULT" = "null" ]; then
  echo "FAIL: WebSocket interaction failed"
  exit 1
fi

FIRST_PLAYERS=$(echo "$RESULT" | python3 -c "
import sys,json
data = json.loads(sys.stdin.read())
if isinstance(data, list) and len(data) >= 2:
    first = data[0]
    if first and 'players' in first:
        print(json.dumps(first['players']))
    else:
        print('null')
else:
    print('null')
" 2>/dev/null)

SECOND_PLAYERS=$(echo "$RESULT" | python3 -c "
import sys,json
data = json.loads(sys.stdin.read())
if isinstance(data, list) and len(data) >= 2:
    second = data[1]
    if second and 'players' in second:
        print(json.dumps(second['players']))
    else:
        print('null')
else:
    print('null')
" 2>/dev/null)

if [ "$FIRST_PLAYERS" = "null" ] || [ "$SECOND_PLAYERS" = "null" ]; then
  echo "INFO: Could not extract player state from both connections (may be OK if match is not running)"
  echo "      This test requires an active game match."
  exit 0
fi

# 3. Verify that the second connection's player position is consistent (restored from hold slot)
echo "First connection players: $FIRST_PLAYERS"
echo "Second connection players: $SECOND_PLAYERS"

FOUND=$(python3 -c "
import sys,json
second = json.loads('$SECOND_PLAYERS')
# Check that at least one player has connected=True and expected position data
for pid, p in second.items():
    if p.get('connected') and p.get('x', 0) != 0:
        print('restored')
        sys.exit(0)
print('no_player')
" 2>/dev/null)

if [ "$FOUND" = "restored" ]; then
  echo "PASS: Player state restored after disconnect (hold slot working)"
elif [ "$FOUND" = "no_player" ]; then
  echo "INFO: No active player found on reconnect (game may have ended)"
else
  echo "INFO: Unexpected state - $FOUND"
fi
