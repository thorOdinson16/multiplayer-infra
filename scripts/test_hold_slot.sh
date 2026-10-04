#!/bin/bash
# Hold-slot restoration test: verify player state survives short disconnect
set -e

BASE_URL="${BASE_URL:-http://localhost:8080}"
WS_URL="${WS_URL:-ws://localhost:8080/ws}"
echo "=== Hold Slot Restoration Test ==="
echo ""

# 1. Register/login test user
USER="holdtest-$(date +%s)"
RESP=$(curl -sk -X POST "$BASE_URL/auth/register" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"$USER\",\"password\":\"pass123\"}" 2>/dev/null || echo "{}")
TOKEN=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null)

if [ -z "$TOKEN" ]; then
  RESP=$(curl -sk -X POST "$BASE_URL/auth/login" \
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
# Right after a failover the gateway can briefly route to a follower (the route map is
# rewritten and NGINX reloaded within a few seconds), so retry until a leader accepts us.
RESULT="null"
for attempt in $(seq 1 20); do
  RESULT=$(python3 scripts/test_ws_helpers.py connect_and_disconnect "$WS_URL" "$TOKEN" 2>/dev/null || echo "null")
  case "$RESULT" in *null*) sleep 2 ;; *) break ;; esac
done
echo "WS result: $RESULT"

if [ "$RESULT" = "null" ]; then
  echo "FAIL: WebSocket interaction failed"
  exit 1
fi

if ! echo "$RESULT" | python3 -c "
import sys, json, base64
first, second = json.loads(sys.stdin.read())
token = '$TOKEN'
payload = token.split('.')[1]
payload += '=' * (-len(payload) % 4)
pid = json.loads(base64.urlsafe_b64decode(payload))['sub']
if not first or not second:
    print('missing state: first=%s second=%s' % (bool(first), bool(second))); sys.exit(1)
a, b = first['players'][pid], second['players'][pid]
print('before disconnect: x=%s y=%s' % (a['x'], a['y']))
print('after reconnect:   x=%s y=%s' % (b['x'], b['y']))
# The player moved away from spawn, sent no input after reconnecting, and must be back where they left.
if not b['connected'] or (a['x'], a['y']) != (b['x'], b['y']) or (b['x'], b['y']) == (0.0, 0.0):
    print('position not restored'); sys.exit(1)
"; then
  echo "FAIL: Player state not restored on reconnect"
  exit 1
fi
echo "PASS: Player state restored after disconnect (hold slot working)"
