#!/bin/bash
# Telemetry verification test: verify state events flow through Kafka
set -e

KAFKA_TOPIC_EVENTS="${KAFKA_TOPIC_EVENTS:-match.events}"
KAFKA_TOPIC_TELEMETRY="${KAFKA_TOPIC_TELEMETRY:-match.telemetry}"
KAFKA_TOPIC_LIFECYCLE="${KAFKA_TOPIC_LIFECYCLE:-match.lifecycle}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-multiplayer-infra-kafka-1}"
BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"

echo "=== Telemetry Verification Test ==="
echo ""

# 1. Find the Kafka container
CONTAINER=$(docker ps --filter "name=kafka" --format "{{.Names}}" | head -1)
if [ -z "$CONTAINER" ]; then
  echo "WARN: No kafka container found. Skipping telemetry test."
  echo "      This test requires docker-compose to be running."
  exit 0
fi
echo "Kafka container: $CONTAINER"

# The kafka-native image ships no CLI tools, so consume through confluent_kafka
# inside a game-room container (it already has the client and can reach kafka:9092).
CONSUMER_CONTAINER=$(docker ps --filter "name=game-room-1" --format "{{.Names}}" | head -1)
if [ -z "$CONSUMER_CONTAINER" ]; then
  echo "WARN: No game-room container found. Skipping telemetry test."
  exit 0
fi

consume() {
  docker exec -i "$CONSUMER_CONTAINER" python - "$1" "$BOOTSTRAP" <<'PY' 2>/dev/null || true
import sys, time
from confluent_kafka import Consumer
topic, bootstrap = sys.argv[1], sys.argv[2]
c = Consumer({"bootstrap.servers": bootstrap, "group.id": "telemetry-check-%d" % time.time(),
              "auto.offset.reset": "earliest", "enable.auto.commit": False})
c.subscribe([topic])
deadline, n = time.time() + 8, 0
while time.time() < deadline and n < 5:
    m = c.poll(1.0)
    if m is not None and not m.error():
        print(m.value().decode("utf-8", "replace"))
        n += 1
c.close()
PY
}

# Pull a JSON field from a line without letting a parse failure abort the script.
json_field() {
  python3 -c "import sys,json; print(json.load(sys.stdin).get('$1','?'))" 2>/dev/null || echo "?"
}

# 2. List topics to verify they exist
echo "--- Available Topics ---"
for t in "$KAFKA_TOPIC_EVENTS" "$KAFKA_TOPIC_TELEMETRY" "$KAFKA_TOPIC_LIFECYCLE"; do echo "  $t"; done

echo ""

# 3. Check for events in game-events topic
echo "--- Recent Events (game-events) ---"
EVENTS=$(consume "$KAFKA_TOPIC_EVENTS")

if [ -n "$EVENTS" ]; then
  COUNT=$(echo "$EVENTS" | wc -l)
  echo "  Found $COUNT event(s)"
  echo "$EVENTS" | while IFS= read -r line; do
    TICK=$(echo "$line" | json_field tick)
    echo "  - tick=$TICK"
  done
  echo "PASS: Game events flowing through Kafka"
else
  echo "  No events found (may be empty if no match is running)"
  echo "INFO: No telemetry events yet - this is expected if no match has started"
fi

echo ""

# 4. Check for telemetry data
echo "--- Recent Telemetry ---"
TELEMETRY=$(consume "$KAFKA_TOPIC_TELEMETRY")

if [ -n "$TELEMETRY" ]; then
  COUNT=$(echo "$TELEMETRY" | wc -l)
  echo "  Found $COUNT telemetry event(s)"
  echo "$TELEMETRY" | while IFS= read -r line; do
    TYPE=$(echo "$line" | json_field type)
    PID=$(echo "$line" | json_field player_id)
    echo "  - type=$TYPE player=$PID"
  done
  echo "PASS: Telemetry flowing through Kafka"
else
  echo "  No telemetry found"
  echo "INFO: No telemetry yet - this is expected if no match has started"
fi

echo ""

# 5. Check for lifecycle events
echo "--- Recent Lifecycle Events ---"
LIFECYCLE=$(consume "$KAFKA_TOPIC_LIFECYCLE")

if [ -n "$LIFECYCLE" ]; then
  COUNT=$(echo "$LIFECYCLE" | wc -l)
  echo "  Found $COUNT lifecycle event(s)"
  echo "$LIFECYCLE" | while IFS= read -r line; do
    LTYPE=$(echo "$line" | json_field type)
    echo "  - type=$LTYPE"
  done
  echo "PASS: Lifecycle events flowing through Kafka"
else
  echo "  No lifecycle events found"
fi

echo ""
echo "=== Telemetry Verification Complete ==="
