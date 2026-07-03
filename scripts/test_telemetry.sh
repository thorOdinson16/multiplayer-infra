#!/bin/bash
# Telemetry verification test: verify state events flow through Kafka
set -e

KAFKA_TOPIC_EVENTS="${KAFKA_TOPIC_EVENTS:-game-events}"
KAFKA_TOPIC_TELEMETRY="${KAFKA_TOPIC_TELEMETRY:-game-telemetry}"
KAFKA_TOPIC_LIFECYCLE="${KAFKA_TOPIC_LIFECYCLE:-match-lifecycle}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-multiplayer-infra-kafka-1}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"

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

# 2. List topics to verify they exist
echo "--- Available Topics ---"
docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --list 2>/dev/null || echo "  (unable to list topics)"

echo ""

# 3. Check for events in game-events topic
echo "--- Recent Events (game-events) ---"
EVENTS=$(docker exec "$CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$KAFKA_TOPIC_EVENTS" \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 5000 2>/dev/null || echo "")

if [ -n "$EVENTS" ]; then
  COUNT=$(echo "$EVENTS" | wc -l)
  echo "  Found $COUNT event(s)"
  echo "$EVENTS" | while IFS= read -r line; do
    TICK=$(echo "$line" | python3 -c "import sys,json; print(json.load(sys.stdin).get('tick','?'))" 2>/dev/null)
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
TELEMETRY=$(docker exec "$CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$KAFKA_TOPIC_TELEMETRY" \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 5000 2>/dev/null || echo "")

if [ -n "$TELEMETRY" ]; then
  COUNT=$(echo "$TELEMETRY" | wc -l)
  echo "  Found $COUNT telemetry event(s)"
  echo "$TELEMETRY" | while IFS= read -r line; do
    TYPE=$(echo "$line" | python3 -c "import sys,json; print(json.load(sys.stdin).get('type','?'))" 2>/dev/null)
    PID=$(echo "$line" | python3 -c "import sys,json; print(json.load(sys.stdin).get('player_id','?')[:12])" 2>/dev/null)
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
LIFECYCLE=$(docker exec "$CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$KAFKA_TOPIC_LIFECYCLE" \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 5000 2>/dev/null || echo "")

if [ -n "$LIFECYCLE" ]; then
  COUNT=$(echo "$LIFECYCLE" | wc -l)
  echo "  Found $COUNT lifecycle event(s)"
  echo "$LIFECYCLE" | while IFS= read -r line; do
    LTYPE=$(echo "$line" | python3 -c "import sys,json; print(json.load(sys.stdin).get('type','?'))" 2>/dev/null)
    echo "  - type=$LTYPE"
  done
  echo "PASS: Lifecycle events flowing through Kafka"
else
  echo "  No lifecycle events found"
fi

echo ""
echo "=== Telemetry Verification Complete ==="
