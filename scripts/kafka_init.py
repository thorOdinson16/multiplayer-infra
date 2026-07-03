#!/usr/bin/env python3
"""Create Kafka topics required by the platform."""
import json
import time
import os
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

TOPICS = {
    "match.events": {"partitions": 3, "replication_factor": 1},
    "match.telemetry": {"partitions": 3, "replication_factor": 1},
    "match.lifecycle": {"partitions": 3, "replication_factor": 1},
}

def main():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    # Wait for Kafka to be ready
    for i in range(30):
        try:
            metadata = admin.list_topics(timeout=5)
            print(f"Kafka ready, broker count: {len(metadata.brokers)}")
            break
        except Exception as e:
            print(f"Waiting for Kafka ({i}/30): {e}")
            time.sleep(2)
    else:
        raise RuntimeError("Kafka did not become ready")

    existing = admin.list_topics().topics
    new_topics = []
    for name, cfg in TOPICS.items():
        if name in existing:
            print(f"Topic '{name}' already exists")
        else:
            new_topics.append(NewTopic(name, num_partitions=cfg["partitions"], replication_factor=cfg["replication_factor"]))
            print(f"Creating topic '{name}'")

    if new_topics:
        futures = admin.create_topics(new_topics)
        for name, future in futures.items():
            future.result()  # will raise if error
            print(f"  Created topic '{name}'")

    print("Kafka topics initialised successfully")

if __name__ == "__main__":
    main()
