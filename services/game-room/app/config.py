import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # etcd
    etcd_host: str = "localhost"
    etcd_port: int = 2379

    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_events: str = "match.events"
    kafka_topic_lifecycle: str = "match.lifecycle"
    kafka_topic_telemetry: str = "match.telemetry"

    # Couchbase
    couchbase_host: str = "localhost"
    couchbase_username: str = "Administrator"
    couchbase_password: str = "password"
    couchbase_matches_bucket: str = "matches"

    # Game settings
    tick_rate: int = 20
    player_slot_hold_seconds: int = 30  # reconnect window

    # Auth service URL (for JWT validation)
    auth_service_url: str = "http://localhost:8000"

    class Config:
        env_file = ".env"

settings = Settings()