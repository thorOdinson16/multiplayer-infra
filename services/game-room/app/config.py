from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

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

    # Postgres (match records, replay checkpoints)
    database_url: str = "postgresql://game:game@localhost:5432/game"

    # Game settings
    tick_rate: int = 20
    match_duration_seconds: int = 300
    player_slot_hold_seconds: int = 30  # reconnect window

    # Auth service URL (for JWT validation)
    auth_service_url: str = "http://localhost:8000"

    # Stable, resolvable address published to etcd as the leader address.
    # Overridden per replica via the ROOM_ADDRESS environment variable.
    room_address: str = ""

settings = Settings()