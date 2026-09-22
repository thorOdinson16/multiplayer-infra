from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    auth_service_url: str = "http://localhost:8000"
    etcd_host: str = "localhost"
    etcd_port: int = 2379
    match_timeout_seconds: int = 30  # base window
    lobby_size_min: int = 2
    lobby_size_max: int = 8
    elo_range_initial: int = 200
    elo_range_expand_per_tick: int = 50
    tick_interval_seconds: int = 2

settings = Settings()