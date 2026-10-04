import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Postgres (players) and Redis (sessions)
    database_url: str = "postgresql://game:game@localhost:5432/game"
    redis_host: str = "localhost"
    redis_port: int = 6379

    # JWT
    jwt_private_key_path: str = os.environ.get("JWT_PRIVATE_KEY_PATH", "private.pem")
    jwt_public_key_path: str = os.environ.get("JWT_PUBLIC_KEY_PATH", "public.pem")
    jwt_algorithm: str = "RS256"
    jwt_expire_minutes: int = 24 * 60  # 24 hours

settings = Settings()
