import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Couchbase
    couchbase_host: str = "localhost"
    couchbase_sessions_bucket: str = "sessions"
    couchbase_players_bucket: str = "players"
    couchbase_username: str = "Administrator"
    couchbase_password: str = "password"

    # JWT
    jwt_private_key_path: str = "private.pem"
    jwt_public_key_path: str = "public.pem"
    jwt_algorithm: str = "RS256"
    jwt_expire_minutes: int = 24 * 60  # 24 hours

    class Config:
        env_file = ".env"

settings = Settings()