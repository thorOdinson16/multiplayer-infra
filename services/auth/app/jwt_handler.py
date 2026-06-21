from datetime import datetime, timedelta, timezone
import jwt
from .config import settings

# Load keys once
with open(settings.jwt_private_key_path, "rb") as f:
    _private_key = f.read()
with open(settings.jwt_public_key_path, "rb") as f:
    _public_key = f.read()

def create_token(player_id: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": player_id,
        "iat": now,
        "exp": now + timedelta(minutes=settings.jwt_expire_minutes),
    }
    return jwt.encode(payload, _private_key, algorithm=settings.jwt_algorithm)

def decode_token(token: str) -> dict:
    return jwt.decode(token, _public_key, algorithms=[settings.jwt_algorithm])