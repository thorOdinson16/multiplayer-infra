"""Postgres access for players and Redis access for sessions."""
import json
import threading

import psycopg
import redis
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import settings

_pool = None
_pool_lock = threading.Lock()
_redis = None

PLAYER_COLUMNS = (
    'player_id AS "playerId", username, password_hash AS "passwordHash", '
    'elo_rating AS "eloRating", wins, losses, total_matches AS "totalMatches", '
    'average_score AS "averageScore"'
)


class UsernameTaken(Exception):
    pass


def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                pool = ConnectionPool(
                    settings.database_url, min_size=1, max_size=10,
                    kwargs={"row_factory": dict_row}, open=False,
                )
                pool.open(wait=True, timeout=30)
                _pool = pool
    return _pool


def get_redis():
    global _redis
    if _redis is None:
        _redis = redis.Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
    return _redis


def ping():
    with get_pool().connection() as conn:
        conn.execute("SELECT 1")
    get_redis().ping()


# ---------- players (Postgres) ----------
def create_player(player_id: str, username: str, password_hash: str):
    """Insert a player; the UNIQUE constraint makes concurrent duplicate registrations fail."""
    try:
        with get_pool().connection() as conn:
            conn.execute(
                "INSERT INTO players (player_id, username, password_hash) VALUES (%s, %s, %s)",
                (player_id, username, password_hash),
            )
    except psycopg.errors.UniqueViolation:
        raise UsernameTaken(username)


def get_player(player_id: str) -> dict | None:
    with get_pool().connection() as conn:
        return conn.execute(f"SELECT {PLAYER_COLUMNS} FROM players WHERE player_id = %s", (player_id,)).fetchone()


def get_player_by_username(username: str) -> dict | None:
    with get_pool().connection() as conn:
        return conn.execute(f"SELECT {PLAYER_COLUMNS} FROM players WHERE username = %s", (username,)).fetchone()


# ---------- sessions (Redis, TTL-based) ----------
def store_session(session_id: str, session_doc: dict, ttl_seconds: int):
    get_redis().set(f"session:{session_id}", json.dumps(session_doc), ex=ttl_seconds)


def store_player_session(player_id: str, session_id: str, session_doc: dict, ttl_seconds: int):
    """Index the player's current session so tokens can be revoked."""
    doc = {
        "type": "player_session",
        "playerId": player_id,
        "sessionId": session_id,
        "token": session_doc.get("token"),
        "expiresAt": session_doc.get("expiresAt"),
    }
    get_redis().set(f"player_session:{player_id}", json.dumps(doc), ex=ttl_seconds)


def get_player_session(player_id: str) -> dict | None:
    raw = get_redis().get(f"player_session:{player_id}")
    return json.loads(raw) if raw else None


def delete_player_session(player_id: str):
    session = get_player_session(player_id)
    keys = [f"player_session:{player_id}"]
    if session and session.get("sessionId"):
        keys.append(f"session:{session['sessionId']}")
    get_redis().delete(*keys)


def close_connections():
    global _pool, _redis
    if _pool is not None:
        _pool.close()
        _pool = None
    if _redis is not None:
        _redis.close()
        _redis = None
