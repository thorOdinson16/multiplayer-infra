"""Postgres access for match records and replay checkpoints.

Calls are synchronous; the game loop invokes them through asyncio.to_thread so
they never block the event loop. One shared pool replaces the per-call cluster
connections used before.
"""
import threading

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .config import settings

_pool = None
_lock = threading.Lock()


def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        with _lock:
            if _pool is None:
                pool = ConnectionPool(settings.database_url, min_size=1, max_size=5,
                                      kwargs={"row_factory": dict_row}, open=False)
                pool.open(wait=True, timeout=10)
                _pool = pool
    return _pool


def load_latest_checkpoint(match_id: str):
    """Return (tick, events) of the newest replay checkpoint, or None."""
    with get_pool().connection() as conn:
        row = conn.execute(
            "SELECT tick, events FROM replay_checkpoints WHERE match_id = %s ORDER BY tick DESC LIMIT 1",
            (match_id,),
        ).fetchone()
    return (row["tick"], row["events"]) if row else None


def save_match(match_id: str, started_at: float, ended_at: float, duration: float, players: list, outcome: dict):
    with get_pool().connection() as conn:
        conn.execute(
            "INSERT INTO matches (match_id, started_at, ended_at, duration_seconds, players, outcome) "
            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (match_id, started_at) DO NOTHING",
            (match_id, started_at, ended_at, duration, Jsonb(players), Jsonb(outcome)),
        )


def close():
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
