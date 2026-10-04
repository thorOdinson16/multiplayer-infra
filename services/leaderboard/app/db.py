"""Postgres access for the leaderboard."""
import logging
import os
import threading

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .elo import new_average, new_ratings

logger = logging.getLogger("leaderboard")

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://game:game@localhost:5432/game")

_pool = None
_pool_lock = threading.Lock()

STAT_COLUMNS = (
    'player_id AS "playerId", username, elo_rating AS "eloRating", wins, losses, '
    'total_matches AS "totalMatches", average_score AS "averageScore"'
)


def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=10,
                                      kwargs={"row_factory": dict_row}, open=False)
                pool.open(wait=True, timeout=30)
                _pool = pool
    return _pool


def ping():
    with get_pool().connection() as conn:
        conn.execute("SELECT 1")


def apply_match(event: dict) -> str:
    """Apply a match.end event to player stats and Elo in one transaction.

    Returns "applied" or "duplicate". Recording the match in processed_matches
    inside the same transaction makes a redelivered event a no-op, and locking
    the player rows (in a fixed order) prevents lost updates between matches.
    """
    outcome = event.get("outcome") or {}
    scores = outcome.get("scores") or {}
    winner = outcome.get("winner")
    player_ids = sorted(set(event.get("players") or []))
    match_id = event["match_id"]
    started_at = float(event.get("started_at") or 0)

    with get_pool().connection() as conn:
        cur = conn.execute(
            "INSERT INTO processed_matches (match_id, started_at) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (match_id, started_at),
        )
        if cur.rowcount == 0:
            return "duplicate"
        if not player_ids:
            return "applied"
        rows = conn.execute(
            "SELECT player_id, elo_rating, wins, losses, total_matches, average_score "
            "FROM players WHERE player_id = ANY(%s) ORDER BY player_id FOR UPDATE",
            (player_ids,),
        ).fetchall()
        known = {r["player_id"]: r for r in rows}
        for missing in set(player_ids) - set(known):
            logger.warning(f"Match {match_id}: player {missing} not found, skipping")
        ratings = new_ratings({pid: r["elo_rating"] for pid, r in known.items()}, scores)
        for pid, row in known.items():
            won = winner == pid
            conn.execute(
                "UPDATE players SET elo_rating=%s, wins=%s, losses=%s, total_matches=%s, "
                "average_score=%s, last_seen=now() WHERE player_id=%s",
                (
                    ratings[pid],
                    row["wins"] + (1 if won else 0),
                    row["losses"] + (0 if won else 1),
                    row["total_matches"] + 1,
                    new_average(row["average_score"], row["total_matches"], scores.get(pid, 0)),
                    pid,
                ),
            )
    return "applied"


def top_players(window: str, limit: int) -> list:
    days = {"daily": 1, "weekly": 7}.get(window)
    sql = f"SELECT {STAT_COLUMNS} FROM players"
    params: list = []
    if days:
        sql += " WHERE last_seen >= now() - make_interval(days => %s)"
        params.append(days)
    sql += " ORDER BY elo_rating DESC, username LIMIT %s"
    params.append(limit)
    with get_pool().connection() as conn:
        return conn.execute(sql, params).fetchall()


def player_with_rank(player_id: str):
    with get_pool().connection() as conn:
        player = conn.execute(f"SELECT {STAT_COLUMNS} FROM players WHERE player_id = %s", (player_id,)).fetchone()
        if not player:
            return None, None
        higher = conn.execute("SELECT COUNT(*) AS r FROM players WHERE elo_rating > %s", (player["eloRating"],)).fetchone()
        return player, higher["r"] + 1
