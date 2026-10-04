-- Schema for the platform database. Idempotent: safe to run on every start.

CREATE TABLE IF NOT EXISTS players (
    player_id     TEXT PRIMARY KEY,
    username      TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    elo_rating    INTEGER NOT NULL DEFAULT 1200,
    wins          INTEGER NOT NULL DEFAULT 0,
    losses        INTEGER NOT NULL DEFAULT 0,
    total_matches INTEGER NOT NULL DEFAULT 0,
    average_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_players_elo ON players (elo_rating DESC);
CREATE INDEX IF NOT EXISTS idx_players_last_seen ON players (last_seen);

-- Room ids are reused across matches, so a match is keyed by (room, start time).
CREATE TABLE IF NOT EXISTS matches (
    match_id         TEXT NOT NULL,
    started_at       DOUBLE PRECISION NOT NULL,
    ended_at         DOUBLE PRECISION NOT NULL,
    duration_seconds DOUBLE PRECISION NOT NULL,
    players          JSONB NOT NULL,
    outcome          JSONB NOT NULL,
    PRIMARY KEY (match_id, started_at)
);

-- Ensures a redelivered match.end event is applied to ratings only once.
CREATE TABLE IF NOT EXISTS processed_matches (
    match_id     TEXT NOT NULL,
    started_at   DOUBLE PRECISION NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (match_id, started_at)
);

CREATE TABLE IF NOT EXISTS replay_checkpoints (
    match_id   TEXT NOT NULL,
    tick       INTEGER NOT NULL,
    events     JSONB NOT NULL,
    snapshot   JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (match_id, tick)
);
