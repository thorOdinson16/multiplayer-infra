"""Pure Elo / stats maths for the leaderboard (no I/O, easy to test)."""

K_FACTOR = 32


def expected(rating_a: float, rating_b: float) -> float:
    return 1.0 / (1.0 + 10 ** ((rating_b - rating_a) / 400.0))


def new_ratings(ratings: dict, scores: dict, k: float = K_FACTOR) -> dict:
    """Return updated Elo ratings for a match with any number of players.

    Every pair of players is treated as one head-to-head game decided by score
    (higher wins, equal is a draw). Each player's change is the average over
    their opponents, so a big lobby does not swing ratings more than a duel.
    """
    ids = list(ratings)
    if len(ids) < 2:
        return dict(ratings)
    result = {}
    for a in ids:
        delta = 0.0
        for b in ids:
            if a == b:
                continue
            sa, sb = scores.get(a, 0), scores.get(b, 0)
            actual = 1.0 if sa > sb else 0.0 if sa < sb else 0.5
            delta += actual - expected(ratings[a], ratings[b])
        result[a] = round(ratings[a] + k * delta / (len(ids) - 1))
    return result


def new_average(old_avg: float, old_total: int, score: float) -> float:
    return (old_avg * old_total + score) / (old_total + 1)
