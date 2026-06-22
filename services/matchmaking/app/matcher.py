import asyncio
import time
from collections import defaultdict
from .config import settings
from .models import MatchRequest

class Matcher:
    def __init__(self):
        self.pending: list[MatchRequest] = []
        self.lock = asyncio.Lock()

    async def add_request(self, req: MatchRequest):
        async with self.lock:
            self.pending.append(req)

    async def match_tick(self) -> list[list[MatchRequest]]:
        """Called periodically. Returns list of formed lobbies."""
        async with self.lock:
            lobbies = []
            now = time.time()
            # Group by Elo range windows (simple approach: sort by elo, pick close ones)
            # For simplicity, sort by elo and group adjacent within range after time window.
            self.pending.sort(key=lambda r: r.elo)
            used = set()
            for i, req in enumerate(self.pending):
                if i in used:
                    continue
                lobby = [req]
                for j in range(i+1, len(self.pending)):
                    if j in used:
                        continue
                    other = self.pending[j]
                    # Check time: if request is older, use expanded range
                    age = now - req.timestamp
                    time_windows = age // settings.match_timeout_seconds
                    range_allowed = settings.elo_range_initial + time_windows * settings.elo_range_expand_per_tick
                    if abs(req.elo - other.elo) <= range_allowed:
                        lobby.append(other)
                        used.add(j)
                        if len(lobby) >= settings.lobby_size_max:
                            break
                if len(lobby) >= settings.lobby_size_min:
                    for item in lobby:
                        used.add(self.pending.index(item))
                    lobbies.append(lobby)
            # Remove used
            self.pending = [r for idx, r in enumerate(self.pending) if idx not in used]
            return lobbies