import asyncio
import time
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
        async with self.lock:
            lobbies = []
            now = time.time()
            self.pending.sort(key=lambda r: r.elo)
            used = set()
            for i, req in enumerate(self.pending):
                if i in used:
                    continue
                lobby = [req]
                used.add(i)
                for j in range(i + 1, len(self.pending)):
                    if j in used:
                        continue
                    other = self.pending[j]
                    age = now - req.timestamp
                    time_windows = int(age // settings.match_timeout_seconds)
                    range_allowed = settings.elo_range_initial + time_windows * settings.elo_range_expand_per_tick
                    if abs(req.elo - other.elo) <= range_allowed:
                        lobby.append(other)
                        used.add(j)
                        if len(lobby) >= settings.lobby_size_max:
                            break
                if len(lobby) >= settings.lobby_size_min:
                    lobbies.append(lobby)
                else:
                    used.discard(i)
                    for m in lobby[1:]:
                        used.discard(self.pending.index(m))
            self.pending = [r for idx, r in enumerate(self.pending) if idx not in used]
            return lobbies
