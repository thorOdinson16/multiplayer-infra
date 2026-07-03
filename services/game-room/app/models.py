from dataclasses import dataclass, field
from typing import Dict
import time

@dataclass
class PlayerState:
    player_id: str
    x: float = 0.0
    y: float = 0.0
    health: int = 100
    score: int = 0
    connected: bool = True

@dataclass
class GameState:
    match_id: str
    players: Dict[str, PlayerState] = field(default_factory=dict)
    tick: int = 0
    started_at: float = field(default_factory=time.time)

    def to_dict(self):
        return {
            "match_id": self.match_id,
            "tick": self.tick,
            "players": {
                pid: {
                    "x": p.x,
                    "y": p.y,
                    "health": p.health,
                    "score": p.score,
                    "connected": p.connected,
                }
                for pid, p in self.players.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict) -> "GameState":
        gs = cls(match_id=data["match_id"], tick=data.get("tick", 0))
        for pid, pdata in data.get("players", {}).items():
            gs.players[pid] = PlayerState(
                player_id=pid,
                x=pdata["x"],
                y=pdata["y"],
                health=pdata["health"],
                score=pdata["score"],
                connected=pdata.get("connected", True),
            )
        return gs