import time
from dataclasses import dataclass, field

@dataclass
class MatchRequest:
    player_id: str
    elo: int
    timestamp: float = field(default_factory=time.time)