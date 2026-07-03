import time


class SpectatorRingBuffer:
    def __init__(self, max_size=600, delay_ticks=200):
        self.buffer = []
        self.max_size = max_size
        self.delay_ticks = delay_ticks

    def append(self, tick, state):
        self.buffer.append({"tick": tick, "state": state, "timestamp": time.time()})
        if len(self.buffer) > self.max_size:
            self.buffer.pop(0)

    def get_delayed_state(self, current_tick):
        target_tick = current_tick - self.delay_ticks
        for entry in reversed(self.buffer):
            if entry["tick"] <= target_tick:
                return entry["state"]
        return self.buffer[0]["state"] if self.buffer else None
