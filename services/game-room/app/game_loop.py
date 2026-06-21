"""Tick-loop and WebSocket handler stubs."""
import asyncio

class GameLoop:
    def __init__(self, tick_rate=20):
        self.tick_rate = tick_rate

    async def run(self):
        while True:
            # TODO: process inputs, advance state, broadcast, commit to Kafka/Redis
            await asyncio.sleep(1 / self.tick_rate)
