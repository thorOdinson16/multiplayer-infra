"""Tests for the Elo matcher (grouping + request expiry)."""
import asyncio
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.matcher import Matcher
from app.models import MatchRequest


class TestMatcher(unittest.TestCase):
    def test_groups_nearby_players(self):
        matcher = Matcher()

        async def run():
            for i in range(3):
                await matcher.add_request(MatchRequest(player_id=f"p{i}", elo=1200))
            return await matcher.match_tick()

        lobbies = asyncio.run(run())
        self.assertEqual(len(lobbies), 1)
        self.assertEqual(len(lobbies[0]), 3)

    def test_single_player_does_not_form_lobby(self):
        matcher = Matcher()

        async def run():
            await matcher.add_request(MatchRequest(player_id="solo", elo=1000))
            return await matcher.match_tick()

        self.assertEqual(asyncio.run(run()), [])

    def test_expire_requests(self):
        matcher = Matcher()

        async def run():
            old = MatchRequest(player_id="old", elo=1200, timestamp=time.time() - 100)
            fresh = MatchRequest(player_id="fresh", elo=1200)
            await matcher.add_request(old)
            await matcher.add_request(fresh)
            return await matcher.expire_requests(60)

        expired = asyncio.run(run())
        self.assertEqual([r.player_id for r in expired], ["old"])


if __name__ == "__main__":
    unittest.main()
