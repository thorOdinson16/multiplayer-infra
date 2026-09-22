"""Tests for the spectator ring buffer."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.spectator_buffer import SpectatorRingBuffer


class TestSpectatorRingBuffer(unittest.TestCase):
    def test_delayed_state_returns_old_entry(self):
        buf = SpectatorRingBuffer(max_size=10, delay_ticks=3)
        for tick in range(1, 6):
            buf.append(tick, {"tick": tick})
        # current tick 5, delay 3 -> target tick 2
        self.assertEqual(buf.get_delayed_state(5), {"tick": 2})

    def test_returns_oldest_before_delay_elapses(self):
        buf = SpectatorRingBuffer(max_size=10, delay_ticks=100)
        buf.append(1, {"tick": 1})
        buf.append(2, {"tick": 2})
        self.assertEqual(buf.get_delayed_state(2), {"tick": 1})

    def test_eviction_respects_max_size(self):
        buf = SpectatorRingBuffer(max_size=3, delay_ticks=0)
        for tick in range(1, 6):
            buf.append(tick, {"tick": tick})
        self.assertEqual(len(buf.buffer), 3)
        self.assertEqual(buf.buffer[0]["tick"], 3)

    def test_empty_buffer_returns_none(self):
        buf = SpectatorRingBuffer()
        self.assertIsNone(buf.get_delayed_state(10))


if __name__ == "__main__":
    unittest.main()
