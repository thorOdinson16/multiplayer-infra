"""Tests for the Elo maths."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.elo import expected, new_ratings, new_average


class EloTests(unittest.TestCase):
    def test_equal_ratings_expect_half(self):
        self.assertAlmostEqual(expected(1200, 1200), 0.5)

    def test_winner_gains_loser_loses_equally(self):
        out = new_ratings({"a": 1200, "b": 1200}, {"a": 10, "b": 5})
        self.assertEqual(out, {"a": 1216, "b": 1184})

    def test_upset_moves_more_than_expected_win(self):
        upset = new_ratings({"low": 1000, "high": 1400}, {"low": 9, "high": 1})
        expected_win = new_ratings({"low": 1000, "high": 1400}, {"low": 1, "high": 9})
        self.assertGreater(upset["low"] - 1000, expected_win["high"] - 1400)

    def test_draw_between_equals_changes_nothing(self):
        self.assertEqual(new_ratings({"a": 1300, "b": 1300}, {"a": 4, "b": 4}), {"a": 1300, "b": 1300})

    def test_multiplayer_is_roughly_zero_sum(self):
        r = {"a": 1200, "b": 1200, "c": 1200}
        out = new_ratings(r, {"a": 3, "b": 2, "c": 1})
        self.assertAlmostEqual(sum(out.values()), sum(r.values()), delta=2)
        self.assertGreater(out["a"], out["b"])
        self.assertGreater(out["b"], out["c"])

    def test_single_player_unchanged(self):
        self.assertEqual(new_ratings({"a": 1200}, {"a": 5}), {"a": 1200})

    def test_running_average(self):
        self.assertAlmostEqual(new_average(10.0, 1, 20.0), 15.0)
        self.assertAlmostEqual(new_average(0.0, 0, 7.0), 7.0)


if __name__ == "__main__":
    unittest.main()
