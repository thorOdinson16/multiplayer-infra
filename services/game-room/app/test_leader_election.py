"""Tests for leader election using a mocked etcd3 client."""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.leader_election import LeaderElection


class TestLeaderElection(unittest.TestCase):
    def _make(self, on_elected=None, on_lost=None):
        with patch("app.leader_election.etcd3.client") as mock_client:
            client = MagicMock()
            mock_client.return_value = client
            election = LeaderElection("m1", "room-1", on_elected=on_elected, on_lost=on_lost)
        return election, client

    def test_campaign_wins_and_publishes_address(self):
        election, client = self._make()
        client.lease.return_value = MagicMock()
        client.transaction.return_value = (True, [])

        self.assertTrue(election._campaign())
        self.assertTrue(election.is_leader)
        client.put.assert_called_once()
        args, _ = client.put.call_args
        self.assertEqual(args[0], "/match/m1/leader-address")
        self.assertEqual(args[1], election.leader_address)

    def test_campaign_loses_when_key_held(self):
        election, client = self._make()
        client.lease.return_value = MagicMock()
        client.transaction.return_value = (False, [])

        self.assertFalse(election._campaign())
        self.assertFalse(election.is_leader)
        client.put.assert_not_called()

    def test_lose_leadership_revokes_lease(self):
        election, client = self._make()
        lease = MagicMock()
        election.is_leader = True
        election.lease = lease

        election._lose_leadership()

        self.assertFalse(election.is_leader)
        self.assertIsNone(election.lease)
        lease.revoke.assert_called_once()

    def test_cleanup_removes_room(self):
        election, client = self._make()
        election.is_leader = True
        with patch("app.leader_election.remove_room") as mock_remove:
            election._cleanup()
        mock_remove.assert_called_once_with("m1")
        self.assertFalse(election.is_leader)


if __name__ == "__main__":
    unittest.main()
