"""Tests for room_pool module using a mock etcd3."""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.room_pool import register_room, mark_in_use, remove_room, get_available_room


class TestRoomPool(unittest.TestCase):

    @patch("room_pool.get_etcd_client")
    def test_register_room(self, mock_get_etcd):
        mock_etcd = MagicMock()
        mock_get_etcd.return_value = mock_etcd

        register_room("test-room-1")

        mock_etcd.put.assert_called_once_with(
            "/rooms/available/test-room-1", "available"
        )

    @patch("room_pool.get_etcd_client")
    def test_mark_in_use(self, mock_get_etcd):
        mock_etcd = MagicMock()
        mock_get_etcd.return_value = mock_etcd

        mark_in_use("test-room-1")

        mock_etcd.put.assert_called_once_with(
            "/rooms/available/test-room-1", "in-use"
        )

    @patch("room_pool.get_etcd_client")
    def test_remove_room(self, mock_get_etcd):
        mock_etcd = MagicMock()
        mock_get_etcd.return_value = mock_etcd

        remove_room("test-room-1")

        mock_etcd.delete.assert_called_once_with(
            "/rooms/available/test-room-1"
        )

    @patch("room_pool.get_etcd_client")
    def test_get_available_room_found(self, mock_get_etcd):
        mock_etcd = MagicMock()
        mock_meta = MagicMock()
        mock_meta.key.decode.return_value = "/rooms/available/test-room-1"
        mock_etcd.get_prefix.return_value = [(b"available", mock_meta)]
        mock_get_etcd.return_value = mock_etcd

        result = get_available_room()
        self.assertEqual(result, "test-room-1")

    @patch("room_pool.get_etcd_client")
    def test_get_available_room_skips_in_use(self, mock_get_etcd):
        mock_etcd = MagicMock()
        mock_meta = MagicMock()
        mock_meta.key.decode.return_value = "/rooms/available/test-room-1"
        mock_etcd.get_prefix.return_value = [(b"in-use", mock_meta)]
        mock_get_etcd.return_value = mock_etcd

        result = get_available_room()
        self.assertIsNone(result)

    @patch("room_pool.get_etcd_client")
    def test_get_available_room_empty(self, mock_get_etcd):
        mock_etcd = MagicMock()
        mock_etcd.get_prefix.return_value = []
        mock_get_etcd.return_value = mock_etcd

        result = get_available_room()
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
