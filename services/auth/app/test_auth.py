"""API tests for the auth service, with Postgres/Redis replaced by in-memory fakes."""
import os
import sys
import tempfile
import time
import unittest

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# jwt_handler reads the key files at import time, so create a throwaway pair first.
_key_dir = tempfile.mkdtemp()
_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
with open(os.path.join(_key_dir, "private.pem"), "wb") as f:
    f.write(_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                               serialization.NoEncryption()))
with open(os.path.join(_key_dir, "public.pem"), "wb") as f:
    f.write(_key.public_key().public_bytes(serialization.Encoding.PEM,
                                           serialization.PublicFormat.SubjectPublicKeyInfo))
os.environ["JWT_PRIVATE_KEY_PATH"] = os.path.join(_key_dir, "private.pem")
os.environ["JWT_PUBLIC_KEY_PATH"] = os.path.join(_key_dir, "public.pem")

from fastapi.testclient import TestClient  # noqa: E402

from app import main  # noqa: E402
from app.db import UsernameTaken  # noqa: E402


class FakeStore:
    """Stands in for the Postgres players table and the Redis session keys."""

    def __init__(self):
        self.players = {}
        self.sessions = {}

    def create_player(self, player_id, username, password_hash):
        if any(p["username"] == username for p in self.players.values()):
            raise UsernameTaken(username)
        self.players[player_id] = {
            "playerId": player_id, "username": username, "passwordHash": password_hash,
            "eloRating": 1200, "wins": 0, "losses": 0, "totalMatches": 0, "averageScore": 0.0,
        }

    def get_player(self, player_id):
        return self.players.get(player_id)

    def get_player_by_username(self, username):
        return next((p for p in self.players.values() if p["username"] == username), None)

    def store_session(self, session_id, doc, ttl):
        pass

    def store_player_session(self, player_id, session_id, doc, ttl):
        self.sessions[player_id] = {"sessionId": session_id, "token": doc["token"]}

    def get_player_session(self, player_id):
        return self.sessions.get(player_id)

    def delete_player_session(self, player_id):
        self.sessions.pop(player_id, None)


class AuthApiTests(unittest.TestCase):
    def setUp(self):
        self.store = FakeStore()
        self._orig = {}
        for name in ["create_player", "get_player", "get_player_by_username", "store_session",
                     "store_player_session", "get_player_session", "delete_player_session"]:
            self._orig[name] = getattr(main, name)
            setattr(main, name, getattr(self.store, name))
        self.client = TestClient(main.app)

    def tearDown(self):
        for name, fn in self._orig.items():
            setattr(main, name, fn)

    def _register(self, username="alice", password="pass123"):
        return self.client.post("/auth/register", json={"username": username, "password": password})

    @staticmethod
    def _bearer(token):
        return {"Authorization": f"Bearer {token}"}

    def test_register_returns_token_and_hashes_password(self):
        r = self._register()
        self.assertEqual(r.status_code, 201)
        self.assertTrue(r.json()["access_token"])
        stored = self.store.get_player_by_username("alice")
        self.assertNotEqual(stored["passwordHash"], "pass123")
        self.assertEqual(stored["eloRating"], 1200)

    def test_duplicate_username_is_rejected(self):
        self.assertEqual(self._register().status_code, 201)
        self.assertEqual(self._register().status_code, 409)

    def test_login_checks_password(self):
        self._register()
        bad = self.client.post("/auth/login", json={"username": "alice", "password": "wrong"})
        self.assertEqual(bad.status_code, 401)
        unknown = self.client.post("/auth/login", json={"username": "nobody", "password": "pass123"})
        self.assertEqual(unknown.status_code, 401)
        ok = self.client.post("/auth/login", json={"username": "alice", "password": "pass123"})
        self.assertEqual(ok.status_code, 200)

    def test_validate_accepts_current_token_only(self):
        first = self._register().json()["access_token"]
        r = self.client.get("/auth/validate", headers=self._bearer(first))
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["valid"])
        # Logging in again issues a new session; the old token is revoked. JWT timestamps
        # have 1s resolution, so wait to get a different token string.
        time.sleep(1.1)
        second = self.client.post("/auth/login", json={"username": "alice", "password": "pass123"}).json()["access_token"]
        self.assertEqual(self.client.get("/auth/validate", headers=self._bearer(second)).status_code, 200)
        self.assertEqual(self.client.get("/auth/validate", headers=self._bearer(first)).status_code, 401)

    def test_logout_revokes_token(self):
        token = self._register().json()["access_token"]
        self.assertEqual(self.client.post("/auth/logout", headers=self._bearer(token)).status_code, 200)
        self.assertEqual(self.client.get("/auth/validate", headers=self._bearer(token)).status_code, 401)

    def test_garbage_token_is_rejected(self):
        r = self.client.get("/auth/validate", headers=self._bearer("not-a-jwt"))
        self.assertEqual(r.status_code, 401)

    def test_profile_is_private_to_the_owner(self):
        token_a = self._register("alice").json()["access_token"]
        self._register("bob")
        bob_id = self.store.get_player_by_username("bob")["playerId"]
        alice_id = self.store.get_player_by_username("alice")["playerId"]
        self.assertEqual(self.client.get(f"/players/{alice_id}", headers=self._bearer(token_a)).status_code, 200)
        self.assertEqual(self.client.get(f"/players/{bob_id}", headers=self._bearer(token_a)).status_code, 403)


if __name__ == "__main__":
    unittest.main()
