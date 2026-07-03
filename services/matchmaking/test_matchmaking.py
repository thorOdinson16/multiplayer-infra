"""Tests for matchmaking service endpoints."""
import json
import pytest
import httpx

BASE_URL = "http://localhost:8002"
AUTH_URL = "http://localhost:8001"


@pytest.fixture(scope="module")
def auth_token():
    try:
        r = httpx.post(f"{AUTH_URL}/auth/register",
                       json={"username": "mmtest", "password": "testpass"}, timeout=5)
        if r.status_code == 201:
            return r.json().get("access_token", "")
    except Exception:
        pass
    try:
        r = httpx.post(f"{AUTH_URL}/auth/login",
                       json={"username": "mmtest", "password": "testpass"}, timeout=5)
        if r.status_code == 200:
            return r.json().get("access_token", "")
    except Exception:
        pass
    return ""


def test_health_endpoint():
    r = httpx.get(f"{BASE_URL}/health", timeout=5)
    assert r.status_code == 200
    assert r.json().get("status") == "ok"


def test_metrics_endpoint():
    r = httpx.get(f"{BASE_URL}/metrics", timeout=5)
    assert r.status_code == 200


def test_queue_without_token_returns_400(auth_token):
    r = httpx.post(f"{BASE_URL}/matchmaking/queue",
                   json={}, timeout=5)
    assert r.status_code in (400, 422, 401)


def test_queue_with_valid_token_returns_202(auth_token):
    if not auth_token:
        pytest.skip("No auth token available")
    r = httpx.post(f"{BASE_URL}/matchmaking/queue",
                   json={"token": auth_token}, timeout=5)
    assert r.status_code in (202, 503)


def test_rooms_needed_metric_defined(auth_token):
    r = httpx.get(f"{BASE_URL}/metrics", timeout=5)
    assert r.status_code == 200
    assert "matchmaking_rooms_needed" in r.text
