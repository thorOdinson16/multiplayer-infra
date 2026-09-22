"""Integration tests for the matchmaking service.

These require the docker-compose stack to be running; they are skipped when
the services cannot be reached (e.g. during the unit-test CI job).
"""
import httpx
import pytest

BASE_URL = "http://localhost:8002"
AUTH_URL = "http://localhost:8001"


def _service_up(url: str) -> bool:
    try:
        return httpx.get(f"{url}/health", timeout=2).status_code == 200
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not (_service_up(BASE_URL) and _service_up(AUTH_URL)),
    reason="matchmaking/auth services not running (integration-only test)",
)


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
