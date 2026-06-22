import httpx
from .config import settings

async def validate_token(token: str) -> str | None:
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(f"{settings.auth_service_url}/auth/validate",
                                 headers={"Authorization": f"Bearer {token}"},
                                 timeout=5)
            if r.status_code == 200:
                return r.json()["player_id"]
        except Exception:
            pass
    return None