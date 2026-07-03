"""Reconnect Handler -- state delta computation for reconnecting players."""
import os
import json
import logging
import redis.asyncio as redis
import etcd3
import httpx
from fastapi import FastAPI, HTTPException

app = FastAPI(title="reconnect-handler-service")
logger = logging.getLogger("reconnect-handler")

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import Resource
    resource = Resource.create({"service.name": "reconnect-handler-service"})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318") + "/v1/traces")
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    FastAPIInstrumentor.instrument_app(app)
except Exception:
    pass

redis_client = None
etcd_client = None
auth_service_url = os.environ.get("AUTH_SERVICE_URL", "http://auth:8000")
redis_host = os.environ.get("REDIS_HOST", "redis")
etcd_host = os.environ.get("ETCD_HOST", "etcd")


def get_redis():
    global redis_client
    if redis_client is None:
        redis_client = redis.Redis(host=redis_host, port=6379, decode_responses=True)
    return redis_client


def get_etcd():
    global etcd_client
    if etcd_client is None:
        etcd_client = etcd3.client(host=etcd_host, port=2379)
    return etcd_client


async def validate_token(token):
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{auth_service_url}/auth/validate",
                                 headers={"Authorization": f"Bearer {token}"}, timeout=5)
            if r.status_code == 200:
                return r.json()["player_id"]
    except Exception:
        pass
    return None


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    try:
        r = get_redis()
        await r.ping()
        return {"status": "ready"}
    except Exception:
        raise HTTPException(status_code=503, detail="Not ready")


@app.post("/reconnect")
async def reconnect(body: dict):
    token = body.get("token")
    match_id = body.get("match_id")
    if not token or not match_id:
        raise HTTPException(status_code=400, detail="Missing token or match_id")
    player_id = await validate_token(token)
    if not player_id:
        raise HTTPException(status_code=401, detail="Invalid token")
    r = get_redis()
    hold_key = f"hold:{match_id}:{player_id}"
    hold_data = await r.get(hold_key)
    state_key = f"match:{match_id}:state"
    state_data = await r.get(state_key)
    if not state_data:
        if hold_data:
            await r.delete(hold_key)
            raise HTTPException(status_code=404, detail="Match ended, hold released")
        raise HTTPException(status_code=404, detail="No state found")
    state = json.loads(state_data)
    etcd = get_etcd()
    value, _ = etcd.get(f"/match/{match_id}/leader-address")
    leader_address = value.decode() if value else "game-room:8000"
    delta = {
        "match_id": match_id,
        "player_id": player_id,
        "current_tick": state.get("tick", 0),
        "players": state.get("players", {}),
        "leader_address": leader_address,
        "hold_available": hold_data is not None,
        "hold_state": json.loads(hold_data) if hold_data else None,
    }
    return delta


@app.get("/state/{match_id}")
async def get_match_state(match_id: str):
    r = get_redis()
    state_data = await r.get(f"match:{match_id}:state")
    if not state_data:
        raise HTTPException(status_code=404, detail="No state found")
    return {"state": json.loads(state_data)}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
