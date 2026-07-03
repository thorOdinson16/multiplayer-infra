"""Auth Service -- JWT authentication, session management."""
import logging
import uuid
from datetime import datetime, timezone, timedelta
import bcrypt
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from prometheus_client import Counter, Histogram, generate_latest
from starlette.responses import Response

logger = logging.getLogger("auth")

from .config import settings
from .models import LoginRequest, RegisterRequest, TokenResponse, ValidateResponse
from .jwt_handler import create_token, decode_token
from .couchbase_client import (
    store_session, get_session, delete_session,
    store_player, get_player_by_username, get_player
)

app = FastAPI(title="auth-service")
security = HTTPBearer()

# Prometheus metrics
login_requests = Counter("auth_login_requests_total", "Total login requests")
register_requests = Counter("auth_register_requests_total", "Total registration requests")
login_latency = Histogram("auth_login_latency_seconds", "Login request latency")

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import Resource
    import os
    resource = Resource.create({"service.name": "auth-service"})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318") + "/v1/traces")
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    FastAPIInstrumentor.instrument_app(app)
except Exception:
    pass


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    try:
        from .couchbase_client import get_cluster
        get_cluster().ping()
        return {"status": "ready"}
    except Exception:
        raise HTTPException(status_code=503, detail="Not ready")


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type="text/plain")


@app.post("/auth/register", response_model=TokenResponse, status_code=201)
async def register(req: RegisterRequest):
    register_requests.inc()
    if get_player_by_username(req.username):
        raise HTTPException(status_code=409, detail="Username already taken")
    player_id = str(uuid.uuid4())
    hashed = bcrypt.hashpw(req.password.encode(), bcrypt.gensalt()).decode()
    now = datetime.now(timezone.utc)
    player_doc = {
        "type": "player",
        "playerId": player_id,
        "username": req.username,
        "passwordHash": hashed,
        "eloRating": 1200,
        "wins": 0,
        "losses": 0,
        "totalMatches": 0,
        "averageScore": 0.0,
        "createdAt": now.isoformat(),
        "lastSeen": now.isoformat(),
    }
    store_player(player_id, player_doc)
    token = create_token(player_id)
    session_id = str(uuid.uuid4())
    session_doc = {
        "type": "session",
        "sessionId": session_id,
        "playerId": player_id,
        "token": token,
        "expiresAt": (now + timedelta(minutes=settings.jwt_expire_minutes)).isoformat(),
        "ipAddress": "0.0.0.0",
    }
    store_session(session_id, session_doc, settings.jwt_expire_minutes * 60)
    return TokenResponse(access_token=token)


@app.post("/auth/login", response_model=TokenResponse)
async def login(req: LoginRequest):
    login_requests.inc()
    player = get_player_by_username(req.username)
    if not player or not bcrypt.checkpw(req.password.encode(), player["passwordHash"].encode()):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = create_token(player["playerId"])
    now = datetime.now(timezone.utc)
    session_id = str(uuid.uuid4())
    session_doc = {
        "type": "session",
        "sessionId": session_id,
        "playerId": player["playerId"],
        "token": token,
        "expiresAt": (now + timedelta(minutes=settings.jwt_expire_minutes)).isoformat(),
        "ipAddress": "0.0.0.0",
    }
    store_session(session_id, session_doc, settings.jwt_expire_minutes * 60)
    return TokenResponse(access_token=token)


@app.post("/auth/refresh", response_model=TokenResponse)
async def refresh(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = decode_token(credentials.credentials)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    player_id = payload["sub"]
    if not get_player(player_id):
        raise HTTPException(status_code=401, detail="User not found")
    new_token = create_token(player_id)
    now = datetime.now(timezone.utc)
    session_id = str(uuid.uuid4())
    session_doc = {
        "type": "session",
        "sessionId": session_id,
        "playerId": player_id,
        "token": new_token,
        "expiresAt": (now + timedelta(minutes=settings.jwt_expire_minutes)).isoformat(),
        "ipAddress": "0.0.0.0",
    }
    store_session(session_id, session_doc, settings.jwt_expire_minutes * 60)
    return TokenResponse(access_token=new_token)


@app.post("/auth/logout")
async def logout(credentials: HTTPAuthorizationCredentials = Depends(security)):
    return {"detail": "Logged out"}


@app.get("/auth/validate", response_model=ValidateResponse)
async def validate(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = decode_token(credentials.credentials)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return ValidateResponse(player_id=payload["sub"], valid=True)


@app.get("/.well-known/jwks.json")
async def jwks():
    from .jwt_handler import _public_key
    from jwcrypto import jwk
    key = jwk.JWK.from_pem(_public_key)
    return {"keys": [key.export(as_dict=True)]}


@app.get("/players/{player_id}")
async def get_player_profile(
    player_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    try:
        payload = decode_token(credentials.credentials)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")
    if payload["sub"] != player_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    player = get_player(player_id)
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    return {
        "playerId": player["playerId"],
        "username": player["username"],
        "eloRating": player["eloRating"],
        "wins": player["wins"],
        "losses": player["losses"],
        "totalMatches": player["totalMatches"],
        "averageScore": player["averageScore"],
    }


@app.on_event("shutdown")
async def shutdown():
    from .couchbase_client import close_connections
    close_connections()
    logger.info("Auth service shut down cleanly")
