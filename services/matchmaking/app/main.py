"""Matchmaking Service -- RabbitMQ consumer, Elo grouping, HTTP API, etcd room pool."""
import asyncio
import json
import logging
import aio_pika
import httpx
from fastapi import FastAPI, HTTPException
from prometheus_client import Gauge, Counter, generate_latest
from starlette.responses import Response
from .config import settings
from .models import MatchRequest
from .matcher import Matcher
from .auth_client import validate_token
from .room_manager import get_available_room
from .k8s_client import ensure_game_rooms

app = FastAPI(title="matchmaking-service")
logger = logging.getLogger("matchmaking")

queue_depth = Gauge("matchmaking_queue_depth", "Pending matchmaking requests")
broker_unavailable = Gauge("matchmaking_broker_unavailable", "1 if RabbitMQ is down")
lobbies_assembled = Counter("matchmaking_lobbies_assembled", "Total lobbies formed")
expired_count = Counter("matchmaking_expired_count", "Total expired requests from queue")
lobby_scale_timeout = Counter("matchmaking_lobby_scale_timeout_total", "Lobbies expired due to room unavailability")
rooms_needed = Gauge("matchmaking_rooms_needed", "Number of additional game rooms needed")

matcher = Matcher()
rabbitmq_connection = None
channel = None
publish_channel = None

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import Resource
    import os
    resource = Resource.create({"service.name": "matchmaking-service"})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318") + "/v1/traces")
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    FastAPIInstrumentor.instrument_app(app)
except Exception:
    pass


async def fetch_player_elo(player_id: str, token: str) -> int | None:
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{settings.auth_service_url}/players/{player_id}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=5,
            )
            if r.status_code == 200:
                return r.json()["eloRating"]
    except Exception as e:
        logger.error(f"Error fetching Elo: {e}")
    return None


async def connect_publish_channel():
    global publish_channel
    try:
        conn = await aio_pika.connect_robust(settings.rabbitmq_url)
        publish_channel = await conn.channel()
        return conn
    except Exception as e:
        logger.error(f"Failed to connect publish channel: {e}")
        return None


async def publish_to_queue(payload: dict):
    conn = await connect_publish_channel()
    if conn:
        ch = await conn.channel()
        await ch.default_exchange.publish(
            aio_pika.Message(body=json.dumps(payload).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key="matchmaking.requests",
        )
        await ch.close()
        await conn.close()


async def consume():
    global rabbitmq_connection, channel
    while True:
        try:
            rabbitmq_connection = await aio_pika.connect_robust(settings.rabbitmq_url)
            broker_unavailable.set(0)
            channel = await rabbitmq_connection.channel()
            await channel.set_qos(prefetch_count=10)
            queue = await channel.get_queue("matchmaking.requests", ensure=False)
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        body = json.loads(message.body.decode())
                        token = body.get("token")
                        if not token:
                            continue
                        player_id = await validate_token(token)
                        if not player_id:
                            continue
                        elo = await fetch_player_elo(player_id, token)
                        if elo is None:
                            elo = 1200
                        req = MatchRequest(player_id=player_id, elo=elo)
                        await matcher.add_request(req)
                        queue_depth.inc()
                        logger.info(f"Player {player_id} (elo={elo}) queued")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            broker_unavailable.set(1)
            await asyncio.sleep(5)


_scale_up_deficit = 0
_pending_lobbies: list[tuple[list, int]] = []  # (lobby, retry_count)
_MAX_SCALE_RETRIES = 15  # 15 ticks * 2s = 30s before expiring


async def _assign_lobby(lobby, room_id):
    if channel is None:
        logger.warning("Channel not ready, skipping notification")
        return
    try:
        exchange = await channel.get_exchange("notifications.exchange")
        payload = {
            "event": "match.found",
            "room_id": room_id,
            "player_ids": [r.player_id for r in lobby],
        }
        await exchange.publish(
            aio_pika.Message(body=json.dumps(payload).encode()),
            routing_key="match.found",
        )
        lobbies_assembled.inc()
        queue_depth.dec(len(lobby))
        logger.info(f"Match: {len(lobby)} players -> {room_id}")
    except Exception as e:
        logger.warning(f"Failed to publish notification: {e}")


async def matchmaker_loop():
    global _scale_up_deficit
    while True:
        await asyncio.sleep(settings.tick_interval_seconds)
        try:
            # Retry pending lobbies first (rooms created by earlier scale-up)
            still_pending = []
            for lobby, retry in _pending_lobbies:
                room_id = get_available_room()
                if room_id:
                    await _assign_lobby(lobby, room_id)
                elif retry < _MAX_SCALE_RETRIES:
                    still_pending.append((lobby, retry + 1))
                else:
                    logger.warning(f"Pending lobby expired after {_MAX_SCALE_RETRIES} retries")
                    lobby_scale_timeout.inc()
                    _scale_up_deficit = max(0, _scale_up_deficit - 1)
                    rooms_needed.set(_scale_up_deficit)
            _pending_lobbies[:] = still_pending

            # Match new lobbies
            lobbies = await matcher.match_tick()
            for lobby in lobbies:
                room_id = get_available_room()
                if room_id:
                    await _assign_lobby(lobby, room_id)
                else:
                    logger.warning("No room available -- re-queuing lobby and scaling up")
                    _scale_up_deficit += 1
                    rooms_needed.set(_scale_up_deficit)
                    await ensure_game_rooms(3 + _scale_up_deficit)
                    _pending_lobbies.append((lobby, 0))
                    logger.info(f"Lobby of {len(lobby)} re-queued (deficit={_scale_up_deficit})")
        except Exception as e:
            logger.error(f"Matchmaker error: {e}")


@app.on_event("startup")
async def startup():
    asyncio.create_task(consume())
    asyncio.create_task(matchmaker_loop())


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    if rabbitmq_connection and not rabbitmq_connection.is_closed:
        return {"status": "ready"}
    raise HTTPException(status_code=503, detail="Not ready")


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type="text/plain")


@app.post("/matchmaking/queue")
async def queue_matchmaking(body: dict):
    token = body.get("token")
    if not token:
        raise HTTPException(status_code=400, detail="Missing token")
    payload = {"token": token, "timestamp": asyncio.get_event_loop().time()}
    try:
        await publish_to_queue(payload)
        return {"status": "queued"}
    except Exception as e:
        logger.error(f"Failed to queue matchmaking: {e}")
        raise HTTPException(status_code=503, detail="Matchmaking unavailable")


@app.on_event("shutdown")
async def shutdown():
    try:
        if channel and not channel.is_closed:
            await channel.close()
    except Exception:
        pass
    try:
        if rabbitmq_connection and not rabbitmq_connection.is_closed:
            await rabbitmq_connection.close()
    except Exception:
        pass
    logger.info("Matchmaking service shut down cleanly")
