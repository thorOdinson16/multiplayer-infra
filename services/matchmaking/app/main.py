"""Matchmaking Service – RabbitMQ consumer, Elo grouping."""
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

app = FastAPI(title="matchmaking-service")
logger = logging.getLogger("matchmaking")

# Prometheus metrics
queue_depth = Gauge("matchmaking_queue_depth", "Pending matchmaking requests")
broker_unavailable = Gauge("matchmaking_broker_unavailable", "1 if RabbitMQ is down")
lobbies_assembled = Counter("matchmaking_lobbies_assembled", "Total lobbies formed")
expired_count = Counter("matchmaking_expired_count", "Total expired requests")  # incremented by notification service later

matcher = Matcher()
rabbitmq_connection = None
channel = None

async def consume():
    global rabbitmq_connection, channel
    while True:
        try:
            rabbitmq_connection = await aio_pika.connect_robust(settings.rabbitmq_url)
            broker_unavailable.set(0)
            channel = await rabbitmq_connection.channel()
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
                            logger.warning("Invalid token in matchmaking request")
                            continue
                        # Fetch player Elo from Auth Service (or Couchbase). For simplicity, assume body contains 'elo'
                        elo = body.get("elo", 1200)
                        req = MatchRequest(player_id=player_id, elo=elo)
                        await matcher.add_request(req)
                        queue_depth.inc()
        except Exception as e:
            logger.error(f"RabbitMQ consumer error: {e}")
            broker_unavailable.set(1)
            await asyncio.sleep(5)

async def matchmaker_loop():
    while True:
        await asyncio.sleep(settings.tick_interval_seconds)
        lobbies = await matcher.match_tick()
        for lobby in lobbies:
            room_id = get_available_room()
            if not room_id:
                logger.info("No room available, would trigger Kubernetes here")
                room_id = "room-placeholder"
            # Publish match.found to notifications.exchange
            if channel:
                exchange = await channel.get_exchange("notifications.exchange")
                payload = {
                    "event": "match.found",
                    "room_id": room_id,
                    "player_ids": [r.player_id for r in lobby]
                }
                await exchange.publish(
                    aio_pika.Message(body=json.dumps(payload).encode()),
                    routing_key="match.found"
                )
                lobbies_assembled.inc()
                queue_depth.dec(len(lobby))
                logger.info(f"Match formed: {len(lobby)} players -> {room_id}")

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