import asyncio
import json
import logging
import os
import time
import uuid
import redis.asyncio as redis
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from starlette.responses import Response
from prometheus_client import generate_latest

from .config import settings
from .leader_election import LeaderElection
from .models import GameState, PlayerState
from .game_loop import GameLoop
from .ws_handler import websocket_endpoint

app = FastAPI(title="game-room-service")
logger = logging.getLogger("game-room")

try:
    from lib.python.common.tracing import setup_opentelemetry, instrument_fastapi
    setup_opentelemetry("game-room-service")
    instrument_fastapi(app)
except Exception:
    pass

match_id = None
instance_id = None
game_loop = None
election = None
redis_client = None
kafka_producer = None

connected_players = {}
connected_spectators = {}


async def handle_leadership_loss():
    logger.warning("Leadership lost - stopping game loop and dropping connections")
    if game_loop:
        game_loop.running = False
    for pid, ws in list(connected_players.items()):
        try:
            await ws.send_json({"type": "leader_changed", "reason": "failover"})
            await ws.close()
        except Exception:
            pass
    for sid, ws in list(connected_spectators.items()):
        try:
            await ws.send_json({"type": "leader_changed", "reason": "failover"})
            await ws.close()
        except Exception:
            pass
    connected_players.clear()
    connected_spectators.clear()
    logger.info("Leadership loss cleanup complete")


@app.on_event("startup")
async def startup():
    global match_id, instance_id, redis_client, kafka_producer, election, game_loop
    match_id = os.environ.get("MATCH_ID", str(uuid.uuid4()))
    instance_id = os.environ.get("INSTANCE_ID", str(uuid.uuid4()))
    logger.info(f"Starting game room for match {match_id}, instance {instance_id}")
    redis_client = redis.Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
    kafka_producer = Producer({'bootstrap.servers': settings.kafka_bootstrap_servers, 'acks': 'all'})
    election = LeaderElection(match_id, instance_id, on_leadership_lost=handle_leadership_loss)

    try:
        from .room_pool import register_room
        register_room(match_id)
        logger.info(f"Room {match_id} registered in pool")
    except Exception as e:
        logger.error(f"Room pool registration failed: {e}")

    is_leader = await election.campaign()
    if is_leader:
        logger.info("Elected leader, starting game loop")
        game_loop = GameLoop(match_id, redis_client, kafka_producer, connected_players, connected_spectators)
        await game_loop.load_state()
        asyncio.create_task(game_loop.run())
    else:
        logger.info("Running as follower")
        asyncio.create_task(election.start_follower_watch())


@app.on_event("shutdown")
async def shutdown():
    if game_loop:
        await game_loop.stop()
    if election:
        await election.step_down()
    if kafka_producer:
        kafka_producer.flush()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    checks = {}
    try:
        await redis_client.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {e}"
    try:
        kafka_producer.produce("healthcheck", key=b"health", value=b"health")
        kafka_producer.flush(timeout=2)
        checks["kafka"] = "ok"
    except Exception as e:
        checks["kafka"] = f"error: {e}"
    try:
        election.etcd.get("/health")
        checks["etcd"] = "ok"
    except Exception as e:
        checks["etcd"] = f"error: {e}"
    all_ok = all(v == "ok" for v in checks.values())
    if not all_ok:
        raise HTTPException(status_code=503, detail=f"Not ready: {checks}")
    return {"status": "ready", "checks": checks}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type="text/plain")


@app.websocket("/ws")
async def ws_endpoint(websocket):
    await websocket_endpoint(websocket, election, game_loop, connected_players, connected_spectators, settings.auth_service_url)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
