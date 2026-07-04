"""Notification Service -- RabbitMQ consumer, Redis pub/sub, WS push."""
import os
import json
import asyncio
import logging
import aio_pika
import redis.asyncio as redis
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException

app = FastAPI(title="notification-service")
logger = logging.getLogger("notification")

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import Resource
    resource = Resource.create({"service.name": "notification-service"})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318") + "/v1/traces")
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    FastAPIInstrumentor.instrument_app(app)
except Exception:
    pass

rabbitmq_url = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
redis_host = os.environ.get("REDIS_HOST", "redis")
auth_service_url = os.environ.get("AUTH_SERVICE_URL", "http://auth:8000")

redis_client = None
rabbitmq_connection = None
client_connections = {}

try:
    from prometheus_client import Counter
    expired_count = Counter("notification_expired_events_total", "Total expired match events processed")
except Exception:
    class _NoopCounter:
        def inc(self, *args, **kwargs): pass
    expired_count = _NoopCounter()


def get_redis():
    global redis_client
    if redis_client is None:
        redis_client = redis.Redis(host=redis_host, port=6379, decode_responses=True)
    return redis_client


async def consume_notifications():
    global rabbitmq_connection
    while True:
        try:
            rabbitmq_connection = await aio_pika.connect_robust(rabbitmq_url)
            channel = await rabbitmq_connection.channel()
            exchange = await channel.get_exchange("notifications.exchange", ensure=False)
            queue = await channel.declare_queue("notifications.match", durable=True)
            await queue.bind(exchange, routing_key="match.#")
            logger.info("Notification consumer started")
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        body = json.loads(message.body.decode())
                        event_type = body.get("event", "unknown")
                        player_ids = body.get("player_ids", [])
                        routing_key = message.routing_key if hasattr(message, 'routing_key') else "unknown"
                        if routing_key == "match.expired":
                            logger.info(f"Expired matchmaking request (dead-lettered)")
                            expired_count.inc()
                            continue
                        logger.info(f"Notification: {event_type} for {player_ids}")
                        await dispatch(event_type, player_ids, body)
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            await asyncio.sleep(5)


async def dispatch(event_type, player_ids, payload):
    message = json.dumps({"type": "notification", "event": event_type, "payload": payload})
    for player_id in player_ids:
        if player_id in client_connections:
            try:
                await client_connections[player_id].send_text(message)
            except Exception:
                client_connections.pop(player_id, None)
    r = get_redis()
    for player_id in player_ids:
        await r.publish(f"notifications:{player_id}", message)


@app.on_event("startup")
async def startup():
    asyncio.create_task(consume_notifications())


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    return {"status": "ready"}


@app.websocket("/ws/notifications")
async def notification_websocket(websocket: WebSocket):
    await websocket.accept()
    try:
        data = await asyncio.wait_for(websocket.receive_json(), timeout=10)
        token = data.get("token")
        if not token:
            await websocket.close(code=4001, reason="Missing token")
            return
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{auth_service_url}/auth/validate",
                                 headers={"Authorization": f"Bearer {token}"}, timeout=5)
            if r.status_code != 200:
                await websocket.close(code=4001, reason="Invalid token")
                return
            player_id = r.json()["player_id"]
        client_connections[player_id] = websocket
        logger.info(f"Player {player_id} subscribed to notifications")
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            client_connections.pop(player_id, None)
    except asyncio.TimeoutError:
        await websocket.close(code=4001, reason="Auth timeout")
    except Exception as e:
        logger.error(f"WS error: {e}")
        try:
            await websocket.close(code=4001, reason="Error")
        except Exception:
            pass


@app.on_event("shutdown")
async def shutdown():
    try:
        if rabbitmq_connection and not rabbitmq_connection.is_closed:
            await rabbitmq_connection.close()
    except Exception:
        pass
    if redis_client:
        try:
            await redis_client.close()
        except Exception:
            pass
    logger.info("Notification service shut down cleanly")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
