"""Leaderboard Service -- Kafka consumer, Elo updates, Postgres queries."""
import os
import json
import logging
import threading
import time
from fastapi import FastAPI, HTTPException, Query
from confluent_kafka import Consumer
from prometheus_client import Histogram, generate_latest
from starlette.responses import Response

from . import db

app = FastAPI(title="leaderboard-service")
logger = logging.getLogger("leaderboard")

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import Resource
    resource = Resource.create({"service.name": "leaderboard-service"})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318") + "/v1/traces")
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    FastAPIInstrumentor.instrument_app(app)
except Exception:
    pass

kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MAX_UPDATE_ATTEMPTS = 5

query_latency = Histogram("leaderboard_query_latency_seconds", "Leaderboard query latency")


def process_event(msg) -> bool:
    """Apply one lifecycle message. Returns True when it is safe to commit its offset."""
    try:
        event = json.loads(msg.value().decode())
    except (ValueError, UnicodeDecodeError):
        logger.error("Skipping undecodable lifecycle message")
        return True
    if event.get("type") != "match.end" or not event.get("match_id"):
        return True
    for attempt in range(1, MAX_UPDATE_ATTEMPTS + 1):
        try:
            result = db.apply_match(event)
            logger.info(f"Match {event['match_id']}: {result}")
            return True
        except Exception as e:
            logger.error(f"Leaderboard update failed (attempt {attempt}/{MAX_UPDATE_ATTEMPTS}): {e}")
            time.sleep(min(2 ** attempt, 15))
    # Give up on a poison event rather than blocking the partition forever.
    logger.error(f"Dropping match.end for {event['match_id']} after {MAX_UPDATE_ATTEMPTS} attempts")
    return True


def consume_lifecycle():
    consumer = Consumer({
        'bootstrap.servers': kafka_bootstrap, 'group.id': 'leaderboard-service',
        'auto.offset.reset': 'earliest', 'enable.auto.commit': False,
    })
    consumer.subscribe(['match.lifecycle'])
    while True:
        msg = consumer.poll(0.5)
        if msg is None or msg.error():
            continue
        # Commit only after the update is durable, so a crash replays the event
        # (apply_match is idempotent) instead of losing it.
        if process_event(msg):
            consumer.commit(message=msg, asynchronous=False)


@app.on_event("startup")
async def startup():
    t = threading.Thread(target=consume_lifecycle, daemon=True)
    t.start()
    logger.info("Leaderboard service started")


@app.get("/health")
async def health():
    return {"status": "ok"}


# Handlers that hit Postgres are plain `def` so they run in FastAPI's threadpool.
@app.get("/ready")
def ready():
    try:
        db.ping()
        return {"status": "ready"}
    except Exception:
        raise HTTPException(status_code=503, detail="Not ready")


@app.get("/leaderboard")
def get_leaderboard(window: str = Query("all", pattern="^(daily|weekly|all)$"), limit: int = Query(50, ge=1, le=200)):
    try:
        with query_latency.time():
            return {"window": window, "rankings": db.top_players(window, limit)}
    except Exception as e:
        logger.error(f"Leaderboard query failed: {e}")
        raise HTTPException(status_code=500, detail="Leaderboard unavailable")


@app.get("/leaderboard/player/{player_id}")
def get_player_stats(player_id: str):
    try:
        player, rank = db.player_with_rank(player_id)
    except Exception as e:
        logger.error(f"Player stats query failed: {e}")
        raise HTTPException(status_code=500, detail="Leaderboard unavailable")
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found")
    return {"player": player, "rank": rank}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type="text/plain")


@app.on_event("shutdown")
async def shutdown():
    logger.info("Leaderboard service shut down cleanly")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
