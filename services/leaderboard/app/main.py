"""Leaderboard Service -- Kafka consumer, N1QL queries."""
import os
import json
import logging
import threading
import time
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, HTTPException, Query
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from confluent_kafka import Consumer
from prometheus_client import Counter, Histogram, generate_latest
from starlette.responses import Response

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

couchbase_host = os.environ.get("COUCHBASE_HOST", "couchbase")
couchbase_username = os.environ.get("COUCHBASE_USERNAME", "Administrator")
couchbase_password = os.environ.get("COUCHBASE_PASSWORD", "password")
kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
cluster = None

query_latency = Histogram("leaderboard_query_latency_seconds", "Leaderboard query latency")


def get_cluster():
    global cluster
    if cluster is None:
        auth = PasswordAuthenticator(couchbase_username, couchbase_password)
        cluster = Cluster(f"couchbase://{couchbase_host}", ClusterOptions(auth))
        cluster.wait_until_ready(timedelta(seconds=30))
    return cluster


def consume_lifecycle():
    consumer = Consumer({
        'bootstrap.servers': kafka_bootstrap, 'group.id': 'leaderboard-service',
        'auto.offset.reset': 'earliest', 'enable.auto.commit': True,
    })
    consumer.subscribe(['match.lifecycle'])
    while True:
        msg = consumer.poll(0.5)
        if msg and not msg.error():
            event = json.loads(msg.value().decode())
            if event.get("type") == "match.end":
                try:
                    update_leaderboard(event)
                except Exception as e:
                    logger.error(f"Leaderboard update error: {e}")
        time.sleep(0.1)


def update_leaderboard(event):
    cl = get_cluster()
    outcome = event.get("outcome", {})
    scores = outcome.get("scores", {})
    players = event.get("players", [])
    for player_id in players:
        score = scores.get(player_id, 0)
        try:
            result = cl.query("SELECT * FROM `players` WHERE playerId = $1", player_id)
            rows = list(result.rows())
            if rows:
                player = rows[0]["players"]
                new_wins = player.get("wins", 0) + (1 if outcome.get("winner") == player_id else 0)
                new_losses = player.get("losses", 0) + (0 if outcome.get("winner") == player_id else 1)
                new_total = player.get("totalMatches", 0) + 1
                new_avg = ((player.get("averageScore", 0) * (new_total - 1)) + score) / new_total
                cl.query(
                    "UPDATE `players` SET wins=$1, losses=$2, totalMatches=$3, averageScore=$4, lastSeen=$5 WHERE playerId=$6",
                    new_wins, new_losses, new_total, new_avg, datetime.now(timezone.utc).isoformat(), player_id,
                )
        except Exception as e:
            logger.error(f"Error updating {player_id}: {e}")


@app.on_event("startup")
async def startup():
    t = threading.Thread(target=consume_lifecycle, daemon=True)
    t.start()
    logger.info("Leaderboard service started")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    try:
        get_cluster().ping()
        return {"status": "ready"}
    except Exception:
        raise HTTPException(status_code=503, detail="Not ready")


@app.get("/leaderboard")
async def get_leaderboard(window: str = Query("all", regex="^(daily|weekly|all)$"), limit: int = Query(50, ge=1, le=200)):
    cl = get_cluster()
    try:
        if window == "daily":
            since = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
            condition = f" AND lastSeen >= '{since}'"
        elif window == "weekly":
            since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            condition = f" AND lastSeen >= '{since}'"
        else:
            condition = ""
        result = cl.query(
            f"SELECT playerId, username, eloRating, wins, losses, totalMatches, averageScore "
            f"FROM `players` WHERE type = 'player'{condition} ORDER BY eloRating DESC LIMIT {limit}"
        )
        return {"window": window, "rankings": list(result.rows())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/leaderboard/player/{player_id}")
async def get_player_stats(player_id: str):
    cl = get_cluster()
    try:
        result = cl.query("SELECT playerId, username, eloRating, wins, losses, totalMatches, averageScore FROM `players` WHERE playerId = $1", player_id)
        rows = list(result.rows())
        if not rows:
            raise HTTPException(status_code=404, detail="Player not found")
        player = rows[0]
        rank_result = cl.query("SELECT COUNT(*) as r FROM `players` WHERE type = 'player' AND eloRating > $1", player.get("eloRating", 0))
        rank_rows = list(rank_result.rows())
        rank = (rank_rows[0].get("r", 0) if rank_rows else 0) + 1
        return {"player": player, "rank": rank}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
