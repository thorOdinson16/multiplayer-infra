"""Replay Service -- Kafka consumer, MinIO archive, checkpoints, seekable replay API."""
import os
import json
import logging
import time
import threading
from io import BytesIO
from confluent_kafka import Consumer
from fastapi import FastAPI, HTTPException, Query
from minio import Minio
from minio.error import S3Error
import couchbase.cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions

app = FastAPI(title="replay-service")
logger = logging.getLogger("replay")

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import Resource
    resource = Resource.create({"service.name": "replay-service"})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318") + "/v1/traces")
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    FastAPIInstrumentor.instrument_app(app)
except Exception:
    pass

kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
minio_endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
minio_access = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
minio_secret = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
replay_bucket = "replays"
checkpoint_interval = int(os.environ.get("CHECKPOINT_INTERVAL", "300"))

minio_client = None
match_events = {}
match_checkpoints = {}


def get_minio():
    global minio_client
    if minio_client is None:
        minio_client = Minio(minio_endpoint, access_key=minio_access, secret_key=minio_secret, secure=False)
        if not minio_client.bucket_exists(replay_bucket):
            minio_client.make_bucket(replay_bucket)
    return minio_client


def get_couchbase():
    auth = PasswordAuthenticator("Administrator", "password")
    return couchbase.cluster.Cluster(f"couchbase://couchbase", ClusterOptions(auth))


def save_checkpoint(match_id, tick, events_snapshot):
    try:
        cluster = get_couchbase()
        bucket = cluster.bucket("matches")
        coll = bucket.default_collection()
        checkpoint_key = f"replay:checkpoint:{match_id}:{tick}"
        doc = {
            "type": "replay_checkpoint",
            "matchId": match_id,
            "tick": tick,
            "events": events_snapshot,
            "timestamp": time.time(),
        }
        coll.upsert(checkpoint_key, doc)
        cluster.close()
        logger.info(f"Checkpoint saved for match {match_id} at tick {tick}")
    except Exception as e:
        logger.error(f"Checkpoint save failed: {e}")


def load_latest_checkpoint(match_id):
    try:
        cluster = get_couchbase()
        bucket = cluster.bucket("matches")
        coll = bucket.default_collection()
        query = (
            f"SELECT tick, events FROM matches "
            f"WHERE type = 'replay_checkpoint' AND matchId = $match_id "
            f"ORDER BY tick DESC LIMIT 1"
        )
        result = cluster.query(query, match_id=match_id)
        for row in result:
            cluster.close()
            return row["tick"], row["events"]
        cluster.close()
    except Exception as e:
        logger.error(f"Checkpoint load failed: {e}")
    return None, None


def consume_events():
    consumer = Consumer({
        'bootstrap.servers': kafka_bootstrap, 'group.id': 'replay-service',
        'auto.offset.reset': 'earliest', 'enable.auto.commit': True,
    })
    consumer.subscribe(['match.events'])
    lifecycle_consumer = Consumer({
        'bootstrap.servers': kafka_bootstrap, 'group.id': 'replay-lifecycle',
        'auto.offset.reset': 'earliest', 'enable.auto.commit': True,
    })
    lifecycle_consumer.subscribe(['match.lifecycle'])
    while True:
        msg = consumer.poll(0.1)
        if msg and not msg.error():
            event = json.loads(msg.value().decode())
            mid = event.get("match_id")
            if mid:
                if mid not in match_events:
                    match_events[mid] = []
                    checkpoint_tick, checkpoint_events = load_latest_checkpoint(mid)
                    if checkpoint_tick is not None:
                        match_events[mid] = list(checkpoint_events)
                        match_checkpoints[mid] = checkpoint_tick
                        logger.info(f"Loaded checkpoint at tick {checkpoint_tick} for match {mid}")
                match_events[mid].append(event)
                tick = event.get("tick", 0)
                if tick > 0 and tick % checkpoint_interval == 0:
                    save_checkpoint(mid, tick, list(match_events[mid]))
        lmsg = lifecycle_consumer.poll(0.1)
        if lmsg and not lmsg.error():
            levent = json.loads(lmsg.value().decode())
            mid = levent.get("match_id")
            if levent.get("type") == "match.end" and mid:
                logger.info(f"Match {mid} ended -- archiving replay")
                finalize_replay(mid, match_events.get(mid, []))
                match_events.pop(mid, None)
                match_checkpoints.pop(mid, None)
        time.sleep(0.05)


def finalize_replay(match_id, events):
    if not events:
        logger.warning(f"No events for match {match_id}")
        return
    try:
        replay_data = json.dumps({
            "match_id": match_id, "event_count": len(events),
            "start_tick": events[0].get("tick", 0), "end_tick": events[-1].get("tick", 0),
            "events": events,
        })
        minio = get_minio()
        object_name = f"{match_id}/replay.json"
        data_bytes = replay_data.encode()
        minio.put_object(replay_bucket, object_name, BytesIO(data_bytes), len(data_bytes), content_type="application/json")
        logger.info(f"Archived replay for match {match_id} ({len(events)} events)")
    except Exception as e:
        logger.error(f"Archive failed: {e}")


@app.on_event("startup")
async def startup():
    t = threading.Thread(target=consume_events, daemon=True)
    t.start()
    logger.info("Replay service started")


@app.on_event("shutdown")
async def shutdown():
    logger.info("Replay service shutting down")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    return {"status": "ready"}


@app.get("/replay/{match_id}")
async def get_replay(match_id: str):
    try:
        minio = get_minio()
        response = minio.get_object(replay_bucket, f"{match_id}/replay.json")
        data = response.read().decode()
        response.close()
        response.release_conn()
        return json.loads(data)
    except S3Error:
        raise HTTPException(status_code=404, detail="Replay not found")


@app.get("/replay/{match_id}/seek")
async def seek_replay(match_id: str, tick: int = Query(0, ge=0)):
    try:
        minio = get_minio()
        response = minio.get_object(replay_bucket, f"{match_id}/replay.json")
        data = json.loads(response.read().decode())
        response.close()
        response.release_conn()
        events = data.get("events", [])
        seeked = [e for e in events if e.get("tick", 0) >= tick]
        return {"match_id": match_id, "tick": tick, "events": seeked[:100], "remaining": max(0, len(seeked) - 100)}
    except S3Error:
        raise HTTPException(status_code=404, detail="Replay not found")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
