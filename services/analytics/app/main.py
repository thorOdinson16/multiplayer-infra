"""Analytics Service -- Kafka consumer, Prometheus metrics."""
import os
import json
import logging
import threading
import time
from confluent_kafka import Consumer
from fastapi import FastAPI
from prometheus_client import Gauge, Counter, Histogram, generate_latest
from starlette.responses import Response

app = FastAPI(title="analytics-service")
logger = logging.getLogger("analytics")

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import Resource
    resource = Resource.create({"service.name": "analytics-service"})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318") + "/v1/traces")
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    FastAPIInstrumentor.instrument_app(app)
except Exception:
    pass

kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

active_matches = Gauge("analytics_active_matches", "Number of active matches")
total_players = Counter("analytics_total_players", "Total players seen")
movement_events = Counter("analytics_movement_events_total", "Movement events")
kill_events = Counter("analytics_kill_events_total", "Kill events")
session_duration = Histogram("analytics_session_duration_seconds", "Session duration", buckets=[30, 60, 120, 180, 240, 300])
consumer_lag = Gauge("analytics_consumer_lag", "Kafka consumer lag")


def consume_telemetry():
    consumer = Consumer({
        'bootstrap.servers': kafka_bootstrap, 'group.id': 'analytics-service',
        'auto.offset.reset': 'earliest', 'enable.auto.commit': True,
    })
    consumer.subscribe(['match.telemetry'])
    active_sessions = {}
    while True:
        msg = consumer.poll(0.5)
        if msg and not msg.error():
            try:
                event = json.loads(msg.value().decode())
                event_type = event.get("type", "")
                if event_type == "move":
                    movement_events.inc()
                elif event_type == "kill":
                    kill_events.inc()
                elif event_type == "session_start":
                    active_sessions[event.get("player_id")] = time.time()
                    total_players.inc()
                    active_matches.inc()
                elif event_type == "session_end":
                    pid = event.get("player_id")
                    if pid in active_sessions:
                        dur = time.time() - active_sessions.pop(pid)
                        session_duration.observe(dur)
                    active_matches.dec()
            except Exception as e:
                logger.error(f"Telemetry error: {e}")
        time.sleep(0.1)


@app.on_event("startup")
async def startup():
    t = threading.Thread(target=consume_telemetry, daemon=True)
    t.start()
    logger.info("Analytics service started")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    return {"status": "ready"}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type="text/plain")


@app.on_event("shutdown")
async def shutdown():
    logger.info("Analytics service shut down cleanly")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
