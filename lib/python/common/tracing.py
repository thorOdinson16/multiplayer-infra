"""OpenTelemetry setup shared across all services."""

import os
import logging
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource

tracer = trace.get_tracer(__name__)

def setup_opentelemetry(service_name: str):
    otlp_endpoint = os.environ.get(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://otel-collector:4318",
    )
    resource = Resource.create({
        "service.name": service_name,
        "service.version": "1.0.0",
    })
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=f"{otlp_endpoint}/v1/traces")
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    logging.getLogger(__name__).info(f"OpenTelemetry initialised for {service_name}")

def instrument_fastapi(app):
    FastAPIInstrumentor.instrument_app(app)
