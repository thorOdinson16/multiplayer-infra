"""Shared common library for all multiplayer-infra services."""

from .tracing import setup_opentelemetry, tracer, instrument_fastapi

__all__ = ["setup_opentelemetry", "tracer", "instrument_fastapi"]
