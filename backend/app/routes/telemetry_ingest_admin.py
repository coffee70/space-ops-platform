"""Telemetry ingest-owned legacy write routes."""

from fastapi import APIRouter

from app.routes import _telemetry_handlers as handlers

router = APIRouter()

router.add_api_route("/data", handlers.ingest_data, methods=["POST"])
