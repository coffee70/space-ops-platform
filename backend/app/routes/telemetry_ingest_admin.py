"""Telemetry ingest-owned legacy write routes."""

from fastapi import APIRouter

from app.routes.handlers import telemetry_ingest_admin as handlers

router = APIRouter()

router.add_api_route("/data", handlers.ingest_data, methods=["POST"])
