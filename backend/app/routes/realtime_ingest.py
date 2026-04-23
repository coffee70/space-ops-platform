"""Realtime ingest-owned routes."""

from fastapi import APIRouter

from app.routes.realtime import ingest_realtime

router = APIRouter()
router.add_api_route("/ingest", ingest_realtime, methods=["POST"])
