"""Ops-events-owned routes."""

from fastapi import APIRouter

from app.routes.ops import get_timeline_events

router = APIRouter()
router.add_api_route("/events", get_timeline_events, methods=["GET"])
