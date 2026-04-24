"""Telemetry ingest feed-health routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.feed_health_service import get_feed_health_status, list_feed_health_statuses, refresh_feed_health_states

router = APIRouter()


@router.get("/feed-health")
def get_feed_health(
    source_id: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    """Read feed health from the ingest-owned durable store."""
    refresh_feed_health_states(db)
    if source_id:
        return get_feed_health_status(db, source_id)
    return {"items": list_feed_health_statuses(db)}
