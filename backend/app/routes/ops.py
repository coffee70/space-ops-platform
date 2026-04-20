"""Ops events (timeline) and feed health API routes."""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.database import get_db
from app.realtime.feed_health import get_feed_health_tracker
from app.models.schemas import OpsEventSchema, OpsEventsResponse
from app.services.source_stream_service import resolve_latest_stream_id
from app.services.ops_events_service import query_events

router = APIRouter()


def _parse_iso_datetime(value: Optional[str], field_name: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}") from exc


@router.get("/feed-status")
def get_feed_status(source_id: str = Query(...)):
    """Get feed health status for a source."""
    status = get_feed_health_tracker().get_status(source_id)
    return {
        "source_id": status.get("source_id", source_id),
        "connected": status.get("connected", False),
        "state": status.get("state", "disconnected"),
        "last_reception_time": status.get("last_reception_time"),
        "approx_rate_hz": status.get("approx_rate_hz"),
        "drop_count": status.get("drop_count"),
    }


@router.get("/events", response_model=OpsEventsResponse)
def get_timeline_events(
    source_id: str = Query(...),
    scope: str = "latest",
    stream_ids: list[str] = Query(default=[]),
    since: Optional[str] = None,
    until: Optional[str] = None,
    since_minutes: int = 60,
    until_minutes: Optional[int] = None,
    event_types: Optional[str] = None,
    entity_type: Optional[str] = None,
    channel_name: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    db=Depends(get_db),
):
    """Query ops events (timeline). since_minutes: lookback from now. until_minutes: optional end (minutes ago); default now."""
    now = datetime.now(timezone.utc)
    parsed_since = _parse_iso_datetime(since, "since")
    parsed_until = _parse_iso_datetime(until, "until")
    since_dt = parsed_since if parsed_since else now - timedelta(minutes=since_minutes)
    until_dt = parsed_until if parsed_until else (now - timedelta(minutes=until_minutes)) if until_minutes is not None else now
    if until_dt <= since_dt:
        raise HTTPException(status_code=400, detail="until must be after since")
    scoped_stream_ids = [stream_id for stream_id in stream_ids if stream_id]
    if scope == "streams" and not scoped_stream_ids:
        raise HTTPException(status_code=400, detail="stream_ids is required for streams scope")
    if scope not in {"latest", "streams", "date_range"}:
        raise HTTPException(status_code=400, detail="Invalid scope")
    if scope == "latest":
        scoped_stream_ids = [resolve_latest_stream_id(db, source_id)]

    types_list = [t.strip() for t in event_types.split(",") if t.strip()] if event_types else None

    events, total = query_events(
        db,
        source_id=source_id,
        stream_ids=scoped_stream_ids if scope in {"latest", "streams"} else None,
        since=since_dt,
        until=until_dt,
        event_types=types_list,
        entity_type=entity_type,
        channel_name=channel_name,
        limit=limit,
        offset=offset,
    )

    return OpsEventsResponse(
        events=[
            OpsEventSchema(
                id=str(e.id),
                source_id=e.source_id,
                stream_id=e.stream_id,
                event_time=e.event_time.isoformat(),
                event_type=e.event_type,
                severity=e.severity,
                summary=e.summary,
                entity_type=e.entity_type,
                entity_id=e.entity_id,
                payload=e.payload,
                created_at=e.created_at.isoformat(),
            )
            for e in events
        ],
        total=total,
    )
