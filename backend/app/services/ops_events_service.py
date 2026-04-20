"""Service for writing and querying ops_events (unified timeline)."""

import logging
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import and_, desc, func, or_, select
from sqlalchemy.orm import Session

from app.models.telemetry import OpsEvent

logger = logging.getLogger(__name__)

ENTITY_TYPES = ("telemetry_channel", "alert", "system", "operator_action")


def _stream_scope_clause(stream_ids: list[str]):
    """Include stream-scoped events plus the shared feed-status event."""
    return or_(
        OpsEvent.stream_id.in_(stream_ids),
        and_(
            OpsEvent.stream_id.is_(None),
            OpsEvent.event_type == "system.feed_status",
        ),
    )


def write_event(
    db: Session,
    *,
    source_id: str,
    stream_id: Optional[str] = None,
    event_time: datetime,
    event_type: str,
    severity: str,
    summary: str,
    entity_type: str,
    entity_id: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
) -> OpsEvent:
    """Write a single ops event. Returns the created event."""
    event = OpsEvent(
        id=uuid4(),
        source_id=source_id,
        stream_id=stream_id,
        event_time=event_time,
        event_type=event_type,
        severity=severity,
        summary=summary,
        entity_type=entity_type,
        entity_id=entity_id,
        payload=payload,
    )
    db.add(event)
    db.flush()
    return event


def query_events(
    db: Session,
    *,
    source_id: str,
    stream_ids: Optional[list[str]] = None,
    since: datetime,
    until: Optional[datetime] = None,
    event_types: Optional[list[str]] = None,
    entity_type: Optional[str] = None,
    channel_name: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[OpsEvent], int]:
    """Query ops events by time window and optional filters. Returns (events, total_count)."""
    stmt = (
        select(OpsEvent)
        .where(OpsEvent.source_id == source_id)
        .where(OpsEvent.event_time >= since)
    )
    if stream_ids:
        stmt = stmt.where(_stream_scope_clause(stream_ids))
    if until is not None:
        stmt = stmt.where(OpsEvent.event_time <= until)
    if event_types:
        stmt = stmt.where(OpsEvent.event_type.in_(event_types))
    if entity_type:
        stmt = stmt.where(OpsEvent.entity_type == entity_type)
    if channel_name:
        stmt = stmt.where(OpsEvent.entity_id == channel_name)

    count_stmt = select(func.count()).select_from(OpsEvent).where(
        OpsEvent.source_id == source_id,
        OpsEvent.event_time >= since,
    )
    if stream_ids:
        count_stmt = count_stmt.where(_stream_scope_clause(stream_ids))
    if until is not None:
        count_stmt = count_stmt.where(OpsEvent.event_time <= until)
    if event_types:
        count_stmt = count_stmt.where(OpsEvent.event_type.in_(event_types))
    if entity_type:
        count_stmt = count_stmt.where(OpsEvent.entity_type == entity_type)
    if channel_name:
        count_stmt = count_stmt.where(OpsEvent.entity_id == channel_name)
    total_count = db.execute(count_stmt).scalar() or 0

    stmt = stmt.order_by(desc(OpsEvent.event_time)).limit(limit).offset(offset)
    rows = db.execute(stmt).scalars().all()
    return list(rows), total_count
