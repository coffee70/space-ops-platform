"""Event writing helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.models.intelligence import AgentEvent
from app.intelligence.redaction import redact


ALLOWED_EVENT_TYPES = {
    "run.started",
    "run.completed",
    "run.failed",
    "context.requested",
    "context.resolved",
    "context.failed",
    "tool.started",
    "tool.completed",
    "tool.failed",
    "document.uploaded",
    "document.ingestion_started",
    "document.ingestion_completed",
    "document.ingestion_failed",
    "code.index_started",
    "code.index_completed",
    "code.index_failed",
    "navigation.requested",
    "message.delta",
    "message.completed",
    "error",
}


def raw_event(
    *,
    event_type: str,
    payload: dict[str, Any],
    emitted_by: str,
    tool_call_id: str | None = None,
) -> dict[str, Any]:
    if event_type not in ALLOWED_EVENT_TYPES:
        raise ValueError(f"unsupported event type: {event_type}")
    return {
        "event_type": event_type,
        "payload": redact(payload),
        "emitted_by": emitted_by,
        "tool_call_id": tool_call_id,
    }


def emit_event(
    db: Session,
    *,
    event_type: str,
    payload: dict[str, Any],
    conversation_id: str | None,
    agent_run_id: str,
    request_id: str,
    sequence: int,
    emitted_by: str,
    tool_call_id: str | None = None,
) -> AgentEvent:
    if event_type not in ALLOWED_EVENT_TYPES:
        raise ValueError(f"unsupported event type: {event_type}")
    event = AgentEvent(
        conversation_id=conversation_id,
        agent_run_id=agent_run_id,
        request_id=request_id,
        tool_call_id=tool_call_id,
        sequence=sequence,
        emitted_by=emitted_by,
        event_type=event_type,
        payload_json=redact(payload),
        created_at=datetime.now(timezone.utc),
    )
    db.add(event)
    db.flush()
    return event
