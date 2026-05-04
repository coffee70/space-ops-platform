"""Event writing helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.models.intelligence import AgentEvent
from app.intelligence.redaction import redact


REQUIRED_PAYLOAD_FIELDS = {
    "run.started": {"execution_mode", "message_id", "user_message_preview"},
    "run.completed": {"assistant_message_id", "tool_call_count"},
    "run.failed": {"error_code", "message"},
    "context.requested": {"retrieval_plan", "limits"},
    "context.resolved": {"context_packet_id", "document_chunk_count", "code_chunk_count", "platform_metadata_bytes", "tool_definition_count", "truncated"},
    "context.failed": {"error_code", "message"},
    "tool.started": {"tool_name", "category", "read_write_classification", "input_preview"},
    "tool.completed": {"tool_name", "status", "result_preview", "duration_ms"},
    "tool.failed": {"tool_name", "error_code", "message", "duration_ms"},
    "document.uploaded": {"document_id", "title", "document_type", "content_hash"},
    "document.ingestion_started": {"document_id", "chunking_strategy", "embedding_model"},
    "document.ingestion_completed": {"document_id", "chunk_count", "embedding_model", "duration_ms"},
    "document.ingestion_failed": {"document_id", "error_code", "message"},
    "code.index_started": {"repository", "branch", "commit_sha"},
    "code.index_completed": {"repository", "branch", "commit_sha", "file_count", "chunk_count", "duration_ms"},
    "code.index_failed": {"repository", "branch", "error_code", "message"},
    "navigation.requested": {"action", "application_id", "route_path"},
    "message.delta": {"text_delta"},
    "message.completed": {"message_id", "content_preview"},
    "error": {"error_code", "message", "source"},
}


ALLOWED_EVENT_TYPES = set(REQUIRED_PAYLOAD_FIELDS)


def validate_event(event_type: str, payload: dict[str, Any], tool_call_id: str | None = None) -> None:
    if event_type not in ALLOWED_EVENT_TYPES:
        raise ValueError(f"unsupported event type: {event_type}")
    missing = sorted(REQUIRED_PAYLOAD_FIELDS[event_type] - payload.keys())
    if missing:
        raise ValueError(f"event {event_type} missing required payload field(s): {', '.join(missing)}")
    if event_type.startswith("tool.") and not tool_call_id:
        raise ValueError(f"event {event_type} requires tool_call_id")


def raw_event(
    *,
    event_type: str,
    payload: dict[str, Any],
    emitted_by: str,
    tool_call_id: str | None = None,
) -> dict[str, Any]:
    validate_event(event_type, payload, tool_call_id)
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
    validate_event(event_type, payload, tool_call_id)
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
