from __future__ import annotations

import pytest

from app.intelligence.events import REQUIRED_PAYLOAD_FIELDS, raw_event


def _payload(event_type: str) -> dict:
    return {field: f"{field}-value" for field in REQUIRED_PAYLOAD_FIELDS[event_type]}


def test_every_fixed_event_type_validates_required_payload_fields() -> None:
    for event_type in REQUIRED_PAYLOAD_FIELDS:
        event = raw_event(
            event_type=event_type,
            payload=_payload(event_type),
            emitted_by="test-service",
            tool_call_id="44444444-4444-4444-4444-444444444444" if event_type.startswith("tool.") else None,
        )
        assert event["event_type"] == event_type


def test_missing_payload_fields_are_rejected() -> None:
    with pytest.raises(ValueError, match="missing required payload"):
        raw_event(event_type="run.started", payload={"execution_mode": "read_only"}, emitted_by="agent-runtime-service")


def test_tool_events_without_tool_call_id_are_rejected() -> None:
    with pytest.raises(ValueError, match="requires tool_call_id"):
        raw_event(event_type="tool.completed", payload=_payload("tool.completed"), emitted_by="tool-execution-service")


def test_unsupported_event_types_are_rejected() -> None:
    with pytest.raises(ValueError, match="unsupported event type"):
        raw_event(event_type="tool.unknown", payload={}, emitted_by="test-service")
