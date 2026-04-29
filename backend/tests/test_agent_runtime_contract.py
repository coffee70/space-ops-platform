from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from starlette.requests import Request

from app.routes.handlers import context_retrieval, tool_execution


def _request(headers: dict[str, str] | None = None) -> Request:
    header_items = []
    for key, value in (headers or {}).items():
        header_items.append((key.lower().encode("latin-1"), value.encode("latin-1")))
    return Request({"type": "http", "headers": header_items})


def test_context_retrieval_returns_raw_events_without_persisting_agent_events(monkeypatch) -> None:
    db = MagicMock()
    db.query.return_value.order_by.return_value.limit.return_value.all.return_value = []

    payload = context_retrieval.context_packet(
        {
            "conversation_id": "11111111-1111-1111-1111-111111111111",
            "agent_run_id": "22222222-2222-2222-2222-222222222222",
            "request_id": "33333333-3333-3333-3333-333333333333",
            "message": "Inspect runtime ownership",
            "retrieval_instructions": {"documents": False, "code": False, "platform": False, "tools": False},
        },
        request=_request(),
        db=db,
    )

    assert payload["raw_events"][0]["event_type"] == "context.resolved"
    db.add.assert_not_called()


@pytest.mark.anyio
async def test_tool_execution_returns_raw_events_and_keeps_tool_call_record(monkeypatch) -> None:
    db = MagicMock()
    db.query.return_value.filter.return_value.one_or_none.return_value = SimpleNamespace(
        name="get_runtime_service",
        enabled=True,
        category="layer1_runtime",
        read_write_classification="read",
        requires_confirmation=False,
    )

    async def fake_execute(*_args, **_kwargs):
        return {"service_slug": "agent-runtime-service"}

    monkeypatch.setattr(tool_execution, "_execute_mapped_tool", fake_execute)

    response = await tool_execution.execute_tool(
        tool_execution.ToolExecutionRequest(
            conversation_id="11111111-1111-1111-1111-111111111111",
            agent_run_id="22222222-2222-2222-2222-222222222222",
            request_id="33333333-3333-3333-3333-333333333333",
            tool_call_id="44444444-4444-4444-4444-444444444444",
            tool_name="get_runtime_service",
            input={"service_slug": "agent-runtime-service"},
            execution_mode="read_only",
        ),
        request=_request(
            {
                "x-agent-run-id": "22222222-2222-2222-2222-222222222222",
                "x-request-id": "33333333-3333-3333-3333-333333333333",
                "x-tool-call-id": "44444444-4444-4444-4444-444444444444",
            }
        ),
        db=db,
    )

    assert response["status"] == "completed"
    assert [event["event_type"] for event in response["raw_events"]] == ["tool.completed"]
    db.add.assert_called_once()
    db.flush.assert_called_once()
