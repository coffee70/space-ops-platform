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
    assert [event["event_type"] for event in response["raw_events"]] == ["tool.started", "tool.completed"]
    started_event = response["raw_events"][0]
    assert started_event["tool_call_id"] == "44444444-4444-4444-4444-444444444444"
    assert started_event["emitted_by"] == "tool-execution-service"
    assert started_event["payload"] == {
        "tool_name": "get_runtime_service",
        "category": "layer1_runtime",
        "read_write_classification": "read",
        "input_preview": {"service_slug": "agent-runtime-service"},
    }
    db.add.assert_called_once()
    db.flush.assert_called_once()


@pytest.mark.anyio
async def test_tool_execution_returns_started_then_failed_on_mapped_failure(monkeypatch) -> None:
    db = MagicMock()
    db.query.return_value.filter.return_value.one_or_none.return_value = SimpleNamespace(
        name="get_runtime_service",
        enabled=True,
        category="layer1_runtime",
        read_write_classification="read",
        requires_confirmation=False,
    )

    async def fake_execute(*_args, **_kwargs):
        raise RuntimeError("control-plane unavailable")

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

    assert response["status"] == "failed"
    assert [event["event_type"] for event in response["raw_events"]] == ["tool.started", "tool.failed"]
    assert all(event["tool_call_id"] == "44444444-4444-4444-4444-444444444444" for event in response["raw_events"])
    assert all(event["emitted_by"] == "tool-execution-service" for event in response["raw_events"])
    db.add.assert_called_once()
    db.flush.assert_called_once()


@pytest.mark.anyio
async def test_tool_execution_confirmation_required_does_not_start_or_persist_running_call() -> None:
    db = MagicMock()
    db.query.return_value.filter.return_value.one_or_none.return_value = SimpleNamespace(
        name="create_working_branch",
        enabled=True,
        category="write_future",
        read_write_classification="write",
        requires_confirmation=True,
    )

    response = await tool_execution.execute_tool(
        tool_execution.ToolExecutionRequest(
            conversation_id="11111111-1111-1111-1111-111111111111",
            agent_run_id="22222222-2222-2222-2222-222222222222",
            request_id="33333333-3333-3333-3333-333333333333",
            tool_call_id="44444444-4444-4444-4444-444444444444",
            tool_name="create_working_branch",
            input={},
            execution_mode="execute",
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

    assert response["status"] == "confirmation_required"
    assert response["raw_events"] == []
    db.add.assert_not_called()
    db.flush.assert_not_called()


@pytest.mark.anyio
async def test_tool_execution_pre_execution_rejections_do_not_emit_started_or_persist_running_call() -> None:
    # tool not found
    db_missing = MagicMock()
    db_missing.query.return_value.filter.return_value.one_or_none.return_value = None
    with pytest.raises(tool_execution.HTTPException) as missing_exc:
        await tool_execution.execute_tool(
            tool_execution.ToolExecutionRequest(
                conversation_id="11111111-1111-1111-1111-111111111111",
                agent_run_id="22222222-2222-2222-2222-222222222222",
                request_id="33333333-3333-3333-3333-333333333333",
                tool_call_id="44444444-4444-4444-4444-444444444444",
                tool_name="unknown_tool",
                input={},
                execution_mode="read_only",
            ),
            request=_request(
                {
                    "x-agent-run-id": "22222222-2222-2222-2222-222222222222",
                    "x-request-id": "33333333-3333-3333-3333-333333333333",
                    "x-tool-call-id": "44444444-4444-4444-4444-444444444444",
                }
            ),
            db=db_missing,
        )
    assert missing_exc.value.status_code == 404
    db_missing.add.assert_not_called()
    db_missing.flush.assert_not_called()

    # tool disabled
    db_disabled = MagicMock()
    db_disabled.query.return_value.filter.return_value.one_or_none.return_value = SimpleNamespace(
        name="get_runtime_service",
        enabled=False,
        category="layer1_runtime",
        read_write_classification="read",
        requires_confirmation=False,
    )
    with pytest.raises(tool_execution.HTTPException) as disabled_exc:
        await tool_execution.execute_tool(
            tool_execution.ToolExecutionRequest(
                conversation_id="11111111-1111-1111-1111-111111111111",
                agent_run_id="22222222-2222-2222-2222-222222222222",
                request_id="33333333-3333-3333-3333-333333333333",
                tool_call_id="44444444-4444-4444-4444-444444444444",
                tool_name="get_runtime_service",
                input={},
                execution_mode="read_only",
            ),
            request=_request(
                {
                    "x-agent-run-id": "22222222-2222-2222-2222-222222222222",
                    "x-request-id": "33333333-3333-3333-3333-333333333333",
                    "x-tool-call-id": "44444444-4444-4444-4444-444444444444",
                }
            ),
            db=db_disabled,
        )
    assert disabled_exc.value.status_code == 400
    db_disabled.add.assert_not_called()
    db_disabled.flush.assert_not_called()

    # write tool in read-only mode
    db_write = MagicMock()
    db_write.query.return_value.filter.return_value.one_or_none.return_value = SimpleNamespace(
        name="create_working_branch",
        enabled=True,
        category="write_future",
        read_write_classification="write",
        requires_confirmation=False,
    )
    with pytest.raises(tool_execution.HTTPException) as mode_exc:
        await tool_execution.execute_tool(
            tool_execution.ToolExecutionRequest(
                conversation_id="11111111-1111-1111-1111-111111111111",
                agent_run_id="22222222-2222-2222-2222-222222222222",
                request_id="33333333-3333-3333-3333-333333333333",
                tool_call_id="44444444-4444-4444-4444-444444444444",
                tool_name="create_working_branch",
                input={},
                execution_mode="read_only",
            ),
            request=_request(
                {
                    "x-agent-run-id": "22222222-2222-2222-2222-222222222222",
                    "x-request-id": "33333333-3333-3333-3333-333333333333",
                    "x-tool-call-id": "44444444-4444-4444-4444-444444444444",
                }
            ),
            db=db_write,
        )
    assert mode_exc.value.status_code == 403
    db_write.add.assert_not_called()
    db_write.flush.assert_not_called()
