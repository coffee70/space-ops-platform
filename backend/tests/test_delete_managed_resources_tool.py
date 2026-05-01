from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from starlette.requests import Request

from app.models.intelligence import ToolCall
from app.routes.handlers import tool_registry
from app.routes.handlers import tool_execution


def _request(headers: dict[str, str] | None = None) -> Request:
    header_items = []
    for key, value in (headers or {}).items():
        header_items.append((key.lower().encode("latin-1"), value.encode("latin-1")))
    return Request({"type": "http", "headers": header_items})


@pytest.mark.anyio
async def test_delete_managed_resources_maps_modes_to_control_plane(monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []

    async def fake_cp_post(path: str, payload: dict) -> dict:
        calls.append((path, payload))
        return {"delete_id": "delete_test", "deleted": [], "removed": []}

    monkeypatch.setattr(tool_execution, "_cp_post", fake_cp_post)
    trace = {
        "conversation_id": "11111111-1111-1111-1111-111111111111",
        "agent_run_id": "22222222-2222-2222-2222-222222222222",
        "request_id": "33333333-3333-3333-3333-333333333333",
        "tool_call_id": "44444444-4444-4444-4444-444444444444",
    }

    await tool_execution._execute_mapped_tool(
        "delete_managed_resources",
        {"mode": "managed_unit", "unit_id": "sample-service"},
        db=object(),
        trace=trace,
    )
    await tool_execution._execute_mapped_tool(
        "delete_managed_resources",
        {"mode": "code", "branch": "feature/delete-me"},
        db=object(),
        trace=trace,
    )
    await tool_execution._execute_mapped_tool(
        "delete_managed_resources",
        {"mode": "stale", "older_than_minutes": 120},
        db=object(),
        trace=trace,
    )

    assert [path for path, _payload in calls] == [
        "internal/delete/managed-units",
        "internal/delete/code",
        "internal/delete/stale",
    ]
    assert calls[0][1]["unit_id"] == "sample-service"
    assert calls[0][1]["tool_call_id"] == trace["tool_call_id"]
    assert calls[2][1]["older_than_minutes"] == 120


@pytest.mark.anyio
async def test_delete_managed_resources_execute_tool_records_call_and_events(monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []

    async def fake_cp_post(path: str, payload: dict) -> dict:
        calls.append((path, payload))
        return {
            "delete_id": "delete_test",
            "deleted": [{"resource_type": "managed_unit", "resource_id": "sample-service"}],
            "removed": [],
            "already_absent": [],
            "skipped": [],
            "refused": [],
            "errors": [],
            "warnings": [],
        }

    monkeypatch.setattr(tool_execution, "_cp_post", fake_cp_post)
    db = MagicMock()
    db.query.return_value.filter.return_value.one_or_none.return_value = SimpleNamespace(
        name="delete_managed_resources",
        enabled=True,
        category="resource_delete",
        read_write_classification="destructive_write",
        requires_confirmation=False,
        required_execution_mode="execute",
        input_schema_json=tool_registry.TOOL_INPUT_SCHEMAS["delete_managed_resources"],
    )

    response = await tool_execution.execute_tool(
        tool_execution.ToolExecutionRequest(
            conversation_id="11111111-1111-1111-1111-111111111111",
            agent_run_id="22222222-2222-2222-2222-222222222222",
            request_id="33333333-3333-3333-3333-333333333333",
            tool_call_id="44444444-4444-4444-4444-444444444444",
            tool_name="delete_managed_resources",
            input={"mode": "managed_unit", "unit_id": "sample-service"},
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

    assert response["status"] == "completed"
    assert [event["event_type"] for event in response["raw_events"]] == ["tool.started", "tool.completed"]
    assert calls == [
        (
            "internal/delete/managed-units",
            {
                "unit_id": "sample-service",
                "conversation_id": "11111111-1111-1111-1111-111111111111",
                "agent_run_id": "22222222-2222-2222-2222-222222222222",
                "request_id": "33333333-3333-3333-3333-333333333333",
                "tool_call_id": "44444444-4444-4444-4444-444444444444",
            },
        )
    ]
    db.add.assert_called_once()
    assert isinstance(db.add.call_args.args[0], ToolCall)
    assert db.add.call_args.args[0].tool_name == "delete_managed_resources"
    assert db.add.call_args.args[0].status == "completed"
