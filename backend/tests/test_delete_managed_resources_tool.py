from __future__ import annotations

import pytest

from app.routes.handlers import tool_execution


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
    await tool_execution._execute_mapped_tool(
        "delete_managed_resources",
        {"mode": "scope", "delete_scope_id": "scope-1", "older_than_minutes": 5},
        db=object(),
        trace=trace,
    )

    assert [path for path, _payload in calls] == [
        "internal/delete/managed-units",
        "internal/delete/code",
        "internal/delete/stale",
        "internal/delete/scopes/scope-1",
    ]
    assert calls[0][1]["unit_id"] == "sample-service"
    assert calls[0][1]["tool_call_id"] == trace["tool_call_id"]
    assert calls[3][1]["older_than_minutes"] == 5
