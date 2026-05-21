from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock
import uuid

import pytest
from starlette.requests import Request

from app.models.intelligence import ToolCall, ToolPermissionRequest
from app.routes.handlers import tool_execution, tool_registry


def _request(headers: dict[str, str] | None = None) -> Request:
    header_items = []
    for key, value in (headers or {}).items():
        header_items.append((key.lower().encode("latin-1"), value.encode("latin-1")))
    return Request({"type": "http", "headers": header_items})


class _Query:
    def __init__(self, value: object | None):
        self.value = value

    def filter(self, *_args, **_kwargs):
        return self

    def one_or_none(self):
        return self.value


class _Db:
    def __init__(self, tool: object | None = None, permission: object | None = None, call: object | None = None):
        self.tool = tool
        self.permission = permission
        self.call = call
        self.added: list[object] = []
        self.flush_count = 0

    def query(self, model):
        if model is ToolPermissionRequest:
            return _Query(self.permission)
        if model is ToolCall:
            return _Query(self.call)
        return _Query(self.tool)

    def add(self, value: object):
        self.added.append(value)

    def flush(self):
        self.flush_count += 1


def _tool(**overrides):
    return SimpleNamespace(
        name="deploy_preview_change",
        enabled=True,
        category="deployment",
        read_write_classification="write",
        requires_confirmation=False,
        required_execution_mode="governed_execute",
        mode_policy_json={
            "read_only": "disabled",
            "suggest": "requires_permission",
            "execute": "requires_permission",
            "governed_execute": "enabled",
        },
        permission_prompt_json={},
        input_schema_json=tool_registry.TOOL_INPUT_SCHEMAS["deploy_preview_change"],
        **overrides,
    )


@pytest.mark.anyio
async def test_permission_required_tool_does_not_execute_before_approval(monkeypatch) -> None:
    mapped_called = False

    async def fake_mapped(*_args, **_kwargs):
        nonlocal mapped_called
        mapped_called = True
        return {}

    monkeypatch.setattr(tool_execution, "_execute_mapped_tool", fake_mapped)
    db = _Db(tool=_tool())

    response = await tool_execution.execute_tool(
        tool_execution.ToolExecutionRequest(
            conversation_id="11111111-1111-1111-1111-111111111111",
            agent_run_id="22222222-2222-2222-2222-222222222222",
            request_id="33333333-3333-3333-3333-333333333333",
            tool_call_id="44444444-4444-4444-4444-444444444444",
            tool_name="deploy_preview_change",
            input={"branch": "preview/test", "target_unit_id": "mission-control-frontend-shell"},
            execution_mode="execute",
        ),
        request=_request(
            {
                "x-agent-run-id": "22222222-2222-2222-2222-222222222222",
                "x-request-id": "33333333-3333-3333-3333-333333333333",
                "x-tool-call-id": "44444444-4444-4444-4444-444444444444",
            }
        ),
        db=db,  # type: ignore[arg-type]
    )

    assert response["status"] == "permission_required"
    assert response["raw_events"][0]["event_type"] == "tool.permission_required"
    assert response["output"]["permission_request_id"]
    assert response["output"]["approval_token"]
    assert mapped_called is False
    assert [type(item) for item in db.added] == [ToolCall, ToolPermissionRequest]
    assert db.added[0].status == "permission_required"
    assert db.added[1].status == "pending"


def test_approve_permission_marks_request_approved_and_emits_event() -> None:
    permission = ToolPermissionRequest(
        id=uuid.UUID("55555555-5555-4555-8555-555555555555"),
        conversation_id=None,
        agent_run_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        request_id=uuid.UUID("33333333-3333-3333-3333-333333333333"),
        tool_call_id=uuid.UUID("44444444-4444-4444-4444-444444444444"),
        tool_name="deploy_preview_change",
        input_json={},
        redacted_input_json={},
        status="pending",
        prompt_json={},
        mode_policy_json={},
        execution_mode="execute",
        approval_token="token",
    )
    db = _Db(permission=permission)

    response = tool_execution.approve_tool_permission(
        "55555555-5555-4555-8555-555555555555",
        tool_execution.ToolPermissionApproveRequest(approval_token="token"),
        db=db,  # type: ignore[arg-type]
    )

    assert response["status"] == "approved"
    assert response["raw_events"][0]["event_type"] == "tool.permission_approved"
    assert permission.status == "approved"
    assert db.flush_count == 1


def test_deny_permission_marks_request_denied_and_emits_event() -> None:
    permission = ToolPermissionRequest(
        id=uuid.UUID("55555555-5555-4555-8555-555555555555"),
        conversation_id=None,
        agent_run_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        request_id=uuid.UUID("33333333-3333-3333-3333-333333333333"),
        tool_call_id=uuid.UUID("44444444-4444-4444-4444-444444444444"),
        tool_name="deploy_preview_change",
        input_json={},
        redacted_input_json={},
        status="pending",
        prompt_json={},
        mode_policy_json={},
        execution_mode="execute",
        approval_token="token",
    )
    db = _Db(permission=permission)

    response = tool_execution.deny_tool_permission(
        "55555555-5555-4555-8555-555555555555",
        tool_execution.ToolPermissionDenyRequest(approval_token="token"),
        db=db,  # type: ignore[arg-type]
    )

    assert response["status"] == "denied"
    assert response["raw_events"][0]["event_type"] == "tool.permission_denied"
    assert permission.response_json["status"] == "permission_denied"
    assert db.flush_count == 1
