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
async def test_deploy_preview_change_posts_kernel_schema_payload(monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []

    async def fake_cp_post(path: str, payload: dict, timeout: float) -> dict:
        assert timeout == 45.0
        calls.append((path, payload))
        return {
            "deployment_id": "preview-1",
            "unit_id": "mission-control-frontend-shell",
            "branch": "preview/test",
            "commit_sha": "abcdef1",
            "status": "healthy",
            "health_status": "passing",
            "logs_url": "/deployments/preview-1/logs",
            "registered": True,
            "target_unit_id": "mission-control-frontend-shell",
            "target_application_id": "telemetry",
        }

    monkeypatch.setattr(tool_execution, "_cp_post_with_timeout", fake_cp_post)

    response = await tool_execution._execute_mapped_tool(
        "deploy_preview_change",
        {
            "branch": "preview/test",
            "commit_sha": "abcdef1",
            "target_unit_id": "mission-control-frontend-shell",
            "target_application_id": "telemetry",
            "changed_files": ["ignored-by-kernel.tsx"],
            "summary": "ignored by kernel",
        },
        db=object(),
        trace={
            "conversation_id": "11111111-1111-1111-1111-111111111111",
            "agent_run_id": "22222222-2222-2222-2222-222222222222",
            "request_id": "33333333-3333-3333-3333-333333333333",
            "tool_call_id": "44444444-4444-4444-4444-444444444444",
        },
    )

    raw_events = response.pop("_raw_events")
    assert response["deployment_id"] == "preview-1"
    assert response["status"] == "healthy"
    assert [event["event_type"] for event in raw_events] == [
        "deployment.requested",
        "deployment.submitted",
        "preview.active",
        "deployment.health_passed",
    ]
    assert calls == [
        (
            "change-previews/deploy",
            {
                "branch": "preview/test",
                "commit_sha": "abcdef1",
                "target_unit_id": "mission-control-frontend-shell",
                "target_application_id": "telemetry",
                "conversation_id": "11111111-1111-1111-1111-111111111111",
                "agent_run_id": "22222222-2222-2222-2222-222222222222",
            },
        )
    ]
    assert "unit_id" not in calls[0][1]
    assert "application_id" not in calls[0][1]
    assert "request_id" not in calls[0][1]
    assert "tool_call_id" not in calls[0][1]
    assert "changed_files" not in calls[0][1]
    assert "summary" not in calls[0][1]


@pytest.mark.anyio
async def test_deployment_diagnostic_tools_call_control_plane_paths(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_cp_get(path: str, params: dict | None = None) -> dict:
        assert params is None
        calls.append(path)
        return {"deployment_id": "dep_1", "status": "healthy"}

    monkeypatch.setattr(tool_execution, "_cp_get", fake_cp_get)

    status = await tool_execution._execute_mapped_tool("get_deployment_status", {"deployment_id": "dep_1"}, db=object())
    logs = await tool_execution._execute_mapped_tool("get_deployment_logs", {"deployment_id": "dep_1"}, db=object())

    assert status == {"deployment_id": "dep_1", "status": "healthy"}
    assert logs == {"deployment_id": "dep_1", "status": "healthy"}
    assert calls == ["deployments/dep_1", "deployments/dep_1/logs"]


@pytest.mark.anyio
async def test_wait_for_deployment_returns_immediate_healthy(monkeypatch) -> None:
    async def fake_cp_get(path: str, params: dict | None = None) -> dict:
        assert path == "deployments/dep_healthy"
        return {
            "deployment_id": "dep_healthy",
            "status": "healthy",
            "health_status": "passing",
            "logs_url": "/deployments/dep_healthy/logs",
        }

    monkeypatch.setattr(tool_execution, "_cp_get", fake_cp_get)

    response = await tool_execution._execute_mapped_tool("wait_for_deployment", {"deployment_id": "dep_healthy"}, db=object())

    assert response["deployment_id"] == "dep_healthy"
    assert response["status"] == "healthy"
    assert response["terminal"] is True
    assert response["elapsed_seconds"] == 0
    assert "next_diagnostic_tools" not in response


@pytest.mark.anyio
async def test_wait_for_deployment_returns_failed_with_log_hint(monkeypatch) -> None:
    async def fake_cp_get(path: str, params: dict | None = None) -> dict:
        assert path == "deployments/dep_failed"
        return {
            "deployment_id": "dep_failed",
            "status": "failed",
            "health_status": "unknown",
            "failure_reason": "Docker Compose exit status 17",
            "logs_url": "/deployments/dep_failed/logs",
        }

    monkeypatch.setattr(tool_execution, "_cp_get", fake_cp_get)

    response = await tool_execution._execute_mapped_tool("wait_for_deployment", {"deployment_id": "dep_failed"}, db=object())

    assert response["status"] == "failed"
    assert response["terminal"] is True
    assert response["failure_reason"] == "Docker Compose exit status 17"
    assert response["next_diagnostic_tools"] == [
        {"tool_name": "get_deployment_logs", "input": {"deployment_id": "dep_failed"}},
    ]


@pytest.mark.anyio
async def test_wait_for_deployment_times_out_with_status_hint(monkeypatch) -> None:
    class FakeLoop:
        def __init__(self) -> None:
            self.current = 0.0

        def time(self) -> float:
            self.current += 2.0
            return self.current

    sleep_calls: list[int] = []

    async def fake_cp_get(path: str, params: dict | None = None) -> dict:
        assert path == "deployments/dep_building"
        return {"deployment_id": "dep_building", "status": "building", "health_status": "starting"}

    async def fake_sleep(seconds: int) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(tool_execution, "_cp_get", fake_cp_get)
    monkeypatch.setattr(tool_execution.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(tool_execution.asyncio, "get_running_loop", lambda: FakeLoop())

    response = await tool_execution._execute_mapped_tool(
        "wait_for_deployment",
        {"deployment_id": "dep_building", "timeout_seconds": 1, "poll_interval_seconds": 2},
        db=object(),
    )

    assert response["status"] == "building"
    assert response["terminal"] is False
    assert response["message"] == "Deployment did not reach a terminal state before timeout."
    assert response["next_diagnostic_tools"] == [
        {"tool_name": "get_deployment_status", "input": {"deployment_id": "dep_building"}},
    ]
    assert sleep_calls == []


@pytest.mark.anyio
async def test_wait_for_deployment_caps_timeout_and_bounds_poll_interval(monkeypatch) -> None:
    class FakeLoop:
        def __init__(self) -> None:
            self.times = iter([0.0, 0.0, 0.0, 181.0, 181.0])

        def time(self) -> float:
            return next(self.times)

    sleep_calls: list[int] = []

    async def fake_cp_get(path: str, params: dict | None = None) -> dict:
        return {"deployment_id": "dep_slow", "status": "building"}

    async def fake_sleep(seconds: int) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(tool_execution, "_cp_get", fake_cp_get)
    monkeypatch.setattr(tool_execution.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(tool_execution.asyncio, "get_running_loop", lambda: FakeLoop())

    response = await tool_execution._wait_for_deployment(
        {"deployment_id": "dep_slow", "timeout_seconds": 999, "poll_interval_seconds": 1},
    )

    assert response["terminal"] is False
    assert response["elapsed_seconds"] == 181
    assert sleep_calls == [2]


@pytest.mark.anyio
async def test_deploy_preview_change_adds_next_diagnostic_tools(monkeypatch) -> None:
    async def fake_cp_post(path: str, payload: dict, timeout: float) -> dict:
        return {
            "deployment_id": "dep_failed",
            "unit_id": "mission-control-frontend-shell",
            "branch": "preview/test",
            "status": "failed",
            "failure_reason": "Docker Compose exit status 17",
            "logs_url": "/deployments/dep_failed/logs",
        }

    monkeypatch.setattr(tool_execution, "_cp_post_with_timeout", fake_cp_post)

    response = await tool_execution._execute_mapped_tool(
        "deploy_preview_change",
        {"branch": "preview/test", "target_unit_id": "mission-control-frontend-shell"},
        db=object(),
    )

    assert response["status"] == "failed"
    assert response["next_diagnostic_tools"] == [
        {"tool_name": "get_deployment_logs", "input": {"deployment_id": "dep_failed"}},
    ]


@pytest.mark.anyio
async def test_deploy_preview_change_returns_queued_handle_without_polling(monkeypatch) -> None:
    get_calls: list[str] = []

    async def fake_cp_post(path: str, payload: dict, timeout: float) -> dict:
        return {
            "deployment_id": "dep_queued",
            "unit_id": "mission-control-frontend-shell",
            "branch": "preview/test",
            "status": "queued",
            "health_status": "pending",
            "logs_url": "/deployments/dep_queued/logs",
        }

    async def fake_cp_get(path: str, params: dict | None = None) -> dict:
        get_calls.append(path)
        return {"deployment_id": "dep_queued", "status": "healthy"}

    monkeypatch.setattr(tool_execution, "_cp_post_with_timeout", fake_cp_post)
    monkeypatch.setattr(tool_execution, "_cp_get", fake_cp_get)

    response = await tool_execution._execute_mapped_tool(
        "deploy_preview_change",
        {"branch": "preview/test", "target_unit_id": "mission-control-frontend-shell"},
        db=object(),
    )

    raw_events = response.pop("_raw_events")
    assert response["status"] == "queued"
    assert response["next_diagnostic_tools"] == [
        {"tool_name": "wait_for_deployment", "input": {"deployment_id": "dep_queued"}},
    ]
    assert [event["event_type"] for event in raw_events] == ["deployment.requested", "deployment.submitted"]
    assert get_calls == []


@pytest.mark.anyio
async def test_resolve_preview_deploy_target_maps_mission_control_ui(monkeypatch) -> None:
    async def fake_cp_get(path: str, params: dict | None = None) -> list[dict]:
        assert path == "registry/units"
        return [
            {
                "unitId": "derived-telemetry-service",
                "runtimeKind": "service",
                "sourcePath": "project/space-ops-platform/backend/services/derived-telemetry-service",
            },
            {
                "unitId": "mission-control-frontend-shell",
                "runtimeKind": "frontend_shell",
                "sourcePath": "project/space-ops-apps/mission-control-ui",
            },
        ]

    monkeypatch.setattr(tool_execution, "_cp_get", fake_cp_get)

    response = await tool_execution._execute_mapped_tool(
        "resolve_preview_deploy_target",
        {
            "branch": "preview/cyan-change",
            "changed_files": ["project/space-ops-apps/mission-control-ui/src/components/telemetry-detail-header.tsx"],
            "target_application_id": "telemetry",
        },
        db=object(),
    )

    assert response["status"] == "resolved"
    assert response["target_unit_id"] == "mission-control-frontend-shell"
    assert response["target_application_id"] == "telemetry"
    assert response["runtime_kind"] == "frontend_shell"
    assert response["source_path"] == "project/space-ops-apps/mission-control-ui"


@pytest.mark.anyio
async def test_revert_preview_change_posts_kernel_schema_payload(monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []

    async def fake_cp_post(path: str, payload: dict) -> dict:
        calls.append((path, payload))
        return {"deployment_id": "baseline-1"}

    monkeypatch.setattr(tool_execution, "_cp_post", fake_cp_post)

    response = await tool_execution._execute_mapped_tool(
        "revert_preview_change",
        {
            "target_unit_id": "mission-control-frontend-shell",
            "target_application_id": "telemetry",
            "baseline_branch": "main",
            "baseline_commit_sha": "1234567",
            "preview_deployment_id": "preview-1",
            "summary": "ignored by kernel",
        },
        db=object(),
        trace={
            "conversation_id": "11111111-1111-1111-1111-111111111111",
            "agent_run_id": "22222222-2222-2222-2222-222222222222",
            "request_id": "33333333-3333-3333-3333-333333333333",
            "tool_call_id": "44444444-4444-4444-4444-444444444444",
        },
    )

    assert response == {"deployment_id": "baseline-1"}
    assert calls == [
        (
            "change-previews/revert",
            {
                "target_unit_id": "mission-control-frontend-shell",
                "target_application_id": "telemetry",
                "baseline_branch": "main",
                "baseline_commit_sha": "1234567",
                "preview_deployment_id": "preview-1",
                "conversation_id": "11111111-1111-1111-1111-111111111111",
                "agent_run_id": "22222222-2222-2222-2222-222222222222",
            },
        )
    ]
    assert "unit_id" not in calls[0][1]
    assert "application_id" not in calls[0][1]
    assert "request_id" not in calls[0][1]
    assert "tool_call_id" not in calls[0][1]
    assert "summary" not in calls[0][1]


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
    removed_token_key = "approval" + "_token"
    assert removed_token_key not in response["output"]
    assert removed_token_key not in response["raw_events"][0]["payload"]
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
    )
    db = _Db(permission=permission)

    response = tool_execution.approve_tool_permission(
        "55555555-5555-4555-8555-555555555555",
        tool_execution.ToolPermissionApproveRequest(),
        db=db,  # type: ignore[arg-type]
    )

    assert response["status"] == "approved"
    assert response["raw_events"][0]["event_type"] == "tool.permission_approved"
    assert permission.status == "approved"
    assert db.flush_count == 1


def test_approve_unknown_permission_returns_404() -> None:
    db = _Db(permission=None)

    with pytest.raises(tool_execution.HTTPException) as exc_info:
        tool_execution.approve_tool_permission(
            "55555555-5555-4555-8555-555555555555",
            tool_execution.ToolPermissionApproveRequest(),
            db=db,  # type: ignore[arg-type]
        )

    assert exc_info.value.status_code == 404


@pytest.mark.parametrize("status", ["executed", "failed"])
def test_approve_resolved_permission_returns_409(status: str) -> None:
    permission = ToolPermissionRequest(
        id=uuid.UUID("55555555-5555-4555-8555-555555555555"),
        conversation_id=None,
        agent_run_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        request_id=uuid.UUID("33333333-3333-3333-3333-333333333333"),
        tool_call_id=uuid.UUID("44444444-4444-4444-4444-444444444444"),
        tool_name="deploy_preview_change",
        input_json={},
        redacted_input_json={},
        status=status,
        prompt_json={},
        mode_policy_json={},
        execution_mode="execute",
    )
    db = _Db(permission=permission)

    with pytest.raises(tool_execution.HTTPException) as exc_info:
        tool_execution.approve_tool_permission(
            "55555555-5555-4555-8555-555555555555",
            tool_execution.ToolPermissionApproveRequest(),
            db=db,  # type: ignore[arg-type]
        )

    assert exc_info.value.status_code == 409


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
    )
    db = _Db(permission=permission)

    response = tool_execution.deny_tool_permission(
        "55555555-5555-4555-8555-555555555555",
        tool_execution.ToolPermissionDenyRequest(),
        db=db,  # type: ignore[arg-type]
    )

    assert response["status"] == "denied"
    assert response["raw_events"][0]["event_type"] == "tool.permission_denied"
    assert permission.response_json["status"] == "permission_denied"
    assert db.flush_count == 1


def test_deny_unknown_permission_returns_404() -> None:
    db = _Db(permission=None)

    with pytest.raises(tool_execution.HTTPException) as exc_info:
        tool_execution.deny_tool_permission(
            "55555555-5555-4555-8555-555555555555",
            tool_execution.ToolPermissionDenyRequest(),
            db=db,  # type: ignore[arg-type]
        )

    assert exc_info.value.status_code == 404


@pytest.mark.anyio
async def test_permission_required_tool_executes_after_approval_with_permission_request_id(monkeypatch) -> None:
    mapped_called = False

    async def fake_mapped(*_args, **_kwargs):
        nonlocal mapped_called
        mapped_called = True
        return {"deployment_id": "preview-1"}

    monkeypatch.setattr(tool_execution, "_execute_mapped_tool", fake_mapped)
    permission = ToolPermissionRequest(
        id=uuid.UUID("55555555-5555-4555-8555-555555555555"),
        conversation_id=None,
        agent_run_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        request_id=uuid.UUID("33333333-3333-3333-3333-333333333333"),
        tool_call_id=uuid.UUID("44444444-4444-4444-4444-444444444444"),
        tool_name="deploy_preview_change",
        input_json={},
        redacted_input_json={},
        status="approved",
        prompt_json={},
        mode_policy_json={},
        execution_mode="execute",
    )
    db = _Db(tool=_tool(), permission=permission)

    response = await tool_execution.execute_tool(
        tool_execution.ToolExecutionRequest(
            conversation_id="11111111-1111-1111-1111-111111111111",
            agent_run_id="22222222-2222-2222-2222-222222222222",
            request_id="33333333-3333-3333-3333-333333333333",
            tool_call_id="44444444-4444-4444-4444-444444444444",
            tool_name="deploy_preview_change",
            input={"branch": "preview/test", "target_unit_id": "mission-control-frontend-shell"},
            execution_mode="execute",
            permission_request_id="55555555-5555-4555-8555-555555555555",
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

    assert mapped_called is True
    assert response["status"] == "completed"
    assert response["output"] == {"deployment_id": "preview-1"}
    assert permission.status == "executed"
    assert response["raw_events"][0]["event_type"] == "tool.permission_approved"


@pytest.mark.anyio
async def test_permission_required_tool_does_not_execute_denied_permission(monkeypatch) -> None:
    mapped_called = False

    async def fake_mapped(*_args, **_kwargs):
        nonlocal mapped_called
        mapped_called = True
        return {}

    monkeypatch.setattr(tool_execution, "_execute_mapped_tool", fake_mapped)
    permission = ToolPermissionRequest(
        id=uuid.UUID("55555555-5555-4555-8555-555555555555"),
        conversation_id=None,
        agent_run_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        request_id=uuid.UUID("33333333-3333-3333-3333-333333333333"),
        tool_call_id=uuid.UUID("44444444-4444-4444-4444-444444444444"),
        tool_name="deploy_preview_change",
        input_json={},
        redacted_input_json={},
        status="denied",
        prompt_json={},
        mode_policy_json={},
        execution_mode="execute",
        response_json={
            "status": "permission_denied",
            "reason": "user_denied",
            "message": "The user denied this tool call. No action was taken.",
        },
    )
    db = _Db(tool=_tool(), permission=permission)

    response = await tool_execution.execute_tool(
        tool_execution.ToolExecutionRequest(
            conversation_id="11111111-1111-1111-1111-111111111111",
            agent_run_id="22222222-2222-2222-2222-222222222222",
            request_id="33333333-3333-3333-3333-333333333333",
            tool_call_id="44444444-4444-4444-4444-444444444444",
            tool_name="deploy_preview_change",
            input={"branch": "preview/test", "target_unit_id": "mission-control-frontend-shell"},
            execution_mode="execute",
            permission_request_id="55555555-5555-4555-8555-555555555555",
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

    assert mapped_called is False
    assert response["status"] == "permission_denied"
    assert response["output"]["status"] == "permission_denied"
    assert response["raw_events"][0]["event_type"] == "tool.permission_denied"
