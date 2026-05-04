from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from starlette.requests import Request

from app.intelligence.redaction import redact
from app.models.intelligence import ToolCall
from app.routes.handlers import context_retrieval, tool_execution


def _request(headers: dict[str, str] | None = None) -> Request:
    header_items = []
    for key, value in (headers or {}).items():
        header_items.append((key.lower().encode("latin-1"), value.encode("latin-1")))
    return Request({"type": "http", "headers": header_items})


def test_context_retrieval_returns_raw_events_without_persisting_agent_events(monkeypatch) -> None:
    payload = context_retrieval.context_packet(
        {
            "conversation_id": "11111111-1111-1111-1111-111111111111",
            "agent_run_id": "22222222-2222-2222-2222-222222222222",
            "request_id": "33333333-3333-3333-3333-333333333333",
            "message": "Inspect runtime ownership",
            "retrieval_instructions": {"documents": False, "code": False, "platform": False, "tools": False},
        },
        request=_request(),
    )

    assert payload["raw_events"][0]["event_type"] == "context.resolved"


@pytest.mark.anyio
async def test_tool_execution_returns_raw_events_and_keeps_tool_call_record(monkeypatch) -> None:
    db = MagicMock()
    db.query.return_value.filter.return_value.one_or_none.return_value = SimpleNamespace(
        name="get_platform_service",
        enabled=True,
        category="platform_discovery",
        read_write_classification="read",
        requires_confirmation=False,
        required_execution_mode="read_only",
        input_schema_json={
            "type": "object",
            "properties": {"service_slug": {"type": "string"}},
            "required": ["service_slug"],
            "additionalProperties": False,
        },
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
            tool_name="get_platform_service",
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
        "tool_name": "get_platform_service",
        "category": "platform_discovery",
        "read_write_classification": "read",
        "input_preview": {"service_slug": "agent-runtime-service"},
    }
    db.add.assert_called_once()
    db.flush.assert_called_once()


@pytest.mark.anyio
async def test_tool_execution_returns_started_then_failed_on_mapped_failure(monkeypatch) -> None:
    db = MagicMock()
    db.query.return_value.filter.return_value.one_or_none.return_value = SimpleNamespace(
        name="get_platform_service",
        enabled=True,
        category="platform_discovery",
        read_write_classification="read",
        requires_confirmation=False,
        required_execution_mode="read_only",
        input_schema_json={
            "type": "object",
            "properties": {"service_slug": {"type": "string"}},
            "required": ["service_slug"],
            "additionalProperties": False,
        },
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
            tool_name="get_platform_service",
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
        name="get_platform_service",
        enabled=False,
        category="platform_discovery",
        read_write_classification="read",
        requires_confirmation=False,
        required_execution_mode="read_only",
        input_schema_json={"type": "object", "properties": {}, "additionalProperties": False},
    )
    with pytest.raises(tool_execution.HTTPException) as disabled_exc:
        await tool_execution.execute_tool(
            tool_execution.ToolExecutionRequest(
                conversation_id="11111111-1111-1111-1111-111111111111",
                agent_run_id="22222222-2222-2222-2222-222222222222",
                request_id="33333333-3333-3333-3333-333333333333",
                tool_call_id="44444444-4444-4444-4444-444444444444",
                tool_name="get_platform_service",
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
        required_execution_mode="execute",
        input_schema_json={"type": "object", "properties": {}, "additionalProperties": False},
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

    # write tool in suggest mode
    db_suggest = MagicMock()
    db_suggest.query.return_value.filter.return_value.one_or_none.return_value = SimpleNamespace(
        name="write_source_file",
        enabled=True,
        category="write_future",
        read_write_classification="write",
        requires_confirmation=False,
        required_execution_mode="execute",
        input_schema_json={"type": "object", "properties": {}, "additionalProperties": False},
    )
    with pytest.raises(tool_execution.HTTPException) as suggest_exc:
        await tool_execution.execute_tool(
            tool_execution.ToolExecutionRequest(
                conversation_id="11111111-1111-1111-1111-111111111111",
                agent_run_id="22222222-2222-2222-2222-222222222222",
                request_id="33333333-3333-3333-3333-333333333333",
                tool_call_id="44444444-4444-4444-4444-444444444444",
                tool_name="write_source_file",
                input={},
                execution_mode="suggest",
            ),
            request=_request(
                {
                    "x-agent-run-id": "22222222-2222-2222-2222-222222222222",
                    "x-request-id": "33333333-3333-3333-3333-333333333333",
                    "x-tool-call-id": "44444444-4444-4444-4444-444444444444",
                }
            ),
            db=db_suggest,
        )
    assert suggest_exc.value.status_code == 403
    db_suggest.add.assert_not_called()
    db_suggest.flush.assert_not_called()

    # schema validation: unknown, missing, wrong type
    db_schema = MagicMock()
    db_schema.query.return_value.filter.return_value.one_or_none.return_value = SimpleNamespace(
        name="get_platform_service",
        enabled=True,
        category="platform_discovery",
        read_write_classification="read",
        requires_confirmation=False,
        required_execution_mode="read_only",
        input_schema_json={
            "type": "object",
            "properties": {"service_slug": {"type": "string"}},
            "required": ["service_slug"],
            "additionalProperties": False,
        },
    )
    with pytest.raises(tool_execution.HTTPException) as unknown_exc:
        await tool_execution.execute_tool(
            tool_execution.ToolExecutionRequest(
                conversation_id="11111111-1111-1111-1111-111111111111",
                agent_run_id="22222222-2222-2222-2222-222222222222",
                request_id="33333333-3333-3333-3333-333333333333",
                tool_call_id="44444444-4444-4444-4444-444444444444",
                tool_name="get_platform_service",
                input={"service_slug": "agent-runtime-service", "extra": "nope"},
                execution_mode="read_only",
            ),
            request=_request(
                {
                    "x-agent-run-id": "22222222-2222-2222-2222-222222222222",
                    "x-request-id": "33333333-3333-3333-3333-333333333333",
                    "x-tool-call-id": "44444444-4444-4444-4444-444444444444",
                }
            ),
            db=db_schema,
        )
    assert unknown_exc.value.status_code == 400
    assert unknown_exc.value.detail["error_code"] == "tool_input_validation_failed"
    db_schema.add.assert_not_called()
    db_schema.flush.assert_not_called()

    with pytest.raises(tool_execution.HTTPException) as missing_required_exc:
        await tool_execution.execute_tool(
            tool_execution.ToolExecutionRequest(
                conversation_id="11111111-1111-1111-1111-111111111111",
                agent_run_id="22222222-2222-2222-2222-222222222222",
                request_id="33333333-3333-3333-3333-333333333333",
                tool_call_id="44444444-4444-4444-4444-444444444444",
                tool_name="get_platform_service",
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
            db=db_schema,
        )
    assert missing_required_exc.value.status_code == 400

    with pytest.raises(tool_execution.HTTPException) as wrong_type_exc:
        await tool_execution.execute_tool(
            tool_execution.ToolExecutionRequest(
                conversation_id="11111111-1111-1111-1111-111111111111",
                agent_run_id="22222222-2222-2222-2222-222222222222",
                request_id="33333333-3333-3333-3333-333333333333",
                tool_call_id="44444444-4444-4444-4444-444444444444",
                tool_name="get_platform_service",
                input={"service_slug": 123},
                execution_mode="read_only",
            ),
            request=_request(
                {
                    "x-agent-run-id": "22222222-2222-2222-2222-222222222222",
                    "x-request-id": "33333333-3333-3333-3333-333333333333",
                    "x-tool-call-id": "44444444-4444-4444-4444-444444444444",
                }
            ),
            db=db_schema,
        )
    assert wrong_type_exc.value.status_code == 400
    db_schema.add.assert_not_called()
    db_schema.flush.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "tool_name",
    [
        "scaffold_service",
        "write_source_file",
        "create_commit",
        "deploy_service_or_application",
        "trigger_document_reingestion",
        "create_working_branch",
    ],
)
async def test_write_tools_reject_read_only_mode(tool_name: str) -> None:
    db = MagicMock()
    db.query.return_value.filter.return_value.one_or_none.return_value = SimpleNamespace(
        name=tool_name,
        enabled=True,
        category="write_future",
        read_write_classification="write",
        requires_confirmation=False,
        required_execution_mode="execute",
        input_schema_json={"type": "object", "properties": {}, "additionalProperties": False},
    )
    with pytest.raises(tool_execution.HTTPException) as mode_exc:
        await tool_execution.execute_tool(
            tool_execution.ToolExecutionRequest(
                conversation_id="11111111-1111-1111-1111-111111111111",
                agent_run_id="22222222-2222-2222-2222-222222222222",
                request_id="33333333-3333-3333-3333-333333333333",
                tool_call_id="44444444-4444-4444-4444-444444444444",
                tool_name=tool_name,
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
            db=db,
        )
    assert mode_exc.value.status_code == 403
    db.add.assert_not_called()
    db.flush.assert_not_called()


_STRICT_EMPTY_INPUT = {"type": "object", "properties": {}, "additionalProperties": False}


def _tool_row(
    *,
    name: str,
    description: str,
    category: str,
    layer_target: str,
    read_write_classification: str,
    required_execution_mode: str,
    enabled: bool,
    requires_confirmation: bool,
) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        description=description,
        category=category,
        layer_target=layer_target,
        read_write_classification=read_write_classification,
        required_execution_mode=required_execution_mode,
        enabled=enabled,
        requires_confirmation=requires_confirmation,
        input_schema_json=_STRICT_EMPTY_INPUT,
    )


@pytest.mark.anyio
async def test_list_available_tools_returns_filtered_supported_metadata(monkeypatch) -> None:
    monkeypatch.setattr(
        tool_execution,
        "SUPPORTED_TOOL_NAMES",
        frozenset({"deploy_service_or_application", "get_platform_service", "list_available_tools"}),
    )
    rows = sorted(
        [
            _tool_row(
                name="deploy_service_or_application",
                description="Deploy managed unit.",
                category="deployment",
                layer_target="layer1",
                read_write_classification="write",
                required_execution_mode="execute",
                enabled=True,
                requires_confirmation=False,
            ),
            _tool_row(
                name="get_platform_service",
                description="Lookup service catalog entry.",
                category="platform_discovery",
                layer_target="layer1",
                read_write_classification="read",
                required_execution_mode="read_only",
                enabled=True,
                requires_confirmation=False,
            ),
            _tool_row(
                name="list_available_tools",
                description="Enumerate registered supported tools.",
                category="platform_discovery",
                layer_target="layer2",
                read_write_classification="read",
                required_execution_mode="read_only",
                enabled=True,
                requires_confirmation=False,
            ),
        ],
        key=lambda r: r.name,
    )

    list_tool_definition = SimpleNamespace(
        name="list_available_tools",
        description="Enumerate registered supported tools.",
        category="platform_discovery",
        layer_target="layer2",
        read_write_classification="read",
        required_execution_mode="read_only",
        enabled=True,
        requires_confirmation=False,
        input_schema_json=_STRICT_EMPTY_INPUT,
    )

    lookup_query = MagicMock()
    lookup_query.filter.return_value.one_or_none.return_value = list_tool_definition

    list_query = MagicMock()
    list_query.filter.return_value.order_by.return_value.all.return_value = rows

    db = MagicMock()
    db.query.side_effect = [lookup_query, list_query]

    response = await tool_execution.execute_tool(
        tool_execution.ToolExecutionRequest(
            conversation_id="11111111-1111-1111-1111-111111111111",
            agent_run_id="22222222-2222-2222-2222-222222222222",
            request_id="33333333-3333-3333-3333-333333333333",
            tool_call_id="44444444-4444-4444-4444-444444444444",
            tool_name="list_available_tools",
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
        db=db,
    )

    assert response["status"] == "completed"
    out = response["output"]
    tools = out["tools"]
    assert len(tools) == 3
    assert [t["name"] for t in tools] == ["deploy_service_or_application", "get_platform_service", "list_available_tools"]
    assert all(t["enabled"] is True for t in tools)

    dumped = json.dumps(response)
    for forbidden in ("Use tool registry", "/intelligence/", "http://", "https://"):
        assert forbidden not in dumped

    assert [e["event_type"] for e in response["raw_events"]] == ["tool.started", "tool.completed"]
    db.add.assert_called_once()
    db.flush.assert_called_once()
    call_arg = db.add.call_args.args[0]
    assert isinstance(call_arg, ToolCall)
    assert call_arg.tool_name == "list_available_tools"
    assert call_arg.output_json == redact(out)


@pytest.mark.anyio
async def test_trigger_document_reingestion_rejects_non_execute_modes() -> None:
    db = MagicMock()
    db.query.return_value.filter.return_value.one_or_none.return_value = SimpleNamespace(
        name="trigger_document_reingestion",
        enabled=True,
        category="documents",
        read_write_classification="write",
        requires_confirmation=False,
        required_execution_mode="execute",
        input_schema_json={
            "type": "object",
            "properties": {"document_id": {"type": "string", "format": "uuid"}},
            "required": ["document_id"],
            "additionalProperties": False,
        },
    )
    with pytest.raises(tool_execution.HTTPException) as exc:
        await tool_execution.execute_tool(
            tool_execution.ToolExecutionRequest(
                conversation_id="11111111-1111-1111-1111-111111111111",
                agent_run_id="22222222-2222-2222-2222-222222222222",
                request_id="33333333-3333-3333-3333-333333333333",
                tool_call_id="44444444-4444-4444-4444-444444444444",
                tool_name="trigger_document_reingestion",
                input={"document_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"},
                execution_mode="suggest",
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
    assert exc.value.status_code == 403
    db.add.assert_not_called()
