from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.intelligence.tool_validation import ToolInputValidationError, validate_tool_input

from app.routes.handlers import tool_registry


def test_tool_registry_handler_exports_definitions_routes_without_execute_patch() -> None:
    assert hasattr(tool_registry, "list_tools")
    assert hasattr(tool_registry, "get_tool")
    assert hasattr(tool_registry, "seed_tools")
    assert not hasattr(tool_registry, "execute_tool")
    assert not hasattr(tool_registry, "patch_tool")


def test_supported_tool_inventory_matches_input_schemas() -> None:
    missing = tool_registry.SUPPORTED_TOOL_NAMES.difference(tool_registry.TOOL_INPUT_SCHEMAS.keys())
    assert not missing


def test_supported_registry_has_exactly_thirty_five_tools() -> None:
    assert len(tool_registry.SUPPORTED_TOOL_NAMES) == 35


def test_write_classification_tools_are_supported() -> None:
    write_tools = {"trigger_document_reingestion", "create_working_branch", "scaffold_service", "write_source_file", "create_commit", "deploy_service_or_application", "deploy_preview_change", "revert_preview_change", "delete_managed_resources"}
    assert write_tools.issubset(tool_registry.SUPPORTED_TOOL_NAMES)


def test_write_tools_have_strict_non_empty_schemas_where_applicable() -> None:
    assert tool_registry.TOOL_INPUT_SCHEMAS["create_working_branch"]["properties"]
    assert tool_registry.TOOL_INPUT_SCHEMAS["write_source_file"]["required"] == ["branch", "path", "content"]
    assert tool_registry.TOOL_INPUT_SCHEMAS["read_source_file"]["required"] == ["branch", "path"]
    assert tool_registry.TOOL_INPUT_SCHEMAS["resolve_preview_deploy_target"]["required"] == ["branch", "changed_files"]


def test_tool_input_validation_accepts_valid_nested_objects() -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS["scaffold_service"]

    validate_tool_input(
        schema,
        {
            "template_id": "python-fastapi-service",
            "unit_id": "phase3-test-fixture-service",
            "display_name": "Phase 3 Test Fixture Service",
            "discovery": {"health_path": "/health"},
        },
    )


def test_get_telemetry_schema_requires_source_id_and_rejects_additional_properties() -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS["get_telemetry_schema"]
    assert schema["required"] == ["source_id"]
    assert schema.get("additionalProperties") is False
    validate_tool_input(schema, {"source_id": "vehicle-main"})
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {})
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {"source_id": "x", "extra": True})


def test_get_telemetry_schema_backing_documents_query_service_inventory() -> None:
    assert tool_registry.GET_TELEMETRY_SCHEMA_TOOL_BACKING == (
        "telemetry-query-service",
        "GET /telemetry/inventory?source_id={source_id}",
    )


def test_query_recent_telemetry_requires_source_id_and_name() -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS["query_recent_telemetry"]
    assert schema["required"] == ["source_id", "name"]
    assert schema.get("additionalProperties") is False
    validate_tool_input(schema, {"source_id": "simulator", "name": "battery_voltage"})
    validate_tool_input(schema, {"source_id": "simulator", "name": "battery_voltage", "limit": 25})
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {"name": "x"})
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {"source_id": "sim", "name": "n", "x": True})


def test_query_recent_telemetry_backing_documents_recent_endpoint() -> None:
    assert tool_registry.QUERY_RECENT_TELEMETRY_TOOL_BACKING == (
        "telemetry-query-service",
        "GET /telemetry/{name}/recent?source_id={source_id}&limit={limit}",
    )


def test_call_platform_http_get_schema_is_strict_and_validates_expected_status() -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS["call_platform_http_get"]
    assert schema["required"] == ["path"]
    assert schema.get("additionalProperties") is False
    validate_tool_input(schema, {"path": "/telemetry/health"})
    validate_tool_input(schema, {"path": "/telemetry/health", "expected_status": 200})
    validate_tool_input(schema, {"path": "/telemetry/health", "expected_status": [200, 204]})
    validate_tool_input(
        schema,
        {
            "path": "/telemetry/health",
            "query": {"source_id": "simulator", "limit": 10, "latest": True},
            "headers": {"x-request-id": "req-1"},
            "timeout_ms": 30000,
            "max_response_bytes": 131072,
        },
    )
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {})
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {"path": "/telemetry/health", "method": "POST"})
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {"path": "/telemetry/health", "timeout_ms": 99})
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {"path": "/telemetry/health", "max_response_bytes": 131073})


@pytest.mark.parametrize("tool_name", ["get_deployment_status", "get_deployment_logs", "wait_for_deployment", "run_deployment_validation", "get_deployment_validation"])
def test_deployment_diagnostic_tools_require_deployment_id(tool_name: str) -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS[tool_name]
    assert schema["required"] == ["deployment_id"]
    assert schema.get("additionalProperties") is False
    validate_tool_input(schema, {"deployment_id": "dep_1"})
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {})


def test_wait_for_deployment_schema_bounds_wait_options() -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS["wait_for_deployment"]
    validate_tool_input(schema, {"deployment_id": "dep_1", "timeout_seconds": 180, "poll_interval_seconds": 30})
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {"deployment_id": "dep_1", "timeout_seconds": 181})
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {"deployment_id": "dep_1", "poll_interval_seconds": 1})


def test_delete_managed_resources_schema_is_strict_and_destructive() -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS["delete_managed_resources"]
    assert schema["required"] == ["mode"]
    assert schema["additionalProperties"] is False
    assert schema["properties"]["mode"]["enum"] == ["managed_unit", "code", "stale"]
    assert set(schema["properties"]) == {
        "mode",
        "unit_id",
        "deployment_id",
        "branch",
        "paths",
        "older_than_minutes",
        "include_code",
        "include_runtime",
        "include_registry",
        "include_intelligence_records",
    }


def test_phase3_write_deploy_delete_tools_remain_metadata_only_and_discoverable() -> None:
    db = MagicMock()
    db.query.return_value.filter.return_value.one_or_none.return_value = None
    db.query.return_value.count.return_value = 0

    tool_registry.seed_tools(db=db)
    seeded = {tool.name: tool for tool in (call.args[0] for call in db.add.call_args_list)}

    assert seeded["create_working_branch"].backing_service == "control-plane"
    assert seeded["deploy_service_or_application"].backing_api == "POST /deployments"
    assert seeded["deploy_preview_change"].backing_api == "POST /change-previews/deploy"
    assert seeded["resolve_preview_deploy_target"].backing_api == "GET /registry/units"
    assert seeded["get_deployment_status"].backing_api == "GET /deployments/{deployment_id}"
    assert seeded["get_deployment_logs"].backing_api == "GET /deployments/{deployment_id}/logs"
    assert seeded["wait_for_deployment"].backing_api == "GET /deployments/{deployment_id}"
    assert seeded["run_deployment_validation"].backing_api == "POST /validation/deployments/{deployment_id}/run"
    assert seeded["get_deployment_validation"].backing_api == "GET /validation/deployments/{deployment_id}"
    assert seeded["call_platform_http_get"].backing_service == "platform-api-gateway"
    assert seeded["call_platform_http_get"].backing_api == "GET {path}"
    assert seeded["get_deployment_status"].required_execution_mode == "read_only"
    assert seeded["get_deployment_logs"].required_execution_mode == "read_only"
    assert seeded["wait_for_deployment"].required_execution_mode == "read_only"
    assert seeded["run_deployment_validation"].required_execution_mode == "read_only"
    assert seeded["get_deployment_validation"].required_execution_mode == "read_only"
    assert seeded["get_deployment_status"].read_write_classification == "read"
    assert seeded["get_deployment_logs"].read_write_classification == "read"
    assert seeded["wait_for_deployment"].read_write_classification == "read"
    assert seeded["run_deployment_validation"].read_write_classification == "read"
    assert seeded["get_deployment_validation"].read_write_classification == "read"
    assert seeded["call_platform_http_get"].required_execution_mode == "read_only"
    assert seeded["call_platform_http_get"].read_write_classification == "read"
    assert seeded["call_platform_http_get"].mode_policy_json["read_only"] == "enabled"
    assert seeded["call_platform_http_get"].mode_policy_json["suggest"] == "enabled"
    assert seeded["call_platform_http_get"].mode_policy_json["execute"] == "enabled"
    assert seeded["call_platform_http_get"].mode_policy_json["governed_execute"] == "enabled"
    assert seeded["resolve_preview_deploy_target"].mode_policy_json["read_only"] == "enabled"
    assert seeded["resolve_preview_deploy_target"].read_write_classification == "read"
    assert seeded["revert_preview_change"].backing_api == "POST /change-previews/revert"
    assert seeded["delete_managed_resources"].read_write_classification == "destructive_write"
    assert seeded["create_working_branch"].required_execution_mode == "execute"
    assert seeded["write_source_file"].required_execution_mode == "execute"
    assert seeded["create_commit"].required_execution_mode == "execute"
    assert seeded["deploy_service_or_application"].required_execution_mode == "execute"
    assert seeded["deploy_service_or_application"].mode_policy_json["execute"] == "requires_permission"
    assert seeded["deploy_preview_change"].mode_policy_json["execute"] == "requires_permission"
    assert seeded["deploy_preview_change"].required_execution_mode == "execute"
    assert seeded["revert_preview_change"].required_execution_mode == "execute"
    assert seeded["revert_preview_change"].mode_policy_json["suggest"] == "requires_permission"
    assert seeded["revert_preview_change"].mode_policy_json["execute"] == "requires_permission"
    assert seeded["delete_managed_resources"].required_execution_mode == "execute"
    assert seeded["delete_managed_resources"].mode_policy_json["execute"] == "requires_permission"
    assert seeded["delete_managed_resources"].mode_policy_json["governed_execute"] == "requires_permission"
    assert all(seeded[name].enabled is True for name in (
        "create_working_branch",
        "scaffold_service",
        "write_source_file",
        "create_commit",
        "deploy_service_or_application",
        "delete_managed_resources",
    ))
