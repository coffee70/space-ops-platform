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


def test_supported_registry_has_exactly_twenty_five_tools() -> None:
    assert len(tool_registry.SUPPORTED_TOOL_NAMES) == 25


def test_workspace_file_navigation_tool_is_not_supported() -> None:
    assert "open_workspace_file" not in tool_registry.SUPPORTED_TOOL_NAMES
    assert "open_workspace_file" not in tool_registry.TOOL_INPUT_SCHEMAS


def test_write_classification_tools_are_execute_only() -> None:
    executes = {"trigger_document_reingestion", "create_working_branch", "scaffold_service", "write_source_file", "create_commit", "deploy_service_or_application", "delete_managed_resources"}
    assert executes.issubset(tool_registry.SUPPORTED_TOOL_NAMES)


def test_write_tools_have_strict_non_empty_schemas_where_applicable() -> None:
    assert tool_registry.TOOL_INPUT_SCHEMAS["create_working_branch"]["properties"]
    assert tool_registry.TOOL_INPUT_SCHEMAS["write_source_file"]["required"] == ["branch", "path", "content"]
    assert tool_registry.TOOL_INPUT_SCHEMAS["read_source_file"]["required"] == ["branch", "path"]


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
    assert seeded["delete_managed_resources"].read_write_classification == "destructive_write"
    assert all(seeded[name].enabled is True for name in (
        "create_working_branch",
        "scaffold_service",
        "write_source_file",
        "create_commit",
        "deploy_service_or_application",
        "delete_managed_resources",
    ))
