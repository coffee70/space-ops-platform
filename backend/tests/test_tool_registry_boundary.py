from __future__ import annotations

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


def test_write_classification_tools_are_execute_only() -> None:
    executes = {"trigger_document_reingestion", "create_working_branch", "scaffold_service", "write_source_file", "create_commit", "deploy_service_or_application"}
    assert executes.issubset(tool_registry.SUPPORTED_TOOL_NAMES)


def test_write_tools_have_strict_non_empty_schemas_where_applicable() -> None:
    assert tool_registry.TOOL_INPUT_SCHEMAS["create_working_branch"]["properties"]
    assert tool_registry.TOOL_INPUT_SCHEMAS["write_source_file"]["required"] == ["branch", "path", "content"]
    assert tool_registry.TOOL_INPUT_SCHEMAS["read_source_file"]["required"] == ["branch", "path"]


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
