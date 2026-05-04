from __future__ import annotations

import pytest

from app.intelligence.tool_validation import ToolInputValidationError, ToolSchemaDefinitionError, validate_tool_input
from app.routes.handlers import tool_registry


def test_scaffold_service_rejects_invalid_package_owner_enum() -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS["scaffold_service"]
    with pytest.raises(ToolInputValidationError) as exc:
        validate_tool_input(
            schema,
            {
                "template_id": "python-service",
                "unit_id": "bad-service",
                "display_name": "Bad Service",
                "package_owner": "not-a-real-owner",
            },
        )
    err = exc.value.errors[0]
    assert err["path"] == "$.package_owner"
    assert "space-ops-platform" in err["message"] or err["expected"]


def test_navigate_to_application_rejects_route_path_not_matching_pattern() -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS["navigate_to_application"]
    with pytest.raises(ToolInputValidationError) as exc:
        validate_tool_input(
            schema,
            {"application_id": "overview", "route_path": "/not-apps/overview"},
        )
    err = exc.value.errors[0]
    assert err["path"] == "$.route_path"
    assert "^/apps/" in str(err["expected"]) or "pattern" in err["message"].lower()


def test_query_recent_telemetry_rejects_limit_below_minimum() -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS["query_recent_telemetry"]
    base = {"source_id": "simulator", "name": "battery_voltage"}
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {**base, "limit": 0})


def test_query_recent_telemetry_rejects_limit_above_maximum() -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS["query_recent_telemetry"]
    base = {"source_id": "simulator", "name": "battery_voltage"}
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {**base, "limit": 501})


def test_delete_managed_resources_requires_unit_id_for_managed_unit() -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS["delete_managed_resources"]
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {"mode": "managed_unit"})


def test_delete_managed_resources_requires_branch_for_code() -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS["delete_managed_resources"]
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {"mode": "code"})


def test_delete_managed_resources_requires_older_than_for_stale() -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS["delete_managed_resources"]
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {"mode": "stale"})


def test_delete_managed_resources_rejects_invalid_mode_enum() -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS["delete_managed_resources"]
    with pytest.raises(ToolInputValidationError) as exc:
        validate_tool_input(schema, {"mode": "invalid-mode"})
    assert any(e["path"] == "$.mode" for e in exc.value.errors)


def test_delete_managed_resources_paths_must_be_array_of_strings() -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS["delete_managed_resources"]
    base = {"mode": "code", "branch": "some-branch"}
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {**base, "paths": "not-an-array"})
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(schema, {**base, "paths": [123]})


def test_positive_cases_still_validate() -> None:
    scaffold = tool_registry.TOOL_INPUT_SCHEMAS["scaffold_service"]
    validate_tool_input(
        scaffold,
        {
            "template_id": "python-service",
            "unit_id": "good-service",
            "display_name": "Good Service",
            "discovery": {"health_path": "/health"},
        },
    )

    delete_schema = tool_registry.TOOL_INPUT_SCHEMAS["delete_managed_resources"]
    validate_tool_input(delete_schema, {"mode": "managed_unit", "unit_id": "sample-service"})

    tel = tool_registry.TOOL_INPUT_SCHEMAS["query_recent_telemetry"]
    validate_tool_input(tel, {"source_id": "vehicle-main", "name": "voltage", "limit": 42})


def test_additional_properties_still_rejected() -> None:
    schema = tool_registry.TOOL_INPUT_SCHEMAS["query_recent_telemetry"]
    with pytest.raises(ToolInputValidationError):
        validate_tool_input(
            schema,
            {"source_id": "s", "name": "n", "unexpected": True},
        )


def test_invalid_schema_raises_definition_error() -> None:
    with pytest.raises(ToolSchemaDefinitionError):
        validate_tool_input(
            {
                "type": "object",
                "properties": "not-an-object",
                "additionalProperties": False,
            },
            {},
        )
