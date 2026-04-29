from __future__ import annotations

from app.routes.handlers import tool_registry


def test_tool_registry_handler_exports_definitions_routes_without_execute_patch() -> None:
    assert hasattr(tool_registry, "list_tools")
    assert hasattr(tool_registry, "get_tool")
    assert hasattr(tool_registry, "seed_tools")
    assert not hasattr(tool_registry, "execute_tool")
    assert not hasattr(tool_registry, "patch_tool")


def test_mvp_tool_inventory_matches_input_schemas() -> None:
    missing = tool_registry.MVP_TOOL_NAMES.difference(tool_registry.TOOL_INPUT_SCHEMAS.keys())
    assert not missing


def test_mvp_registry_has_exactly_twenty_five_tools() -> None:
    assert len(tool_registry.MVP_TOOL_NAMES) == 25


def test_write_classification_tools_are_execute_only() -> None:
    executes = {"trigger_document_reingestion", "create_working_branch", "scaffold_service", "write_source_file", "create_commit", "deploy_service_or_application"}
    assert executes.issubset(tool_registry.MVP_TOOL_NAMES)


def test_write_tools_have_strict_non_empty_schemas_where_applicable() -> None:
    assert tool_registry.TOOL_INPUT_SCHEMAS["create_working_branch"]["properties"]
    assert tool_registry.TOOL_INPUT_SCHEMAS["write_source_file"]["required"] == ["branch", "path", "content"]
    assert tool_registry.TOOL_INPUT_SCHEMAS["read_source_file"]["required"] == ["branch", "path"]
