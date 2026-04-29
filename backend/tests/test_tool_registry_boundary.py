from __future__ import annotations

from app.routes.handlers import tool_registry


def test_tool_registry_handler_exports_metadata_routes_only() -> None:
    assert hasattr(tool_registry, "list_tools")
    assert hasattr(tool_registry, "get_tool")
    assert hasattr(tool_registry, "seed_tools")
    assert not hasattr(tool_registry, "execute_tool")
