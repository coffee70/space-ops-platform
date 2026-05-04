"""Assertions on API inventory entries for telemetry read tools."""

from app.intelligence.tooling.api_inventory import API_INVENTORY


def test_get_telemetry_maps_to_source_scoped_inventory_get() -> None:
    telemetry = API_INVENTORY["layer2"]["telemetry"]
    dumped = repr(telemetry)
    assert "/telemetry/schema" not in dumped
    assert "GET /telemetry/inventory?source_id={source_id}" in telemetry
    assert telemetry["GET /telemetry/inventory?source_id={source_id}"] == "read_only_tool:get_telemetry_schema"


def test_query_recent_telemetry_maps_to_source_scoped_recent_get() -> None:
    telemetry = API_INVENTORY["layer2"]["telemetry"]
    keys = telemetry.keys()
    assert "GET /telemetry/{name}/recent" not in keys
    scoped = "GET /telemetry/{name}/recent?source_id={source_id}&limit={limit}"
    assert scoped in telemetry
    assert telemetry[scoped] == "read_only_tool:query_recent_telemetry"


def test_navigation_inventory_contains_supported_higher_level_tools() -> None:
    navigation = API_INVENTORY["layer3"]["navigation"]

    assert navigation == {
        "platform.openApplication": "higher_level_tool_only:navigate_to_application",
        "registry.listTools": "higher_level_tool_only:list_available_tools",
    }
