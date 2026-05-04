from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.routes.handlers import tool_execution


@pytest.mark.anyio
async def test_query_recent_telemetry_uses_source_scoped_recent_endpoint(monkeypatch) -> None:
    calls: list[tuple[str, str, dict | None]] = []

    async def fake_runtime_get(slug: str, path: str, params: dict | None = None) -> dict:
        calls.append((slug, path, params))
        return {"data": []}

    monkeypatch.setattr(tool_execution, "_runtime_get", fake_runtime_get)

    result = await tool_execution._execute_mapped_tool(
        "query_recent_telemetry",
        {"source_id": "simulator", "name": "battery_voltage", "limit": 25},
        db=MagicMock(),
    )

    assert result == {"data": []}
    assert calls == [
        (
            "telemetry-query-service",
            "telemetry/battery_voltage/recent",
            {"source_id": "simulator", "limit": 25},
        )
    ]


@pytest.mark.anyio
async def test_query_recent_telemetry_defaults_limit_to_one_hundred(monkeypatch) -> None:
    calls: list[tuple[str, str, dict | None]] = []

    async def fake_runtime_get(slug: str, path: str, params: dict | None = None) -> dict:
        calls.append((slug, path, params))
        return []

    monkeypatch.setattr(tool_execution, "_runtime_get", fake_runtime_get)

    await tool_execution._execute_mapped_tool(
        "query_recent_telemetry",
        {"source_id": "simulator", "name": "battery_voltage"},
        db=MagicMock(),
    )

    assert calls == [
        (
            "telemetry-query-service",
            "telemetry/battery_voltage/recent",
            {"source_id": "simulator", "limit": 100},
        )
    ]
