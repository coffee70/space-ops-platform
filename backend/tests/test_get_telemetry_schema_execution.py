from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.routes.handlers import tool_execution


@pytest.mark.anyio
async def test_get_telemetry_schema_calls_telemetry_query_inventory(monkeypatch) -> None:
    calls: list[tuple[str, str, dict | None]] = []

    async def fake_runtime_get(slug: str, path: str, params: dict | None = None) -> dict:
        calls.append((slug, path, params))
        return {"channels": []}

    monkeypatch.setattr(tool_execution, "_runtime_get", fake_runtime_get)

    result = await tool_execution._execute_mapped_tool(
        "get_telemetry_schema",
        {"source_id": "simulator"},
        db=MagicMock(),
    )

    assert result == {"channels": []}
    assert calls == [("telemetry-query-service", "telemetry/inventory", {"source_id": "simulator"})]
