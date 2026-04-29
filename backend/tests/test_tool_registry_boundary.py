from __future__ import annotations

from dataclasses import dataclass

from app.routes.handlers import tool_registry
from app.models.intelligence import ToolDefinition


@dataclass
class _FakeCondition:
    value: str


class _FakeColumn:
    def __eq__(self, other: object) -> _FakeCondition:  # type: ignore[override]
        return _FakeCondition(str(other))

    def asc(self) -> "_FakeColumn":
        return self


class _FakeQuery:
    def __init__(self, storage: dict[str, ToolDefinition]):
        self._storage = storage
        self._condition: _FakeCondition | None = None

    def filter(self, condition: _FakeCondition) -> "_FakeQuery":
        self._condition = condition
        return self

    def one_or_none(self) -> ToolDefinition | None:
        if not self._condition:
            return None
        return self._storage.get(self._condition.value)

    def order_by(self, *_args, **_kwargs) -> "_FakeQuery":
        return self

    def all(self) -> list[ToolDefinition]:
        return list(self._storage.values())

    def count(self) -> int:
        return len(self._storage)


class _FakeDB:
    def __init__(self) -> None:
        self.storage: dict[str, ToolDefinition] = {}

    def query(self, _model: type[ToolDefinition]) -> _FakeQuery:
        return _FakeQuery(self.storage)

    def add(self, tool: ToolDefinition) -> None:
        self.storage[tool.name] = tool


def test_tool_registry_handler_exports_metadata_routes_only() -> None:
    assert hasattr(tool_registry, "list_tools")
    assert hasattr(tool_registry, "get_tool")
    assert hasattr(tool_registry, "seed_tools")
    assert not hasattr(tool_registry, "execute_tool")


def test_seeded_future_write_tools_keep_domain_only_strict_input_schema(monkeypatch) -> None:
    db = _FakeDB()
    original_name = ToolDefinition.name
    monkeypatch.setattr(ToolDefinition, "name", _FakeColumn())
    try:
        payload = tool_registry.seed_tools(db=db)
    finally:
        monkeypatch.setattr(ToolDefinition, "name", original_name)

    assert payload["total"] > 0
    seeded = db.storage["create_working_branch"]
    assert seeded.requires_confirmation is True
    assert seeded.input_schema_json == {"type": "object", "properties": {}, "additionalProperties": False}
    assert "confirmation_token" not in seeded.input_schema_json.get("properties", {})
