from __future__ import annotations

from app.models.intelligence import ToolDefinition


def tool_summary(tool: ToolDefinition) -> dict:
    return {
        "name": tool.name,
        "description": tool.description,
        "category": tool.category,
        "layer_target": tool.layer_target,
        "read_write_classification": tool.read_write_classification,
        "required_execution_mode": tool.required_execution_mode,
        "enabled": tool.enabled,
        "requires_confirmation": tool.requires_confirmation,
        "input_schema_json": tool.input_schema_json,
    }
