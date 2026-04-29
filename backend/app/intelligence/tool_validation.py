from __future__ import annotations

from dataclasses import dataclass
from typing import Any

@dataclass
class ToolInputValidationError(Exception):
    errors: list[dict[str, Any]]


@dataclass
class ToolSchemaDefinitionError(Exception):
    message: str


def _type_matches(expected: str, value: Any) -> bool:
    if expected == "string":
        return isinstance(value, str)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return (isinstance(value, int) or isinstance(value, float)) and not isinstance(value, bool)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    return True


def _validate_object(schema: dict[str, Any], payload: dict[str, Any], path_prefix: str = "$") -> list[dict[str, Any]]:
    properties = schema.get("properties") or {}
    required = set(schema.get("required") or [])
    additional_properties = schema.get("additionalProperties", True)
    errors: list[dict[str, Any]] = []

    if not isinstance(properties, dict):
        raise ToolSchemaDefinitionError("tool schema properties must be an object")

    for name in required:
        if name not in payload:
            errors.append(
                {
                    "code": "invalid_input",
                    "path": f"{path_prefix}.{name}",
                    "message": f"'{name}' is a required property",
                    "expected": "present",
                    "actual": None,
                }
            )

    if additional_properties is False:
        for key in payload:
            if key not in properties:
                errors.append(
                    {
                        "code": "invalid_input",
                        "path": f"{path_prefix}.{key}",
                        "message": f"Additional properties are not allowed ('{key}' was unexpected)",
                        "expected": "known_property",
                        "actual": payload[key],
                    }
                )

    for key, prop_schema in properties.items():
        if key not in payload:
            continue
        if not isinstance(prop_schema, dict):
            raise ToolSchemaDefinitionError("tool schema property definitions must be objects")
        expected_type = prop_schema.get("type")
        value = payload[key]
        if isinstance(expected_type, str) and not _type_matches(expected_type, value):
            errors.append(
                {
                    "code": "invalid_input",
                    "path": f"{path_prefix}.{key}",
                    "message": f"Expected type '{expected_type}'",
                    "expected": expected_type,
                    "actual": value,
                }
            )
            continue
        if expected_type == "object" and isinstance(value, dict):
            errors.extend(_validate_object(prop_schema, value, path_prefix=f"{path_prefix}.{key}"))
    if errors:
        raise ToolInputValidationError(errors)


def validate_tool_input(schema: dict[str, Any], payload: dict[str, Any]) -> None:
    if not isinstance(schema, dict):
        raise ToolSchemaDefinitionError("tool schema must be an object")
    if schema.get("type") != "object":
        raise ToolSchemaDefinitionError("tool schema root type must be object")
    if not isinstance(payload, dict):
        raise ToolInputValidationError(
            [
                {
                    "code": "invalid_input",
                    "path": "$",
                    "message": "tool input must be an object",
                    "expected": "object",
                    "actual": payload,
                }
            ]
        )
    _validate_object(schema, payload)
