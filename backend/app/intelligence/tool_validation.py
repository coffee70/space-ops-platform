from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError as JsonSchemaCompileError
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError


@dataclass
class ToolInputValidationError(Exception):
    errors: list[dict[str, Any]]


@dataclass
class ToolSchemaDefinitionError(Exception):
    message: str


def _json_path_from_validator_error(error: JsonSchemaValidationError) -> str:
    segments = list(error.absolute_path)
    if not segments:
        return "$"
    buf = "$"
    for seg in segments:
        if isinstance(seg, int):
            buf += f"[{seg}]"
        else:
            buf += f".{seg}"
    return buf


def _summarize_validator_value(error: JsonSchemaValidationError) -> str | Any | None:
    val = getattr(error, "validator_value", None)
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        try:
            return json.dumps(val, sort_keys=True, default=str)
        except TypeError:
            return str(val)
    return val


def _leaf_validation_errors(errors: list[JsonSchemaValidationError]) -> list[JsonSchemaValidationError]:
    """Prefer leaf diagnostics; parent validators (allOf/if) only add generic wrapper messages."""

    sorted_errors = sorted(
        errors,
        key=lambda e: (tuple(e.absolute_path), getattr(e, "validator", ""), repr(getattr(e, "validator_value", None))),
    )
    out: list[JsonSchemaValidationError] = []
    for err in sorted_errors:
        if err.context:
            out.extend(_leaf_validation_errors(list(err.context)))
        else:
            out.append(err)
    return out


def _validation_error_dict(error: JsonSchemaValidationError) -> dict[str, Any]:
    actual: Any | None = error.instance
    if error.validator == "required":
        actual = None
    return {
        "code": "invalid_input",
        "path": _json_path_from_validator_error(error),
        "message": error.message,
        "expected": _summarize_validator_value(error),
        "actual": actual,
    }


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

    try:
        Draft7Validator.check_schema(schema)
    except JsonSchemaCompileError as exc:
        raise ToolSchemaDefinitionError(str(exc)) from exc

    validator = Draft7Validator(schema)
    collected = sorted(
        _leaf_validation_errors(list(validator.iter_errors(payload))),
        key=lambda e: (tuple(e.absolute_path), getattr(e, "validator", ""), e.message),
    )
    if not collected:
        return

    flattened: list[dict[str, Any]] = [_validation_error_dict(e) for e in collected]
    raise ToolInputValidationError(flattened)
