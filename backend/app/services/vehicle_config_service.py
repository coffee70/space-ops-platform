"""Service helpers for vehicle configuration file management."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from app.models.schemas import (
    VehicleConfigFetchResponse,
    VehicleConfigListItem,
    VehicleConfigParsedSummary,
    VehicleConfigSaveResponse,
    VehicleConfigValidationError,
    VehicleConfigValidationResponse,
)
from telemetry_catalog.definitions import (
    VehicleConfigurationFile,
    canonical_vehicle_config_path,
    resolve_vehicle_config_path,
    vehicle_config_root,
)

ALLOWED_SUFFIXES = {".json", ".yaml", ".yml"}


class VehicleConfigServiceError(ValueError):
    """Raised when a vehicle configuration request cannot be fulfilled."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = 400,
        errors: list[VehicleConfigValidationError] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.errors = errors or []


@dataclass(frozen=True)
class ParsedVehicleConfig:
    path: str | None
    content: str
    format: str
    payload: dict[str, Any]
    parsed: VehicleConfigurationFile


def _resolve_relative_path(path_str: str, *, must_exist: bool) -> tuple[Path, str]:
    root = vehicle_config_root()
    if must_exist:
        resolved = resolve_vehicle_config_path(path_str, root=root)
        return resolved, canonical_vehicle_config_path(path_str, root=root)

    raw = Path(path_str)
    candidate = raw if raw.is_absolute() else (root / raw)
    resolved = candidate.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise VehicleConfigServiceError(
            f"Vehicle configuration path must stay under {root}",
            status_code=400,
        ) from exc
    if resolved.suffix.lower() not in ALLOWED_SUFFIXES:
        raise VehicleConfigServiceError(
            "Vehicle configuration file must be .json, .yaml, or .yml",
            status_code=400,
        )
    return resolved, resolved.relative_to(root).as_posix()


def _detect_format(*, path: str | None = None, filename: str | None = None, format_hint: str | None = None) -> str:
    if format_hint:
        normalized = format_hint.strip().lower()
        if normalized in {"yaml", "yml"}:
            return "yaml"
        if normalized == "json":
            return "json"
        raise VehicleConfigServiceError("Unsupported vehicle configuration format", status_code=400)

    candidate = path or filename
    if candidate:
        suffix = Path(candidate).suffix.lower()
        if suffix in {".yaml", ".yml"}:
            return "yaml"
        if suffix == ".json":
            return "json"
        raise VehicleConfigServiceError(
            "Vehicle configuration file must be .json, .yaml, or .yml",
            status_code=400,
        )
    raise VehicleConfigServiceError(
        "Vehicle configuration format could not be determined",
        status_code=400,
    )


def _structured_errors(exc: ValidationError) -> list[VehicleConfigValidationError]:
    return [
        VehicleConfigValidationError(
            loc=[str(part) for part in error.get("loc", ())],
            message=error.get("msg", "Invalid vehicle configuration"),
            type=error.get("type", "validation_error"),
        )
        for error in exc.errors()
    ]


def _parsed_summary(parsed: VehicleConfigurationFile) -> VehicleConfigParsedSummary:
    return VehicleConfigParsedSummary(
        version=parsed.version,
        name=parsed.name,
        channel_count=len(parsed.channels),
        scenario_names=sorted(parsed.scenarios),
        has_position_mapping=parsed.position_mapping is not None,
        has_ingestion=parsed.ingestion is not None,
    )


def _load_payload(content: str, fmt: str) -> dict[str, Any]:
    try:
        payload = json.loads(content) if fmt == "json" else yaml.safe_load(content)
    except (json.JSONDecodeError, yaml.YAMLError) as exc:
        raise VehicleConfigServiceError(
            "Vehicle configuration content could not be parsed",
            status_code=400,
            errors=[
                VehicleConfigValidationError(
                    loc=[],
                    message=str(exc),
                    type=exc.__class__.__name__,
                )
            ],
        ) from exc
    if not isinstance(payload, dict):
        raise VehicleConfigServiceError(
            "Vehicle configuration file must contain an object at the top level",
            status_code=400,
            errors=[
                VehicleConfigValidationError(
                    loc=[],
                    message="Top-level content must be an object",
                    type="type_error.object",
                )
            ],
        )
    return payload


def parse_vehicle_config_content(
    content: str,
    *,
    path: str | None = None,
    filename: str | None = None,
    format_hint: str | None = None,
) -> ParsedVehicleConfig:
    fmt = _detect_format(path=path, filename=filename, format_hint=format_hint)
    payload = _load_payload(content, fmt)
    try:
        parsed = VehicleConfigurationFile.model_validate(payload)
    except ValidationError as exc:
        raise VehicleConfigServiceError(
            "Vehicle configuration validation failed",
            status_code=400,
            errors=_structured_errors(exc),
        ) from exc
    return ParsedVehicleConfig(path=path, content=content, format=fmt, payload=payload, parsed=parsed)


def validate_vehicle_config_content(
    content: str,
    *,
    path: str | None = None,
    filename: str | None = None,
    format_hint: str | None = None,
) -> VehicleConfigValidationResponse:
    try:
        parsed = parse_vehicle_config_content(
            content,
            path=path,
            filename=filename,
            format_hint=format_hint,
        )
    except VehicleConfigServiceError as exc:
        return VehicleConfigValidationResponse(valid=False, parsed=None, errors=exc.errors)
    return VehicleConfigValidationResponse(valid=True, parsed=_parsed_summary(parsed.parsed), errors=[])


def _serialize_vehicle_config(parsed: VehicleConfigurationFile, fmt: str) -> str:
    payload = parsed.model_dump(mode="json", exclude_none=True)
    if fmt == "json":
        return json.dumps(payload, indent=2, sort_keys=False) + "\n"
    return yaml.safe_dump(
        payload,
        sort_keys=False,
        allow_unicode=False,
        default_flow_style=False,
    )


def _normalize_line_endings(content: str) -> str:
    return content.replace("\r\n", "\n").replace("\r", "\n")


def _serialize_vehicle_config_for_save(
    parsed: VehicleConfigurationFile,
    fmt: str,
    raw_content: str,
) -> str:
    if fmt == "yaml":
        return _normalize_line_endings(raw_content)
    return _serialize_vehicle_config(parsed, fmt)


def list_vehicle_configs() -> list[VehicleConfigListItem]:
    root = vehicle_config_root()
    items: list[VehicleConfigListItem] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in ALLOWED_SUFFIXES:
            continue
        rel_path = path.relative_to(root).as_posix()
        category = path.relative_to(root).parts[0] if len(path.relative_to(root).parts) > 0 else ""
        modified_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
        display_name: str | None = None
        try:
            display_name = load_vehicle_config(rel_path).parsed.name
        except VehicleConfigServiceError:
            display_name = None
        items.append(
            VehicleConfigListItem(
                path=rel_path,
                filename=path.name,
                name=display_name,
                category=category,
                format=path.suffix.lower().lstrip(".").replace("yml", "yaml"),
                modified_at=modified_at,
            )
        )
    return items


def load_vehicle_config(path: str) -> VehicleConfigFetchResponse:
    resolved, canonical_path = _resolve_relative_path(path, must_exist=True)
    content = resolved.read_text(encoding="utf-8")
    validation = validate_vehicle_config_content(
        content,
        path=canonical_path,
        format_hint=resolved.suffix.lstrip("."),
    )
    fmt = resolved.suffix.lower().lstrip(".").replace("yml", "yaml")
    return VehicleConfigFetchResponse(
        path=canonical_path,
        content=content,
        format=fmt,
        parsed=validation.parsed,
        validation_errors=validation.errors,
    )


def create_vehicle_config(path: str, content: str) -> VehicleConfigSaveResponse:
    resolved, canonical_path = _resolve_relative_path(path, must_exist=False)
    if resolved.exists():
        raise VehicleConfigServiceError("Vehicle configuration file already exists", status_code=409)
    parsed = parse_vehicle_config_content(content, path=canonical_path)
    normalized = _serialize_vehicle_config_for_save(parsed.parsed, parsed.format, content)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(normalized, encoding="utf-8")
    return VehicleConfigSaveResponse(path=canonical_path, parsed=_parsed_summary(parsed.parsed), saved=True)


def update_vehicle_config(path: str, content: str) -> VehicleConfigSaveResponse:
    resolved, canonical_path = _resolve_relative_path(path, must_exist=True)
    parsed = parse_vehicle_config_content(content, path=canonical_path)
    normalized = _serialize_vehicle_config_for_save(parsed.parsed, parsed.format, content)
    resolved.write_text(normalized, encoding="utf-8")
    return VehicleConfigSaveResponse(path=canonical_path, parsed=_parsed_summary(parsed.parsed), saved=True)
