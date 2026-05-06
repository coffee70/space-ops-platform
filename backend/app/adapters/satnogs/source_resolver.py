"""Backend vehicle source resolution for adapter startup."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from time import sleep
from typing import Any

import httpx

from app.adapters.satnogs.config import RetryConfig, VehicleConfig


class SourceResolutionError(RuntimeError):
    """Raised when the adapter cannot resolve its backend source."""


@dataclass(frozen=True, slots=True)
class ResolvedSource:
    id: str
    name: str
    source_type: str
    vehicle_config_path: str
    created: bool
    monitoring_start_time: datetime
    last_reconciled_at: datetime | None
    history_mode: str
    live_state: str
    backfill_state: str
    chunk_size_hours: int
    active_backfill_target_time: datetime | None = None
    description: str | None = None


class BackendSourceResolver:
    """Resolve a canonical backend vehicle source id."""

    def __init__(
        self,
        *,
        resolve_url: str,
        retry: RetryConfig,
        timeout_seconds: float,
        client: httpx.Client | None = None,
    ) -> None:
        self.resolve_url = resolve_url
        self.retry = retry
        self.client = client or httpx.Client(timeout=timeout_seconds)

    def resolve_vehicle_source(self, vehicle: VehicleConfig) -> ResolvedSource:
        payload = {
            "source_type": "vehicle",
            "name": vehicle.name,
            "description": f"Auto-resolved from vehicle configuration: {vehicle.vehicle_config_path}",
            "vehicle_config_path": vehicle.vehicle_config_path,
        }
        if vehicle.monitoring_start_time is not None:
            payload["monitoring_start_time"] = vehicle.monitoring_start_time.isoformat()
        attempts = 0
        backoff = self.retry.backoff_seconds
        retryable = set(self.retry.retryable_status_codes)

        while attempts < self.retry.max_attempts:
            attempts += 1
            try:
                response = self.client.post(self.resolve_url, json=payload)
            except httpx.RequestError as exc:
                if attempts >= self.retry.max_attempts:
                    raise SourceResolutionError(
                        f"Failed to resolve backend source for vehicle_config_path={vehicle.vehicle_config_path}: {exc!r}"
                    ) from exc
                sleep(min(backoff, 5.0))
                backoff *= self.retry.backoff_multiplier
                continue

            if 200 <= response.status_code < 300:
                try:
                    response_payload = response.json()
                except ValueError as exc:
                    raise SourceResolutionError(
                        "Malformed source resolve response for "
                        f"vehicle_config_path={vehicle.vehicle_config_path}: invalid JSON"
                    ) from exc
                return _parse_resolved_source(response_payload, vehicle_config_path=vehicle.vehicle_config_path)

            if response.status_code < 500 and response.status_code not in retryable:
                raise SourceResolutionError(
                    "Failed to resolve backend source for "
                    f"vehicle_config_path={vehicle.vehicle_config_path}: "
                    f"status={response.status_code} body={response.text}"
                )

            if attempts >= self.retry.max_attempts:
                raise SourceResolutionError(
                    "Failed to resolve backend source for "
                    f"vehicle_config_path={vehicle.vehicle_config_path}: "
                    f"status={response.status_code} body={response.text}"
                )

            sleep(min(backoff, 5.0))
            backoff *= self.retry.backoff_multiplier

        raise SourceResolutionError(f"Failed to resolve backend source for vehicle_config_path={vehicle.vehicle_config_path}")


def _parse_resolved_source(payload: Any, *, vehicle_config_path: str) -> ResolvedSource:
    if not isinstance(payload, dict):
        raise SourceResolutionError(
            f"Malformed source resolve response for vehicle_config_path={vehicle_config_path}: expected object"
        )
    source_id = payload.get("source_id") or payload.get("id")
    name = payload.get("name")
    source_type = payload.get("source_type")
    resolved_path = payload.get("vehicle_config_path")
    created = payload.get("created")
    monitoring_start_time = _parse_datetime(payload.get("monitoring_start_time"), "monitoring_start_time", vehicle_config_path)
    last_reconciled_at = _parse_optional_datetime(payload.get("last_reconciled_at"), "last_reconciled_at", vehicle_config_path)
    active_backfill_target_time = _parse_optional_datetime(
        payload.get("active_backfill_target_time"),
        "active_backfill_target_time",
        vehicle_config_path,
    )
    history_mode = payload.get("history_mode")
    live_state = payload.get("live_state")
    backfill_state = payload.get("backfill_state")
    chunk_size_hours = payload.get("chunk_size_hours")
    if (
        not isinstance(source_id, str)
        or not source_id
        or not isinstance(name, str)
        or source_type != "vehicle"
        or not isinstance(resolved_path, str)
        or not isinstance(created, bool)
        or history_mode not in {"live_only", "time_window_replay", "cursor_replay"}
        or live_state not in {"idle", "active", "error"}
        or backfill_state not in {"idle", "running", "complete", "error"}
        or not isinstance(chunk_size_hours, int)
        or chunk_size_hours < 1
    ):
        raise SourceResolutionError(
            f"Malformed source resolve response for vehicle_config_path={vehicle_config_path}: missing source fields"
        )
    description = payload.get("description")
    if description is not None and not isinstance(description, str):
        raise SourceResolutionError(
            f"Malformed source resolve response for vehicle_config_path={vehicle_config_path}: invalid description"
        )
    return ResolvedSource(
        id=source_id,
        name=name,
        source_type=source_type,
        vehicle_config_path=resolved_path,
        created=created,
        monitoring_start_time=monitoring_start_time,
        last_reconciled_at=last_reconciled_at,
        history_mode=history_mode,
        live_state=live_state,
        backfill_state=backfill_state,
        active_backfill_target_time=active_backfill_target_time,
        chunk_size_hours=chunk_size_hours,
        description=description,
    )


def _parse_datetime(value: Any, field: str, vehicle_config_path: str) -> datetime:
    parsed = _parse_optional_datetime(value, field, vehicle_config_path)
    if parsed is None:
        raise SourceResolutionError(
            f"Malformed source resolve response for vehicle_config_path={vehicle_config_path}: missing {field}"
        )
    return parsed


def _parse_optional_datetime(value: Any, field: str, vehicle_config_path: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise SourceResolutionError(
            f"Malformed source resolve response for vehicle_config_path={vehicle_config_path}: invalid {field}"
        )
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SourceResolutionError(
            f"Malformed source resolve response for vehicle_config_path={vehicle_config_path}: invalid {field}"
        ) from exc
