"""Telemetry inventory service."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Any

from sqlalchemy import func, select, tuple_
from sqlalchemy.orm import Session

from app.models.telemetry import (
    TelemetryCurrent,
    TelemetryData,
    TelemetryMetadata,
    TelemetryStatistics,
)
from app.services.channel_alias_service import get_aliases_by_telemetry_ids
from app.services.source_stream_service import get_stream_source_id, normalize_source_id
from app.services.telemetry_service import _compute_state
from app.utils.subsystem import infer_subsystem


def _resolve_logical_source_id(db: Session, source_id: str) -> str:
    return get_stream_source_id(db, source_id) or normalize_source_id(source_id)


def _latest_current_rows(
    db: Session,
    logical_source_id: str,
    telemetry_ids: Iterable[Any],
) -> dict[Any, TelemetryCurrent]:
    telemetry_ids = list(telemetry_ids)
    if not telemetry_ids:
        return {}

    ranked = (
        select(
            TelemetryCurrent.telemetry_id.label("telemetry_id"),
            TelemetryCurrent.stream_id.label("stream_id"),
            TelemetryCurrent.generation_time.label("generation_time"),
            TelemetryCurrent.reception_time.label("reception_time"),
            TelemetryCurrent.value.label("value"),
            func.row_number()
            .over(
                partition_by=TelemetryCurrent.telemetry_id,
                order_by=(
                    TelemetryCurrent.reception_time.desc(),
                    TelemetryCurrent.generation_time.desc(),
                    TelemetryCurrent.stream_id.desc(),
                ),
            )
            .label("rn"),
        )
        .join(TelemetryMetadata, TelemetryMetadata.id == TelemetryCurrent.telemetry_id)
        .where(
            TelemetryMetadata.source_id == logical_source_id,
            TelemetryCurrent.telemetry_id.in_(telemetry_ids),
        )
        .subquery()
    )

    rows = db.execute(select(ranked).where(ranked.c.rn == 1)).mappings().all()
    return {row["telemetry_id"]: row for row in rows}


def _latest_data_rows(
    db: Session,
    logical_source_id: str,
    telemetry_ids: Iterable[Any],
) -> dict[Any, Any]:
    telemetry_ids = list(telemetry_ids)
    if not telemetry_ids:
        return {}

    ranked = (
        select(
            TelemetryData.telemetry_id.label("telemetry_id"),
            TelemetryData.stream_id.label("stream_id"),
            TelemetryData.timestamp.label("timestamp"),
            TelemetryData.value.label("value"),
            func.row_number()
            .over(
                partition_by=TelemetryData.telemetry_id,
                order_by=(
                    TelemetryData.timestamp.desc(),
                    TelemetryData.sequence.desc(),
                    TelemetryData.stream_id.desc(),
                ),
            )
            .label("rn"),
        )
        .join(TelemetryMetadata, TelemetryMetadata.id == TelemetryData.telemetry_id)
        .where(
            TelemetryMetadata.source_id == logical_source_id,
            TelemetryData.telemetry_id.in_(telemetry_ids),
        )
        .subquery()
    )

    rows = db.execute(select(ranked).where(ranked.c.rn == 1)).mappings().all()
    return {row["telemetry_id"]: row for row in rows}


def _statistics_rows(
    db: Session,
    telemetry_stream_pairs: Iterable[tuple[str, Any]],
) -> dict[tuple[str, Any], TelemetryStatistics]:
    telemetry_stream_pairs = list(telemetry_stream_pairs)
    if not telemetry_stream_pairs:
        return {}

    rows = db.execute(
        select(TelemetryStatistics).where(
            tuple_(TelemetryStatistics.stream_id, TelemetryStatistics.telemetry_id).in_(
                telemetry_stream_pairs
            )
        )
    ).scalars().all()
    return {(row.stream_id, row.telemetry_id): row for row in rows}


def get_telemetry_inventory_for_source(db: Session, source_id: str) -> list[dict]:
    """Build the telemetry inventory for a logical source."""
    logical_source_id = _resolve_logical_source_id(db, source_id)
    metadata_rows = (
        db.execute(
            select(TelemetryMetadata)
            .where(TelemetryMetadata.source_id == logical_source_id)
            .order_by(TelemetryMetadata.name)
        )
        .scalars()
        .all()
    )
    if not metadata_rows:
        return []

    telemetry_ids = [row.id for row in metadata_rows]
    aliases_by_id = get_aliases_by_telemetry_ids(
        db,
        source_id=logical_source_id,
        telemetry_ids=telemetry_ids,
    )
    current_by_id = _latest_current_rows(db, logical_source_id, telemetry_ids)
    latest_data_by_id = _latest_data_rows(db, logical_source_id, telemetry_ids)

    stats_keys: list[tuple[str, Any]] = []
    for meta in metadata_rows:
        current_row = current_by_id.get(meta.id)
        if current_row is not None:
            stats_keys.append((current_row["stream_id"], meta.id))
            continue
        latest_data_row = latest_data_by_id.get(meta.id)
        if latest_data_row is not None:
            stats_keys.append((latest_data_row["stream_id"], meta.id))
    stats_by_key = _statistics_rows(db, stats_keys)

    result: list[dict] = []
    for meta in metadata_rows:
        current_row = current_by_id.get(meta.id)
        latest_data_row = latest_data_by_id.get(meta.id)
        snapshot_stream_id = None
        value: float | None = None
        timestamp: datetime | None = None

        if current_row is not None:
            snapshot_stream_id = current_row["stream_id"]
            value = float(current_row["value"])
            timestamp = current_row["generation_time"]
        elif latest_data_row is not None:
            snapshot_stream_id = latest_data_row["stream_id"]
            value = float(latest_data_row["value"])
            timestamp = latest_data_row["timestamp"]

        has_data = value is not None and timestamp is not None
        stats = (
            stats_by_key.get((snapshot_stream_id, meta.id))
            if snapshot_stream_id is not None
            else None
        )

        red_low = float(meta.red_low) if meta.red_low is not None else None
        red_high = float(meta.red_high) if meta.red_high is not None else None

        if not has_data:
            state = "no_data"
            state_reason = "waiting_for_data"
            z_score = None
            is_anomalous = False
            n_samples = None
        else:
            std_dev = float(stats.std_dev) if stats is not None else 0.0
            mean = float(stats.mean) if stats is not None else float(value)
            z_score = ((float(value) - mean) / std_dev) if std_dev > 0 else None
            state, state_reason = _compute_state(float(value), z_score, red_low, red_high, std_dev)
            is_anomalous = state == "warning" or (z_score is not None and abs(z_score) > 2)
            n_samples = stats.n_samples if stats is not None else None

        result.append(
            {
                "name": meta.name,
                "aliases": aliases_by_id.get(meta.id, []),
                "description": meta.description,
                "units": meta.units,
                "subsystem_tag": infer_subsystem(meta.name, meta),
                "channel_origin": meta.channel_origin or "catalog",
                "discovery_namespace": meta.discovery_namespace,
                "current_value": value,
                "last_timestamp": timestamp.isoformat() if timestamp is not None else None,
                "state": state,
                "state_reason": state_reason,
                "z_score": z_score,
                "is_anomalous": is_anomalous,
                "has_data": has_data,
                "red_low": red_low,
                "red_high": red_high,
                "n_samples": n_samples,
            }
        )

    return result
