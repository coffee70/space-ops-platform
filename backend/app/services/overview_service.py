"""Overview dashboard and watchlist service."""

import logging
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.models.telemetry import (
    TelemetryCurrent,
    TelemetryData,
    TelemetryMetadata,
    TelemetryStatistics,
    WatchlistEntry,
)
from app.services.channel_alias_service import (
    get_aliases_by_telemetry_ids,
    resolve_channel_metadata,
    resolve_channel_name,
)
from app.services.source_stream_service import (
    get_stream_source_id,
    normalize_source_id,
    resolve_latest_stream_id,
)
from app.services.telemetry_service import _compute_state
from app.utils.subsystem import infer_subsystem

logger = logging.getLogger(__name__)

SPARKLINE_POINTS = 30


def _resolve_logical_source_id(db: Session, source_id: str) -> str:
    return get_stream_source_id(db, source_id) or normalize_source_id(source_id)


def get_all_telemetry_names(db: Session) -> list[str]:
    """Get all telemetry names for watchlist config."""
    stmt = select(TelemetryMetadata.name).order_by(TelemetryMetadata.name)
    return [r[0] for r in db.execute(stmt).fetchall()]


def get_all_telemetry_channels_for_source(db: Session, source_id: str) -> list[dict]:
    """Get source-scoped telemetry channel metadata for picker/search UIs."""
    logical_source_id = _resolve_logical_source_id(db, source_id)
    stmt = (
        select(
            TelemetryMetadata.id,
            TelemetryMetadata.name,
            TelemetryMetadata.channel_origin,
            TelemetryMetadata.discovery_namespace,
        )
        .where(TelemetryMetadata.source_id == logical_source_id)
        .order_by(TelemetryMetadata.name)
    )
    rows = db.execute(stmt).fetchall()
    aliases_by_id = get_aliases_by_telemetry_ids(
        db,
        source_id=logical_source_id,
        telemetry_ids=[row[0] for row in rows],
    )
    return [
        {
            "name": row[1],
            "aliases": aliases_by_id.get(row[0], []),
            "channel_origin": row[2] or "catalog",
            "discovery_namespace": row[3],
        }
        for row in rows
    ]


def get_all_telemetry_names_for_source(db: Session, source_id: str) -> list[str]:
    """Get telemetry names for one source."""
    return [row["name"] for row in get_all_telemetry_channels_for_source(db, source_id)]


def get_watchlist(db: Session, source_id: str) -> list[dict]:
    """Get watchlist entries ordered by display_order."""
    logical_source_id = _resolve_logical_source_id(db, source_id)
    stmt = (
        select(
            WatchlistEntry.source_id,
            WatchlistEntry.telemetry_name,
            WatchlistEntry.display_order,
            TelemetryMetadata.id,
            TelemetryMetadata.channel_origin,
            TelemetryMetadata.discovery_namespace,
        )
        .join(
            TelemetryMetadata,
            (TelemetryMetadata.source_id == WatchlistEntry.source_id)
            & (TelemetryMetadata.name == WatchlistEntry.telemetry_name),
            isouter=True,
        )
        .where(WatchlistEntry.source_id == logical_source_id)
        .order_by(WatchlistEntry.display_order)
    )
    rows = db.execute(stmt).fetchall()
    aliases_by_id = get_aliases_by_telemetry_ids(
        db,
        source_id=logical_source_id,
        telemetry_ids=[r[3] for r in rows if r[3] is not None],
    )
    return [
        {
            "source_id": r[0],
            "name": r[1],
            "aliases": aliases_by_id.get(r[3], []),
            "display_order": r[2],
            "channel_origin": r[4] or "catalog",
            "discovery_namespace": r[5],
        }
        for r in rows
    ]


def add_to_watchlist(db: Session, source_id: str, telemetry_name: str) -> None:
    """Add a channel to the watchlist."""
    logical_source_id = _resolve_logical_source_id(db, source_id)
    # Verify telemetry exists
    meta = resolve_channel_metadata(db, source_id=logical_source_id, channel_name=telemetry_name)
    if not meta:
        raise ValueError(f"Telemetry not found: {telemetry_name}")

    existing = db.execute(
        select(WatchlistEntry).where(
            WatchlistEntry.source_id == logical_source_id,
            WatchlistEntry.telemetry_name == meta.name,
        )
    ).scalar_one_or_none()
    if existing:
        return  # Already in watchlist

    max_result = db.execute(
        select(func.max(WatchlistEntry.display_order)).where(WatchlistEntry.source_id == logical_source_id)
    ).scalar()
    next_order = (max_result or -1) + 1

    entry = WatchlistEntry(
        source_id=logical_source_id,
        telemetry_name=meta.name,
        display_order=next_order,
    )
    db.add(entry)


def remove_from_watchlist(db: Session, source_id: str, telemetry_name: str) -> None:
    """Remove a channel from the watchlist."""
    logical_source_id = _resolve_logical_source_id(db, source_id)
    canonical_name = (
        resolve_channel_name(db, source_id=logical_source_id, channel_name=telemetry_name)
        or telemetry_name
    )
    entry = db.execute(
        select(WatchlistEntry).where(
            WatchlistEntry.source_id == logical_source_id,
            WatchlistEntry.telemetry_name == canonical_name,
        )
    ).scalar_one_or_none()
    if entry:
        db.delete(entry)


def _get_latest_value_and_ts(
    db: Session, telemetry_id, source_id: str
) -> Optional[tuple[float, datetime]]:
    """Get latest value and timestamp for a telemetry point, filtered by source."""
    data_source_id = normalize_source_id(source_id)
    stmt = (
        select(TelemetryData.timestamp, TelemetryData.value)
        .where(
            TelemetryData.telemetry_id == telemetry_id,
            TelemetryData.stream_id == data_source_id,
        )
        .order_by(desc(TelemetryData.timestamp), desc(TelemetryData.sequence))
        .limit(1)
    )
    row = db.execute(stmt).fetchone()
    if row:
        return (float(row[1]), row[0])
    return None


def _get_recent_for_sparkline(
    db: Session, telemetry_id, source_id: str, limit: int = SPARKLINE_POINTS
) -> list[dict]:
    """Get recent data points for sparkline (oldest first for chart), filtered by source."""
    data_source_id = normalize_source_id(source_id)
    stmt = (
        select(TelemetryData.timestamp, TelemetryData.value)
        .where(
            TelemetryData.telemetry_id == telemetry_id,
            TelemetryData.stream_id == data_source_id,
        )
        .order_by(desc(TelemetryData.timestamp), desc(TelemetryData.sequence))
        .limit(limit)
    )
    rows = db.execute(stmt).fetchall()
    # Reverse so oldest first for chart
    return [
        {"timestamp": r[0].isoformat(), "value": float(r[1])}
        for r in reversed(rows)
    ]


def get_overview(db: Session, source_id: str) -> list[dict]:
    """Get overview data for all watchlist channels, optionally filtered by source."""
    data_source_id = resolve_latest_stream_id(db, source_id)
    logical_source_id = _resolve_logical_source_id(db, source_id)
    watchlist = get_watchlist(db, source_id)
    if not watchlist:
        return []

    result = []
    aliases_by_name = {
        channel["name"]: channel["aliases"]
        for channel in get_all_telemetry_channels_for_source(db, source_id)
    }
    for entry in watchlist:
        name = entry["name"]
        meta = db.execute(
            select(TelemetryMetadata).where(
                TelemetryMetadata.source_id == logical_source_id,
                TelemetryMetadata.name == name,
            )
        ).scalars().first()
        if not meta:
            continue

        stats = db.get(TelemetryStatistics, (data_source_id, meta.id))

        # Prefer TelemetryCurrent for the source; fall back to TelemetryData
        current = db.get(TelemetryCurrent, (data_source_id, meta.id))
        if current:
            value, ts = float(current.value), current.generation_time
        else:
            latest = _get_latest_value_and_ts(db, meta.id, source_id=data_source_id)
            if not latest:
                # No data at all for this channel+source; skip
                continue
            value, ts = latest

        if stats:
            std_dev = float(stats.std_dev)
            mean = float(stats.mean)
            z_score = (value - mean) / std_dev if std_dev > 0 else None
        else:
            # Allow channels without statistics to still appear in the watchlist.
            std_dev = 0.0
            mean = float(value)
            z_score = None

        red_low = float(meta.red_low) if meta.red_low is not None else None
        red_high = float(meta.red_high) if meta.red_high is not None else None
        state, state_reason = _compute_state(value, z_score, red_low, red_high, std_dev)

        sparkline_data = _get_recent_for_sparkline(db, meta.id, source_id=data_source_id)

        result.append({
            "name": meta.name,
            "aliases": aliases_by_name.get(meta.name, []),
            "units": meta.units,
            "description": meta.description,
            "subsystem_tag": infer_subsystem(name, meta),
            "channel_origin": meta.channel_origin or "catalog",
            "discovery_namespace": meta.discovery_namespace,
            "current_value": value,
            "last_timestamp": ts.isoformat(),
            "state": state,
            "state_reason": state_reason,
            "z_score": z_score,
            "sparkline_data": sparkline_data,
        })

    return result


def get_anomalies(db: Session, source_id: str) -> dict[str, list[dict]]:
    """Get anomalous channels grouped by subsystem, optionally filtered by source."""
    data_source_id = resolve_latest_stream_id(db, source_id)
    logical_source_id = _resolve_logical_source_id(db, source_id)
    stmt = (
        select(TelemetryMetadata, TelemetryStatistics)
        .join(
            TelemetryStatistics,
            (TelemetryMetadata.id == TelemetryStatistics.telemetry_id)
            & (TelemetryStatistics.stream_id == data_source_id),
        )
        .where(TelemetryMetadata.source_id == logical_source_id)
    )
    rows = db.execute(stmt).fetchall()

    anomalies_by_subsystem: dict[str, list[dict]] = defaultdict(list)

    for meta, stats in rows:
        current = db.get(TelemetryCurrent, (data_source_id, meta.id))
        if current:
            value, ts = float(current.value), current.generation_time
        else:
            latest = _get_latest_value_and_ts(db, meta.id, source_id=data_source_id)
            if not latest:
                continue
            value, ts = latest
        std_dev = float(stats.std_dev)
        mean = float(stats.mean)
        z_score = (value - mean) / std_dev if std_dev > 0 else None
        red_low = float(meta.red_low) if meta.red_low is not None else None
        red_high = float(meta.red_high) if meta.red_high is not None else None
        state, state_reason = _compute_state(value, z_score, red_low, red_high, std_dev)

        if state != "warning":
            continue

        subsystem = infer_subsystem(meta.name, meta)
        anomalies_by_subsystem[subsystem].append({
            "name": meta.name,
            "units": meta.units,
            "current_value": value,
            "last_timestamp": ts.isoformat(),
            "z_score": z_score,
            "state_reason": state_reason,
        })

    # Sort each group by last_timestamp descending
    for subsystem in anomalies_by_subsystem:
        anomalies_by_subsystem[subsystem].sort(
            key=lambda x: x["last_timestamp"],
            reverse=True,
        )

    # Normalize to expected subsystem keys; put unknown in "other"
    known = {"power", "thermal", "adcs", "comms"}
    result = {k: anomalies_by_subsystem.get(k, []) for k in known}
    other = []
    for k, v in anomalies_by_subsystem.items():
        if k not in known:
            other.extend(v)
    other.sort(key=lambda x: x["last_timestamp"], reverse=True)
    result["other"] = other
    return result
