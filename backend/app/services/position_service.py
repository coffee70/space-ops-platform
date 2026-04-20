"""Service helpers for position channel mappings and latest positions."""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Iterable, Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.telemetry import (
    PositionChannelMapping,
    TelemetryCurrent,
    TelemetryMetadata,
    TelemetrySource,
)
from app.models.schemas import (
    PositionChannelMappingUpsert,
    PositionSample,
)
from app.services.channel_alias_service import resolve_channel_metadata, resolve_channel_name
from app.services.overview_service import _get_latest_value_and_ts
from app.services.source_stream_service import resolve_latest_stream_id
from app.utils.coordinates import ecef_to_lla, eci_to_lla

logger = logging.getLogger(__name__)


VALID_FRAMES = {"gps_lla", "ecef", "eci"}


def _resolve_mapping_channel_name(db: Session, source_id: str, channel_name: str | None) -> str | None:
    if not channel_name:
        return None
    resolved = resolve_channel_name(db, source_id=source_id, channel_name=channel_name)
    if resolved is None:
        raise ValueError(f"Telemetry not found: {channel_name}")
    return resolved


def _get_meta_by_name(db: Session, source_id: str, name: str) -> Optional[TelemetryMetadata]:
    return resolve_channel_metadata(db, source_id=source_id, channel_name=name)


def _get_latest_for_channel(
    db: Session,
    *,
    source_id: str,
    channel_name: Optional[str],
    data_source_id: str | None = None,
) -> tuple[Optional[float], Optional[datetime]]:
    """Get latest value and timestamp for a named channel for a source."""
    if not channel_name:
        return None, None
    meta = _get_meta_by_name(db, source_id, channel_name)
    if not meta:
        return None, None

    resolved_stream_id = data_source_id or resolve_latest_stream_id(db, source_id)
    current = db.get(TelemetryCurrent, (resolved_stream_id, meta.id))
    if current:
        return float(current.value), current.generation_time

    latest = _get_latest_value_and_ts(db, meta.id, source_id=resolved_stream_id)
    if not latest:
        return None, None
    value, ts = latest
    return float(value), ts


def list_mappings(
    db: Session,
    vehicle_id: Optional[str] = None,
) -> list[PositionChannelMapping]:
    """List active position mappings, optionally filtered by vehicle."""
    stmt = select(PositionChannelMapping).where(PositionChannelMapping.active.is_(True))
    if vehicle_id:
        stmt = stmt.where(PositionChannelMapping.source_id == vehicle_id)
    return db.execute(stmt).scalars().all()


def upsert_mapping(
    db: Session,
    body: PositionChannelMappingUpsert,
) -> PositionChannelMapping:
    """Create or update a position mapping for a source."""
    if body.frame_type not in VALID_FRAMES:
        raise ValueError(f"Unsupported frame_type: {body.frame_type}")

    # Lock the source row to serialize concurrent upserts for the same source_id
    src = (
        db.execute(
            select(TelemetrySource)
            .where(TelemetrySource.id == body.vehicle_id)
            .with_for_update()
        )
        .scalars()
        .first()
    )
    if not src:
        raise ValueError(f"Source not found: {body.vehicle_id}")

    if body.frame_type == "gps_lla":
        if not body.lat_channel_name or not body.lon_channel_name:
            raise ValueError("gps_lla mappings require lat_channel_name and lon_channel_name")
    elif body.frame_type in {"ecef", "eci"}:
        if not body.x_channel_name or not body.y_channel_name or not body.z_channel_name:
            raise ValueError(f"{body.frame_type} mappings require x/y/z channel names")

    body.lat_channel_name = _resolve_mapping_channel_name(db, body.vehicle_id, body.lat_channel_name)
    body.lon_channel_name = _resolve_mapping_channel_name(db, body.vehicle_id, body.lon_channel_name)
    body.alt_channel_name = _resolve_mapping_channel_name(db, body.vehicle_id, body.alt_channel_name)
    body.x_channel_name = _resolve_mapping_channel_name(db, body.vehicle_id, body.x_channel_name)
    body.y_channel_name = _resolve_mapping_channel_name(db, body.vehicle_id, body.y_channel_name)
    body.z_channel_name = _resolve_mapping_channel_name(db, body.vehicle_id, body.z_channel_name)

    existing = (
        db.execute(
            select(PositionChannelMapping).where(
                PositionChannelMapping.source_id == body.vehicle_id,
                PositionChannelMapping.active.is_(True),
            )
        )
        .scalars()
        .first()
    )

    if existing:
        mapping = existing
    else:
        mapping = PositionChannelMapping(source_id=body.vehicle_id)
        db.add(mapping)

    mapping.frame_type = body.frame_type
    mapping.lat_channel_name = body.lat_channel_name
    mapping.lon_channel_name = body.lon_channel_name
    mapping.alt_channel_name = body.alt_channel_name
    mapping.x_channel_name = body.x_channel_name
    mapping.y_channel_name = body.y_channel_name
    mapping.z_channel_name = body.z_channel_name
    mapping.active = body.active

    db.flush()  # ensure new rows get id (session has autoflush=False)
    return mapping


def delete_mapping(db: Session, mapping_id: UUID) -> bool:
    """Delete a position mapping by id."""
    mapping = db.get(PositionChannelMapping, mapping_id)
    if not mapping:
        return False
    db.delete(mapping)
    return True


def _build_sample_for_mapping(
    db: Session,
    mapping: PositionChannelMapping,
    source: TelemetrySource,
    *,
    data_source_id: str,
    now: datetime,
    staleness: timedelta,
) -> PositionSample:
    """Resolve a single PositionChannelMapping into a PositionSample."""
    frame = mapping.frame_type
    latest_ts: Optional[datetime] = None
    lat_deg: Optional[float] = None
    lon_deg: Optional[float] = None
    alt_m: Optional[float] = None
    raw_channels: dict[str, Optional[float]] = {}

    try:
        if frame == "gps_lla":
            lat, ts_lat = _get_latest_for_channel(
                db,
                source_id=data_source_id,
                channel_name=mapping.lat_channel_name,
                data_source_id=data_source_id,
            )
            lon, ts_lon = _get_latest_for_channel(
                db,
                source_id=data_source_id,
                channel_name=mapping.lon_channel_name,
                data_source_id=data_source_id,
            )
            alt, ts_alt = _get_latest_for_channel(
                db,
                source_id=data_source_id,
                channel_name=mapping.alt_channel_name,
                data_source_id=data_source_id,
            )
            raw_channels = {
                "lat": lat,
                "lon": lon,
                "alt": alt,
            }
            if lat is None or lon is None:
                # Not enough information to produce a point
                return PositionSample(
                    vehicle_id=source.id,
                    vehicle_name=source.name,
                    vehicle_type=source.source_type,
                    lat_deg=None,
                    lon_deg=None,
                    alt_m=None,
                    timestamp=None,
                    valid=False,
                    frame_type=frame,
                    raw_channels=raw_channels,
                )
            lat_deg = float(lat)
            lon_deg = float(lon)
            alt_m = float(alt) if alt is not None else 0.0
            latest_ts = max(
                [ts for ts in (ts_lat, ts_lon, ts_alt) if ts is not None],
                default=None,
            )
        elif frame in {"ecef", "eci"}:
            x, ts_x = _get_latest_for_channel(
                db,
                source_id=data_source_id,
                channel_name=mapping.x_channel_name,
                data_source_id=data_source_id,
            )
            y, ts_y = _get_latest_for_channel(
                db,
                source_id=data_source_id,
                channel_name=mapping.y_channel_name,
                data_source_id=data_source_id,
            )
            z, ts_z = _get_latest_for_channel(
                db,
                source_id=data_source_id,
                channel_name=mapping.z_channel_name,
                data_source_id=data_source_id,
            )
            raw_channels = {"x": x, "y": y, "z": z}
            if x is None or y is None or z is None:
                return PositionSample(
                    vehicle_id=source.id,
                    vehicle_name=source.name,
                    vehicle_type=source.source_type,
                    lat_deg=None,
                    lon_deg=None,
                    alt_m=None,
                    timestamp=None,
                    valid=False,
                    frame_type=frame,
                    raw_channels=raw_channels,
                )
            latest_ts = max(
                [ts for ts in (ts_x, ts_y, ts_z) if ts is not None],
                default=None,
            )
            if frame == "ecef":
                lat_deg, lon_deg, alt_m = ecef_to_lla(float(x), float(y), float(z))
            else:
                # ECI is not currently supported; treat as invalid but logged.
                try:
                    lat_deg, lon_deg, alt_m = eci_to_lla(
                        float(x),
                        float(y),
                        float(z),
                        latest_ts or now,
                    )
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning("eci_to_lla failed for source %s: %s", source.id, exc)
                    lat_deg = lon_deg = alt_m = None
    except Exception as exc:  # pragma: no cover - defensive guardrail
        logger.exception("Failed to build position sample for source %s: %s", source.id, exc)
        return PositionSample(
            vehicle_id=source.id,
            vehicle_name=source.name,
            vehicle_type=source.source_type,
            lat_deg=None,
            lon_deg=None,
            alt_m=None,
            timestamp=None,
            valid=False,
            frame_type=frame,
            raw_channels=raw_channels or None,
        )

    if latest_ts is None or lat_deg is None or lon_deg is None or alt_m is None:
        return PositionSample(
            vehicle_id=source.id,
            vehicle_name=source.name,
            vehicle_type=source.source_type,
            lat_deg=None,
            lon_deg=None,
            alt_m=None,
            timestamp=None,
            valid=False,
            frame_type=frame,
            raw_channels=raw_channels or None,
        )

    age = now - latest_ts
    valid = age <= staleness

    return PositionSample(
        vehicle_id=source.id,
        vehicle_name=source.name,
        vehicle_type=source.source_type,
        lat_deg=lat_deg,
        lon_deg=lon_deg,
        alt_m=alt_m,
        timestamp=latest_ts.isoformat(),
        valid=valid,
        frame_type=frame,
        raw_channels=raw_channels or None,
    )


def get_latest_positions(
    db: Session,
    *,
    vehicle_ids: Optional[Iterable[str]] = None,
    staleness_seconds: int = 300,
) -> list[PositionSample]:
    """Get latest resolved positions for all vehicles with active mappings.

    When vehicle_ids is provided, restrict to that set.
    """
    now = datetime.now(timezone.utc)
    staleness = timedelta(seconds=staleness_seconds)

    stmt = select(PositionChannelMapping).where(
        PositionChannelMapping.active.is_(True),
    )
    # When vehicle_ids is provided:
    # - None  -> no restriction (all active mappings)
    # - []    -> explicit empty filter (return no results)
    # - [ids] -> restrict to that set
    if vehicle_ids is not None:
        ids = list(vehicle_ids)
        if not ids:
            return []
        stmt = stmt.where(PositionChannelMapping.source_id.in_(ids))
    mappings = db.execute(stmt).scalars().all()
    if not mappings:
        return []

    source_ids_set = {m.source_id for m in mappings}
    src_stmt = select(TelemetrySource).where(TelemetrySource.id.in_(source_ids_set))
    sources = db.execute(src_stmt).scalars().all()
    sources_by_id = {s.id: s for s in sources}

    samples: list[PositionSample] = []
    for mapping in mappings:
        src = sources_by_id.get(mapping.source_id)
        if not src:
            continue
        data_source_id = resolve_latest_stream_id(db, mapping.source_id)
        samples.append(
            _build_sample_for_mapping(
                db,
                mapping,
                src,
                data_source_id=data_source_id,
                now=now,
                staleness=staleness,
            )
        )

    return samples
