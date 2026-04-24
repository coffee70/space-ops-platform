from datetime import datetime
from typing import Optional
from urllib.parse import unquote

from fastapi import Depends, HTTPException, Query
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.database import get_db
from app.lib.audit import audit_log
from app.models.schemas import (
    AnomaliesResponse,
    ChannelSourcesResponse,
    OverviewChannel,
    OverviewResponse,
    RecentDataPoint,
    RecentDataResponse,
    RecomputeStatsResponse,
    TelemetryInventoryItem,
    TelemetryInventoryResponse,
    TelemetryListResponse,
    WatchlistAddRequest,
    WatchlistResponse,
)
from app.models.telemetry import TelemetryData, TelemetryMetadata
from app.routes.handlers.scope import (
    _get_channel_meta,
    _get_scoped_recent_values,
    _parse_iso_datetime_param,
    _resolve_detail_data_scope,
    _stream_metadata_by_id,
    _stream_option,
)
from app.services.overview_service import (
    add_to_watchlist,
    get_all_telemetry_channels_for_source,
    get_anomalies,
    get_overview,
    get_watchlist,
    remove_from_watchlist,
)
from app.services.source_stream_service import normalize_source_id, resolve_latest_stream_id
from app.services.statistics_service import StatisticsService
from app.services.telemetry_inventory_service import get_telemetry_inventory_for_source
from app.utils.subsystem import infer_subsystem


def recompute_stats(
    source_id: Optional[str] = None,
    all_sources: bool = False,
    db: Session = Depends(get_db),
):
    """Recompute statistics. source_id filters to one source; all_sources recomputes per source."""
    if not all_sources and not source_id:
        raise HTTPException(status_code=400, detail="source_id is required unless all_sources=true")
    stats_service = StatisticsService(db)
    try:
        count = stats_service.recompute_all(source_id=source_id, all_sources=all_sources)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    audit_log(
        "stats.recompute",
        source_id=source_id,
        all_sources=all_sources,
        telemetry_processed=count,
    )
    return RecomputeStatsResponse(telemetry_processed=count)


def overview(
    source_id: str = Query(...),
    db: Session = Depends(get_db),
):
    """Get overview data for watchlist channels, optionally filtered by source."""
    channels = get_overview(db, source_id=source_id)
    return OverviewResponse(channels=[OverviewChannel(**c) for c in channels])


def anomalies(
    source_id: str = Query(...),
    db: Session = Depends(get_db),
):
    """Get anomalous channels grouped by subsystem, optionally filtered by source."""
    data = get_anomalies(db, source_id=source_id)
    return AnomaliesResponse(**data)


def list_watchlist(
    source_id: str = Query(...),
    db: Session = Depends(get_db),
):
    """List watchlist entries."""
    entries = get_watchlist(db, source_id)
    return WatchlistResponse(
        entries=[
            {
                "source_id": e["source_id"],
                "name": e["name"],
                "aliases": e.get("aliases", []),
                "display_order": e["display_order"],
                "channel_origin": e["channel_origin"],
                "discovery_namespace": e["discovery_namespace"],
            }
            for e in entries
        ]
    )


def add_watchlist(
    body: WatchlistAddRequest,
    db: Session = Depends(get_db),
):
    """Add a channel to the watchlist."""
    try:
        add_to_watchlist(db, body.source_id, body.telemetry_name)
        db.flush()
        audit_log(
            "watchlist.add",
            source_id=body.source_id,
            telemetry_name=body.telemetry_name,
        )
        return {"status": "added"}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


def delete_watchlist(
    name: str,
    source_id: str = Query(...),
    db: Session = Depends(get_db),
):
    """Remove a channel from the watchlist."""
    name = unquote(name)
    remove_from_watchlist(db, source_id, name)
    audit_log("watchlist.remove", source_id=source_id, name=name)
    return {"status": "removed"}


def list_telemetry(
    source_id: str = Query(...),
    db: Session = Depends(get_db),
):
    """List all telemetry names for watchlist config."""
    channels = get_all_telemetry_channels_for_source(db, source_id)
    return TelemetryListResponse(
        names=[channel["name"] for channel in channels],
        channels=channels,
    )


def list_telemetry_inventory(
    source_id: str = Query(...),
    db: Session = Depends(get_db),
):
    """List source-scoped telemetry inventory with operational state."""
    channels = get_telemetry_inventory_for_source(db, source_id)
    audit_log(
        "telemetry.inventory.read",
        source_id=source_id,
        channel_count=len(channels),
    )
    return TelemetryInventoryResponse(
        channels=[TelemetryInventoryItem(**channel) for channel in channels]
    )


def list_subsystems(
    source_id: str = Query(...),
    db: Session = Depends(get_db),
):
    """Get distinct subsystem tags for filter dropdown."""
    logical_source_id = normalize_source_id(source_id)
    stmt = (
        select(TelemetryMetadata)
        .where(TelemetryMetadata.source_id == logical_source_id)
        .order_by(TelemetryMetadata.name)
    )
    rows = db.execute(stmt).scalars().all()
    subsystems = set()
    for meta in rows:
        subsystems.add(infer_subsystem(meta.name, meta))
    return {"subsystems": sorted(subsystems)}


def list_units(
    source_id: str = Query(...),
    db: Session = Depends(get_db),
):
    """Get distinct units for filter dropdown."""
    logical_source_id = normalize_source_id(source_id)
    stmt = (
        select(TelemetryMetadata.units)
        .where(TelemetryMetadata.source_id == logical_source_id)
        .distinct()
        .order_by(TelemetryMetadata.units)
    )
    rows = db.execute(stmt).fetchall()
    return {"units": [r[0] for r in rows]}


def get_channel_streams(
    name: str,
    source_id: str,
    db: Session = Depends(get_db),
):
    """List streams for a source that have data for this channel.

    Works for simulators, vehicles, and any future source type.
    Returns stream ids with labels, newest first.
    """
    name = unquote(name)

    meta = _get_channel_meta(db, source_id, name)
    if not meta:
        raise HTTPException(status_code=404, detail="Telemetry not found")

    logical_source_id = normalize_source_id(source_id)
    rows = db.execute(
        select(
            TelemetryData.stream_id,
            func.min(TelemetryData.timestamp).label("start_time"),
            func.max(TelemetryData.timestamp).label("last_seen_at"),
            func.count().label("sample_count"),
        )
        .join(TelemetryMetadata, TelemetryMetadata.id == TelemetryData.telemetry_id)
        .where(
            TelemetryMetadata.source_id == logical_source_id,
            TelemetryData.telemetry_id == meta.id,
        )
        .group_by(TelemetryData.stream_id)
        .order_by(
            desc(func.max(TelemetryData.timestamp)),
            TelemetryData.stream_id.desc(),
        )
    ).fetchall()
    registry_by_id = _stream_metadata_by_id(db, [row[0] for row in rows])
    return ChannelSourcesResponse(
        sources=[
            _stream_option(
                row[0],
                stream=registry_by_id.get(row[0]),
                start_time=row[1],
                last_timestamp=row[2],
                sample_count=row[3],
            )
            for row in rows
        ]
    )


def _get_recent_values_db_only(
    db: Session,
    name: str,
    source_id: str,
    limit: int = 100,
    since=None,
    until=None,
) -> list[tuple[datetime, float]]:
    """Get recent values using only DB—no embedding/LLM cold start. source_id filters when telemetry_data is source-aware."""
    data_source_id = resolve_latest_stream_id(db, source_id)
    meta = _get_channel_meta(db, source_id, name)
    if not meta:
        raise ValueError(f"Telemetry not found: {name}")
    stmt = (
        select(TelemetryData.timestamp, TelemetryData.value)
        .where(
            TelemetryData.telemetry_id == meta.id,
            TelemetryData.stream_id == data_source_id,
        )
        .order_by(desc(TelemetryData.timestamp), desc(TelemetryData.sequence))
        .limit(limit)
    )
    if since is not None:
        stmt = stmt.where(TelemetryData.timestamp >= since)
    if until is not None:
        stmt = stmt.where(TelemetryData.timestamp <= until)
    rows = db.execute(stmt).fetchall()
    return [(r[0], float(r[1])) for r in rows]


def get_recent(
    name: str,
    limit: int = 100,
    since: Optional[str] = None,
    until: Optional[str] = None,
    source_id: str = Query(...),
    db: Session = Depends(get_db),
):
    """Get most recent data points for charting. Use since/until for time-range filter."""
    name = unquote(name)
    since_dt = _parse_iso_datetime_param(since, "since") if since else None
    until_dt = _parse_iso_datetime_param(until, "until") if until else None
    requested_time_filter = since_dt is not None or until_dt is not None
    try:
        rows = _get_recent_values_db_only(
            db,
            name,
            limit=limit,
            since=since_dt,
            until=until_dt,
            source_id=source_id,
        )
        fallback_to_recent = False
        if not rows and requested_time_filter:
            # Time filter yielded no data but the channel may still have history.
            # Fall back to most recent points and surface that explicitly via metadata.
            rows = _get_recent_values_db_only(db, name, limit=limit, source_id=source_id)
            fallback_to_recent = bool(rows)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    data_points = [
        RecentDataPoint(
            timestamp=r[0].isoformat(),
            value=r[1],
        )
        for r in reversed(rows)
    ]

    effective_since = data_points[0].timestamp if data_points else None
    effective_until = data_points[-1].timestamp if data_points else None

    # applied_time_filter indicates that a user-specified time window returned data
    # without falling back to the unfiltered "most recent" range.
    applied_time_filter = bool(data_points) and requested_time_filter and not fallback_to_recent

    return RecentDataResponse(
        data=data_points,
        requested_since=since if since else None,
        requested_until=until if until else None,
        effective_since=effective_since,
        effective_until=effective_until,
        applied_time_filter=applied_time_filter,
        fallback_to_recent=fallback_to_recent,
    )


def get_recent_for_source(
    source_id: str,
    name: str,
    scope: str = "latest",
    stream_ids: list[str] = Query(default=[]),
    limit: int = 100,
    since: Optional[str] = None,
    until: Optional[str] = None,
    db: Session = Depends(get_db),
):
    name = unquote(name)
    meta, detail_scope = _resolve_detail_data_scope(
        db,
        source_id=source_id,
        name=name,
        scope=scope,
        stream_ids=stream_ids,
        since=since,
        until=until,
    )
    rows = _get_scoped_recent_values(
        db,
        meta=meta,
        scope=detail_scope,
        limit=limit,
    )
    data_points = [
        RecentDataPoint(
            timestamp=row[0].isoformat(),
            value=row[1],
            stream_id=row[2],
        )
        for row in reversed(rows)
    ]
    return RecentDataResponse(
        data=data_points,
        requested_since=since if since else None,
        requested_until=until if until else None,
        effective_since=data_points[0].timestamp if data_points else None,
        effective_until=data_points[-1].timestamp if data_points else None,
        applied_time_filter=bool(data_points)
        and (detail_scope.since is not None or detail_scope.until is not None),
        fallback_to_recent=False,
    )


def get_channel_streams_for_source(
    source_id: str,
    name: str,
    db: Session = Depends(get_db),
):
    name = unquote(name)
    return get_channel_streams(name=name, source_id=source_id, db=db)
