"""
Shared telemetry domain logic for route handlers.

This module owns cross-service telemetry scoping, stream validation,
recent-value lookup, scoped statistics, and response-shaping helpers.

Do not add endpoint-specific behavior here.
Do not duplicate this logic across service-owned handler modules.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Optional

from fastapi import HTTPException
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.models.schemas import (
    ChannelSourceItem,
    StatisticsResponse,
    TelemetryDetailPageScope,
    TelemetryDetailScopeWindow,
)
from app.models.telemetry import (
    TelemetryCurrent,
    TelemetryData,
    TelemetryMetadata,
    TelemetryStream,
)
from app.services.channel_alias_service import resolve_channel_metadata
from app.services.source_stream_service import (
    ensure_stream_belongs_to_source,
    normalize_source_id,
    resolve_active_stream_id,
)


@dataclass(frozen=True)
class DetailDataScope:
    mode: Literal["latest", "streams", "date_range"]
    stream_ids: tuple[str, ...] = ()
    since: datetime | None = None
    until: datetime | None = None


def _get_channel_meta(db: Session, source_id: str, name: str) -> TelemetryMetadata | None:
    return resolve_channel_metadata(db, source_id=source_id, channel_name=name)


def _resolve_scoped_stream_id(db: Session, source_id: str, stream_id: Optional[str] = None) -> str:
    """Return the active stream id or validate an explicit stream id for a source."""
    if stream_id is None:
        logical_source_id = normalize_source_id(source_id)
        resolved_stream_id = resolve_active_stream_id(db, logical_source_id)
        if resolved_stream_id == logical_source_id:
            latest_stream_id = (
                db.execute(
                    select(TelemetryStream.id)
                    .where(TelemetryStream.source_id == logical_source_id)
                    .order_by(TelemetryStream.last_seen_at.desc(), TelemetryStream.id.desc())
                )
                .scalars()
                .first()
            )
            if isinstance(latest_stream_id, str) and latest_stream_id:
                return latest_stream_id
        return resolved_stream_id
    try:
        return ensure_stream_belongs_to_source(db, source_id, stream_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Stream not found for source")


def _resolve_latest_stream_id_for_channel(db: Session, source_id: str, name: str) -> str:
    """Resolve the latest stream for a source that actually contains the channel."""
    logical_source_id = normalize_source_id(source_id)
    meta = _get_channel_meta(db, logical_source_id, name)
    if not meta:
        raise HTTPException(status_code=404, detail="Telemetry not found")

    current_stream_id = (
        db.execute(
            select(TelemetryCurrent.stream_id)
            .where(TelemetryCurrent.telemetry_id == meta.id)
            .order_by(
                TelemetryCurrent.reception_time.desc(),
                TelemetryCurrent.generation_time.desc(),
            )
        )
        .scalars()
        .first()
    )
    if isinstance(current_stream_id, str) and current_stream_id:
        return current_stream_id

    historical_stream_id = (
        db.execute(
            select(TelemetryData.stream_id)
            .where(TelemetryData.telemetry_id == meta.id)
            .order_by(TelemetryData.timestamp.desc(), TelemetryData.sequence.desc())
        )
        .scalars()
        .first()
    )
    if isinstance(historical_stream_id, str) and historical_stream_id:
        return historical_stream_id

    return _resolve_scoped_stream_id(db, logical_source_id)


def _scope_timestamp_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    aware = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    return aware.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _detail_page_scope_payload(detail: DetailDataScope) -> TelemetryDetailPageScope:
    window: TelemetryDetailScopeWindow | None = None
    if detail.since is not None or detail.until is not None:
        window = TelemetryDetailScopeWindow(
            since=_scope_timestamp_iso(detail.since),
            until=_scope_timestamp_iso(detail.until),
        )
    if detail.mode == "latest":
        resolved = detail.stream_ids[0] if detail.stream_ids else None
        return TelemetryDetailPageScope(
            mode="latest",
            resolved_stream_id=resolved,
            window=window,
        )
    if detail.mode == "streams":
        return TelemetryDetailPageScope(
            mode="streams",
            stream_count=len(detail.stream_ids),
            stream_ids=list(detail.stream_ids),
            window=window,
        )
    return TelemetryDetailPageScope(mode="date_range", window=window)


def _parse_iso_datetime_param(value: Optional[str], name: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid {name} format, use ISO8601")
    return parsed


def _parse_detail_scope_params(
    *,
    scope: str,
    stream_ids: list[str],
    since: Optional[str],
    until: Optional[str],
) -> DetailDataScope:
    if scope == "latest":
        return DetailDataScope(mode="latest")

    since_dt = _parse_iso_datetime_param(since, "since") if since else None
    until_dt = _parse_iso_datetime_param(until, "until") if until else None
    if since_dt is not None and until_dt is not None and since_dt > until_dt:
        raise HTTPException(status_code=400, detail="since must be before until")
    if scope == "streams":
        cleaned_stream_ids = tuple(stream_id for stream_id in stream_ids if stream_id)
        if not cleaned_stream_ids:
            raise HTTPException(status_code=400, detail="stream_ids is required for streams scope")
        return DetailDataScope(
            mode="streams",
            stream_ids=cleaned_stream_ids,
            since=since_dt,
            until=until_dt,
        )
    if scope == "date_range":
        if since_dt is None and until_dt is None:
            raise HTTPException(status_code=400, detail="since or until is required for date_range scope")
        return DetailDataScope(mode="date_range", since=since_dt, until=until_dt)
    raise HTTPException(status_code=400, detail="Invalid scope")


def _validate_stream_ids_for_source(
    db: Session,
    *,
    source_id: str,
    stream_ids: tuple[str, ...],
) -> tuple[str, ...]:
    logical_source_id = normalize_source_id(source_id)
    requested = tuple(dict.fromkeys(stream_ids))
    if not requested:
        return ()

    registry_rows = db.execute(
        select(TelemetryStream.id).where(
            TelemetryStream.source_id == logical_source_id,
            TelemetryStream.id.in_(requested),
        )
    ).fetchall()
    valid = {row[0] for row in registry_rows}
    missing = [stream_id for stream_id in requested if stream_id not in valid]
    if missing:
        history_rows = db.execute(
            select(TelemetryData.stream_id)
            .join(TelemetryMetadata, TelemetryMetadata.id == TelemetryData.telemetry_id)
            .where(
                TelemetryMetadata.source_id == logical_source_id,
                TelemetryData.stream_id.in_(missing),
            )
            .distinct()
        ).fetchall()
        valid.update(row[0] for row in history_rows)

    invalid = [stream_id for stream_id in requested if stream_id not in valid]
    if invalid:
        raise HTTPException(status_code=404, detail="Stream not found for source")
    return tuple(stream_id for stream_id in stream_ids if stream_id in valid)


def _resolve_detail_data_scope(
    db: Session,
    *,
    source_id: str,
    name: str,
    scope: str,
    stream_ids: list[str],
    since: Optional[str],
    until: Optional[str],
) -> tuple[TelemetryMetadata, DetailDataScope]:
    logical_source_id = normalize_source_id(source_id)
    meta = _get_channel_meta(db, logical_source_id, name)
    if not meta:
        raise HTTPException(status_code=404, detail="Telemetry not found")

    parsed_scope = _parse_detail_scope_params(
        scope=scope,
        stream_ids=stream_ids,
        since=since,
        until=until,
    )
    if parsed_scope.mode == "latest":
        latest_stream_id = _resolve_latest_stream_id_for_channel(db, logical_source_id, name)
        return meta, DetailDataScope(mode="latest", stream_ids=(latest_stream_id,))
    if parsed_scope.mode == "streams":
        valid_stream_ids = _validate_stream_ids_for_source(
            db,
            source_id=logical_source_id,
            stream_ids=parsed_scope.stream_ids,
        )
        return meta, DetailDataScope(
            mode="streams",
            stream_ids=valid_stream_ids,
            since=parsed_scope.since,
            until=parsed_scope.until,
        )
    return meta, parsed_scope


def _scoped_telemetry_filters(meta: TelemetryMetadata, scope: DetailDataScope):
    filters = [TelemetryData.telemetry_id == meta.id]
    if scope.stream_ids:
        filters.append(TelemetryData.stream_id.in_(scope.stream_ids))
    if scope.since is not None:
        filters.append(TelemetryData.timestamp >= scope.since)
    if scope.until is not None:
        filters.append(TelemetryData.timestamp <= scope.until)
    return filters


def _get_scoped_recent_values(
    db: Session,
    *,
    meta: TelemetryMetadata,
    scope: DetailDataScope,
    limit: int,
) -> list[tuple[datetime, float, str]]:
    stmt = (
        select(TelemetryData.timestamp, TelemetryData.value, TelemetryData.stream_id)
        .where(*_scoped_telemetry_filters(meta, scope))
        .order_by(desc(TelemetryData.timestamp), desc(TelemetryData.sequence))
        .limit(limit)
    )
    rows = db.execute(stmt).fetchall()
    return [(row[0], float(row[1]), row[2]) for row in rows]


def _get_scoped_statistics(
    db: Session,
    *,
    meta: TelemetryMetadata,
    scope: DetailDataScope,
) -> StatisticsResponse:
    stats_row = db.execute(
        select(
            func.avg(TelemetryData.value),
            func.coalesce(func.stddev_pop(TelemetryData.value), 0),
            func.min(TelemetryData.value),
            func.max(TelemetryData.value),
            func.percentile_cont(0.05).within_group(TelemetryData.value),
            func.percentile_cont(0.50).within_group(TelemetryData.value),
            func.percentile_cont(0.95).within_group(TelemetryData.value),
            func.count(),
        ).where(*_scoped_telemetry_filters(meta, scope))
    ).first()
    if not stats_row or not stats_row[7]:
        return StatisticsResponse(
            mean=None,
            std_dev=None,
            min_value=None,
            max_value=None,
            p5=None,
            p50=None,
            p95=None,
            n_samples=0,
        )
    return StatisticsResponse(
        mean=float(stats_row[0]),
        std_dev=float(stats_row[1]),
        min_value=float(stats_row[2]),
        max_value=float(stats_row[3]),
        p5=float(stats_row[4]),
        p50=float(stats_row[5]),
        p95=float(stats_row[6]),
        n_samples=int(stats_row[7]),
    )


def _confidence_indicator(n_samples: int, last_timestamp: str | None) -> str | None:
    if n_samples <= 0:
        return None
    if n_samples < 100:
        return "limited data"
    if not last_timestamp:
        return "historical baseline"
    return "high confidence"


def _stream_metadata_by_id(db: Session, stream_ids: list[str]) -> dict[str, TelemetryStream]:
    if not stream_ids:
        return {}
    rows = db.execute(select(TelemetryStream).where(TelemetryStream.id.in_(stream_ids))).scalars().all()
    return {row.id: row for row in rows}


def _metadata_string(metadata: object, key: str) -> str | None:
    if isinstance(metadata, dict):
        value = metadata.get(key)
        return value if isinstance(value, str) and value else None
    return None


def _stream_option(
    stream_id: str,
    *,
    stream: TelemetryStream | None,
    start_time: datetime | None,
    last_timestamp: datetime | None,
    sample_count: int | None,
) -> ChannelSourceItem:
    metadata = stream.metadata_json if stream is not None else None
    label = _metadata_string(metadata, "label")
    summary = _metadata_string(metadata, "summary")
    provider = _metadata_string(metadata, "provider")
    stream_start = stream.started_at if stream is not None else None
    stream_seen = stream.last_seen_at if stream is not None else None
    effective_start = start_time or stream_start
    effective_last = last_timestamp or stream_seen
    return ChannelSourceItem(
        stream_id=stream_id,
        label=label,
        start_time=effective_start.isoformat() if effective_start is not None else None,
        end_time=stream_seen.isoformat() if stream_seen is not None else None,
        sample_count=sample_count,
        last_timestamp=effective_last.isoformat() if effective_last is not None else None,
        provider=provider,
        summary=summary,
    )
