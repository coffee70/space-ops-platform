"""Shared telemetry handler implementations."""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Optional
from urllib.parse import unquote

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.telemetry import (
    TelemetryCurrent,
    TelemetryData,
    TelemetryMetadata,
    TelemetrySource,
    TelemetryStatistics,
    TelemetryStream,
)
from app.models.schemas import (
    ActiveStreamUpdate,
    AnomaliesResponse,
    BackfillProgressUpdate,
    ChannelSourceItem,
    ChannelSourcesResponse,
    DataPoint,
    ExplainResponse,
    TelemetryDetailPageScope,
    TelemetryDetailScopeWindow,
    RecentDataPoint,
    RelatedChannel,
    LiveStateUpdate,
    SourceCreate,
    SourceObservationBatchUpsert,
    SourceObservationBatchUpsertResponse,
    SourceObservationSchema,
    SourceResolveRequest,
    SourceResolveResponse,
    SourceUpdate,
    StatisticsResponse,
    OverviewChannel,
    OverviewResponse,
    RecentDataResponse,
    RecomputeStatsResponse,
    SearchResponse,
    TelemetryDataIngest,
    TelemetryDataResponse,
    TelemetryInventoryItem,
    TelemetryInventoryResponse,
    TelemetryListResponse,
    TelemetrySchemaCreate,
    TelemetrySchemaResponse,
    UpcomingObservationsResponse,
    WatchlistAddRequest,
    WatchlistResponse,
)
from app.services.embedding_service import SentenceTransformerEmbeddingProvider
from app.services.llm_service import MockLLMProvider, OpenAICompatibleLLMProvider
from app.services.channel_alias_service import get_aliases_by_telemetry_ids, resolve_channel_metadata
from app.services.overview_service import (
    add_to_watchlist,
    get_all_telemetry_channels_for_source,
    get_anomalies,
    get_overview,
    get_watchlist,
    remove_from_watchlist,
)
from app.services.telemetry_inventory_service import get_telemetry_inventory_for_source
from app.services.realtime_service import (
    create_source,
    get_telemetry_sources,
    resolve_source,
    update_backfill_progress,
    update_live_state,
    update_source,
)
from app.utils.subsystem import infer_subsystem
from app.services.statistics_service import StatisticsService
from app.services.telemetry_service import TelemetryService, _compute_state
from app.services.source_stream_service import (
    clear_active_stream,
    normalize_source_id,
    ensure_stream_belongs_to_source,
    get_stream_source_id,
    SourceNotFoundError,
    register_stream,
    StreamIdConflictError,
    resolve_active_stream_id,
    resolve_latest_stream_id,
)
from app.services.source_observation_service import (
    SourceObservationNotFoundError,
    get_next_observation,
    list_upcoming_observations,
    upsert_source_observations,
)
from app.config import get_settings
from app.lib.audit import audit_log

logger = logging.getLogger(__name__)

router = APIRouter()

# Lazy-load providers (embedding model is heavy)
_embedding_provider = None
_llm_provider = None


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


@dataclass(frozen=True)
class DetailDataScope:
    mode: Literal["latest", "streams", "date_range"]
    stream_ids: tuple[str, ...] = ()
    since: datetime | None = None
    until: datetime | None = None


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


def _build_scoped_explain_response(
    db: Session,
    *,
    meta: TelemetryMetadata,
    source_id: str,
    scope: DetailDataScope,
    include_llm: bool = False,
    llm: object | None = None,
) -> ExplainResponse:
    aliases = get_aliases_by_telemetry_ids(
        db,
        source_id=source_id,
        telemetry_ids=[meta.id],
    ).get(meta.id, [])
    stats = _get_scoped_statistics(db, meta=meta, scope=scope)
    recent_rows = _get_scoped_recent_values(db, meta=meta, scope=scope, limit=1)
    recent_value = recent_rows[0][1] if recent_rows else None
    last_timestamp = recent_rows[0][0].isoformat() if recent_rows else None

    mean = stats.mean
    std_dev = stats.std_dev or 0
    z_score: Optional[float] = None
    is_anomalous = False
    if recent_value is not None and mean is not None and std_dev > 0:
        z_score = (recent_value - mean) / std_dev
        is_anomalous = abs(z_score) > 2
    if recent_value is None:
        recent_value = mean

    red_low = float(meta.red_low) if meta.red_low is not None else None
    red_high = float(meta.red_high) if meta.red_high is not None else None
    if recent_value is None:
        state = "no_data"
        state_reason = "no_samples"
    else:
        state, state_reason = _compute_state(recent_value, z_score, red_low, red_high, std_dev)

    llm_explanation = ""
    what_this_means = ""
    related: list[RelatedChannel] = []
    if include_llm and llm is not None:
        prompt = (
            f"Telemetry Name: {meta.name}\n"
            f"Units: {meta.units}\n"
            f"Description: {meta.description or 'N/A'}\n"
            f"Scoped Recent Value: {recent_value}\n"
            f"Scoped Mean: {stats.mean}\n"
            f"Scoped Std Dev: {stats.std_dev}\n"
            f"Scoped P5: {stats.p5}\n"
            f"Scoped P95: {stats.p95}\n"
            f"Scoped Samples: {stats.n_samples}\n"
            f"Z-Score: {z_score if z_score is not None else 'N/A'}\n"
            f"Is Anomalous: {is_anomalous}\n\n"
            "Provide a concise explanation for operators based only on this scoped dataset."
        )
        llm_explanation = llm.generate(prompt)
        what_this_means = llm_explanation.split("\n\n")[0].strip() if llm_explanation else ""
        service = TelemetryService(db, get_embedding_provider(), llm)
        related = service.get_related_channels(meta.name, source_id=source_id, limit=5)

    return ExplainResponse(
        name=meta.name,
        aliases=aliases,
        description=meta.description,
        units=meta.units,
        channel_origin=meta.channel_origin or "catalog",
        discovery_namespace=meta.discovery_namespace,
        statistics=stats,
        recent_value=recent_value,
        z_score=z_score,
        is_anomalous=is_anomalous,
        state=state,
        state_reason=state_reason,
        last_timestamp=last_timestamp,
        red_low=red_low,
        red_high=red_high,
        what_this_means=what_this_means,
        what_to_check_next=related,
        confidence_indicator=_confidence_indicator(stats.n_samples, last_timestamp),
        llm_explanation=llm_explanation,
        scope=_detail_page_scope_payload(scope),
    )


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


def get_embedding_provider() -> SentenceTransformerEmbeddingProvider:
    """Dependency for embedding provider."""
    global _embedding_provider
    if _embedding_provider is None:
        _embedding_provider = SentenceTransformerEmbeddingProvider()
    return _embedding_provider


def get_llm_provider():
    """Dependency for LLM provider (mock if no API key)."""
    global _llm_provider
    if _llm_provider is None:
        settings = get_settings()
        if settings.openai_api_key:
            _llm_provider = OpenAICompatibleLLMProvider()
        else:
            logger.info("No OPENAI_API_KEY configured, using mock LLM provider")
            _llm_provider = MockLLMProvider()
    return _llm_provider


@router.post("/schema", response_model=TelemetrySchemaResponse)
def create_schema(
    body: TelemetrySchemaCreate,
    db: Session = Depends(get_db),
    embedding: SentenceTransformerEmbeddingProvider = Depends(get_embedding_provider),
    llm: object = Depends(get_llm_provider),
):
    """Create telemetry schema with embedding."""
    service = TelemetryService(db, embedding, llm)
    try:
        telemetry_id = service.create_schema(
            source_id=body.source_id,
            name=body.name,
            units=body.units,
            description=body.description,
            subsystem_tag=body.subsystem_tag,
            red_low=body.red_low,
            red_high=body.red_high,
        )
    except IntegrityError:
        raise HTTPException(status_code=409, detail="Telemetry name already exists")
    audit_log(
        "schema.create",
        source_id=body.source_id,
        name=body.name,
        telemetry_id=str(telemetry_id),
    )
    return TelemetrySchemaResponse(
        status="created",
        telemetry_id=telemetry_id,
    )


@router.post("/data", response_model=TelemetryDataResponse)
def ingest_data(
    body: TelemetryDataIngest,
    db: Session = Depends(get_db),
    embedding: SentenceTransformerEmbeddingProvider = Depends(get_embedding_provider),
    llm: object = Depends(get_llm_provider),
):
    """Ingest batch of telemetry data scoped by the source_id in the body."""
    service = TelemetryService(db, embedding, llm)
    try:
        data = []
        for pt in body.data:
            ts = datetime.fromisoformat(pt.timestamp.replace("Z", "+00:00"))
            data.append((ts, pt.value))
        rows = service.insert_data(
            body.stream_id,
            body.telemetry_name,
            data,
            source_id=body.source_id,
            packet_source=body.packet_source,
            receiver_id=body.receiver_id,
        )
    except StreamIdConflictError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    audit_log(
        "ingest.batch",
        telemetry_name=body.telemetry_name,
        count=rows,
        source_id=body.source_id,
        stream_id=body.stream_id,
    )
    return TelemetryDataResponse(rows_inserted=rows)


@router.post("/recompute-stats", response_model=RecomputeStatsResponse)
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


@router.get("/overview", response_model=OverviewResponse)
def overview(
    source_id: str = Query(...),
    db: Session = Depends(get_db),
):
    """Get overview data for watchlist channels, optionally filtered by source."""
    channels = get_overview(db, source_id=source_id)
    return OverviewResponse(channels=[OverviewChannel(**c) for c in channels])


@router.get("/anomalies", response_model=AnomaliesResponse)
def anomalies(
    source_id: str = Query(...),
    db: Session = Depends(get_db),
):
    """Get anomalous channels grouped by subsystem, optionally filtered by source."""
    data = get_anomalies(db, source_id=source_id)
    return AnomaliesResponse(**data)


@router.get("/sources")
def list_sources(db: Session = Depends(get_db)):
    """List registered telemetry stream sources."""
    return get_telemetry_sources(db)


@router.post("/sources")
def create_source_route(
    body: SourceCreate,
    db: Session = Depends(get_db),
    embedding: SentenceTransformerEmbeddingProvider = Depends(get_embedding_provider),
):
    """Create a new telemetry source and seed its telemetry catalog."""
    try:
        result = create_source(
            db,
            embedding_provider=embedding,
            source_type=body.source_type,
            name=body.name,
            description=body.description,
            base_url=body.base_url,
            vehicle_config_path=body.vehicle_config_path,
            monitoring_start_time=body.monitoring_start_time,
            history_mode=body.history_mode,
        )
        audit_log("sources.create", source_id=result["id"], name=body.name)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except IntegrityError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail="source already exists for vehicle_config_path") from e


@router.post("/sources/resolve", response_model=SourceResolveResponse)
def resolve_source_route(
    body: SourceResolveRequest,
    db: Session = Depends(get_db),
    embedding: SentenceTransformerEmbeddingProvider = Depends(get_embedding_provider),
):
    """Resolve or create a vehicle source for adapter startup."""
    try:
        result, created = resolve_source(
            db,
            embedding_provider=embedding,
            source_type=body.source_type,
            name=body.name,
            description=body.description,
            vehicle_config_path=body.vehicle_config_path,
            monitoring_start_time=body.monitoring_start_time,
        )
        audit_log(
            "sources.resolve",
            source_id=result["id"],
            vehicle_config_path=result["vehicle_config_path"],
            created=created,
        )
        return {
            **result,
            "created": created,
            "chunk_size_hours": get_settings().source_reconciliation_chunk_size_hours,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/sources/{source_id}")
def update_source_route(
    source_id: str,
    body: SourceUpdate,
    db: Session = Depends(get_db),
    embedding: SentenceTransformerEmbeddingProvider = Depends(get_embedding_provider),
):
    """Update a telemetry source (name, description, base_url for simulators)."""
    updates = body.model_dump(exclude_unset=True)
    try:
        result = update_source(
            db,
            embedding_provider=embedding,
            source_id=source_id,
            name=updates.get("name"),
            description=updates.get("description"),
            base_url=updates.get("base_url"),
            vehicle_config_path=updates.get("vehicle_config_path"),
            monitoring_start_time=updates.get("monitoring_start_time"),
            history_mode=updates.get("history_mode"),
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    if result is None:
        raise HTTPException(status_code=404, detail="Source not found")
    audit_log("sources.update", source_id=source_id)
    return result


@router.post("/sources/{source_id}/backfill-progress")
def update_source_backfill_progress(
    source_id: str,
    body: BackfillProgressUpdate,
    db: Session = Depends(get_db),
):
    """Update durable platform-owned historical backfill progress for a source."""
    logical_source_id = normalize_source_id(source_id)
    try:
        result = update_backfill_progress(
            db,
            source_id=logical_source_id,
            status=body.status,
            target_time=body.target_time,
            chunk_end=body.chunk_end,
            backlog_drained=body.backlog_drained,
            error=body.error,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if result is None:
        raise HTTPException(status_code=404, detail="Source not found")
    audit_log("sources.backfill_progress", source_id=logical_source_id, status=body.status)
    return result


@router.post("/sources/{source_id}/live-state")
def update_source_live_state(
    source_id: str,
    body: LiveStateUpdate,
    db: Session = Depends(get_db),
):
    """Update durable adapter live-worker state for a source."""
    logical_source_id = normalize_source_id(source_id)
    try:
        result = update_live_state(
            db,
            source_id=logical_source_id,
            state=body.state,
            error=body.error,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if result is None:
        raise HTTPException(status_code=404, detail="Source not found")
    audit_log("sources.live_state", source_id=logical_source_id, state=body.state)
    return result


@router.get(
    "/sources/{source_id}/observations/upcoming",
    response_model=UpcomingObservationsResponse,
)
def get_upcoming_source_observations(
    source_id: str,
    limit: int = Query(default=5, ge=1, le=25),
    provider: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """List upcoming observation windows for a source."""
    logical_source_id = normalize_source_id(source_id)
    try:
        observations = list_upcoming_observations(
            db,
            source_id=logical_source_id,
            now=datetime.now(timezone.utc),
            limit=limit,
            provider=provider,
        )
    except SourceObservationNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    audit_log(
        "source_observations.read_upcoming",
        source_id=logical_source_id,
        provider=provider or "",
        limit=limit,
    )
    return UpcomingObservationsResponse(
        observations=[SourceObservationSchema.model_validate(item) for item in observations]
    )


@router.get(
    "/sources/{source_id}/observations/next",
    response_model=SourceObservationSchema | None,
)
def get_next_source_observation(
    source_id: str,
    provider: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Return the next observation window for a source, if known."""
    logical_source_id = normalize_source_id(source_id)
    try:
        observation = get_next_observation(
            db,
            source_id=logical_source_id,
            now=datetime.now(timezone.utc),
            provider=provider,
        )
    except SourceObservationNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    audit_log(
        "source_observations.read_next",
        source_id=logical_source_id,
        provider=provider or "",
    )
    return SourceObservationSchema.model_validate(observation) if observation is not None else None


@router.post(
    "/sources/{source_id}/observations:batch-upsert",
    response_model=SourceObservationBatchUpsertResponse,
)
def batch_upsert_source_observations(
    source_id: str,
    body: SourceObservationBatchUpsert,
    db: Session = Depends(get_db),
):
    """Write a provider snapshot of observation windows for a source."""
    logical_source_id = normalize_source_id(source_id)
    try:
        result = upsert_source_observations(
            db,
            source_id=logical_source_id,
            batch=body,
            now=datetime.now(timezone.utc),
        )
    except SourceObservationNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    audit_log(
        "source_observations.batch_upsert",
        source_id=logical_source_id,
        provider=body.provider,
        inserted=result.inserted,
        deleted=result.deleted,
    )
    return SourceObservationBatchUpsertResponse(
        inserted=result.inserted,
        deleted=result.deleted,
    )


@router.get("/sources/{source_id}/streams", response_model=ChannelSourcesResponse)
def get_source_streams(
    source_id: str,
    db: Session = Depends(get_db),
):
    """List stream ids for a source (any channel). Newest first."""
    logical_source_id = normalize_source_id(source_id)
    registry_rows = db.execute(
        select(
            TelemetryStream.id,
            TelemetryStream.started_at,
            TelemetryStream.last_seen_at,
        ).where(TelemetryStream.source_id == logical_source_id)
    ).fetchall()
    history_rows = db.execute(
        select(
            TelemetryData.stream_id,
            func.min(TelemetryData.timestamp).label("start_time"),
            func.max(TelemetryData.timestamp).label("last_seen_at"),
            func.count().label("sample_count"),
        )
        .join(TelemetryMetadata, TelemetryMetadata.id == TelemetryData.telemetry_id)
        .where(TelemetryMetadata.source_id == logical_source_id)
        .group_by(TelemetryData.stream_id)
    ).fetchall()

    stream_stats: dict[str, dict[str, object]] = {}
    for stream_id, started_at, seen_at in registry_rows:
        stream_stats[stream_id] = {
            "start_time": started_at,
            "last_timestamp": seen_at,
            "sample_count": None,
        }
    for stream_id, start_time, seen_at, sample_count in history_rows:
        prior = stream_stats.get(stream_id, {})
        prior_seen = prior.get("last_timestamp")
        latest_seen = prior_seen if isinstance(prior_seen, datetime) else None
        stream_stats[stream_id] = {
            "start_time": prior.get("start_time") or start_time,
            "last_timestamp": seen_at if latest_seen is None or (seen_at is not None and seen_at > latest_seen) else latest_seen,
            "sample_count": sample_count,
        }

    rows = sorted(
        stream_stats.items(),
        key=lambda item: (
            item[1]["last_timestamp"].timestamp()
            if isinstance(item[1].get("last_timestamp"), datetime)
            else float("-inf"),
            item[0],
        ),
        reverse=True,
    )
    registry_by_id = _stream_metadata_by_id(db, [stream_id for stream_id, _stats in rows])
    return ChannelSourcesResponse(
        sources=[
            _stream_option(
                stream_id,
                stream=registry_by_id.get(stream_id),
                start_time=stats.get("start_time") if isinstance(stats.get("start_time"), datetime) else None,
                last_timestamp=stats.get("last_timestamp") if isinstance(stats.get("last_timestamp"), datetime) else None,
                sample_count=stats.get("sample_count") if isinstance(stats.get("sample_count"), int) else None,
            )
            for stream_id, stats in rows
        ]
    )


@router.get("/watchlist", response_model=WatchlistResponse)
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


@router.post("/watchlist")
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


@router.delete("/watchlist/{name}")
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


@router.get("/list", response_model=TelemetryListResponse)
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


@router.get("/inventory", response_model=TelemetryInventoryResponse)
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


@router.get("/subsystems")
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


@router.get("/units")
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


@router.get("/search", response_model=SearchResponse)
def search(
    q: str = "",
    subsystem: Optional[str] = None,
    anomalous_only: bool = False,
    units: Optional[str] = None,
    recent_minutes: Optional[int] = None,
    limit: int = 10,
    source_id: str = Query(...),
    db: Session = Depends(get_db),
    embedding: SentenceTransformerEmbeddingProvider = Depends(get_embedding_provider),
    llm: object = Depends(get_llm_provider),
):
    """Semantic search over telemetry with optional filters. source_id scopes current value/stats."""
    service = TelemetryService(db, embedding, llm)
    results = service.semantic_search(
        q,
        limit=limit,
        subsystem=subsystem,
        anomalous_only=anomalous_only,
        units=units,
        recent_minutes=recent_minutes,
        source_id=source_id,
    )
    audit_log(
        "search",
        q=q,
        subsystem=subsystem,
        anomalous_only=anomalous_only,
        limit=limit,
        source_id=source_id,
        result_count=len(results),
    )
    return SearchResponse(results=results)


def _summary_db_only_page_scope(db: Session, data_source_id: str, meta: TelemetryMetadata) -> TelemetryDetailPageScope:
    try:
        sid = _resolve_latest_stream_id_for_channel(db, data_source_id, meta.name)
        return TelemetryDetailPageScope(mode="latest", resolved_stream_id=sid)
    except HTTPException:
        return TelemetryDetailPageScope(mode="latest", resolved_stream_id=None)


def _get_explanation_summary_db_only(db: Session, name: str, source_id: str) -> ExplainResponse:
    """Build explain response using only DB—no embedding/LLM cold start."""
    data_source_id = normalize_source_id(source_id)
    meta = _get_channel_meta(db, source_id, name)
    if not meta:
        raise ValueError(f"Telemetry not found: {name}")
    aliases = get_aliases_by_telemetry_ids(
        db,
        source_id=source_id,
        telemetry_ids=[meta.id],
    ).get(meta.id, [])

    stats_row = db.get(TelemetryStatistics, (data_source_id, meta.id))
    if not stats_row:
        # Compute stats on-the-fly when missing (e.g. new simulator source)
        stats_service = StatisticsService(db)
        stats_service._recompute_one(meta.id, source_id=data_source_id)
        db.flush()
        stats_row = db.get(TelemetryStatistics, (data_source_id, meta.id))
    if not stats_row:
        red_low = float(meta.red_low) if meta.red_low is not None else None
        red_high = float(meta.red_high) if meta.red_high is not None else None
        return ExplainResponse(
            name=meta.name,
            aliases=aliases,
            description=meta.description,
            units=meta.units,
            channel_origin=meta.channel_origin or "catalog",
            discovery_namespace=meta.discovery_namespace,
            statistics=StatisticsResponse(
                mean=None,
                std_dev=None,
                min_value=None,
                max_value=None,
                p5=None,
                p50=None,
                p95=None,
                n_samples=0,
            ),
            recent_value=None,
            z_score=None,
            is_anomalous=False,
            state="no_data",
            state_reason="no_samples",
            last_timestamp=None,
            red_low=red_low,
            red_high=red_high,
            what_this_means="",
            what_to_check_next=[],
            confidence_indicator=None,
            llm_explanation="",
            scope=_summary_db_only_page_scope(db, data_source_id, meta),
        )

    rows = _get_recent_values_db_only(db, name, limit=1, source_id=data_source_id)
    recent_value: Optional[float] = float(rows[0][1]) if rows else None
    last_timestamp: Optional[str] = rows[0][0].isoformat() if rows else None

    mean = float(stats_row.mean)
    std_dev = float(stats_row.std_dev)
    z_score: Optional[float] = None
    is_anomalous = False

    if recent_value is not None and std_dev > 0:
        z_score = (recent_value - mean) / std_dev
        is_anomalous = abs(z_score) > 2

    if recent_value is None:
        recent_value = mean

    red_low = float(meta.red_low) if meta.red_low is not None else None
    red_high = float(meta.red_high) if meta.red_high is not None else None
    state, state_reason = _compute_state(recent_value, z_score, red_low, red_high, std_dev)

    return ExplainResponse(
        name=meta.name,
        aliases=aliases,
        description=meta.description,
        units=meta.units,
        channel_origin=meta.channel_origin or "catalog",
        discovery_namespace=meta.discovery_namespace,
        statistics=StatisticsResponse(
            mean=mean,
            std_dev=std_dev,
            min_value=float(stats_row.min_value),
            max_value=float(stats_row.max_value),
            p5=float(stats_row.p5),
            p50=float(stats_row.p50),
            p95=float(stats_row.p95),
            n_samples=getattr(stats_row, "n_samples", 0),
        ),
        recent_value=recent_value,
        z_score=z_score,
        is_anomalous=is_anomalous,
        state=state,
        state_reason=state_reason,
        last_timestamp=last_timestamp,
        red_low=red_low,
        red_high=red_high,
        what_this_means="",
        what_to_check_next=[],
        confidence_indicator=None,
        llm_explanation="",
        scope=_summary_db_only_page_scope(db, data_source_id, meta),
    )


@router.get("/{name}/summary", response_model=ExplainResponse)
def get_summary(
    name: str,
    source_id: str,
    db: Session = Depends(get_db),
):
    """Fast summary for initial page load—DB only, no embedding/LLM. source_id filters by stream source."""
    name = unquote(name)
    try:
        return _get_explanation_summary_db_only(db, name, source_id=source_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/sources/{source_id}/channels/{name}/summary", response_model=ExplainResponse)
def get_summary_for_source(
    source_id: str,
    name: str,
    scope: str = "latest",
    stream_ids: list[str] = Query(default=[]),
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
    return _build_scoped_explain_response(
        db,
        meta=meta,
        source_id=normalize_source_id(source_id),
        scope=detail_scope,
        include_llm=False,
    )


@router.get("/{name}/explain", response_model=ExplainResponse)
def explain(
    name: str,
    skip_llm: bool = False,
    source_id: str = Query(...),
    db: Session = Depends(get_db),
    embedding: SentenceTransformerEmbeddingProvider = Depends(get_embedding_provider),
    llm: object = Depends(get_llm_provider),
):
    """Get explanation for a telemetry point. Use skip_llm=1 for fast initial load. source_id filters by stream source."""
    name = unquote(name)
    service = TelemetryService(db, embedding, llm)
    try:
        return service.get_explanation(name, skip_llm=skip_llm, source_id=source_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/sources/{source_id}/channels/{name}/explain", response_model=ExplainResponse)
def explain_for_source(
    source_id: str,
    name: str,
    scope: str = "latest",
    stream_ids: list[str] = Query(default=[]),
    since: Optional[str] = None,
    until: Optional[str] = None,
    skip_llm: bool = False,
    db: Session = Depends(get_db),
    embedding: SentenceTransformerEmbeddingProvider = Depends(get_embedding_provider),
    llm: object = Depends(get_llm_provider),
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
    return _build_scoped_explain_response(
        db=db,
        meta=meta,
        source_id=normalize_source_id(source_id),
        scope=detail_scope,
        include_llm=not skip_llm,
        llm=llm,
    )


@router.get("/{name}/streams", response_model=ChannelSourcesResponse)
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


@router.get("/{name}/recent", response_model=RecentDataResponse)
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
    since_dt: Optional[datetime] = None
    until_dt: Optional[datetime] = None
    if since:
        try:
            since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid since format, use ISO8601")
    if until:
        try:
            until_dt = datetime.fromisoformat(until.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid until format, use ISO8601")
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


@router.get("/sources/{source_id}/channels/{name}/recent", response_model=RecentDataResponse)
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
        applied_time_filter=bool(data_points) and (detail_scope.since is not None or detail_scope.until is not None),
        fallback_to_recent=False,
    )


@router.get("/sources/{source_id}/channels/{name}/streams", response_model=ChannelSourcesResponse)
def get_channel_streams_for_source(
    source_id: str,
    name: str,
    db: Session = Depends(get_db),
):
    name = unquote(name)
    return get_channel_streams(name=name, source_id=source_id, db=db)


@router.post("/sources/active-stream")
def set_active_stream(
    body: ActiveStreamUpdate,
    db: Session = Depends(get_db),
):
    """Set or clear the active stream for any logical source.

    External adapters (e.g. SatNOGS/FUNcube-1) use this to mark AOS/LOS
    without needing simulator-specific /status polling.
    """
    logical_source_id = normalize_source_id(body.source_id)

    if body.state == "active":
        if not body.stream_id:
            raise HTTPException(status_code=400, detail="stream_id is required when state=active")
        existing_owner = get_stream_source_id(db, body.stream_id)
        if existing_owner is not None and normalize_source_id(existing_owner) != logical_source_id:
            raise HTTPException(status_code=404, detail="stream_id does not belong to source")
        try:
            register_stream(db, source_id=logical_source_id, stream_id=body.stream_id)
        except StreamIdConflictError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except SourceNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))
        audit_log(
            "sources.active_stream.set",
            source_id=logical_source_id,
            stream_id=body.stream_id,
            state="active",
        )
        return {
            "status": "active",
            "source_id": logical_source_id,
            "stream_id": body.stream_id,
        }

    if body.state == "idle":
        clear_active_stream(logical_source_id, db=db)
        audit_log(
            "sources.active_stream.set",
            source_id=logical_source_id,
            state="idle",
        )
        return {
            "status": "idle",
            "source_id": logical_source_id,
        }

    raise HTTPException(status_code=400, detail="state must be 'active' or 'idle'")
