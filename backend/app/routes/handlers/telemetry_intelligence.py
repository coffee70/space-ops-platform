from typing import Optional
from urllib.parse import unquote

from fastapi import Depends, HTTPException, Query
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.database import get_db
from app.lib.audit import audit_log
from app.models.schemas import (
    ExplainResponse,
    RelatedChannel,
    SearchResponse,
    StatisticsResponse,
    TelemetryDetailPageScope,
    TelemetrySchemaCreate,
    TelemetrySchemaResponse,
)
from app.models.telemetry import TelemetryMetadata, TelemetryStatistics
from app.routes.handlers.providers import get_embedding_provider, get_llm_provider
from app.routes.handlers.scope import (
    DetailDataScope,
    _confidence_indicator,
    _detail_page_scope_payload,
    _get_channel_meta,
    _get_recent_values_db_only,
    _get_scoped_recent_values,
    _get_scoped_statistics,
    _resolve_detail_data_scope,
    _resolve_latest_stream_id_for_channel,
)
from app.services.channel_alias_service import get_aliases_by_telemetry_ids
from app.services.embedding_service import SentenceTransformerEmbeddingProvider
from app.services.source_stream_service import normalize_source_id
from app.services.statistics_service import StatisticsService
from app.services.telemetry_service import TelemetryService, _compute_state


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


def _summary_db_only_page_scope(
    db: Session,
    data_source_id: str,
    meta: TelemetryMetadata,
) -> TelemetryDetailPageScope:
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
