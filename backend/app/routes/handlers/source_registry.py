from datetime import datetime, timezone
from typing import Optional

from fastapi import Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import get_settings
from app.database import get_db
from app.lib.audit import audit_log
from app.models.schemas import (
    ActiveStreamUpdate,
    BackfillProgressUpdate,
    ChannelSourcesResponse,
    LiveStateUpdate,
    SourceCreate,
    SourceObservationBatchUpsert,
    SourceObservationBatchUpsertResponse,
    SourceObservationSchema,
    SourceResolveRequest,
    SourceResolveResponse,
    SourceUpdate,
    UpcomingObservationsResponse,
)
from app.models.telemetry import TelemetryData, TelemetryMetadata, TelemetryStream
from app.routes.handlers.providers import get_embedding_provider
from app.routes.handlers.scope import _stream_metadata_by_id, _stream_option
from app.services.embedding_service import SentenceTransformerEmbeddingProvider
from app.services.realtime_service import (
    create_source,
    get_telemetry_sources,
    resolve_source,
    update_backfill_progress,
    update_live_state,
    update_source,
)
from app.services.source_observation_service import (
    SourceObservationNotFoundError,
    get_next_observation,
    list_upcoming_observations,
    upsert_source_observations,
)
from app.services.source_stream_service import (
    SourceNotFoundError,
    StreamIdConflictError,
    clear_active_stream,
    get_stream_source_id,
    normalize_source_id,
    register_stream,
)


def list_sources(db: Session = Depends(get_db)):
    """List registered telemetry stream sources."""
    return get_telemetry_sources(db)


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
            "last_timestamp": seen_at
            if latest_seen is None or (seen_at is not None and seen_at > latest_seen)
            else latest_seen,
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
                last_timestamp=stats.get("last_timestamp")
                if isinstance(stats.get("last_timestamp"), datetime)
                else None,
                sample_count=stats.get("sample_count") if isinstance(stats.get("sample_count"), int) else None,
            )
            for stream_id, stats in rows
        ]
    )


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
