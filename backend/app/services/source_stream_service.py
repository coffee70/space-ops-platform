"""Resolve logical sources to active telemetry streams."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Optional, TypeVar

import httpx
from sqlalchemy import desc, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.models.telemetry import TelemetryCurrent, TelemetryData, TelemetryMetadata, TelemetrySource, TelemetryStream

ACTIVE_STREAM_CACHE_TTL_SEC = 30.0
SIMULATOR_STATUS_CACHE_TTL_SEC = 2.0

_active_stream_by_source: dict[str, tuple[str, float]] = {}
_simulator_status_by_source: dict[str, tuple[dict[str, object], float]] = {}
_stream_owner_by_stream: dict[str, tuple[str, float]] = {}

T = TypeVar("T")


class StreamIdConflictError(ValueError):
    """Raised when a stream id collides with a reserved source id."""


class SourceNotFoundError(ValueError):
    """Raised when stream registration targets an unknown source."""


def normalize_source_id(source_id: str) -> str:
    """Return the persisted source id unchanged."""
    return source_id


def _cache_stream_owner(stream_id: str, source_id: str, *, seen_at: float | None = None) -> None:
    _stream_owner_by_stream[stream_id] = (source_id, seen_at if seen_at is not None else time.time())


def _get_ttl_cache_entry(
    cache: dict[str, tuple[T, float]],
    key: str,
    *,
    ttl_sec: float,
) -> Optional[tuple[T, float]]:
    cached = cache.get(key)
    if cached is None:
        return None
    value, seen_at = cached
    if time.time() - seen_at > ttl_sec:
        cache.pop(key, None)
        return None
    return value, seen_at


def _get_cached_stream_source_id(
    stream_id: str,
    *,
    max_age_sec: float = ACTIVE_STREAM_CACHE_TTL_SEC,
) -> Optional[str]:
    cached = _get_ttl_cache_entry(_stream_owner_by_stream, stream_id, ttl_sec=max_age_sec)
    return cached[0] if cached is not None else None


def get_stream_source_id(db: Session | None, stream_id: str) -> Optional[str]:
    """Resolve a stream id to its owning source, if known."""
    cached_source_id = _get_cached_stream_source_id(stream_id)
    if db is None:
        return cached_source_id

    row = db.get(TelemetryStream, stream_id)
    if row is not None:
        _cache_stream_owner(stream_id, row.source_id)
        return row.source_id
    if cached_source_id is not None:
        return cached_source_id

    source_id = (
        db.execute(
            select(TelemetryMetadata.source_id)
            .join(TelemetryCurrent, TelemetryCurrent.telemetry_id == TelemetryMetadata.id)
            .where(TelemetryCurrent.stream_id == stream_id)
            .distinct()
        )
        .scalars()
        .first()
    )
    if source_id is not None:
        _cache_stream_owner(stream_id, source_id)
        return source_id

    source_id = (
        db.execute(
            select(TelemetryMetadata.source_id)
            .join(TelemetryData, TelemetryData.telemetry_id == TelemetryMetadata.id)
            .where(TelemetryData.stream_id == stream_id)
            .distinct()
        )
        .scalars()
        .first()
    )
    if source_id is not None:
        _cache_stream_owner(stream_id, source_id)
    return source_id


def ensure_stream_belongs_to_source(
    db: Session,
    source_id: str,
    stream_id: str | None = None,
) -> str:
    """Return a stream id only if it belongs to the scoped source."""
    logical_source_id = normalize_source_id(source_id)
    if not stream_id:
        return logical_source_id

    owning_source_id = get_stream_source_id(db, stream_id)
    if owning_source_id != logical_source_id:
        raise ValueError("Stream not found for source")
    return stream_id


def get_logical_source(db: Session, source_id: str) -> Optional[TelemetrySource]:
    """Return the logical source row."""
    return db.get(TelemetrySource, normalize_source_id(source_id))


def register_stream(
    db: Session,
    *,
    source_id: str,
    stream_id: str,
    packet_source: str | None = None,
    receiver_id: str | None = None,
    started_at: datetime | None = None,
    seen_at: datetime | None = None,
    activate: bool = True,
) -> TelemetryStream:
    """Create or update a telemetry stream row and optionally mark it active in cache."""
    logical_source_id = normalize_source_id(source_id)
    source = db.get(TelemetrySource, logical_source_id)
    if source is None:
        raise SourceNotFoundError(f"Source not found: {logical_source_id}")

    reserved_source_id = normalize_source_id(stream_id)
    existing_source = db.get(TelemetrySource, reserved_source_id)
    if existing_source is not None and reserved_source_id != logical_source_id:
        raise StreamIdConflictError("stream_id conflicts with an existing source_id")

    observed_at = seen_at or started_at or datetime.now(timezone.utc)
    started_at = started_at or observed_at
    stream = db.get(TelemetryStream, stream_id)
    if stream is None:
        db.execute(
            pg_insert(TelemetryStream)
            .values(
                id=stream_id,
                source_id=logical_source_id,
                packet_source=packet_source,
                receiver_id=receiver_id,
                status="active" if activate else "idle",
                started_at=started_at,
                last_seen_at=observed_at,
            )
            .on_conflict_do_nothing(index_elements=[TelemetryStream.id])
        )
        stream = db.get(TelemetryStream, stream_id)

    if stream is None:
        raise RuntimeError("Telemetry stream registration failed")
    if stream.source_id != logical_source_id:
        raise StreamIdConflictError("stream_id does not belong to source")

    stream.source_id = logical_source_id
    if activate:
        stream.status = "active"
    elif getattr(stream, "status", None) is None:
        stream.status = "idle"
    if getattr(stream, "started_at", None) is None:
        stream.started_at = started_at
    stream.last_seen_at = observed_at
    if packet_source is not None:
        stream.packet_source = packet_source
    if receiver_id is not None:
        stream.receiver_id = receiver_id

    _cache_stream_owner(stream_id, logical_source_id)
    if activate:
        _active_stream_by_source[logical_source_id] = (stream_id, time.time())
    return stream


def clear_active_stream(source_id: str, *, db: Session | None = None) -> None:
    """Forget the active stream for a source and mark it idle when possible."""
    logical_source_id = normalize_source_id(source_id)
    _active_stream_by_source.pop(logical_source_id, None)
    _simulator_status_by_source.pop(logical_source_id, None)
    if db is None:
        return
    streams = (
        db.execute(
            select(TelemetryStream).where(
                TelemetryStream.source_id == logical_source_id,
                TelemetryStream.status == "active",
            )
        )
        .scalars()
        .all()
    )
    for stream in streams:
        stream.status = "idle"


def _cache_simulator_status(
    source_id: str,
    *,
    state: str,
    active_stream_id: str | None,
    packet_source: str | None = None,
    receiver_id: str | None = None,
    seen_at: float | None = None,
) -> None:
    logical_source_id = normalize_source_id(source_id)
    _simulator_status_by_source[logical_source_id] = (
        {
            "state": state,
            "active_stream_id": active_stream_id,
            "packet_source": packet_source,
            "receiver_id": receiver_id,
        },
        seen_at if seen_at is not None else time.time(),
    )


def _get_cached_simulator_status_entry(source_id: str) -> Optional[tuple[dict[str, object], float]]:
    logical_source_id = normalize_source_id(source_id)
    return _get_ttl_cache_entry(
        _simulator_status_by_source,
        logical_source_id,
        ttl_sec=SIMULATOR_STATUS_CACHE_TTL_SEC,
    )


def _should_refresh_simulator_status(
    source_id: str,
    *,
    min_poll_interval_sec: float = SIMULATOR_STATUS_CACHE_TTL_SEC,
) -> bool:
    logical_source_id = normalize_source_id(source_id)
    cached = _simulator_status_by_source.get(logical_source_id)
    if cached is None:
        return True
    _status, seen_at = cached
    return (time.time() - seen_at) > min_poll_interval_sec


def _get_cached_active_stream_entry(
    source_id: str,
    *,
    max_age_sec: float = ACTIVE_STREAM_CACHE_TTL_SEC,
) -> Optional[tuple[str, float]]:
    logical_source_id = normalize_source_id(source_id)
    return _get_ttl_cache_entry(
        _active_stream_by_source,
        logical_source_id,
        ttl_sec=max_age_sec,
    )


def _resolve_simulator_status(
    db: Session,
    logical_source_id: str,
    payload: dict[str, object],
    *,
    refresh_cache: bool,
) -> str | None:
    state = payload.get("state")
    config = payload.get("config") or {}
    if not isinstance(config, dict):
        return None

    active_stream_id = config.get("stream_id")
    packet_source = config.get("packet_source")
    receiver_id = config.get("receiver_id")

    if refresh_cache:
        _cache_simulator_status(
            logical_source_id,
            state=state if isinstance(state, str) else "idle",
            active_stream_id=active_stream_id if isinstance(active_stream_id, str) else None,
            packet_source=packet_source if isinstance(packet_source, str) else None,
            receiver_id=receiver_id if isinstance(receiver_id, str) else None,
        )

    if state == "idle":
        clear_active_stream(logical_source_id, db=db)
        return logical_source_id

    if state and state != "idle" and isinstance(active_stream_id, str) and active_stream_id:
        try:
            register_stream(
                db,
                source_id=logical_source_id,
                stream_id=active_stream_id,
                packet_source=packet_source if isinstance(packet_source, str) else None,
                receiver_id=receiver_id if isinstance(receiver_id, str) else None,
            )
        except (SourceNotFoundError, StreamIdConflictError):
            return None
        return active_stream_id

    return None


def _resolve_simulator_backed_active_stream(
    db: Session,
    logical_source_id: str,
    *,
    base_url: str,
    timeout: float,
    cached_stream_entry: tuple[str, float] | None,
) -> str | None:
    cached_status_entry = _get_cached_simulator_status_entry(logical_source_id)

    if (
        cached_stream_entry is not None
        and cached_status_entry is not None
        and cached_stream_entry[1] >= cached_status_entry[1]
    ):
        return cached_stream_entry[0]

    if _should_refresh_simulator_status(logical_source_id):
        payload: dict[str, object] | None = None
        try:
            with httpx.Client(timeout=timeout) as client:
                res = client.get(f"{base_url.rstrip('/')}/status")
            if res.status_code < 400:
                raw_payload = res.json()
                if isinstance(raw_payload, dict):
                    payload = raw_payload
        except Exception:
            payload = None

        if payload is not None:
            resolved = _resolve_simulator_status(
                db,
                logical_source_id,
                payload,
                refresh_cache=True,
            )
            if resolved is not None:
                return resolved

    if cached_status_entry is not None:
        cached_status, _ = cached_status_entry
        resolved = _resolve_simulator_status(
            db,
            logical_source_id,
            cached_status,
            refresh_cache=False,
        )
        if resolved is not None:
            return resolved

    return None


def resolve_active_stream_id(db: Session, source_id: str, *, timeout: float = 2.0) -> str:
    """Resolve a logical source id to the active telemetry stream id when available."""
    logical_source_id = normalize_source_id(source_id)
    cached_stream_entry = _get_cached_active_stream_entry(logical_source_id)

    src = get_logical_source(db, logical_source_id)
    if src is not None and src.source_type == "simulator" and src.base_url:
        resolved = _resolve_simulator_backed_active_stream(
            db,
            logical_source_id,
            base_url=src.base_url,
            timeout=timeout,
            cached_stream_entry=cached_stream_entry,
        )
        if resolved is not None:
            return resolved

    if cached_stream_entry is not None:
        return cached_stream_entry[0]

    freshness_cutoff = datetime.now(timezone.utc) - timedelta(seconds=60)
    row = (
        db.execute(
            select(TelemetryStream)
            .where(
                TelemetryStream.source_id == logical_source_id,
                TelemetryStream.status == "active",
                TelemetryStream.last_seen_at >= freshness_cutoff,
            )
            .order_by(TelemetryStream.last_seen_at.desc())
        )
        .scalars()
        .first()
    )
    if isinstance(row, TelemetryStream):
        _active_stream_by_source[logical_source_id] = (row.id, time.time())
        return row.id

    latest_row = (
        db.execute(
            select(TelemetryStream)
            .where(TelemetryStream.source_id == logical_source_id)
            .order_by(TelemetryStream.last_seen_at.desc())
        )
        .scalars()
        .first()
    )
    current_row = (
        db.execute(
            select(TelemetryCurrent)
            .join(TelemetryMetadata, TelemetryMetadata.id == TelemetryCurrent.telemetry_id)
            .where(
                TelemetryMetadata.source_id == logical_source_id,
                TelemetryCurrent.reception_time >= freshness_cutoff,
            )
            .order_by(
                TelemetryCurrent.reception_time.desc(),
                TelemetryCurrent.generation_time.desc(),
            )
        )
        .scalars()
        .first()
    )
    current_stream_id = getattr(current_row, "stream_id", None)
    if isinstance(current_stream_id, str) and current_stream_id:
        try:
            register_stream(
                db,
                source_id=logical_source_id,
                stream_id=current_stream_id,
                packet_source=getattr(current_row, "packet_source", None),
                receiver_id=getattr(current_row, "receiver_id", None),
                seen_at=getattr(current_row, "reception_time", None),
            )
        except (SourceNotFoundError, StreamIdConflictError):
            pass
        else:
            return current_stream_id

    if latest_row is not None:
        latest_status = getattr(latest_row, "status", None)
        latest_seen_at = getattr(latest_row, "last_seen_at", None)
        if (
            latest_status == "active"
            and isinstance(latest_seen_at, datetime)
            and latest_seen_at >= freshness_cutoff
        ):
            _active_stream_by_source[logical_source_id] = (latest_row.id, time.time())
            return latest_row.id

    return logical_source_id


def resolve_latest_stream_id(db: Session, source_id: str, *, timeout: float = 2.0) -> str:
    """Resolve a source to its latest concrete stream, preserving explicit stream ids."""
    if get_stream_source_id(db, source_id) is not None:
        return source_id

    logical_source_id = normalize_source_id(source_id)
    resolved = resolve_active_stream_id(db, logical_source_id, timeout=timeout)
    if resolved != logical_source_id:
        return resolved

    latest_registry_row = (
        db.execute(
            select(TelemetryStream.id, TelemetryStream.last_seen_at)
            .where(TelemetryStream.source_id == logical_source_id)
            .order_by(TelemetryStream.last_seen_at.desc(), TelemetryStream.id.desc())
        )
        .first()
    )
    latest_history_row = (
        db.execute(
            select(
                TelemetryData.stream_id,
                func.max(TelemetryData.timestamp).label("last_seen_at"),
            )
            .join(TelemetryMetadata, TelemetryMetadata.id == TelemetryData.telemetry_id)
            .where(TelemetryMetadata.source_id == logical_source_id)
            .group_by(TelemetryData.stream_id)
            .order_by(desc(func.max(TelemetryData.timestamp)), desc(TelemetryData.stream_id))
        )
        .first()
    )

    latest_visible_stream_id: str | None = None
    latest_visible_seen_at: datetime | None = None

    if latest_registry_row is not None:
        latest_visible_stream_id = latest_registry_row[0]
        latest_visible_seen_at = latest_registry_row[1]

    if latest_history_row is not None:
        history_stream_id, history_seen_at = latest_history_row
        if (
            latest_visible_stream_id is None
            or (
                isinstance(history_seen_at, datetime)
                and (
                    latest_visible_seen_at is None
                    or history_seen_at >= latest_visible_seen_at
                )
            )
        ):
            latest_visible_stream_id = history_stream_id
            latest_visible_seen_at = history_seen_at

    if isinstance(latest_visible_stream_id, str) and latest_visible_stream_id:
        return latest_visible_stream_id
    return logical_source_id
