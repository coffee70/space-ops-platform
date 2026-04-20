"""Helpers for resolving source-scoped channel aliases to canonical telemetry metadata."""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.telemetry import TelemetryChannelAlias, TelemetryMetadata
from app.services.source_stream_service import get_stream_source_id, normalize_source_id


def resolve_channel_metadata(
    db: Session,
    *,
    source_id: str,
    channel_name: str,
) -> TelemetryMetadata | None:
    """Resolve an exact canonical name or configured alias for one source."""
    logical_source_id = get_stream_source_id(db, source_id) or normalize_source_id(source_id)
    meta = db.execute(
        select(TelemetryMetadata).where(
            TelemetryMetadata.source_id == logical_source_id,
            TelemetryMetadata.name == channel_name,
        )
    ).scalars().first()
    if meta is not None:
        return meta

    return db.execute(
        select(TelemetryMetadata)
        .join(TelemetryChannelAlias, TelemetryChannelAlias.telemetry_id == TelemetryMetadata.id)
        .where(TelemetryChannelAlias.source_id == logical_source_id)
        .where(TelemetryChannelAlias.alias_name == channel_name)
    ).scalars().first()


def resolve_channel_name(
    db: Session,
    *,
    source_id: str,
    channel_name: str,
) -> str | None:
    meta = resolve_channel_metadata(db, source_id=source_id, channel_name=channel_name)
    return meta.name if meta is not None else None


def get_aliases_by_telemetry_ids(
    db: Session,
    *,
    source_id: str,
    telemetry_ids: Iterable[UUID],
) -> dict[UUID, list[str]]:
    ids = list(telemetry_ids)
    if not ids:
        return {}

    logical_source_id = get_stream_source_id(db, source_id) or normalize_source_id(source_id)
    rows = db.execute(
        select(TelemetryChannelAlias.telemetry_id, TelemetryChannelAlias.alias_name)
        .where(TelemetryChannelAlias.source_id == logical_source_id)
        .where(TelemetryChannelAlias.telemetry_id.in_(ids))
        .order_by(TelemetryChannelAlias.alias_name)
    ).fetchall()

    aliases: dict[UUID, list[str]] = defaultdict(list)
    for telemetry_id, alias_name in rows:
        aliases[telemetry_id].append(alias_name)
    return dict(aliases)
