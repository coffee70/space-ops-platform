"""Realtime snapshot and subscription helpers."""

import logging
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from sqlalchemy import delete, desc, func, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.interfaces.embedding_provider import EmbeddingProvider
from app.lib.audit import audit_log
from app.models.schemas import RealtimeChannelUpdate, RecentDataPoint, TelemetryAlertSchema
from app.models.telemetry import (
    PositionChannelMapping,
    TelemetryAlert,
    TelemetryChannelAlias,
    TelemetryCurrent,
    TelemetryData,
    TelemetryMetadata,
    TelemetrySource,
    TelemetryStream,
    WatchlistEntry,
)
from app.services.channel_alias_service import get_aliases_by_telemetry_ids
from app.services.source_stream_service import (
    get_stream_source_id,
    normalize_source_id,
    resolve_latest_stream_id,
)
from app.services.vehicle_config_service import list_vehicle_configs, load_vehicle_config
from app.utils.subsystem import infer_subsystem
from telemetry_catalog.definitions import (
    canonical_vehicle_config_path,
    load_vehicle_config_file,
)

logger = logging.getLogger(__name__)

SPARKLINE_POINTS = 30
CHANNEL_ORIGIN_CATALOG = "catalog"
CHANNEL_ORIGIN_DISCOVERED = "discovered"
HISTORY_MODES = {"live_only", "time_window_replay", "cursor_replay"}
LIVE_STATES = {"idle", "active", "error"}
BACKFILL_STATES = {"idle", "running", "complete", "error"}


def _resolve_stream_source_id(db: Session, source_id: str) -> str:
    """Resolve a stream-scoped request to the owning source id."""
    return get_stream_source_id(db, source_id) or normalize_source_id(source_id)


def _resolve_realtime_stream_scope(
    db: Session,
    *,
    source_id: str,
    stream_id: str | None = None,
) -> tuple[str, str]:
    """Return the concrete stream id and logical source id for realtime lookups."""
    logical_source_id = _resolve_stream_source_id(db, source_id)
    if stream_id is not None:
        return stream_id, logical_source_id
    return resolve_latest_stream_id(db, logical_source_id), logical_source_id


def _source_to_dict(src: TelemetrySource) -> dict:
    return {
        "id": src.id,
        "source_id": src.id,
        "name": src.name,
        "description": src.description,
        "source_type": src.source_type,
        "base_url": src.base_url,
        "vehicle_config_path": src.vehicle_config_path,
        "monitoring_start_time": src.monitoring_start_time,
        "last_reconciled_at": src.last_reconciled_at,
        "history_mode": src.history_mode,
        "live_state": src.live_state,
        "backfill_state": src.backfill_state,
        "active_backfill_target_time": src.active_backfill_target_time,
        "last_backfill_started_at": src.last_backfill_started_at,
        "last_backfill_completed_at": src.last_backfill_completed_at,
        "last_backfill_error": src.last_backfill_error,
    }


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_aware_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _default_history_mode(source_type: str, history_mode: str | None) -> str:
    resolved = history_mode or ("live_only" if source_type == "simulator" else "time_window_replay")
    if resolved not in HISTORY_MODES:
        raise ValueError("history_mode must be live_only, time_window_replay, or cursor_replay")
    return resolved


def _default_backfill_state(history_mode: str) -> str:
    return "complete" if history_mode == "live_only" else "idle"


def create_discovered_channel_metadata(
    db: Session,
    *,
    source_id: str,
    channel_name: str,
    discovery_namespace: str | None = None,
    observed_at: datetime | None = None,
) -> TelemetryMetadata:
    """Create or update metadata for a runtime-discovered channel."""
    seen_at = observed_at or _now_utc()
    meta = db.execute(
        select(TelemetryMetadata).where(
            TelemetryMetadata.source_id == source_id,
            TelemetryMetadata.name == channel_name,
        )
    ).scalars().first()
    if meta is None:
        meta = TelemetryMetadata(
            source_id=source_id,
            name=channel_name,
            units="",
            description=None,
            subsystem_tag="dynamic",
            channel_origin=CHANNEL_ORIGIN_DISCOVERED,
            discovery_namespace=discovery_namespace,
            discovered_at=seen_at,
            last_seen_at=seen_at,
        )
        savepoint = db.begin_nested()
        try:
            db.add(meta)
            db.flush()
            savepoint.commit()
            return meta
        except IntegrityError:
            # Another ingest worker won the race to create the same discovered channel.
            savepoint.rollback()
            meta = db.execute(
                select(TelemetryMetadata).where(
                    TelemetryMetadata.source_id == source_id,
                    TelemetryMetadata.name == channel_name,
                )
            ).scalars().first()
            if meta is None:
                raise

    if meta.channel_origin == CHANNEL_ORIGIN_DISCOVERED:
        meta.last_seen_at = seen_at
        if discovery_namespace and not meta.discovery_namespace:
            meta.discovery_namespace = discovery_namespace
    return meta


def _retarget_watchlist_entries(
    db: Session,
    *,
    source_id: str,
    old_name: str,
    new_name: str,
) -> None:
    if old_name == new_name:
        return
    params = {
        "source_id": source_id,
        "old_name": old_name,
        "new_name": new_name,
    }
    db.execute(
        text(
            """
            UPDATE watchlist
            SET telemetry_name = :new_name
            WHERE source_id = :source_id
              AND telemetry_name = :old_name
            """
        ),
        params,
    )
    db.execute(
        text(
            """
            DELETE FROM watchlist
            WHERE id IN (
              SELECT id
              FROM (
                SELECT
                  id,
                  row_number() OVER (
                    PARTITION BY source_id, telemetry_name
                    ORDER BY display_order, created_at, id
                  ) AS row_num
                FROM watchlist
                WHERE source_id = :source_id
                  AND telemetry_name = :new_name
              ) AS ranked
              WHERE row_num > 1
            )
            """
        ),
        params,
    )


def _create_stream_scope_table(db: Session, *, table_name: str, source_id: str) -> None:
    db.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
    db.execute(
        text(
            f"""
            CREATE TEMP TABLE {table_name} ON COMMIT DROP AS
            SELECT :source_id AS stream_id
            UNION
            SELECT ts.id AS stream_id
            FROM telemetry_streams AS ts
            WHERE ts.source_id = :source_id
            """
        ),
        {"source_id": source_id},
    )


def _merge_same_source_metadata(
    db: Session,
    *,
    source_id: str,
    old_meta: TelemetryMetadata,
    new_meta: TelemetryMetadata,
) -> None:
    if old_meta.id == new_meta.id:
        return

    params = {
        "source_id": source_id,
        "old_id": old_meta.id,
        "new_id": new_meta.id,
    }
    scope_table = "tmp_same_source_stream_scope"

    _create_stream_scope_table(db, table_name=scope_table, source_id=source_id)

    db.execute(
        text(
            """
            INSERT INTO telemetry_channel_aliases (
              source_id,
              alias_name,
              telemetry_id,
              created_at
            )
            SELECT
              :source_id,
              tca.alias_name,
              :new_id,
              tca.created_at
            FROM telemetry_channel_aliases AS tca
            WHERE tca.source_id = :source_id
              AND tca.telemetry_id = :old_id
            ON CONFLICT (source_id, alias_name) DO UPDATE
            SET telemetry_id = EXCLUDED.telemetry_id
            """
        ),
        params,
    )
    db.execute(
        text(
            """
            DELETE FROM telemetry_channel_aliases
            WHERE source_id = :source_id
              AND telemetry_id = :old_id
            """
        ),
        params,
    )
    db.execute(
        text(
            """
            INSERT INTO telemetry_data (
              source_id,
              telemetry_id,
              timestamp,
              sequence,
              value,
              packet_source,
              receiver_id
            )
            SELECT
              td.source_id,
              :new_id,
              td.timestamp,
              td.sequence,
              td.value,
              td.packet_source,
              td.receiver_id
            FROM telemetry_data AS td
            WHERE (
              td.source_id = :source_id
              OR td.source_id IN (SELECT stream_id FROM tmp_same_source_stream_scope)
            )
              AND td.telemetry_id = :old_id
            ON CONFLICT (source_id, telemetry_id, timestamp, sequence) DO NOTHING
            """
        ),
        params,
    )
    db.execute(
        text(
            """
            DELETE FROM telemetry_data
            WHERE (
              source_id = :source_id
              OR source_id IN (SELECT stream_id FROM tmp_same_source_stream_scope)
            )
              AND telemetry_id = :old_id
            """
        ),
        params,
    )
    db.execute(
        text(
            """
            INSERT INTO telemetry_current (
              source_id,
              telemetry_id,
              generation_time,
              reception_time,
              value,
              state,
              state_reason,
              z_score,
              quality,
              sequence,
              packet_source,
              receiver_id
            )
            SELECT
              tc.source_id,
              :new_id,
              tc.generation_time,
              tc.reception_time,
              tc.value,
              tc.state,
              tc.state_reason,
              tc.z_score,
              tc.quality,
              tc.sequence,
              tc.packet_source,
              tc.receiver_id
            FROM telemetry_current AS tc
            WHERE (
              tc.source_id = :source_id
              OR tc.source_id IN (SELECT stream_id FROM tmp_same_source_stream_scope)
            )
              AND tc.telemetry_id = :old_id
            ON CONFLICT (source_id, telemetry_id) DO UPDATE
            SET
              generation_time = CASE
                WHEN EXCLUDED.generation_time > telemetry_current.generation_time
                  THEN EXCLUDED.generation_time
                WHEN EXCLUDED.generation_time = telemetry_current.generation_time
                  AND EXCLUDED.reception_time >= telemetry_current.reception_time
                  THEN EXCLUDED.generation_time
                ELSE telemetry_current.generation_time
              END,
              reception_time = CASE
                WHEN EXCLUDED.generation_time > telemetry_current.generation_time
                  THEN EXCLUDED.reception_time
                WHEN EXCLUDED.generation_time = telemetry_current.generation_time
                  AND EXCLUDED.reception_time >= telemetry_current.reception_time
                  THEN EXCLUDED.reception_time
                ELSE telemetry_current.reception_time
              END,
              value = CASE
                WHEN EXCLUDED.generation_time > telemetry_current.generation_time
                  THEN EXCLUDED.value
                WHEN EXCLUDED.generation_time = telemetry_current.generation_time
                  AND EXCLUDED.reception_time >= telemetry_current.reception_time
                  THEN EXCLUDED.value
                ELSE telemetry_current.value
              END,
              state = CASE
                WHEN EXCLUDED.generation_time > telemetry_current.generation_time
                  THEN EXCLUDED.state
                WHEN EXCLUDED.generation_time = telemetry_current.generation_time
                  AND EXCLUDED.reception_time >= telemetry_current.reception_time
                  THEN EXCLUDED.state
                ELSE telemetry_current.state
              END,
              state_reason = CASE
                WHEN EXCLUDED.generation_time > telemetry_current.generation_time
                  THEN EXCLUDED.state_reason
                WHEN EXCLUDED.generation_time = telemetry_current.generation_time
                  AND EXCLUDED.reception_time >= telemetry_current.reception_time
                  THEN EXCLUDED.state_reason
                ELSE telemetry_current.state_reason
              END,
              z_score = CASE
                WHEN EXCLUDED.generation_time > telemetry_current.generation_time
                  THEN EXCLUDED.z_score
                WHEN EXCLUDED.generation_time = telemetry_current.generation_time
                  AND EXCLUDED.reception_time >= telemetry_current.reception_time
                  THEN EXCLUDED.z_score
                ELSE telemetry_current.z_score
              END,
              quality = CASE
                WHEN EXCLUDED.generation_time > telemetry_current.generation_time
                  THEN EXCLUDED.quality
                WHEN EXCLUDED.generation_time = telemetry_current.generation_time
                  AND EXCLUDED.reception_time >= telemetry_current.reception_time
                  THEN EXCLUDED.quality
                ELSE telemetry_current.quality
              END,
              sequence = CASE
                WHEN EXCLUDED.generation_time > telemetry_current.generation_time
                  THEN EXCLUDED.sequence
                WHEN EXCLUDED.generation_time = telemetry_current.generation_time
                  AND EXCLUDED.reception_time >= telemetry_current.reception_time
                  THEN EXCLUDED.sequence
                ELSE telemetry_current.sequence
              END,
              packet_source = CASE
                WHEN EXCLUDED.generation_time > telemetry_current.generation_time
                  THEN EXCLUDED.packet_source
                WHEN EXCLUDED.generation_time = telemetry_current.generation_time
                  AND EXCLUDED.reception_time >= telemetry_current.reception_time
                  THEN EXCLUDED.packet_source
                ELSE telemetry_current.packet_source
              END,
              receiver_id = CASE
                WHEN EXCLUDED.generation_time > telemetry_current.generation_time
                  THEN EXCLUDED.receiver_id
                WHEN EXCLUDED.generation_time = telemetry_current.generation_time
                  AND EXCLUDED.reception_time >= telemetry_current.reception_time
                  THEN EXCLUDED.receiver_id
                ELSE telemetry_current.receiver_id
              END
            """
        ),
        params,
    )
    db.execute(
        text(
            """
            DELETE FROM telemetry_current
            WHERE (
              source_id = :source_id
              OR source_id IN (SELECT stream_id FROM tmp_same_source_stream_scope)
            )
              AND telemetry_id = :old_id
            """
        ),
        params,
    )
    db.execute(
        text(
            """
            DELETE FROM telemetry_statistics
            WHERE (
              source_id = :source_id
              OR source_id IN (SELECT stream_id FROM tmp_same_source_stream_scope)
            )
              AND telemetry_id IN (:old_id, :new_id)
            """
        ),
        params,
    )
    db.execute(
        text(
            """
            INSERT INTO telemetry_statistics (
              source_id,
              telemetry_id,
              mean,
              std_dev,
              min_value,
              max_value,
              p5,
              p50,
              p95,
              n_samples,
              last_computed_at
            )
            SELECT
              td.source_id,
              :new_id,
              AVG(td.value),
              COALESCE(STDDEV_POP(td.value), 0),
              MIN(td.value),
              MAX(td.value),
              PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY td.value),
              PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY td.value),
              PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY td.value),
              COUNT(*),
              NOW()
            FROM telemetry_data AS td
            WHERE (
              td.source_id = :source_id
              OR td.source_id IN (SELECT stream_id FROM tmp_same_source_stream_scope)
            )
              AND td.telemetry_id = :new_id
            GROUP BY td.source_id
            """
        ),
        params,
    )
    db.execute(
        text(
            """
            UPDATE telemetry_alerts
            SET telemetry_id = :new_id
            WHERE telemetry_id = :old_id
              AND (
                source_id = :source_id
                OR source_id IN (SELECT stream_id FROM tmp_same_source_stream_scope)
              )
            """
        ),
        params,
    )

    _retarget_watchlist_entries(
        db,
        source_id=source_id,
        old_name=old_meta.name,
        new_name=new_meta.name,
    )
    db.delete(old_meta)


def _seed_metadata_for_source(
    db: Session,
    *,
    source_id: str,
    vehicle_config_path: str,
    embedding_provider: EmbeddingProvider | None = None,
    prune_missing: bool = False,
    refresh_embeddings: bool = True,
    preserve_existing_embeddings: bool = False,
    overwrite_position_mapping: bool = True,
) -> bool:
    needs_embedding_backfill = False
    vehicle_config = load_vehicle_config_file(vehicle_config_path)
    existing_rows = db.execute(
        select(TelemetryMetadata).where(TelemetryMetadata.source_id == source_id)
    ).scalars().all()
    existing_by_name = {row.name: row for row in existing_rows}
    existing_aliases = db.execute(
        select(TelemetryChannelAlias).where(TelemetryChannelAlias.source_id == source_id)
    ).scalars().all()
    existing_aliases_by_name = {row.alias_name: row for row in existing_aliases}
    expected_names = {channel.name for channel in vehicle_config.channels}
    expected_aliases = {alias for channel in vehicle_config.channels for alias in channel.aliases}
    preserved_alias_names = expected_aliases - expected_names
    removed_names: set[str] = set()

    db.execute(
        delete(TelemetryChannelAlias).where(
            TelemetryChannelAlias.source_id == source_id,
            TelemetryChannelAlias.alias_name.not_in(expected_aliases),
        )
    )

    if prune_missing:
        removed_names = {
            row.name
            for row in existing_rows
            if row.channel_origin != CHANNEL_ORIGIN_DISCOVERED
            and row.name not in expected_names
            and row.name not in preserved_alias_names
        }
        if removed_names:
            db.execute(
                delete(WatchlistEntry).where(
                    WatchlistEntry.source_id == source_id,
                    WatchlistEntry.telemetry_name.in_(removed_names),
                )
            )
        for row in existing_rows:
            if (
                row.channel_origin != CHANNEL_ORIGIN_DISCOVERED
                and row.name not in expected_names
                and row.name not in preserved_alias_names
            ):
                db.delete(row)
        if removed_names:
            existing_by_name = {
                name: row for name, row in existing_by_name.items() if name not in removed_names
            }

    for channel in vehicle_config.channels:
        meta = existing_by_name.get(channel.name)
        created_meta = False
        if meta is None:
            renamed_alias = next(
                (
                    existing_by_name[alias_name]
                    for alias_name in channel.aliases
                    if alias_name in existing_by_name
                    and existing_by_name[alias_name].channel_origin != CHANNEL_ORIGIN_DISCOVERED
                    and alias_name in preserved_alias_names
                ),
                None,
            )
            if renamed_alias is not None:
                old_name = renamed_alias.name
                existing_by_name.pop(renamed_alias.name, None)
                renamed_alias.name = channel.name
                existing_by_name[channel.name] = renamed_alias
                _retarget_watchlist_entries(
                    db,
                    source_id=source_id,
                    old_name=old_name,
                    new_name=channel.name,
                )
                meta = renamed_alias
        if meta is None:
            meta = TelemetryMetadata(
                id=uuid.uuid4(),
                source_id=source_id,
                name=channel.name,
                channel_origin=CHANNEL_ORIGIN_CATALOG,
            )
            db.add(meta)
            created_meta = True
        elif meta.channel_origin == CHANNEL_ORIGIN_DISCOVERED:
            meta.channel_origin = CHANNEL_ORIGIN_CATALOG
            if meta.embedding is None:
                needs_embedding_backfill = True
        if refresh_embeddings and not (preserve_existing_embeddings and meta.embedding is not None):
            if embedding_provider is None:
                raise ValueError("embedding_provider is required when refresh_embeddings=True")
            text_for_embedding = f"{channel.name} {channel.units} {channel.description}".strip()
            meta.embedding = embedding_provider.embed(text_for_embedding)
        meta.units = channel.units
        meta.description = channel.description
        meta.subsystem_tag = channel.subsystem
        meta.discovery_namespace = None
        meta.red_low = Decimal(str(channel.red_low)) if channel.red_low is not None else None
        meta.red_high = Decimal(str(channel.red_high)) if channel.red_high is not None else None
        if created_meta:
            # Flush new metadata rows before inserting alias rows that reference them
            # so clean baseline bootstraps satisfy FK constraints on first startup.
            db.flush()
        for alias_name in channel.aliases:
            conflicting_meta = existing_by_name.get(alias_name)
            if conflicting_meta is not None and conflicting_meta.name != channel.name:
                if conflicting_meta.channel_origin == CHANNEL_ORIGIN_DISCOVERED:
                    _merge_same_source_metadata(
                        db,
                        source_id=source_id,
                        old_meta=conflicting_meta,
                        new_meta=meta,
                    )
                    existing_by_name.pop(alias_name, None)
                else:
                    raise ValueError(
                        f"channel alias {alias_name} conflicts with existing channel {conflicting_meta.name}"
                    )
            alias = existing_aliases_by_name.get(alias_name)
            if alias is None:
                alias = TelemetryChannelAlias(
                    source_id=source_id,
                    alias_name=alias_name,
                    telemetry_id=meta.id,
                )
                db.add(alias)
                existing_aliases_by_name[alias_name] = alias
            else:
                alias.telemetry_id = meta.id

    mapping = vehicle_config.position_mapping
    existing_mapping = db.execute(
        select(PositionChannelMapping).where(
            PositionChannelMapping.source_id == source_id,
            PositionChannelMapping.active.is_(True),
        )
    ).scalars().first()
    if not overwrite_position_mapping and existing_mapping is not None:
        return needs_embedding_backfill
    if mapping is None:
        if existing_mapping is not None:
            db.delete(existing_mapping)
        return needs_embedding_backfill

    if existing_mapping is None:
        existing_mapping = PositionChannelMapping(source_id=source_id)
        db.add(existing_mapping)

    existing_mapping.frame_type = mapping.frame_type
    existing_mapping.lat_channel_name = mapping.lat_channel_name
    existing_mapping.lon_channel_name = mapping.lon_channel_name
    existing_mapping.alt_channel_name = mapping.alt_channel_name
    existing_mapping.x_channel_name = mapping.x_channel_name
    existing_mapping.y_channel_name = mapping.y_channel_name
    existing_mapping.z_channel_name = mapping.z_channel_name
    existing_mapping.active = True
    return needs_embedding_backfill


def source_has_telemetry_history(db: Session, source_id: str) -> bool:
    owned_stream_ids = select(TelemetryStream.id).where(
        TelemetryStream.source_id == source_id
    )
    history_count = db.execute(
        select(func.count())
        .select_from(TelemetryData)
        .where(
            or_(
                TelemetryData.stream_id == source_id,
                TelemetryData.stream_id.in_(owned_stream_ids),
            )
        )
    ).scalar_one()
    return history_count > 0


def get_realtime_snapshot_for_channels(
    db: Session,
    channel_names: list[str],
    *,
    source_id: str,
    stream_id: str | None = None,
) -> list[RealtimeChannelUpdate]:
    """Get current values from telemetry_current for given channels and source."""
    if not channel_names:
        return []
    data_source_id, logical_source_id = _resolve_realtime_stream_scope(
        db,
        source_id=source_id,
        stream_id=stream_id,
    )

    stmt = (
        select(TelemetryMetadata, TelemetryCurrent)
        .join(TelemetryCurrent, TelemetryMetadata.id == TelemetryCurrent.telemetry_id)
        .where(TelemetryCurrent.stream_id == data_source_id)
        .where(TelemetryMetadata.source_id == logical_source_id)
        .where(TelemetryMetadata.name.in_(channel_names))
    )
    rows = db.execute(stmt).fetchall()
    result = []

    for meta, curr in rows:
        # Sparkline from telemetry_data
        spark_stmt = (
            select(TelemetryData.timestamp, TelemetryData.value)
            .where(
                TelemetryData.telemetry_id == meta.id,
                TelemetryData.stream_id == data_source_id,
            )
            .order_by(desc(TelemetryData.timestamp), desc(TelemetryData.sequence))
            .limit(SPARKLINE_POINTS)
        )
        spark_rows = db.execute(spark_stmt).fetchall()
        sparkline_data = [
            RecentDataPoint(timestamp=r[0].isoformat(), value=float(r[1]))
            for r in reversed(spark_rows)
        ]

        result.append(
            RealtimeChannelUpdate(
                source_id=logical_source_id,
                stream_id=data_source_id,
                name=meta.name,
                units=meta.units,
                description=meta.description,
                subsystem_tag=infer_subsystem(meta.name, meta),
                channel_origin=meta.channel_origin or CHANNEL_ORIGIN_CATALOG,
                discovery_namespace=meta.discovery_namespace,
                current_value=float(curr.value),
                generation_time=curr.generation_time.isoformat(),
                reception_time=curr.reception_time.isoformat(),
                state=curr.state,
                state_reason=curr.state_reason,
                z_score=float(curr.z_score) if curr.z_score is not None else None,
                quality=curr.quality,
                sparkline_data=sparkline_data,
            )
        )
    return result


def get_watchlist_channel_names(db: Session, source_id: str) -> list[str]:
    """Get watchlist channel names in display order."""
    logical_source_id = _resolve_stream_source_id(db, source_id)
    stmt = (
        select(WatchlistEntry.telemetry_name)
        .where(WatchlistEntry.source_id == logical_source_id)
        .order_by(WatchlistEntry.display_order)
    )
    return [r[0] for r in db.execute(stmt).fetchall()]


def get_active_alerts(
    db: Session,
    *,
    source_id: str,
    stream_id: str | None = None,
    subsystems: list[str] | None = None,
    severities: list[str] | None = None,
) -> list[TelemetryAlertSchema]:
    """Get active (non-resolved, non-cleared) alerts for a source."""
    data_source_id, logical_source_id = _resolve_realtime_stream_scope(
        db,
        source_id=source_id,
        stream_id=stream_id,
    )
    stmt = (
        select(TelemetryAlert, TelemetryMetadata)
        .join(TelemetryMetadata, TelemetryAlert.telemetry_id == TelemetryMetadata.id)
        .where(TelemetryAlert.stream_id == data_source_id)
        .where(TelemetryMetadata.source_id == logical_source_id)
        .where(TelemetryAlert.cleared_at.is_(None))
        .where(TelemetryAlert.resolved_at.is_(None))
        .order_by(desc(TelemetryAlert.opened_at))
    )
    rows = db.execute(stmt).fetchall()
    result = []

    for alert, meta in rows:
        subsys = infer_subsystem(meta.name, meta)
        if subsystems and subsys not in subsystems:
            continue
        if severities and alert.severity not in severities:
            continue

        result.append(
            TelemetryAlertSchema(
                id=str(alert.id),
                source_id=logical_source_id,
                stream_id=alert.stream_id,
                channel_name=meta.name,
                telemetry_id=str(meta.id),
                subsystem=subsys,
                units=meta.units,
                severity=alert.severity,
                reason=alert.reason,
                status=alert.status,
                opened_at=alert.opened_at.isoformat(),
                opened_reception_at=alert.opened_reception_at.isoformat(),
                last_update_at=alert.last_update_at.isoformat(),
                current_value=float(alert.current_value_at_open),
                red_low=float(meta.red_low) if meta.red_low else None,
                red_high=float(meta.red_high) if meta.red_high else None,
                z_score=None,
                acked_at=alert.acked_at.isoformat() if alert.acked_at else None,
                acked_by=alert.acked_by,
                cleared_at=None,
                resolved_at=None,
                resolved_by=None,
                resolution_text=None,
                resolution_code=None,
            )
        )
    return result


def get_telemetry_sources(db: Session) -> list[dict]:
    """Get list of registered telemetry sources."""
    stmt = select(TelemetrySource).order_by(TelemetrySource.id)
    rows = db.execute(stmt).scalars().all()
    return [_source_to_dict(r) for r in rows]


def _create_source_row(
    db: Session,
    *,
    source_type: str,
    name: str,
    description: str | None,
    base_url: str | None,
    vehicle_config_path: str,
    monitoring_start_time: datetime | None = None,
    history_mode: str | None = None,
    source_id: str | None = None,
) -> TelemetrySource:
    if source_type not in ("vehicle", "simulator"):
        raise ValueError("source_type must be 'vehicle' or 'simulator'")
    if source_type == "simulator" and not base_url:
        raise ValueError("base_url is required for simulator sources")
    resolved_history_mode = _default_history_mode(source_type, history_mode)
    resolved_vehicle_config_path = canonical_vehicle_config_path(vehicle_config_path)
    src = TelemetrySource(
        id=source_id or str(uuid.uuid4()),
        name=name,
        description=description,
        source_type=source_type,
        base_url=base_url if source_type == "simulator" else None,
        vehicle_config_path=resolved_vehicle_config_path,
        monitoring_start_time=_coerce_aware_utc(monitoring_start_time) or _now_utc(),
        history_mode=resolved_history_mode,
        live_state="idle",
        backfill_state=_default_backfill_state(resolved_history_mode),
    )
    db.add(src)
    db.flush()
    return src


def get_source_by_vehicle_config_path(
    db: Session,
    vehicle_config_path: str,
) -> TelemetrySource | None:
    resolved_vehicle_config_path = canonical_vehicle_config_path(vehicle_config_path)
    return db.execute(
        select(TelemetrySource).where(
            TelemetrySource.vehicle_config_path == resolved_vehicle_config_path,
        )
    ).scalars().first()


def create_source(
    db: Session,
    embedding_provider: EmbeddingProvider,
    source_type: str,
    name: str,
    *,
    description: str | None = None,
    base_url: str | None = None,
    vehicle_config_path: str,
    monitoring_start_time: datetime | None = None,
    history_mode: str | None = None,
) -> dict:
    """Create a new telemetry source. Returns the created source dict."""
    src = _create_source_row(
        db,
        source_type=source_type,
        name=name,
        description=description,
        base_url=base_url,
        vehicle_config_path=vehicle_config_path,
        monitoring_start_time=monitoring_start_time,
        history_mode=history_mode,
    )
    _seed_metadata_for_source(
        db,
        source_id=src.id,
        vehicle_config_path=src.vehicle_config_path,
        embedding_provider=embedding_provider,
    )
    db.commit()
    db.refresh(src)
    return _source_to_dict(src)


def update_source(
    db: Session,
    embedding_provider: EmbeddingProvider,
    source_id: str,
    *,
    name: str | None = None,
    description: str | None = None,
    base_url: str | None = None,
    vehicle_config_path: str | None = None,
    monitoring_start_time: datetime | None = None,
    history_mode: str | None = None,
) -> dict | None:
    """Update a telemetry source. Returns updated source dict or None if not found."""
    src = db.get(TelemetrySource, source_id)
    if not src:
        return None
    if name is not None:
        src.name = name
    if description is not None:
        src.description = description
    if base_url is not None and src.source_type == "simulator":
        src.base_url = base_url
    if monitoring_start_time is not None:
        src.monitoring_start_time = _coerce_aware_utc(monitoring_start_time) or monitoring_start_time
    if history_mode is not None and history_mode != src.history_mode:
        if history_mode not in HISTORY_MODES:
            raise ValueError("history_mode must be live_only, time_window_replay, or cursor_replay")
        old_history_mode = src.history_mode
        src.history_mode = history_mode
        src.last_backfill_error = None
        if history_mode == "live_only":
            src.backfill_state = "complete"
            src.active_backfill_target_time = None
        elif old_history_mode == "live_only":
            src.backfill_state = "idle"
        elif src.backfill_state != "running":
            src.backfill_state = "idle"
    if vehicle_config_path is not None:
        next_path = canonical_vehicle_config_path(vehicle_config_path)
        if src.source_type == "simulator" and next_path != src.vehicle_config_path:
            raise ValueError("Cannot change vehicle_config_path for simulator sources")
        if next_path != src.vehicle_config_path and source_has_telemetry_history(db, src.id):
            raise ValueError("Cannot change vehicle_config_path after telemetry has been ingested")
        src.vehicle_config_path = next_path
        _seed_metadata_for_source(
            db,
            source_id=src.id,
            vehicle_config_path=src.vehicle_config_path,
            embedding_provider=embedding_provider,
            prune_missing=True,
        )
    db.commit()
    db.refresh(src)
    return _source_to_dict(src)


def get_source_by_id(db: Session, source_id: str) -> dict | None:
    """Get a single source by id."""
    src = db.get(TelemetrySource, source_id)
    if not src:
        return None
    return _source_to_dict(src)


def refresh_source_embeddings(
    db: Session,
    *,
    source_ids: list[str],
    embedding_provider: EmbeddingProvider,
) -> None:
    """Backfill real embeddings for the given sources without touching mappings."""
    for source_id in source_ids:
        src = db.get(TelemetrySource, source_id)
        if src is None:
            continue
        _seed_metadata_for_source(
            db,
            source_id=src.id,
            vehicle_config_path=src.vehicle_config_path,
            embedding_provider=embedding_provider,
            refresh_embeddings=True,
            preserve_existing_embeddings=True,
            overwrite_position_mapping=False,
        )
    db.commit()


def register_source_if_missing(
    db: Session,
    embedding_provider: EmbeddingProvider,
    source_type: str,
    name: str,
    *,
    description: str | None = None,
    base_url: str | None = None,
    vehicle_config_path: str,
    monitoring_start_time: datetime | None = None,
    history_mode: str | None = None,
) -> tuple[dict, bool]:
    resolved_vehicle_config_path = canonical_vehicle_config_path(vehicle_config_path)
    existing = get_source_by_vehicle_config_path(db, resolved_vehicle_config_path)
    if existing is not None:
        if monitoring_start_time is not None and existing.last_reconciled_at is None:
            existing.monitoring_start_time = _coerce_aware_utc(monitoring_start_time) or monitoring_start_time
            if existing.backfill_state != "running":
                existing.backfill_state = _default_backfill_state(existing.history_mode)
            db.commit()
            db.refresh(existing)
        return _source_to_dict(existing), False
    try:
        create_kwargs = {
            "embedding_provider": embedding_provider,
            "source_type": source_type,
            "name": name,
            "description": description,
            "base_url": base_url,
            "vehicle_config_path": resolved_vehicle_config_path,
        }
        if monitoring_start_time is not None:
            create_kwargs["monitoring_start_time"] = monitoring_start_time
        if history_mode is not None:
            create_kwargs["history_mode"] = history_mode
        created = create_source(
            db,
            **create_kwargs,
        )
    except IntegrityError:
        db.rollback()
        existing = get_source_by_vehicle_config_path(db, resolved_vehicle_config_path)
        if existing is None:
            raise
        return _source_to_dict(existing), False
    return created, True


def resolve_source(
    db: Session,
    embedding_provider: EmbeddingProvider,
    source_type: str,
    name: str,
    *,
    description: str | None = None,
    vehicle_config_path: str,
    monitoring_start_time: datetime | None = None,
    history_mode: str | None = None,
) -> tuple[dict, bool]:
    """Resolve or create a canonical vehicle source for adapter startup."""
    if source_type != "vehicle":
        raise ValueError("source_type must be 'vehicle'")
    return register_source_if_missing(
        db,
        embedding_provider=embedding_provider,
        source_type="vehicle",
        name=name,
        description=description,
        base_url=None,
        vehicle_config_path=vehicle_config_path,
        monitoring_start_time=monitoring_start_time,
        history_mode=history_mode,
    )


def update_backfill_progress(
    db: Session,
    *,
    source_id: str,
    status: str,
    target_time: datetime,
    chunk_end: datetime | None = None,
    backlog_drained: bool | None = None,
    error: str | None = None,
) -> dict | None:
    src = db.get(TelemetrySource, source_id)
    if src is None:
        return None
    target = _coerce_aware_utc(target_time) or target_time
    now = _now_utc()
    if status == "started":
        if src.backfill_state == "running":
            audit_log(
                "sources.backfill_superseded",
                level="warning",
                source_id=source_id,
                old_target_time=src.active_backfill_target_time,
                new_target_time=target,
            )
        src.active_backfill_target_time = target
        src.backfill_state = "running"
        src.last_backfill_started_at = now
        src.last_backfill_error = None
    elif status == "completed":
        if chunk_end is None:
            raise ValueError("chunk_end is required for completed backfill progress")
        if src.active_backfill_target_time != target:
            raise ValueError("target_time does not match active backfill target")
        src.last_reconciled_at = _coerce_aware_utc(chunk_end) or chunk_end
        src.last_backfill_completed_at = now
        src.last_backfill_error = None
        if backlog_drained:
            src.backfill_state = "complete"
            src.active_backfill_target_time = None
        else:
            src.backfill_state = "running"
    elif status == "failed":
        if src.active_backfill_target_time is not None and src.active_backfill_target_time != target:
            raise ValueError("target_time does not match active backfill target")
        src.backfill_state = "error"
        src.last_backfill_error = error or "Backfill failed"
        src.active_backfill_target_time = None
    else:
        raise ValueError("status must be started, completed, or failed")
    db.commit()
    db.refresh(src)
    return _source_to_dict(src)


def update_live_state(
    db: Session,
    *,
    source_id: str,
    state: str,
    error: str | None = None,
) -> dict | None:
    src = db.get(TelemetrySource, source_id)
    if src is None:
        return None
    if state not in LIVE_STATES:
        raise ValueError("live_state must be idle, active, or error")
    src.live_state = state
    if state == "error":
        src.last_backfill_error = error or src.last_backfill_error
    db.commit()
    db.refresh(src)
    return _source_to_dict(src)


def _is_simulator_category(category: str | None) -> bool:
    normalized = (category or "").strip().lower().rstrip("/\\")
    return normalized in {"simulator", "simulators"}


def infer_auto_registration_fields(
    config_path: str,
    config_item,
    loaded_config,
) -> dict:
    source_type = "simulator" if _is_simulator_category(getattr(config_item, "category", None)) else "vehicle"
    display_name = loaded_config.parsed.name or Path(config_path).stem
    base_url = None
    if source_type == "simulator":
        try:
            base_url = load_vehicle_config_file(loaded_config.path).base_url
        except Exception:
            base_url = None
    return {
        "source_type": source_type,
        "name": display_name,
        "description": f"Auto-registered from vehicle configuration: {loaded_config.path}",
        "base_url": base_url,
        "vehicle_config_path": loaded_config.path,
    }


def auto_register_sources_from_configs(
    db: Session,
    embedding_provider: EmbeddingProvider,
) -> dict:
    items = list_vehicle_configs()
    summary = {
        "examined": len(items),
        "created": [],
        "existing": [],
        "invalid": [],
        "skipped": [],
    }

    for item in items:
        try:
            loaded = load_vehicle_config(item.path)
            if loaded.validation_errors:
                summary["invalid"].append(
                    {
                        "path": item.path,
                        "errors": [error.model_dump() for error in loaded.validation_errors],
                    }
                )
                logger.warning(
                    "Skipping auto-registration for invalid vehicle configuration %s",
                    item.path,
                    extra={"vehicle_config_path": item.path, "reason": "validation_errors"},
                )
                continue

            fields = infer_auto_registration_fields(item.path, item, loaded)
            if fields["source_type"] == "simulator" and not fields["base_url"]:
                summary["skipped"].append(
                    {
                        "path": loaded.path,
                        "reason": "missing_base_url",
                        "source_type": fields["source_type"],
                    }
                )
                logger.info(
                    "Skipping auto-registration for simulator configuration without base_url: %s",
                    loaded.path,
                    extra={"vehicle_config_path": loaded.path, "reason": "missing_base_url"},
                )
                continue

            result, created = register_source_if_missing(
                db,
                embedding_provider=embedding_provider,
                source_type=fields["source_type"],
                name=fields["name"],
                description=fields["description"],
                base_url=fields["base_url"],
                vehicle_config_path=fields["vehicle_config_path"],
            )
            bucket = "created" if created else "existing"
            summary[bucket].append(result)
            if created:
                audit_log(
                    "sources.auto_register",
                    source_id=result["id"],
                    vehicle_config_path=result["vehicle_config_path"],
                    source_type=result["source_type"],
                    name=result["name"],
                )
        except Exception:
            logger.exception(
                "Failed auto-registration reconciliation for vehicle configuration %s",
                item.path,
            )
            summary["invalid"].append(
                {
                    "path": item.path,
                    "errors": [{"message": "Unexpected reconciliation failure", "type": "startup_error"}],
                }
            )

    logger.info(
        "Auto-registration reconciliation examined=%s created=%s existing=%s invalid=%s skipped=%s",
        summary["examined"],
        len(summary["created"]),
        len(summary["existing"]),
        len(summary["invalid"]),
        len(summary["skipped"]),
    )
    return summary


def repair_registered_sources_on_startup(
    db: Session,
) -> list[str]:
    """Repair metadata for already-registered sources from their own config paths."""
    repaired_source_ids: set[str] = set()
    sources_needing_embedding_backfill: set[str] = set()
    all_sources = db.execute(select(TelemetrySource).order_by(TelemetrySource.id)).scalars().all()

    for src in all_sources:
        try:
            needs_embedding_backfill = _seed_metadata_for_source(
                db,
                source_id=src.id,
                vehicle_config_path=src.vehicle_config_path,
                refresh_embeddings=False,
                overwrite_position_mapping=False,
            )
            repaired_source_ids.add(src.id)
            if needs_embedding_backfill:
                sources_needing_embedding_backfill.add(src.id)
        except Exception:
            logger.exception(
                "Skipping startup metadata repair for source %s due to invalid vehicle configuration path %s",
                src.id,
                src.vehicle_config_path,
            )

    if sources_needing_embedding_backfill:
        try:
            from app.services.embedding_service import SentenceTransformerEmbeddingProvider

            provider = SentenceTransformerEmbeddingProvider()
        except Exception:
            logger.exception(
                "Skipping startup embedding backfill for promoted channels due to provider initialization failure"
            )
        else:
            for source_id in sorted(sources_needing_embedding_backfill):
                src = db.get(TelemetrySource, source_id)
                if src is None:
                    continue
                try:
                    _seed_metadata_for_source(
                        db,
                        source_id=src.id,
                        vehicle_config_path=src.vehicle_config_path,
                        embedding_provider=provider,
                        refresh_embeddings=True,
                        preserve_existing_embeddings=True,
                        overwrite_position_mapping=False,
                    )
                except Exception:
                    logger.exception(
                        "Skipping startup embedding backfill for source %s",
                        src.id,
                    )
    db.commit()
    return sorted(repaired_source_ids)
