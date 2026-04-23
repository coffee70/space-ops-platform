"""Telemetry database models."""

import uuid
from datetime import datetime, timezone
from typing import Optional

from pgvector.sqlalchemy import Vector
from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Numeric, Text, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class TelemetryMetadata(Base):
    """Telemetry schema and metadata with semantic embedding."""

    __tablename__ = "telemetry_metadata"
    __table_args__ = (
        Index(
            "ix_telemetry_metadata_source_name",
            "source_id",
            "name",
            unique=True,
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    source_id: Mapped[str] = mapped_column(
        "source_id",
        Text,
        ForeignKey("telemetry_sources.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    name: Mapped[str] = mapped_column(Text, index=True, nullable=False)
    units: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    subsystem_tag: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    channel_origin: Mapped[str] = mapped_column(Text, nullable=False, default="catalog")
    discovery_namespace: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    discovered_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    archived_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    red_low: Mapped[Optional[float]] = mapped_column(Numeric(20, 10), nullable=True)
    red_high: Mapped[Optional[float]] = mapped_column(Numeric(20, 10), nullable=True)
    embedding: Mapped[Optional[list]] = mapped_column(Vector(384), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
class TelemetryChannelAlias(Base):
    """Source-scoped external names for one canonical telemetry channel."""

    __tablename__ = "telemetry_channel_aliases"
    __table_args__ = (
        Index(
            "ix_telemetry_channel_aliases_source_alias",
            "source_id",
            "alias_name",
            unique=True,
        ),
        Index(
            "ix_telemetry_channel_aliases_source_telemetry",
            "source_id",
            "telemetry_id",
        ),
    )

    source_id: Mapped[str] = mapped_column(
        "source_id",
        Text,
        ForeignKey("telemetry_sources.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    alias_name: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False)
    telemetry_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("telemetry_metadata.id", ondelete="CASCADE"),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
class TelemetryData(Base):
    """Time-series telemetry data (TimescaleDB hypertable).
    stream_id scopes data per telemetry stream.
    """

    __tablename__ = "telemetry_data"

    stream_id: Mapped[str] = mapped_column("source_id", Text, primary_key=True)
    telemetry_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("telemetry_metadata.id", ondelete="CASCADE"),
        primary_key=True,
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
    )
    sequence: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    value: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    packet_source: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    receiver_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index(
            "ix_telemetry_data_source_telemetry_timestamp",
            "source_id",
            "telemetry_id",
            "timestamp",
            "sequence",
        ),
        Index(
            "ix_telemetry_data_telemetry_timestamp_source",
            "telemetry_id",
            "timestamp",
            "sequence",
            "source_id",
        ),
    )
class TelemetryStatistics(Base):
    """Precomputed statistics for each telemetry point per source."""

    __tablename__ = "telemetry_statistics"

    stream_id: Mapped[str] = mapped_column("source_id", Text, primary_key=True)
    telemetry_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("telemetry_metadata.id", ondelete="CASCADE"),
        primary_key=True,
    )
    mean: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    std_dev: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    min_value: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    max_value: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    p5: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    p50: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    p95: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    n_samples: Mapped[int] = mapped_column(nullable=False, default=0)
    last_computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
class WatchlistEntry(Base):
    """Operator watchlist configuration."""

    __tablename__ = "watchlist"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    source_id: Mapped[str] = mapped_column(
        "source_id",
        Text,
        ForeignKey("telemetry_sources.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    telemetry_name: Mapped[str] = mapped_column(Text, index=True, nullable=False)
    display_order: Mapped[int] = mapped_column(nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
class TelemetrySource(Base):
    """Registry of telemetry stream sources (vehicles, simulators)."""

    __tablename__ = "telemetry_sources"
    __table_args__ = (
        Index(
            "ix_telemetry_sources_vehicle_config_path",
            "vehicle_config_path",
            unique=True,
        ),
    )

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_type: Mapped[str] = mapped_column(Text, nullable=False)  # vehicle | simulator
    base_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # for simulators
    vehicle_config_path: Mapped[str] = mapped_column(Text, nullable=False)
    monitoring_start_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    last_reconciled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    history_mode: Mapped[str] = mapped_column(Text, nullable=False, default="time_window_replay")
    live_state: Mapped[str] = mapped_column(Text, nullable=False, default="idle")
    backfill_state: Mapped[str] = mapped_column(Text, nullable=False, default="idle")
    active_backfill_target_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_backfill_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_backfill_completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_backfill_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )


class TelemetryCurrent(Base):
    """Latest value per channel per source for fast realtime reads."""

    __tablename__ = "telemetry_current"

    stream_id: Mapped[str] = mapped_column("source_id", Text, primary_key=True)
    telemetry_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("telemetry_metadata.id", ondelete="CASCADE"),
        primary_key=True,
    )
    generation_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    reception_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    value: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)  # normal, caution, warning
    state_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    z_score: Mapped[Optional[float]] = mapped_column(Numeric(20, 10), nullable=True)
    quality: Mapped[str] = mapped_column(Text, nullable=False, default="valid")
    sequence: Mapped[Optional[int]] = mapped_column(nullable=True)
    packet_source: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    receiver_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
class TelemetryAlert(Base):
    """Alert lifecycle: opened, acked, cleared, resolved."""

    __tablename__ = "telemetry_alerts"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    stream_id: Mapped[str] = mapped_column("source_id", Text, nullable=False, index=True)
    telemetry_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("telemetry_metadata.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    opened_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    opened_reception_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    last_update_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    severity: Mapped[str] = mapped_column(Text, nullable=False)  # caution, warning
    reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)  # new, acked, resolved
    current_value_at_open: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    acked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    acked_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    cleared_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    resolution_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    resolution_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class TelemetryFeedHealth(Base):
    """Durable feed-health state owned by telemetry ingest."""

    __tablename__ = "telemetry_feed_health"

    source_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("telemetry_sources.id", ondelete="CASCADE"),
        primary_key=True,
    )
    connected: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, default="disconnected")
    last_reception_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    approx_rate_hz: Mapped[Optional[float]] = mapped_column(Numeric(20, 10), nullable=True)
    drop_count: Mapped[int] = mapped_column(nullable=False, default=0)
    last_transition_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )


class OpsEvent(Base):
    """Operational event for unified timeline (alerts, operator actions, data-path)."""

    __tablename__ = "ops_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    source_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    stream_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True, index=True)
    event_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    entity_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    payload: Mapped[Optional[dict]] = mapped_column(
        JSONB(),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        nullable=False,
    )

    __table_args__ = (
        Index("ix_ops_events_source_time", "source_id", "event_time"),
        Index("ix_ops_events_type_time", "event_type", "event_time"),
    )
class TelemetryAlertNote(Base):
    """Notes attached to alerts (resolutions, operator comments)."""

    __tablename__ = "telemetry_alert_notes"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    alert_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("telemetry_alerts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        nullable=False,
    )
    author: Mapped[str] = mapped_column(Text, nullable=False)
    note_text: Mapped[str] = mapped_column(Text, nullable=False)
    note_type: Mapped[str] = mapped_column(Text, nullable=False)  # resolution, comment


class PositionChannelMapping(Base):
    """Per-source mapping from telemetry channels to position vectors for Earth view."""

    __tablename__ = "position_channel_mappings"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    source_id: Mapped[str] = mapped_column(
        "source_id",
        Text,
        ForeignKey("telemetry_sources.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    # gps_lla | ecef | eci
    frame_type: Mapped[str] = mapped_column(Text, nullable=False)

    # GPS LLA channels
    lat_channel_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    lon_channel_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    alt_channel_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Cartesian channels (ECEF/ECI)
    x_channel_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    y_channel_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    z_channel_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    __table_args__ = (
        Index(
            "ix_position_channel_mappings_source_active",
            "source_id",
            "active",
        ),
    )
class TelemetryStream(Base):
    """Runtime telemetry stream identity for one source/session."""

    __tablename__ = "telemetry_streams"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    source_id: Mapped[str] = mapped_column(
        "source_id",
        Text,
        ForeignKey("telemetry_sources.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    packet_source: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    receiver_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="active")
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    metadata_json: Mapped[Optional[dict]] = mapped_column("metadata", JSONB(), nullable=True)


class SourceObservation(Base):
    """Expected observation/contact window for a telemetry source."""

    __tablename__ = "source_observations"
    __table_args__ = (
        Index("ix_source_observations_source_start", "source_id", "start_time"),
        Index("ix_source_observations_source_status_start", "source_id", "status", "start_time"),
        Index(
            "uq_source_observations_source_external",
            "source_id",
            "external_id",
            unique=True,
            postgresql_where=text("external_id IS NOT NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    source_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("telemetry_sources.id", ondelete="CASCADE"),
        nullable=False,
    )
    external_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    provider: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="scheduled")
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    station_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    station_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    receiver_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    max_elevation_deg: Mapped[Optional[float]] = mapped_column(Numeric(20, 10), nullable=True)
    details_json: Mapped[Optional[dict]] = mapped_column(JSONB(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
