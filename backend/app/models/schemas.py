"""Pydantic request/response schemas."""

from datetime import datetime
from typing import Any, Literal, Optional
from uuid import UUID

from pydantic import AliasChoices, BaseModel, Field, field_validator, model_validator


class ChannelListItem(BaseModel):
    """Single telemetry channel entry for source-scoped pickers."""

    name: str
    aliases: list[str] = []
    channel_origin: str = "catalog"
    discovery_namespace: Optional[str] = None


# --- Schema ingestion ---
class TelemetrySchemaCreate(BaseModel):
    """Request body for POST /telemetry/schema."""

    source_id: str
    name: str
    units: str
    description: Optional[str] = None
    subsystem_tag: Optional[str] = None
    red_low: Optional[float] = None
    red_high: Optional[float] = None


class TelemetrySchemaResponse(BaseModel):
    """Response for POST /telemetry/schema."""

    status: str = "created"
    telemetry_id: UUID


# --- Data ingestion ---
class DataPoint(BaseModel):
    """Single telemetry data point."""

    timestamp: str
    value: float


class TelemetryDataIngest(BaseModel):
    """Request body for POST /telemetry/data."""

    telemetry_name: str
    data: list[DataPoint]
    source_id: str
    stream_id: str
    packet_source: Optional[str] = None
    receiver_id: Optional[str] = None


class TelemetryDataResponse(BaseModel):
    """Response for POST /telemetry/data."""

    rows_inserted: int


# --- Search ---
class SearchResult(BaseModel):
    """Single search result."""

    name: str
    aliases: list[str] = []
    match_confidence: float
    description: Optional[str] = None
    subsystem_tag: Optional[str] = None
    units: str = ""
    channel_origin: str = "catalog"
    discovery_namespace: Optional[str] = None
    current_value: Optional[float] = None
    current_status: Optional[str] = None  # normal, caution, warning
    last_timestamp: Optional[str] = None


class SearchResponse(BaseModel):
    """Response for GET /telemetry/search."""

    results: list[SearchResult]


# --- Explain ---
class StatisticsResponse(BaseModel):
    """Statistics for explain response."""

    mean: Optional[float] = None
    std_dev: Optional[float] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    p5: Optional[float] = None
    p50: Optional[float] = None
    p95: Optional[float] = None
    n_samples: int


class RelatedChannel(BaseModel):
    """Channel linked by subsystem/physics for 'What to check next'."""

    name: str
    subsystem_tag: str
    link_reason: str  # e.g. "same subsystem", "same units"
    current_value: Optional[float] = None
    current_status: Optional[str] = None  # normal, caution, warning
    last_timestamp: Optional[str] = None
    units: Optional[str] = None


class TelemetryDetailScopeWindow(BaseModel):
    """UTC window bounds for channel detail scope metadata."""

    since: Optional[str] = None
    until: Optional[str] = None


class TelemetryDetailPageScope(BaseModel):
    """Structured scope for telemetry detail UI (strip, modals); not preformatted copy."""

    mode: Literal["latest", "streams", "date_range"]
    stream_count: Optional[int] = None
    stream_ids: list[str] = []
    resolved_stream_id: Optional[str] = None
    window: Optional[TelemetryDetailScopeWindow] = None
    preset: Optional[str] = None


class ExplainResponse(BaseModel):
    """Response for GET /telemetry/{name}/explain."""

    name: str
    aliases: list[str] = []
    description: Optional[str] = None
    units: Optional[str] = None
    channel_origin: str = "catalog"
    discovery_namespace: Optional[str] = None
    statistics: StatisticsResponse
    recent_value: Optional[float] = None
    z_score: Optional[float] = None
    is_anomalous: bool
    state: str  # normal, caution, warning
    state_reason: Optional[str] = None  # out_of_limits, out_of_family
    last_timestamp: Optional[str] = None
    red_low: Optional[float] = None
    red_high: Optional[float] = None
    what_this_means: str
    what_to_check_next: list[RelatedChannel] = []
    confidence_indicator: Optional[str] = None
    llm_explanation: str
    scope: Optional[TelemetryDetailPageScope] = Field(
        default=None,
        description="Applied data scope metadata for the detail page (modes, windows, stream ids).",
    )


# --- Recent data ---
class RecentDataPoint(BaseModel):
    """Single point for recent data endpoint."""

    timestamp: str
    value: float
    stream_id: Optional[str] = None


class RecentDataResponse(BaseModel):
    """Response for GET /telemetry/{name}/recent."""

    data: list[RecentDataPoint]
    requested_since: Optional[str] = None
    requested_until: Optional[str] = None
    effective_since: Optional[str] = None
    effective_until: Optional[str] = None
    applied_time_filter: bool = False
    fallback_to_recent: bool = False


class ChannelSourceItem(BaseModel):
    """Stream that has data for a channel; display metadata is backend-owned."""

    stream_id: str
    label: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    sample_count: Optional[int] = None
    last_timestamp: Optional[str] = None
    provider: Optional[str] = None
    summary: Optional[str] = None


class ChannelSourcesResponse(BaseModel):
    """Response for channel run/source listing endpoints."""

    sources: list[ChannelSourceItem]


# --- Recompute stats ---
class RecomputeStatsResponse(BaseModel):
    """Response for POST /telemetry/recompute-stats."""

    telemetry_processed: int


# --- Overview ---
class OverviewChannel(BaseModel):
    """Single channel in overview response."""

    name: str
    aliases: list[str] = []
    units: Optional[str] = None
    description: Optional[str] = None
    subsystem_tag: str
    channel_origin: str = "catalog"
    current_value: float
    last_timestamp: str
    state: str
    state_reason: Optional[str] = None
    z_score: Optional[float] = None
    sparkline_data: list[RecentDataPoint]


class OverviewResponse(BaseModel):
    """Response for GET /telemetry/overview."""

    channels: list[OverviewChannel]


# --- Anomalies ---
class AnomalyEntry(BaseModel):
    """Single anomaly entry."""

    name: str
    units: Optional[str] = None
    current_value: float
    last_timestamp: str
    z_score: Optional[float] = None
    state_reason: Optional[str] = None


class AnomaliesResponse(BaseModel):
    """Response for GET /telemetry/anomalies. Grouped by subsystem."""

    model_config = {"extra": "allow"}

    power: list[AnomalyEntry] = []
    thermal: list[AnomalyEntry] = []
    adcs: list[AnomalyEntry] = []
    comms: list[AnomalyEntry] = []
    other: list[AnomalyEntry] = []


# --- Watchlist ---
class WatchlistEntrySchema(BaseModel):
    """Single watchlist entry."""

    source_id: str
    name: str
    aliases: list[str] = []
    display_order: int
    channel_origin: str = "catalog"
    discovery_namespace: Optional[str] = None


class WatchlistResponse(BaseModel):
    """Response for GET /telemetry/watchlist."""

    entries: list[WatchlistEntrySchema]


class WatchlistAddRequest(BaseModel):
    """Request body for POST /telemetry/watchlist."""

    source_id: str
    telemetry_name: str


class TelemetryListResponse(BaseModel):
    """Response for GET /telemetry/list."""

    names: list[str]
    channels: list[ChannelListItem] = []


class TelemetryInventoryItem(BaseModel):
    """Single telemetry channel row for the inventory page."""

    name: str
    aliases: list[str] = []
    description: Optional[str] = None
    units: Optional[str] = None
    subsystem_tag: str
    channel_origin: str = "catalog"
    discovery_namespace: Optional[str] = None
    current_value: Optional[float] = None
    last_timestamp: Optional[str] = None
    state: str
    state_reason: Optional[str] = None
    z_score: Optional[float] = None
    is_anomalous: bool
    has_data: bool
    red_low: Optional[float] = None
    red_high: Optional[float] = None
    n_samples: Optional[int] = None


class TelemetryInventoryResponse(BaseModel):
    """Response for GET /telemetry/inventory."""

    channels: list[TelemetryInventoryItem]


# --- Realtime: canonical measurement event (ingest) ---
class MeasurementEvent(BaseModel):
    """Canonical internal measurement event from realtime ingest."""

    source_id: str
    stream_id: str
    channel_name: Optional[str] = None
    generation_time: Optional[str] = None  # RFC3339; may be synthesized from reception_time
    reception_time: Optional[str] = None  # RFC3339; server assigns if omitted
    value: float
    quality: str = "valid"  # valid | suspect | invalid
    sequence: Optional[int] = None  # Required by realtime ingest; persisted as historical sample identity.
    packet_source: Optional[str] = None
    receiver_id: Optional[str] = None
    tags: Optional[dict[str, Any]] = None

    @model_validator(mode="after")
    def validate_channel_identifier(self) -> "MeasurementEvent":
        channel_name = (self.channel_name or "").strip()
        tags = self.tags or {}

        dynamic_channel_name = tags.get("dynamic_channel_name")
        field_name = tags.get("field_name") or tags.get("field") or tags.get("key")
        decoder = tags.get("decoder") or tags.get("decoder_name") or tags.get("parser")
        namespace = tags.get("namespace")

        has_dynamic_name = isinstance(dynamic_channel_name, str) and dynamic_channel_name.strip() != ""
        has_field_name = isinstance(field_name, str) and field_name.strip() != ""
        has_decoder = isinstance(decoder, str) and decoder.strip() != ""
        has_namespace = isinstance(namespace, str) and namespace.strip() != ""

        if not self.generation_time and not self.reception_time:
            raise ValueError("measurement event requires generation_time or reception_time")

        if channel_name or has_dynamic_name or (has_field_name and (has_decoder or has_namespace)):
            return self

        raise ValueError("measurement event requires channel_name or dynamic channel tags")


class MeasurementEventBatch(BaseModel):
    """Batch of measurement events for POST /telemetry/realtime/ingest."""

    events: list[MeasurementEvent]


# --- Realtime: telemetry update (to UI) ---
class RealtimeChannelUpdate(BaseModel):
    """Single channel update pushed to WebSocket clients."""

    source_id: str
    stream_id: str
    packet_source: Optional[str] = None
    receiver_id: Optional[str] = None
    name: str
    units: Optional[str] = None
    description: Optional[str] = None
    subsystem_tag: str
    channel_origin: str = "catalog"
    discovery_namespace: Optional[str] = None
    current_value: float
    generation_time: str
    reception_time: str
    state: str  # normal, caution, warning
    state_reason: Optional[str] = None
    z_score: Optional[float] = None
    quality: str = "valid"
    sparkline_data: list[RecentDataPoint] = []


# --- Realtime: alert lifecycle ---
class TelemetryAlertSchema(BaseModel):
    """Alert as sent over WebSocket and stored."""

    id: str
    source_id: str
    stream_id: str
    channel_name: str
    telemetry_id: str
    subsystem: str
    units: Optional[str] = None
    severity: str  # caution, warning
    reason: Optional[str] = None  # out_of_limits, out_of_family
    status: str  # new, acked, resolved
    opened_at: str
    opened_reception_at: str
    last_update_at: str
    current_value: float
    red_low: Optional[float] = None
    red_high: Optional[float] = None
    z_score: Optional[float] = None
    acked_at: Optional[str] = None
    acked_by: Optional[str] = None
    cleared_at: Optional[str] = None
    resolved_at: Optional[str] = None
    resolved_by: Optional[str] = None
    resolution_text: Optional[str] = None
    resolution_code: Optional[str] = None


class AlertEventMessage(BaseModel):
    """Server -> client: alert lifecycle event."""

    type: str  # opened, updated, cleared, acked, resolved
    alert: TelemetryAlertSchema


# --- Realtime: WebSocket client -> server messages ---
class WsHello(BaseModel):
    """Client hello."""

    type: str = "hello"
    client_version: Optional[str] = None
    requested_features: Optional[list[str]] = None


class WsSubscribeWatchlist(BaseModel):
    """Subscribe to watchlist channels."""

    type: str = "subscribe_watchlist"
    channels: list[str]


class WsSubscribeChannel(BaseModel):
    """Subscribe to single channel for detail view."""

    type: str = "subscribe_channel"
    name: str
    window_points: Optional[int] = 100


class WsSubscribeAlerts(BaseModel):
    """Subscribe to alert stream."""

    type: str = "subscribe_alerts"
    subsystems: Optional[list[str]] = None
    severities: Optional[list[str]] = None


class WsAckAlert(BaseModel):
    """Ack an alert."""

    type: str = "ack_alert"
    alert_id: str


class WsResolveAlert(BaseModel):
    """Resolve an alert with optional resolution text."""

    type: str = "resolve_alert"
    alert_id: str
    resolution_text: str = ""
    resolution_code: Optional[str] = None


# --- Realtime: WebSocket server -> client messages ---
class WsSnapshotWatchlist(BaseModel):
    """Initial snapshot of watchlist channels."""

    type: str = "snapshot_watchlist"
    channels: list[RealtimeChannelUpdate]


class WsTelemetryUpdate(BaseModel):
    """Incremental telemetry update."""

    type: str = "telemetry_update"
    channel: RealtimeChannelUpdate


class WsSnapshotAlerts(BaseModel):
    """Initial snapshot of active alerts."""

    type: str = "snapshot_alerts"
    active: list[TelemetryAlertSchema]


class WsAlertEvent(BaseModel):
    """Alert lifecycle event."""

    type: str = "alert_event"
    event_type: str  # opened, updated, cleared, acked, resolved
    alert: TelemetryAlertSchema


# --- Ops events (timeline) ---
class OpsEventSchema(BaseModel):
    """Single ops event for timeline API."""

    id: str
    source_id: str
    stream_id: Optional[str] = None
    event_time: str
    event_type: str
    severity: str
    summary: str
    entity_type: str
    entity_id: Optional[str] = None
    payload: Optional[dict] = None
    created_at: str


class OpsEventsResponse(BaseModel):
    """Response for GET /ops/events."""

    events: list[OpsEventSchema]
    total: int


class WsFeedStatus(BaseModel):
    """Feed health status (best-effort in dev/demo)."""

    type: str = "feed_status"
    source_id: str
    stream_id: Optional[str] = None
    connected: bool
    state: str = "disconnected"  # connected | degraded | disconnected
    last_reception_time: Optional[str] = None
    approx_rate_hz: Optional[float] = None
    drop_count: Optional[int] = None


class WsOrbitStatus(BaseModel):
    """Orbit validation status update (real-time push)."""

    type: str = "orbit_status"
    vehicle_id: str
    status: str
    reason: str = ""
    orbit_type: Optional[str] = None
    perigee_km: Optional[float] = None
    apogee_km: Optional[float] = None
    eccentricity: Optional[float] = None
    velocity_kms: Optional[float] = None
    period_sec: Optional[float] = None


# --- Sources (constellation) ---
class SourceCreate(BaseModel):
    """Request body for POST /telemetry/sources."""

    source_type: str = Field(..., description="vehicle | simulator")
    name: str
    description: Optional[str] = None
    base_url: Optional[str] = None  # required for simulator
    vehicle_config_path: str
    monitoring_start_time: Optional[datetime] = None
    history_mode: Optional[Literal["live_only", "time_window_replay", "cursor_replay"]] = None


class SourceResolveRequest(BaseModel):
    """Request body for POST /telemetry/sources/resolve."""

    source_type: Literal["vehicle"]
    name: str
    description: Optional[str] = None
    vehicle_config_path: str
    monitoring_start_time: Optional[datetime] = None


class SourceResolveResponse(BaseModel):
    """Response body for vehicle source resolution."""

    id: str
    source_id: str
    name: str
    description: Optional[str] = None
    source_type: str
    base_url: Optional[str] = None
    vehicle_config_path: str
    created: bool
    monitoring_start_time: datetime
    last_reconciled_at: Optional[datetime] = None
    history_mode: Literal["live_only", "time_window_replay", "cursor_replay"]
    live_state: Literal["idle", "active", "error"]
    backfill_state: Literal["idle", "running", "complete", "error"]
    active_backfill_target_time: Optional[datetime] = None
    chunk_size_hours: int


class SourceUpdate(BaseModel):
    """Request body for PATCH /telemetry/sources/{id}."""

    name: Optional[str] = None
    description: Optional[str] = None
    base_url: Optional[str] = None  # for simulators
    vehicle_config_path: Optional[str] = None
    monitoring_start_time: Optional[datetime] = None
    history_mode: Optional[Literal["live_only", "time_window_replay", "cursor_replay"]] = None


class BackfillProgressUpdate(BaseModel):
    """Durable source backfill progress reported by an adapter."""

    status: Literal["started", "completed", "failed"]
    target_time: datetime
    chunk_start: Optional[datetime] = None
    chunk_end: Optional[datetime] = None
    backlog_drained: Optional[bool] = None
    error: Optional[str] = None


class LiveStateUpdate(BaseModel):
    """Durable source live worker state reported by an adapter."""

    state: Literal["idle", "active", "error"]
    error: Optional[str] = None


SourceObservationStatus = Literal["scheduled", "in_progress", "completed", "cancelled", "missed"]


class SourceObservationUpsert(BaseModel):
    """Observation/contact window published for a source."""

    external_id: str = Field(..., min_length=1)
    status: SourceObservationStatus = "scheduled"
    start_time: datetime
    end_time: datetime
    station_name: Optional[str] = None
    station_id: Optional[str] = None
    receiver_id: Optional[str] = None
    max_elevation_deg: Optional[float] = None
    details: Optional[dict[str, Any]] = None

    @model_validator(mode="after")
    def validate_time_range(self) -> "SourceObservationUpsert":
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")
        return self


class SourceObservationBatchUpsert(BaseModel):
    """Batch write request for provider observation snapshots."""

    provider: str = Field(..., min_length=1)
    replace_future_scheduled: bool = True
    observations: list[SourceObservationUpsert]


class SourceObservationBatchUpsertResponse(BaseModel):
    """Response for provider observation snapshot writes."""

    inserted: int
    deleted: int


class SourceObservationSchema(BaseModel):
    """Stored source observation/contact window."""

    model_config = {"from_attributes": True}

    id: UUID
    source_id: str
    external_id: Optional[str] = None
    provider: Optional[str] = None
    status: SourceObservationStatus
    start_time: datetime
    end_time: datetime
    station_name: Optional[str] = None
    station_id: Optional[str] = None
    receiver_id: Optional[str] = None
    max_elevation_deg: Optional[float] = None
    details: Optional[dict[str, Any]] = Field(default=None, validation_alias="details_json")
    created_at: datetime
    updated_at: datetime


class UpcomingObservationsResponse(BaseModel):
    """Upcoming observation windows for a source."""

    observations: list[SourceObservationSchema]


class VehicleConfigValidationError(BaseModel):
    """Structured validation error for vehicle configuration APIs."""

    loc: list[str]
    message: str
    type: str


class VehicleConfigListItem(BaseModel):
    """Single item for the vehicle configuration list endpoint."""

    path: str
    filename: str
    name: Optional[str] = None
    category: str
    format: str
    modified_at: Optional[str] = None


class VehicleConfigParsedSummary(BaseModel):
    """Lightweight parsed summary for editor and list UIs."""

    version: int = 1
    name: Optional[str] = None
    channel_count: int = 0
    scenario_names: list[str] = []
    has_position_mapping: bool = False
    has_ingestion: bool = False


class VehicleConfigFetchResponse(BaseModel):
    """Response for loading a single vehicle configuration file."""

    path: str
    content: str
    format: str
    parsed: Optional[VehicleConfigParsedSummary] = None
    validation_errors: list[VehicleConfigValidationError] = Field(default_factory=list)


class VehicleConfigValidationRequest(BaseModel):
    """Request body for POST /vehicle-configs/validate."""

    content: str
    path: Optional[str] = None
    filename: Optional[str] = None
    format: Optional[str] = None


class VehicleConfigValidationResponse(BaseModel):
    """Validation response for vehicle configuration content."""

    valid: bool
    parsed: Optional[VehicleConfigParsedSummary] = None
    errors: list[VehicleConfigValidationError] = Field(default_factory=list)


class VehicleConfigCreateRequest(BaseModel):
    """Create a new vehicle configuration file on disk."""

    path: str
    content: str


class VehicleConfigSaveResponse(BaseModel):
    """Response for create/update vehicle configuration writes."""

    path: str
    parsed: VehicleConfigParsedSummary
    saved: bool = True


# --- Position mapping and samples ---
class PositionChannelMappingSchema(BaseModel):
    """Per-source mapping from telemetry channels to position vectors."""

    model_config = {"from_attributes": True}

    id: str
    vehicle_id: str = Field(validation_alias=AliasChoices("vehicle_id", "source_id"))
    frame_type: str  # gps_lla | ecef | eci

    @field_validator("id", mode="before")
    @classmethod
    def coerce_id_to_str(cls, v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, UUID):
            return str(v)
        return v
    lat_channel_name: Optional[str] = None
    lon_channel_name: Optional[str] = None
    alt_channel_name: Optional[str] = None
    x_channel_name: Optional[str] = None
    y_channel_name: Optional[str] = None
    z_channel_name: Optional[str] = None
    active: bool = True


class PositionChannelMappingUpsert(BaseModel):
    """Create or update a position mapping for a source."""

    vehicle_id: str
    frame_type: str  # gps_lla | ecef | eci
    lat_channel_name: Optional[str] = None
    lon_channel_name: Optional[str] = None
    alt_channel_name: Optional[str] = None
    x_channel_name: Optional[str] = None
    y_channel_name: Optional[str] = None
    z_channel_name: Optional[str] = None
    active: bool = True


class PositionSample(BaseModel):
    """Canonical geodetic position sample for Earth view."""

    vehicle_id: str
    vehicle_name: str
    vehicle_type: str
    stream_id: Optional[str] = None
    lat_deg: Optional[float] = None
    lon_deg: Optional[float] = None
    alt_m: Optional[float] = None
    timestamp: Optional[str] = None
    valid: bool = False
    frame_type: str
    raw_channels: Optional[dict[str, Optional[float]]] = None

class ActiveStreamUpdate(BaseModel):
    source_id: str
    stream_id: Optional[str] = None
    state: str  # "active" | "idle"
