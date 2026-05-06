"""Configuration loading for the SatNOGS adapter."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.adapters.satnogs.decoders.models import DecoderConfig
from telemetry_catalog.definitions import VehicleConfigurationFile, load_vehicle_config_file

DEFAULT_PLATFORM_API_BASE_URL = "http://platform-api:8000"


def _platform_api_base_url() -> str:
    return os.environ.get("PLATFORM_API_BASE_URL", DEFAULT_PLATFORM_API_BASE_URL).rstrip("/")


class PlatformConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ingest_url: str
    observations_batch_upsert_url: str
    source_resolve_url: str
    backfill_progress_url: str = "http://platform-api:8000/telemetry/sources/{source_id}/backfill-progress"
    live_state_url: str = "http://platform-api:8000/telemetry/sources/{source_id}/live-state"

    @model_validator(mode="after")
    def validate_source_identity(self) -> "PlatformConfig":
        if not self.source_resolve_url.strip():
            raise ValueError("platform.source_resolve_url is required")
        return self


class DownloadConfig(BaseModel):
    max_concurrent_observation_fetches: int = 2
    max_concurrent_artifact_downloads: int = 2


class SatnogsConfig(BaseModel):
    base_url: str = "https://network.satnogs.org"
    api_token: str = ""
    transmitter_uuid: str
    status: str
    upcoming_status: str = "future"
    upcoming_lookahead_hours: int = 24
    observation_sync_interval_seconds: int = 600
    poll_interval_seconds: int = 60
    download: DownloadConfig = Field(default_factory=DownloadConfig)

    @model_validator(mode="after")
    def validate_pair_fields(self) -> "SatnogsConfig":
        if not self.transmitter_uuid.strip():
            raise ValueError("satnogs.transmitter_uuid is required")
        if not self.status.strip():
            raise ValueError("satnogs.status is required")
        return self


class RetryConfig(BaseModel):
    max_attempts: int = 3
    backoff_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    retryable_status_codes: list[int] = Field(default_factory=lambda: [408, 425, 429, 500, 502, 503, 504])


class PublisherConfig(BaseModel):
    batch_size_events: int = 50
    timeout_seconds: float = 10.0
    retry: RetryConfig = Field(default_factory=RetryConfig)


class DlqConfig(BaseModel):
    root_dir: str = "/app/runtime/satnogs-adapter/dlq"
    write_observation_dlq: bool = True


class VehicleConfig(BaseModel):
    slug: str = "lasarsat"
    name: str = "LASARSAT"
    norad_id: int
    allowed_source_callsigns: list[str] = Field(default_factory=lambda: ["OK0LSR"])
    vehicle_config_path: str = "vehicles/lasarsat.yaml"
    monitoring_start_time: datetime | None = None
    stable_field_mappings: dict[str, str] = Field(default_factory=dict)
    decoder: DecoderConfig = Field(default_factory=DecoderConfig)


class AdapterConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    platform: PlatformConfig
    vehicle: VehicleConfig
    satnogs: SatnogsConfig
    publisher: PublisherConfig = Field(default_factory=PublisherConfig)
    dlq: DlqConfig = Field(default_factory=DlqConfig)

    def load_definition(self) -> VehicleConfigurationFile:
        return load_vehicle_config_file(self.vehicle.vehicle_config_path)

    def resolve_stable_field_mappings(self) -> dict[str, str]:
        definition = self.load_definition()
        if definition.ingestion and definition.ingestion.stable_field_mappings:
            return dict(definition.ingestion.stable_field_mappings)
        return dict(self.vehicle.stable_field_mappings)


def load_config(path: str) -> AdapterConfig:
    raw = Path(path).read_text(encoding="utf-8")
    payload = yaml.safe_load(raw) or {}
    if not isinstance(payload, dict):
        raise ValueError("adapter config must contain a top-level object")

    platform = payload.get("platform")
    if isinstance(platform, dict):
        platform_api_base_url = os.environ.get("PLATFORM_API_BASE_URL")
        if platform_api_base_url:
            base_url = _platform_api_base_url()
            platform["ingest_url"] = f"{base_url}/telemetry/realtime/ingest"
            platform["source_resolve_url"] = f"{base_url}/telemetry/sources/resolve"
            platform["observations_batch_upsert_url"] = f"{base_url}/telemetry/sources/{{source_id}}/observations:batch-upsert"
            platform["backfill_progress_url"] = f"{base_url}/telemetry/sources/{{source_id}}/backfill-progress"
            platform["live_state_url"] = f"{base_url}/telemetry/sources/{{source_id}}/live-state"
        for key, env_names in {
            "ingest_url": ("SATNOGS_INGEST_URL", "PLATFORM_INGEST_URL"),
            "source_resolve_url": ("SATNOGS_SOURCE_RESOLVE_URL", "PLATFORM_SOURCE_RESOLVE_URL"),
            "observations_batch_upsert_url": (
                "SATNOGS_OBSERVATIONS_BATCH_UPSERT_URL",
                "PLATFORM_OBSERVATIONS_BATCH_UPSERT_URL",
            ),
            "backfill_progress_url": ("SATNOGS_BACKFILL_PROGRESS_URL", "PLATFORM_BACKFILL_PROGRESS_URL"),
            "live_state_url": ("SATNOGS_LIVE_STATE_URL", "PLATFORM_LIVE_STATE_URL"),
        }.items():
            for env_name in env_names:
                if os.environ.get(env_name):
                    platform[key] = os.environ[env_name]
                    break

    satnogs = payload.get("satnogs")
    if isinstance(satnogs, dict):
        env_token = os.environ.get("SATNOGS_API_TOKEN")
        if env_token:
            satnogs["api_token"] = env_token
        if os.environ.get("SATNOGS_POLL_INTERVAL_SECONDS"):
            satnogs["poll_interval_seconds"] = int(os.environ["SATNOGS_POLL_INTERVAL_SECONDS"])
        if os.environ.get("SATNOGS_OBSERVATION_SYNC_INTERVAL_SECONDS"):
            satnogs["observation_sync_interval_seconds"] = int(os.environ["SATNOGS_OBSERVATION_SYNC_INTERVAL_SECONDS"])

    dlq = payload.get("dlq")
    if isinstance(dlq, dict) and os.environ.get("SATNOGS_DLQ_ROOT"):
        dlq["root_dir"] = os.environ["SATNOGS_DLQ_ROOT"]
    return AdapterConfig.model_validate(payload)
