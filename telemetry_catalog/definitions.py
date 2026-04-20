"""Vehicle configuration models and file loading."""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator

FrameType = Literal["gps_lla", "ecef", "eci"]


class TelemetryEventDefinition(BaseModel):
    t0: float
    duration: float = 999.0
    type: Literal["offset", "ramp", "set"]
    channels: list[str]
    magnitude: float


class TelemetryDropoutDefinition(BaseModel):
    t0: float
    duration: float


class TelemetryScenarioDefinition(BaseModel):
    description: str
    anomaly_fraction: float = 0.0
    orbit_profile: str | None = None
    dropout: TelemetryDropoutDefinition | None = None
    events: list[TelemetryEventDefinition] = Field(default_factory=list)


class PositionMappingDefinition(BaseModel):
    frame_type: FrameType
    lat_channel_name: str | None = None
    lon_channel_name: str | None = None
    alt_channel_name: str | None = None
    x_channel_name: str | None = None
    y_channel_name: str | None = None
    z_channel_name: str | None = None

    @model_validator(mode="after")
    def validate_frame_fields(self) -> "PositionMappingDefinition":
        if self.frame_type == "gps_lla":
            if not self.lat_channel_name or not self.lon_channel_name:
                raise ValueError("gps_lla mappings require lat_channel_name and lon_channel_name")
        elif not self.x_channel_name or not self.y_channel_name or not self.z_channel_name:
            raise ValueError(f"{self.frame_type} mappings require x/y/z channel names")
        return self


class VehicleProfileDefinition(BaseModel):
    bus_class: str | None = None
    propulsion_layout: str | None = None
    tank_count: int | None = None
    computer_count: int | None = None
    payloads: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class TelemetryChannelDefinition(BaseModel):
    name: str
    aliases: list[str] = Field(default_factory=list)
    units: str
    description: str
    subsystem: str
    mean: float
    std_dev: float
    red_low: float | None = None
    red_high: float | None = None
    sample_rate_hz: float | None = None


class TelemetryIngestionDefinition(BaseModel):
    stable_field_mappings: dict[str, str] = Field(default_factory=dict)


class VehicleConfigurationFile(BaseModel):
    version: int = 1
    name: str | None = None
    base_url: str | None = None
    vehicle_profile: VehicleProfileDefinition | None = None
    channels: list[TelemetryChannelDefinition]
    position_mapping: PositionMappingDefinition | None = None
    ingestion: TelemetryIngestionDefinition | None = None
    scenarios: dict[str, TelemetryScenarioDefinition] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_channels(self) -> "VehicleConfigurationFile":
        seen: set[str] = set()
        aliases_seen: dict[str, str] = {}
        for channel in self.channels:
            if channel.name in seen:
                raise ValueError(f"duplicate channel name: {channel.name}")
            seen.add(channel.name)
            local_aliases: set[str] = set()
            for alias in channel.aliases:
                if alias == channel.name:
                    raise ValueError(f"channel alias duplicates canonical name: {alias}")
                if alias in seen:
                    raise ValueError(f"channel alias collides with canonical name: {alias}")
                if alias in local_aliases:
                    raise ValueError(f"duplicate alias for channel {channel.name}: {alias}")
                existing_owner = aliases_seen.get(alias)
                if existing_owner is not None:
                    raise ValueError(
                        f"channel alias {alias} is already assigned to {existing_owner}"
                    )
                local_aliases.add(alias)
                aliases_seen[alias] = channel.name

        for alias_name, owner in aliases_seen.items():
            if alias_name in seen and alias_name != owner:
                raise ValueError(f"channel alias collides with canonical name: {alias_name}")

        if self.position_mapping is not None:
            referenced = [
                self.position_mapping.lat_channel_name,
                self.position_mapping.lon_channel_name,
                self.position_mapping.alt_channel_name,
                self.position_mapping.x_channel_name,
                self.position_mapping.y_channel_name,
                self.position_mapping.z_channel_name,
            ]
            for channel_name in referenced:
                if channel_name and channel_name not in seen:
                    raise ValueError(f"position mapping references unknown channel: {channel_name}")

        for scenario_name, scenario in self.scenarios.items():
            for event in scenario.events:
                for channel_name in event.channels:
                    if channel_name not in seen:
                        raise ValueError(
                            f"scenario {scenario_name} references unknown channel: {channel_name}"
                        )
        return self


def vehicle_config_root() -> Path:
    configured = os.environ.get("VEHICLE_CONFIG_ROOT")
    if configured:
        return Path(configured).resolve()
    return (Path(__file__).resolve().parent.parent / "vehicle-configurations").resolve()


def resolve_vehicle_config_path(path_str: str, *, root: Path | None = None) -> Path:
    base = (root or vehicle_config_root()).resolve()
    raw = Path(path_str)
    candidate = raw if raw.is_absolute() else (base / raw)
    resolved = candidate.resolve()
    try:
        resolved.relative_to(base)
    except ValueError as exc:
        raise ValueError(f"Vehicle configuration path must stay under {base}") from exc
    if not resolved.exists():
        raise ValueError(f"Vehicle configuration file not found: {path_str}")
    if resolved.suffix.lower() not in {".json", ".yaml", ".yml"}:
        raise ValueError("Vehicle configuration file must be .json, .yaml, or .yml")
    return resolved


def canonical_vehicle_config_path(path_str: str, *, root: Path | None = None) -> str:
    resolved = resolve_vehicle_config_path(path_str, root=root)
    base = (root or vehicle_config_root()).resolve()
    return resolved.relative_to(base).as_posix()


def load_vehicle_config_file(path_str: str, *, root: Path | None = None) -> VehicleConfigurationFile:
    resolved = resolve_vehicle_config_path(path_str, root=root)
    raw = resolved.read_text(encoding="utf-8")
    if resolved.suffix.lower() == ".json":
        payload = json.loads(raw)
    else:
        payload = yaml.safe_load(raw)
    if not isinstance(payload, dict):
        raise ValueError("Vehicle configuration file must contain an object at the top level")
    return VehicleConfigurationFile.model_validate(payload)


def channel_rate_hz(channel: TelemetryChannelDefinition) -> float:
    if channel.sample_rate_hz is not None:
        return max(channel.sample_rate_hz, 0.0)
    subsystem_defaults = {
        "power": 1.0,
        "thermal": 0.2,
        "adcs": 5.0,
        "comms": 0.5,
        "obc": 0.5,
        "payload": 0.2,
        "propulsion": 0.1,
        "gps": 1.0,
        "safety": 0.2,
        "nav": 1.0,
    }
    return subsystem_defaults.get(channel.subsystem, 0.5)


def lla_to_ecef_m(lat_deg: float, lon_deg: float, alt_m: float) -> tuple[float, float, float]:
    lat = math.radians(lat_deg)
    lon = math.radians(lon_deg)
    a = 6378137.0
    e_sq = 6.69437999014e-3
    n = a / math.sqrt(1 - e_sq * math.sin(lat) ** 2)
    x = (n + alt_m) * math.cos(lat) * math.cos(lon)
    y = (n + alt_m) * math.cos(lat) * math.sin(lon)
    z = ((1 - e_sq) * n + alt_m) * math.sin(lat)
    return x, y, z
