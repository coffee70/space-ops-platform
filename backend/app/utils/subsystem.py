"""Subsystem inference from telemetry names."""

from typing import Optional

from app.models.telemetry import TelemetryMetadata

SUBSYSTEM_PREFIXES = {
    "power": ["PWR_", "EPS_"],
    "thermal": ["THERM_"],
    "adcs": ["ADCS_"],
    "comms": ["COMM_"],
    "obc": ["OBC_"],
    "payload": ["PAY_"],
    "propulsion": ["PROP_"],
    "gps": ["GPS_"],
    "safety": ["SAFE_", "WATCHDOG_", "ERR_", "HEALTH_"],
}


def infer_subsystem(name: str, meta: Optional[TelemetryMetadata]) -> str:
    """Infer subsystem from metadata or name prefix."""
    if meta and meta.subsystem_tag:
        return meta.subsystem_tag
    for tag, prefixes in SUBSYSTEM_PREFIXES.items():
        for prefix in prefixes:
            if name.startswith(prefix):
                return tag
    return "other"
