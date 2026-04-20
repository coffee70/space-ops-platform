"""In-memory state: per-source position buffer and latest orbit status."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, Optional

BUFFER_SIZE = 10


@dataclass
class PositionSample:
    """Single position sample (t, lat, lon, alt)."""

    timestamp: float  # Unix seconds
    lat_deg: float
    lon_deg: float
    alt_m: float


@dataclass
class OrbitStatusResult:
    """Latest orbit status for a source."""

    source_id: str
    status: str  # VALID | ESCAPE_TRAJECTORY | SUBORBITAL | ORBIT_DECAY | HIGHLY_ELLIPTICAL | INSUFFICIENT_DATA
    reason: str
    orbit_type: Optional[str] = None
    perigee_km: Optional[float] = None
    apogee_km: Optional[float] = None
    eccentricity: Optional[float] = None
    velocity_kms: Optional[float] = None
    period_sec: Optional[float] = None


class OrbitState:
    """Singleton in-memory state for orbit buffers and status."""

    def __init__(self) -> None:
        self._buffers: Dict[str, deque] = {}
        self._status: Dict[str, OrbitStatusResult] = {}

    def get_buffer(self, source_id: str) -> deque:
        if source_id not in self._buffers:
            self._buffers[source_id] = deque(maxlen=BUFFER_SIZE)
        return self._buffers[source_id]

    def set_status(self, source_id: str, status_result: OrbitStatusResult) -> None:
        self._status[source_id] = status_result

    def reset_source(self, source_id: str) -> None:
        """Drop buffered samples and latest status for a source."""
        self._buffers.pop(source_id, None)
        self._status.pop(source_id, None)

    def get_status(self, source_id: Optional[str] = None) -> Dict[str, Any]:
        """Return latest status per source. If source_id given, filter to that source."""
        if source_id is not None:
            s = self._status.get(source_id)
            if s is None:
                return {}
            return _status_to_dict(s)
        return {sid: _status_to_dict(s) for sid, s in self._status.items()}


_state: Optional[OrbitState] = None


def get_orbit_state() -> OrbitState:
    global _state
    if _state is None:
        _state = OrbitState()
    return _state


def _status_to_dict(s: OrbitStatusResult) -> dict:
    return {
        "source_id": s.source_id,
        "status": s.status,
        "reason": s.reason,
        "orbit_type": s.orbit_type,
        "perigee_km": s.perigee_km,
        "apogee_km": s.apogee_km,
        "eccentricity": s.eccentricity,
        "velocity_kms": s.velocity_kms,
        "period_sec": s.period_sec,
    }
