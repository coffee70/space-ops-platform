"""Orbit validation: public API and pipeline."""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from app.orbit.anomaly import check_anomaly
from app.orbit.classifier import classify_orbit
from app.orbit.math import (
    EARTH_RADIUS_KM,
    MU_KM3_S2,
    inertial_velocity_from_ecef_positions,
    lla_to_ecef_km,
    orbital_parameters,
)
from app.orbit.state import (
    OrbitStatusResult,
    PositionSample,
    get_orbit_state,
)

_on_status_callbacks: list[Callable[[str, dict], None]] = []


def register_on_status_change(callback: Callable[[str, dict], None]) -> None:
    """Register a callback to be invoked when orbit status is updated. Called with (source_id, status_dict)."""
    _on_status_callbacks.append(callback)


def submit_position_sample(
    source_id: str,
    timestamp: float,
    lat_deg: float,
    lon_deg: float,
    alt_m: float,
) -> None:
    """Append a position sample and update orbit status for the source."""
    state = get_orbit_state()
    buf = state.get_buffer(source_id)
    buf.append(
        PositionSample(
            timestamp=timestamp,
            lat_deg=lat_deg,
            lon_deg=lon_deg,
            alt_m=alt_m,
        )
    )
    _update_status(source_id, state)


def _notify_status(source_id: str, state: Any) -> None:
    status_dict = state.get_status(source_id)
    if not status_dict:
        return
    for cb in _on_status_callbacks:
        try:
            cb(source_id, status_dict)
        except Exception:
            pass


def _update_status(source_id: str, state: Any) -> None:
    buf = state.get_buffer(source_id)
    if len(buf) < 2:
        state.set_status(
            source_id,
            OrbitStatusResult(
                source_id=source_id,
                status="INSUFFICIENT_DATA",
                reason="Need at least 2 position samples to compute orbit",
            ),
        )
        _notify_status(source_id, state)
        return

    prev = buf[-2]
    curr = buf[-1]
    dt = curr.timestamp - prev.timestamp
    if dt <= 0:
        state.set_status(
            source_id,
            OrbitStatusResult(
                source_id=source_id,
                status="INSUFFICIENT_DATA",
                reason="Invalid time step between samples",
            ),
        )
        _notify_status(source_id, state)
        return

    r_prev = lla_to_ecef_km(prev.lat_deg, prev.lon_deg, prev.alt_m)
    r_curr = lla_to_ecef_km(curr.lat_deg, curr.lon_deg, curr.alt_m)
    v = inertial_velocity_from_ecef_positions(r_prev, r_curr, dt)
    params = orbital_parameters(r_curr, v)

    specific_energy = params["specific_energy_km2_s2"]
    speed_km_s = params["speed_km_s"]
    perigee_km = params["perigee_alt_km"]
    apogee_km = params["apogee_alt_km"]
    eccentricity = params["eccentricity"]
    current_alt_km = curr.alt_m / 1000.0  # m -> km

    orbit_type = classify_orbit(perigee_km, apogee_km)
    anomaly_status, reason = check_anomaly(
        status_valid=True,
        specific_energy=specific_energy,
        speed_km_s=speed_km_s,
        current_alt_km=current_alt_km,
        perigee_alt_km=perigee_km,
        eccentricity=eccentricity,
        orbit_type=orbit_type,
    )

    result = OrbitStatusResult(
        source_id=source_id,
        status=anomaly_status,
        reason=reason,
        orbit_type=orbit_type,
        perigee_km=perigee_km,
        apogee_km=apogee_km,
        eccentricity=eccentricity,
        velocity_kms=speed_km_s,
        period_sec=params["period_sec"],
    )
    state.set_status(source_id, result)
    _notify_status(source_id, state)


def get_status(source_id: Optional[str] = None) -> Dict[str, Any]:
    """Return latest orbit status per source. If source_id is set, return only that source."""
    state = get_orbit_state()
    return state.get_status(source_id)


def reset_source(source_id: str) -> None:
    """Clear buffered orbit samples and status for a source."""
    state = get_orbit_state()
    state.reset_source(source_id)
