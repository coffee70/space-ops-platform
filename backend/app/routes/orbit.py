"""Orbit validation status API."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Request

from app.orbit import get_status
from app.services.source_stream_service import normalize_source_id

router = APIRouter()


@router.get("/orbit/status")
def orbit_status(request: Request, vehicle_id: Optional[str] = None):
    """Return latest orbit status per vehicle. Optional vehicle_id to filter."""
    if "source_id" in request.query_params:
        raise HTTPException(status_code=400, detail="Use vehicle_id")
    logical_source_id = normalize_source_id(vehicle_id) if vehicle_id else None
    data = get_status(source_id=logical_source_id)
    if logical_source_id is not None:
        if not data:
            return {}
        return {
            "vehicle_id": logical_source_id,
            "status": data.get("status", ""),
            "reason": data.get("reason", ""),
            "orbit_type": data.get("orbit_type"),
            "perigee_km": data.get("perigee_km"),
            "apogee_km": data.get("apogee_km"),
            "eccentricity": data.get("eccentricity"),
            "velocity_kms": data.get("velocity_kms"),
            "period_sec": data.get("period_sec"),
        }
    return {
        resolved_source_id: {
            "vehicle_id": resolved_source_id,
            "status": status.get("status", ""),
            "reason": status.get("reason", ""),
            "orbit_type": status.get("orbit_type"),
            "perigee_km": status.get("perigee_km"),
            "apogee_km": status.get("apogee_km"),
            "eccentricity": status.get("eccentricity"),
            "velocity_kms": status.get("velocity_kms"),
            "period_sec": status.get("period_sec"),
        }
        for resolved_source_id, status in data.items()
    }
