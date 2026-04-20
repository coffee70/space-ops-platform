"""Proxy routes for the telemetry simulator service."""

import logging
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.database import get_db
from app.lib.audit import audit_log
from app.models.telemetry import TelemetrySource
from app.orbit import reset_source as reset_orbit_source
from app.services.source_stream_service import (
    StreamIdConflictError,
    SourceNotFoundError,
    clear_active_stream,
    register_stream,
)

router = APIRouter()
logger = logging.getLogger(__name__)


def _resolve_simulator_url(db: Session, source_id: str) -> str:
    """Resolve simulator base URL from DB. Raises 404 if not found or not a simulator."""
    src = db.get(TelemetrySource, source_id)
    if not src:
        raise HTTPException(status_code=404, detail="Source not found")
    if src.source_type != "simulator":
        raise HTTPException(status_code=400, detail="Source is not a simulator")
    if not src.base_url:
        raise HTTPException(status_code=400, detail="Simulator has no base_url configured")
    return src.base_url.rstrip("/")


def _resolve_simulator_source(db: Session, source_id: str) -> TelemetrySource:
    """Return canonical simulator row or raise HTTPException."""
    src = db.get(TelemetrySource, source_id)
    if not src or src.source_type != "simulator":
        raise HTTPException(status_code=404, detail="Simulator source not found")
    return src


class StartConfig(BaseModel):
    scenario: str = Field(default="nominal", description="Scenario name")
    duration: float = Field(default=300, ge=0, description="Duration in seconds (0 = infinite)")
    speed: float = Field(default=1.0, ge=0.1, description="Time speed factor")
    drop_prob: float = Field(default=0.0, ge=0, le=1, description="Link dropout probability")
    jitter: float = Field(default=0.1, ge=0, le=1, description="Inter-sample jitter")
    vehicle_id: str = Field(..., description="Vehicle ID for ingest (must be simulator)")
    base_url: str | None = Field(default=None, description="Backend ingest URL")
    vehicle_config_path: str | None = Field(
        default=None, description="Override catalog file for simulator runtime"
    )
    packet_source: str | None = Field(default="simulator-link", description="Packet origin identifier")
    receiver_id: str | None = Field(default=None, description="Receiving endpoint identifier")


async def _proxy_get(base_url: str, path: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(f"{base_url}{path}")
        if r.status_code >= 400:
            detail = r.text
            try:
                payload = r.json()
                detail = payload.get("detail", detail)
            except ValueError:
                pass
            raise HTTPException(status_code=r.status_code, detail=detail)
        return r.json()


async def _proxy_post(base_url: str, path: str, json: dict[str, Any] | None = None) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(f"{base_url}{path}", json=json)
        if r.status_code >= 400:
            detail = r.text
            try:
                payload = r.json()
                detail = payload.get("detail", detail)
            except ValueError:
                pass
            raise HTTPException(status_code=r.status_code, detail=detail)
        return r.json()


async def _rollback_simulator_start(base_url: str, source_id: str) -> None:
    """Try to stop a simulator after a start-side bookkeeping failure."""
    try:
        await _proxy_post(base_url, "/stop")
    except Exception:
        logger.exception(
            "Failed to roll back simulator start after stream registration failure",
            extra={
                "event": {
                    "action": "simulator.start.rollback_failed",
                    "component": "backend",
                    "destination": source_id,
                }
            },
        )


def _resolve_with_audit(db: Session, source_id: str, action: str) -> str:
    """Resolve simulator URL, audit-log on failure, then re-raise."""
    try:
        return _resolve_simulator_url(db, source_id)
    except HTTPException as e:
        audit_log(
            "simulator.source_resolve_failed",
            origin="frontend",
            destination=source_id,
            operation=action,
            status_code=e.status_code,
            detail=str(e.detail),
            level="error",
        )
        raise


@router.get("/status")
async def simulator_status(
    vehicle_id: str | None = None,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Get simulator state and config. Always returns 200; use 'connected' to detect reachability."""
    if vehicle_id is None:
        raise HTTPException(status_code=400, detail="vehicle_id is required")
    resolved_source_id = vehicle_id
    try:
        base_url = _resolve_with_audit(db, resolved_source_id, "status")
        payload = await _proxy_get(base_url, "/status")
        state = payload.get("state")
        config = payload.get("config") or {}
        active_stream_id = config.get("stream_id")
        packet_source = config.get("packet_source")
        receiver_id = config.get("receiver_id")
        registration_failed = False
        if state and state != "idle" and isinstance(active_stream_id, str) and active_stream_id:
            try:
                register_stream(
                    db,
                    source_id=resolved_source_id,
                    stream_id=active_stream_id,
                    packet_source=packet_source if isinstance(packet_source, str) else None,
                    receiver_id=receiver_id if isinstance(receiver_id, str) else None,
                )
            except Exception as e:
                logger.exception(
                    "Simulator status stream registration failed",
                    extra={
                        "event": {
                            "action": "simulator.status.stream_registration_failed",
                            "component": "backend",
                            "destination": resolved_source_id,
                            "stream_id": active_stream_id,
                        }
                    },
                )
                audit_log(
                    "simulator.status.stream_registration_failed",
                    origin="frontend",
                    destination=resolved_source_id,
                    stream_id=active_stream_id,
                    error=str(e),
                    level="warning",
                )
                registration_failed = True
        elif state == "idle":
            clear_active_stream(resolved_source_id, db=db)
    except (httpx.ConnectError, httpx.TimeoutException, HTTPException) as e:
        audit_log(
            "simulator.status.proxy_failed",
            origin="frontend",
            destination=resolved_source_id,
            error=str(e),
            level="error",
        )
        return {"connected": False, "supported_scenarios": []}
    if not isinstance(payload.get("supported_scenarios"), list):
        payload["supported_scenarios"] = []
    if registration_failed:
        payload = {
            **payload,
            "state": "degraded",
            "error": "backend stream registration failed",
        }
    return {"connected": True, **payload}


@router.post("/start")
async def simulator_start(
    config: StartConfig,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Start the simulator with given config."""
    resolved_source_id = config.vehicle_id
    src = _resolve_simulator_source(db, resolved_source_id)
    base_url = _resolve_with_audit(db, resolved_source_id, "start")
    audit_log(
        "simulator.start.received",
        origin="frontend",
        scenario=config.scenario,
        duration=config.duration,
        speed=config.speed,
        vehicle_id=resolved_source_id,
    )
    try:
        body = config.model_dump(exclude_none=True)
        body["vehicle_id"] = resolved_source_id
        if src.vehicle_config_path:
            body["vehicle_config_path"] = src.vehicle_config_path
        result = await _proxy_post(base_url, "/start", body)
        stream_id = result.get("stream_id")
        try:
            clear_active_stream(resolved_source_id, db=db)
            reset_orbit_source(resolved_source_id)
            if isinstance(stream_id, str) and stream_id:
                register_stream(
                    db,
                    source_id=resolved_source_id,
                    stream_id=stream_id,
                    packet_source=body.get("packet_source"),
                    receiver_id=body.get("receiver_id"),
                )
        except StreamIdConflictError as e:
            await _rollback_simulator_start(base_url, resolved_source_id)
            raise HTTPException(status_code=400, detail=str(e))
        except SourceNotFoundError as e:
            await _rollback_simulator_start(base_url, resolved_source_id)
            raise HTTPException(status_code=404, detail=str(e))
        except Exception:
            await _rollback_simulator_start(base_url, resolved_source_id)
            logger.exception(
                "Simulator start bookkeeping failed after remote start",
                extra={
                    "event": {
                        "action": "simulator.start.bookkeeping_failed",
                        "component": "backend",
                        "destination": resolved_source_id,
                        "stream_id": stream_id,
                    }
                },
            )
            raise HTTPException(
                status_code=500,
                detail="Simulator started remotely, but backend stream registration failed",
            )
        audit_log(
            "simulator.start.proxied",
            origin="frontend",
            destination=resolved_source_id,
            scenario=config.scenario,
            duration=config.duration,
            speed=config.speed,
            vehicle_id=resolved_source_id,
            base_url=config.base_url,
        )
        return result
    except (httpx.ConnectError, httpx.TimeoutException) as e:
        audit_log(
            "simulator.start.proxy_failed",
            origin="frontend",
            destination=resolved_source_id,
            error=str(e),
            level="error",
        )
        raise HTTPException(status_code=503, detail=f"Simulator unavailable: {e}")
    except HTTPException as e:
        audit_log(
            "simulator.start.rejected",
            origin="frontend",
            destination=resolved_source_id,
            error=str(e.detail),
            status_code=e.status_code,
            level="warning" if e.status_code < 500 else "error",
        )
        raise


@router.post("/pause")
async def simulator_pause(
    vehicle_id: str | None = None,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Pause the simulator."""
    if vehicle_id is None:
        raise HTTPException(status_code=400, detail="vehicle_id is required")
    resolved_source_id = vehicle_id
    base_url = _resolve_with_audit(db, resolved_source_id, "pause")
    try:
        result = await _proxy_post(base_url, "/pause")
        audit_log("simulator.pause", destination=resolved_source_id)
        return result
    except (httpx.ConnectError, httpx.TimeoutException) as e:
        audit_log(
            "simulator.pause.proxy_failed",
            origin="frontend",
            destination=resolved_source_id,
            error=str(e),
            level="error",
        )
        raise HTTPException(status_code=503, detail=f"Simulator unavailable: {e}")
    except HTTPException:
        raise


@router.post("/resume")
async def simulator_resume(
    vehicle_id: str | None = None,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Resume the simulator."""
    if vehicle_id is None:
        raise HTTPException(status_code=400, detail="vehicle_id is required")
    resolved_source_id = vehicle_id
    base_url = _resolve_with_audit(db, resolved_source_id, "resume")
    try:
        result = await _proxy_post(base_url, "/resume")
        audit_log("simulator.resume", destination=resolved_source_id)
        return result
    except (httpx.ConnectError, httpx.TimeoutException) as e:
        audit_log(
            "simulator.resume.proxy_failed",
            origin="frontend",
            destination=resolved_source_id,
            error=str(e),
            level="error",
        )
        raise HTTPException(status_code=503, detail=f"Simulator unavailable: {e}")
    except HTTPException:
        raise


@router.post("/stop")
async def simulator_stop(
    vehicle_id: str | None = None,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Stop the simulator."""
    if vehicle_id is None:
        raise HTTPException(status_code=400, detail="vehicle_id is required")
    resolved_source_id = vehicle_id
    base_url = _resolve_with_audit(db, resolved_source_id, "stop")
    try:
        result = await _proxy_post(base_url, "/stop")
        clear_active_stream(resolved_source_id, db=db)
        reset_orbit_source(resolved_source_id)
        audit_log("simulator.stop", destination=resolved_source_id)
        return result
    except (httpx.ConnectError, httpx.TimeoutException) as e:
        audit_log(
            "simulator.stop.proxy_failed",
            origin="frontend",
            destination=resolved_source_id,
            error=str(e),
            level="error",
        )
        raise HTTPException(status_code=503, detail=f"Simulator unavailable: {e}")
    except HTTPException:
        raise
