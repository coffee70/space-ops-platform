"""Realtime telemetry ingest and WebSocket routes."""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from sqlalchemy.orm import Session

from app.database import get_db, get_session_factory
from app.models.schemas import (
    MeasurementEvent,
    MeasurementEventBatch,
    RealtimeChannelUpdate,
    TelemetryAlertSchema,
    WsSnapshotAlerts,
    WsSnapshotWatchlist,
)
from app.models.telemetry import TelemetryAlert, TelemetryMetadata, TelemetrySource
from app.realtime.bus import get_realtime_bus
from app.realtime.ws_hub import get_ws_hub
from app.services.ops_events_service import write_event as write_ops_event
from app.services.realtime_service import (
    get_active_alerts,
    get_realtime_snapshot_for_channels,
    get_watchlist_channel_names,
)
from app.services.source_stream_service import (
    StreamIdConflictError,
    SourceNotFoundError,
    ensure_stream_belongs_to_source,
    get_stream_source_id,
    normalize_source_id,
    resolve_latest_stream_id,
)
from app.lib.audit import audit_log

logger = logging.getLogger(__name__)

router = APIRouter()


def _normalize_event_times(events: list[MeasurementEvent]) -> list[MeasurementEvent]:
    """Ensure ingest events carry both generation and reception timestamps."""
    now = datetime.now(timezone.utc).isoformat()
    return [
        MeasurementEvent(
            source_id=e.source_id,
            stream_id=e.stream_id,
            channel_name=e.channel_name,
            generation_time=e.generation_time or e.reception_time or now,
            reception_time=e.reception_time or now,
            value=e.value,
            quality=e.quality,
            sequence=e.sequence,
            packet_source=e.packet_source,
            receiver_id=e.receiver_id,
            tags=e.tags,
        )
        for e in events
    ]


def _resolve_stream_source_id(db: Session, stream_id: str) -> str:
    """Resolve a stream id to its owning source."""
    owner = get_stream_source_id(db, stream_id)
    if owner is None:
        raise HTTPException(status_code=404, detail="Unknown stream_id")
    return owner


def _resolve_requested_stream_id(
    session_factory: Callable[[], Session],
    source_id: str,
    stream_id: str | None,
) -> str:
    """Resolve a concrete stream id for websocket snapshots and scoped queries."""
    if stream_id:
        session = session_factory()
        try:
            try:
                return ensure_stream_belongs_to_source(session, source_id, stream_id)
            except ValueError as exc:
                raise HTTPException(status_code=404, detail=str(exc))
        finally:
            session.close()
    session = session_factory()
    try:
        return resolve_latest_stream_id(session, source_id)
    finally:
        session.close()


def _validate_stream_batch_identities(db: Session, events: list[MeasurementEvent]) -> None:
    """Reject colliding or foreign-owned stream ids before queueing realtime events."""
    stream_owners: dict[str, str] = {}
    seen_sources: set[str] = set()
    for event in events:
        logical_source_id = normalize_source_id(event.source_id)
        if logical_source_id not in seen_sources:
            source = db.get(TelemetrySource, logical_source_id)
            if source is None:
                raise SourceNotFoundError(f"Source not found: {logical_source_id}")
            seen_sources.add(logical_source_id)
        prior_owner = stream_owners.get(event.stream_id)
        if prior_owner is not None:
            if prior_owner != logical_source_id:
                raise HTTPException(
                    status_code=400,
                    detail="stream_id maps to multiple sources in the same batch",
                )
            continue
        stream_owners[event.stream_id] = logical_source_id

        existing_owner = get_stream_source_id(db, event.stream_id)
        if existing_owner is not None and normalize_source_id(existing_owner) != logical_source_id:
            raise HTTPException(status_code=400, detail="stream_id does not belong to source")


def _validate_required_sequences(events: list[MeasurementEvent]) -> None:
    if any(event.sequence is None for event in events):
        raise HTTPException(status_code=400, detail="measurement event sequence is required")


@router.post("/ingest")
async def ingest_realtime(
    body: MeasurementEventBatch,
    request: Request,
) -> dict[str, Any]:
    """Ingest batch of realtime measurement events. Async to avoid thread pool contention with processor."""
    started_at = time.perf_counter()
    request_id = getattr(request.state, "request_id", None) or request.headers.get("X-Request-ID")
    session = get_session_factory()()
    bus = get_realtime_bus()
    raw_events = body.events

    try:
        _validate_required_sequences(raw_events)
        _validate_stream_batch_identities(session, raw_events)
        source_ids = sorted({e.source_id for e in raw_events})
        filled_reception = sum(1 for e in raw_events if not e.reception_time)
        synthesized_generation = sum(1 for e in raw_events if not e.generation_time and e.reception_time)

        audit_log(
            "ingest.request.start",
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            count=len(raw_events),
            source_ids=source_ids,
            queue_size_before=bus.measurement_queue_size(),
            queue_maxsize=bus.measurement_queue_maxsize(),
        )

        assign_started = time.perf_counter()
        events = _normalize_event_times(body.events)
        assign_duration_ms = round((time.perf_counter() - assign_started) * 1000, 3)
        audit_log(
            "ingest.stage.normalize_event_times",
            request_id=request_id,
            count=len(events),
            filled_missing_reception_time=filled_reception,
            synthesized_generation_time=synthesized_generation,
            duration_ms=assign_duration_ms,
        )

        enqueue_started = time.perf_counter()
        accepted = 0
        dropped = 0
        for e in events:
            if bus.publish_measurement(e):
                accepted += 1
            else:
                dropped += 1

        enqueue_duration_ms = round((time.perf_counter() - enqueue_started) * 1000, 3)
        queue_after = bus.measurement_queue_size()
        audit_log(
            "ingest.stage.enqueue_complete",
            request_id=request_id,
            count=len(events),
            accepted=accepted,
            dropped=dropped,
            duration_ms=enqueue_duration_ms,
            queue_size_after=queue_after,
        )

        audit_log(
            "ingest.received",
            direction="external_to_backend",
            request_id=request_id,
            count=accepted,
            dropped=dropped,
            source_ids=source_ids,
        )
        total_duration_ms = round((time.perf_counter() - started_at) * 1000, 3)
        audit_log(
            "ingest.ack",
            request_id=request_id,
            accepted=accepted,
            dropped=dropped,
            total_duration_ms=total_duration_ms,
        )
        logger.info(
            "Realtime ingest ack",
            extra={
                "event": {
                    "action": "ingest.ack.debug",
                    "component": "backend",
                    "request_id": request_id,
                    "accepted": accepted,
                    "dropped": dropped,
                    "queue_size_after": queue_after,
                    "total_duration_ms": total_duration_ms,
                }
            },
        )
        return {"accepted": accepted}
    except StreamIdConflictError as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    except SourceNotFoundError as e:
        session.rollback()
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@router.websocket("/ws")
async def websocket_realtime(websocket: WebSocket) -> None:
    """WebSocket endpoint for realtime subscriptions and ack/resolve."""
    hub = get_ws_hub()
    await hub.connect(websocket)

    # Get DB session factory for snapshot/ack/resolve
    session_factory = get_session_factory()

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({"error": "invalid json"}))
                continue

            msg_type = msg.get("type", "")

            if msg_type == "hello":
                # Optional: validate client_version, requested_features
                await websocket.send_text(
                    json.dumps({"type": "hello_ack", "server_version": "1.0"})
                )

            elif msg_type == "subscribe_watchlist":
                channels = msg.get("channels", [])
                source_id = msg.get("source_id")
                if not source_id:
                    await websocket.send_text(json.dumps({"error": "source_id required"}))
                    continue
                stream_id = msg.get("stream_id")
                snapshot_stream_id = _resolve_requested_stream_id(session_factory, source_id, stream_id)
                if not channels:
                    # Default to watchlist
                    session = session_factory()
                    try:
                        channels = get_watchlist_channel_names(session, source_id)
                    finally:
                        session.close()
                await hub.subscribe_watchlist(
                    websocket,
                    channels,
                    source_id=source_id,
                    stream_id=stream_id,
                )

                # Send snapshot
                session = session_factory()
                try:
                    snapshot = get_realtime_snapshot_for_channels(
                        session,
                        channels,
                        source_id=source_id,
                        stream_id=snapshot_stream_id,
                    )
                    # Fallback: if telemetry_current is empty, channels may be empty
                    # Use overview-style fallback from telemetry_data
                    if len(snapshot) < len(channels):
                        from app.services.overview_service import get_overview
                        overview = get_overview(session, source_id=snapshot_stream_id)
                        overview_by_name = {c["name"]: c for c in overview}
                        for name in channels:
                            if name not in [s.name for s in snapshot] and name in overview_by_name:
                                o = overview_by_name[name]
                                from app.models.schemas import RecentDataPoint
                                snapshot.append(
                                    RealtimeChannelUpdate(
                                        source_id=source_id,
                                        stream_id=snapshot_stream_id,
                                        name=o["name"],
                                        units=o.get("units"),
                                        description=o.get("description"),
                                        subsystem_tag=o["subsystem_tag"],
                                        channel_origin=o.get("channel_origin", "catalog"),
                                        current_value=o["current_value"],
                                        generation_time=o["last_timestamp"],
                                        reception_time=o["last_timestamp"],
                                        state=o["state"],
                                        state_reason=o.get("state_reason"),
                                        z_score=o.get("z_score"),
                                        discovery_namespace=o.get("discovery_namespace"),
                                        sparkline_data=[
                                            RecentDataPoint(timestamp=p["timestamp"], value=p["value"])
                                            for p in o.get("sparkline_data", [])
                                        ],
                                    )
                                )
                    payload = WsSnapshotWatchlist(channels=snapshot).model_dump_json()
                    await websocket.send_text(payload)
                finally:
                    session.close()

            elif msg_type == "subscribe_channel":
                name = msg.get("name", "")
                source_id = msg.get("source_id")
                if not source_id:
                    await websocket.send_text(json.dumps({"error": "source_id required"}))
                    continue
                stream_id = msg.get("stream_id")
                snapshot_stream_id = _resolve_requested_stream_id(session_factory, source_id, stream_id)
                if name:
                    await hub.subscribe_channel(
                        websocket,
                        name,
                        source_id=source_id,
                        stream_id=stream_id,
                    )
                    session = session_factory()
                    try:
                        snapshot = get_realtime_snapshot_for_channels(
                            session,
                            [name],
                            source_id=source_id,
                            stream_id=snapshot_stream_id,
                        )
                        if snapshot:
                            from app.models.schemas import WsTelemetryUpdate
                            await websocket.send_text(
                                WsTelemetryUpdate(channel=snapshot[0]).model_dump_json()
                            )
                    finally:
                        session.close()

            elif msg_type == "subscribe_alerts":
                source_id = msg.get("source_id")
                if not source_id:
                    await websocket.send_text(json.dumps({"error": "source_id required"}))
                    continue
                stream_id = msg.get("stream_id")
                snapshot_stream_id = _resolve_requested_stream_id(session_factory, source_id, stream_id)
                await hub.subscribe_alerts(
                    websocket,
                    source_id=source_id,
                    stream_id=stream_id,
                )
                session = session_factory()
                try:
                    active = get_active_alerts(
                        session,
                        source_id=source_id,
                        stream_id=snapshot_stream_id,
                        subsystems=msg.get("subsystems"),
                        severities=msg.get("severities"),
                    )
                    payload = WsSnapshotAlerts(active=active).model_dump_json()
                    await websocket.send_text(payload)
                finally:
                    session.close()

            elif msg_type == "ack_alert":
                alert_id = msg.get("alert_id", "")
                session = session_factory()
                try:
                    from uuid import UUID
                    aid = UUID(alert_id)
                    alert = session.get(TelemetryAlert, aid)
                    if alert and alert.cleared_at is None and alert.resolved_at is None:
                        alert.acked_at = datetime.now(timezone.utc)
                        alert.acked_by = "operator"
                        alert.status = "acked"
                        alert.last_update_at = alert.acked_at
                        meta = session.get(TelemetryMetadata, alert.telemetry_id)
                        write_ops_event(
                            session,
                            source_id=meta.source_id if meta else _resolve_stream_source_id(session, alert.stream_id),
                            stream_id=alert.stream_id,
                            event_time=alert.acked_at,
                            event_type="alert.acked",
                            severity="info",
                            summary=f"{meta.name if meta else 'channel'} acked by operator",
                            entity_type="operator_action",
                            entity_id=meta.name if meta else None,
                            payload={"alert_id": alert_id, "actor": "operator"},
                        )
                        session.commit()
                        audit_log(
                            "alert.acked",
                            alert_id=alert_id,
                            channel_name=meta.name if meta else None,
                            source_id=meta.source_id if meta else _resolve_stream_source_id(session, alert.stream_id),
                        )
                        from app.utils.subsystem import infer_subsystem
                        subsys = infer_subsystem(meta.name, meta) if meta else "other"
                        schema = TelemetryAlertSchema(
                            id=str(alert.id),
                            source_id=meta.source_id if meta else _resolve_stream_source_id(session, alert.stream_id),
                            stream_id=alert.stream_id,
                            channel_name=meta.name if meta else "",
                            telemetry_id=str(alert.telemetry_id),
                            subsystem=subsys,
                            units=meta.units if meta else None,
                            severity=alert.severity,
                            reason=alert.reason,
                            status=alert.status,
                            opened_at=alert.opened_at.isoformat(),
                            opened_reception_at=alert.opened_reception_at.isoformat(),
                            last_update_at=alert.last_update_at.isoformat(),
                            current_value=float(alert.current_value_at_open),
                            acked_at=alert.acked_at.isoformat(),
                            acked_by=alert.acked_by,
                        )
                        await hub.broadcast_alert_event("acked", schema)
                finally:
                    session.close()

            elif msg_type == "resolve_alert":
                alert_id = msg.get("alert_id", "")
                resolution_text = msg.get("resolution_text", "")
                resolution_code = msg.get("resolution_code")
                session = session_factory()
                try:
                    from uuid import UUID
                    aid = UUID(alert_id)
                    alert = session.get(TelemetryAlert, aid)
                    if alert:
                        alert.resolved_at = datetime.now(timezone.utc)
                        alert.resolved_by = "operator"
                        alert.resolution_text = resolution_text
                        alert.resolution_code = resolution_code
                        alert.status = "resolved"
                        alert.last_update_at = alert.resolved_at
                        meta = session.get(TelemetryMetadata, alert.telemetry_id)
                        write_ops_event(
                            session,
                            source_id=meta.source_id if meta else _resolve_stream_source_id(session, alert.stream_id),
                            stream_id=alert.stream_id,
                            event_time=alert.resolved_at,
                            event_type="alert.resolved",
                            severity="info",
                            summary=f"{meta.name if meta else 'channel'} resolved: {resolution_text or resolution_code or 'no notes'}",
                            entity_type="operator_action",
                            entity_id=meta.name if meta else None,
                            payload={
                                "alert_id": alert_id,
                                "actor": "operator",
                                "resolution_text": resolution_text,
                                "resolution_code": resolution_code,
                            },
                        )
                        session.commit()
                        audit_log(
                            "alert.resolved",
                            alert_id=alert_id,
                            channel_name=meta.name if meta else None,
                            source_id=meta.source_id if meta else _resolve_stream_source_id(session, alert.stream_id),
                            resolution_code=resolution_code,
                        )
                        from app.utils.subsystem import infer_subsystem
                        subsys = infer_subsystem(meta.name, meta) if meta else "other"
                        schema = TelemetryAlertSchema(
                            id=str(alert.id),
                            source_id=meta.source_id if meta else _resolve_stream_source_id(session, alert.stream_id),
                            stream_id=alert.stream_id,
                            channel_name=meta.name if meta else "",
                            telemetry_id=str(alert.telemetry_id),
                            subsystem=subsys,
                            units=meta.units if meta else None,
                            severity=alert.severity,
                            reason=alert.reason,
                            status=alert.status,
                            opened_at=alert.opened_at.isoformat(),
                            opened_reception_at=alert.opened_reception_at.isoformat(),
                            last_update_at=alert.last_update_at.isoformat(),
                            current_value=float(alert.current_value_at_open),
                            resolved_at=alert.resolved_at.isoformat(),
                            resolved_by=alert.resolved_by,
                            resolution_text=alert.resolution_text,
                            resolution_code=alert.resolution_code,
                        )
                        await hub.broadcast_alert_event("resolved", schema)
                finally:
                    session.close()

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.exception("WebSocket error: %s", e)
    finally:
        await hub.disconnect(websocket)
