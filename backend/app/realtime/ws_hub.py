"""WebSocket hub: manages connections, subscriptions, and broadcasts."""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import WebSocket

from app.models.schemas import (
    RealtimeChannelUpdate,
    TelemetryAlertSchema,
    WsFeedStatus,
    WsOrbitStatus,
    WsSnapshotAlerts,
    WsSnapshotWatchlist,
    WsTelemetryUpdate,
    WsAlertEvent,
)

logger = logging.getLogger(__name__)

BROADCAST_QUEUE_MAXSIZE = 50  # Drop oldest when full to avoid event loop starvation
SUBSCRIPTION_MODE_SOURCE_WIDE = "source_wide"
SUBSCRIPTION_MODE_EXPLICIT_STREAM = "explicit_stream"


class RealtimeWsHub:
    """Manages WebSocket connections and broadcasts."""

    def __init__(self) -> None:
        self._connections: dict[WebSocket, dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._broadcast_queue: asyncio.Queue[RealtimeChannelUpdate] | None = None
        self._drain_task: asyncio.Task[None] | None = None

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Set event loop for scheduling broadcasts from sync threads."""
        self._loop = loop
        self._broadcast_queue = asyncio.Queue(maxsize=BROADCAST_QUEUE_MAXSIZE)
        self._drain_task = asyncio.create_task(self._drain_broadcast_queue())

    async def connect(self, ws: WebSocket) -> None:
        """Accept connection and register."""
        await ws.accept()
        async with self._lock:
            self._connections[ws] = {
                "active_source_id": None,
                "active_stream_id": None,
                "stream_subscription_mode": SUBSCRIPTION_MODE_SOURCE_WIDE,
                "watchlist_channels": set(),
                "channel_detail": set(),
                "alerts_subscribed": True,
            }
        logger.info(
            "WebSocket client connected, total=%d",
            len(self._connections),
            extra={"ws_clients": len(self._connections)},
        )

    async def disconnect(self, ws: WebSocket) -> None:
        """Remove connection."""
        async with self._lock:
            self._connections.pop(ws, None)
        logger.info("WebSocket client disconnected, total=%d", len(self._connections))

    def _get_subscribed_connections(
        self,
        channel_name: str | None = None,
        for_alerts: bool = False,
        source_id: str | None = None,
        stream_id: str | None = None,
    ) -> list[WebSocket]:
        """Get connections that should receive this update."""
        result = []
        for ws, subs in self._connections.items():
            conn_source_id = subs.get("active_source_id")
            conn_stream_id = subs.get("active_stream_id")
            subscription_mode = subs.get(
                "stream_subscription_mode",
                SUBSCRIPTION_MODE_SOURCE_WIDE
                if conn_stream_id is None
                else SUBSCRIPTION_MODE_EXPLICIT_STREAM,
            )
            scope_matches = (
                conn_source_id == source_id
                if subscription_mode == SUBSCRIPTION_MODE_SOURCE_WIDE
                else conn_stream_id == stream_id
            )
            if for_alerts and subs.get("alerts_subscribed"):
                if scope_matches:
                    result.append(ws)
            elif channel_name and channel_name in subs.get("watchlist_channels", set()):
                if scope_matches:
                    result.append(ws)
            elif channel_name and channel_name in subs.get("channel_detail", set()):
                if scope_matches:
                    result.append(ws)
        return result

    def schedule_telemetry_update(self, update: RealtimeChannelUpdate) -> None:
        """Enqueue broadcast from sync context. Single drain prevents event loop starvation."""
        if self._loop is None or self._broadcast_queue is None:
            return

        def _enqueue():
            try:
                self._broadcast_queue.put_nowait(update)
            except asyncio.QueueFull:
                pass

        self._loop.call_soon_threadsafe(_enqueue)

    async def _drain_broadcast_queue(self) -> None:
        """Single coroutine drains queue; prevents flooding event loop with broadcast tasks."""
        if self._broadcast_queue is None:
            return
        while True:
            try:
                update = await self._broadcast_queue.get()
                await self._do_broadcast_telemetry_update(update)
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Broadcast drain error: %s", e)

    def schedule_feed_status(self, status: dict) -> None:
        """Schedule feed_status broadcast from sync context."""
        if self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(
            self.broadcast_feed_status(status),
            self._loop,
        )

    async def broadcast_feed_status(self, status: dict) -> None:
        """Broadcast feed_status to all connected clients."""
        if not self._connections:
            return
        ts = status.get("last_reception_time")
        ts_str = None
        if ts is not None:
            try:
                ts_str = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            except (TypeError, OSError):
                ts_str = str(ts)
        msg = WsFeedStatus(
            source_id=status["source_id"],
            connected=status.get("connected", False),
            state=status.get("state", "disconnected"),
            last_reception_time=ts_str,
            approx_rate_hz=status.get("approx_rate_hz"),
            drop_count=status.get("drop_count"),
        ).model_dump_json()
        dead = []
        for ws in list(self._connections.keys()):
            try:
                await ws.send_text(msg)
            except Exception as e:
                logger.warning("Feed status broadcast failed: %s", e)
                dead.append(ws)
        for ws in dead:
            await self.disconnect(ws)

    def schedule_alert_event(
        self,
        event_type: str,
        alert: TelemetryAlertSchema | dict,
    ) -> None:
        """Schedule alert broadcast from sync context."""
        if self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(
            self.broadcast_alert_event(event_type, alert),
            self._loop,
        )

    def schedule_orbit_status(self, vehicle_id: str, payload: dict) -> None:
        """Schedule orbit status broadcast from sync context."""
        if self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(
            self.broadcast_orbit_status(vehicle_id, payload),
            self._loop,
        )

    async def broadcast_orbit_status(self, vehicle_id: str, payload: dict) -> None:
        """Broadcast orbit status to all connected clients."""
        if not self._connections:
            return
        msg = WsOrbitStatus(
            vehicle_id=vehicle_id,
            status=payload.get("status", ""),
            reason=payload.get("reason", ""),
            orbit_type=payload.get("orbit_type"),
            perigee_km=payload.get("perigee_km"),
            apogee_km=payload.get("apogee_km"),
            eccentricity=payload.get("eccentricity"),
            velocity_kms=payload.get("velocity_kms"),
            period_sec=payload.get("period_sec"),
        ).model_dump_json()
        dead = []
        for ws in list(self._connections.keys()):
            try:
                await ws.send_text(msg)
            except Exception as e:
                logger.warning("Orbit status broadcast failed: %s", e)
                dead.append(ws)
        for ws in dead:
            await self.disconnect(ws)

    async def _do_broadcast_telemetry_update(self, update: RealtimeChannelUpdate) -> None:
        """Broadcast to clients subscribed to this channel and source."""
        targets = self._get_subscribed_connections(
            channel_name=update.name,
            source_id=update.source_id,
            stream_id=update.stream_id,
        )
        if not targets:
            return
        msg = WsTelemetryUpdate(channel=update).model_dump_json()
        dead = []
        for ws in targets:
            try:
                await ws.send_text(msg)
            except Exception as e:
                logger.warning("Broadcast failed to client: %s", e)
                dead.append(ws)
        for ws in dead:
            await self.disconnect(ws)

    async def broadcast_telemetry_update(self, update: RealtimeChannelUpdate) -> None:
        """Legacy entry point; now delegates to queue-based drain."""
        await self._do_broadcast_telemetry_update(update)

    async def broadcast_alert_event(
        self,
        event_type: str,
        alert: TelemetryAlertSchema | dict,
    ) -> None:
        """Broadcast alert event to subscribed clients for the alert's source."""
        if isinstance(alert, dict):
            alert_obj = TelemetryAlertSchema(**alert)
        else:
            alert_obj = alert
        targets = self._get_subscribed_connections(
            for_alerts=True,
            source_id=alert_obj.source_id,
            stream_id=alert_obj.stream_id,
        )
        if not targets:
            return
        msg = WsAlertEvent(event_type=event_type, alert=alert_obj).model_dump_json()
        dead = []
        for ws in targets:
            try:
                await ws.send_text(msg)
            except Exception as e:
                logger.warning("Alert broadcast failed to client: %s", e)
                dead.append(ws)
        for ws in dead:
            await self.disconnect(ws)

    async def subscribe_watchlist(
        self,
        ws: WebSocket,
        channels: list[str],
        source_id: str,
        stream_id: str | None = None,
    ) -> None:
        """Subscribe client to watchlist channels for a source."""
        async with self._lock:
            if ws in self._connections:
                self._connections[ws]["active_source_id"] = source_id
                self._connections[ws]["active_stream_id"] = stream_id
                self._connections[ws]["stream_subscription_mode"] = (
                    SUBSCRIPTION_MODE_SOURCE_WIDE
                    if stream_id is None
                    else SUBSCRIPTION_MODE_EXPLICIT_STREAM
                )
                self._connections[ws]["watchlist_channels"] = set(channels)

    async def subscribe_alerts(
        self,
        ws: WebSocket,
        source_id: str,
        stream_id: str | None = None,
    ) -> None:
        """Subscribe client to alert stream for a source."""
        async with self._lock:
            if ws in self._connections:
                self._connections[ws]["active_source_id"] = source_id
                self._connections[ws]["active_stream_id"] = stream_id
                self._connections[ws]["stream_subscription_mode"] = (
                    SUBSCRIPTION_MODE_SOURCE_WIDE
                    if stream_id is None
                    else SUBSCRIPTION_MODE_EXPLICIT_STREAM
                )
                self._connections[ws]["alerts_subscribed"] = True

    async def subscribe_channel(
        self,
        ws: WebSocket,
        name: str,
        source_id: str,
        stream_id: str | None = None,
    ) -> None:
        """Subscribe client to single channel detail for a source."""
        async with self._lock:
            if ws in self._connections:
                self._connections[ws]["active_source_id"] = source_id
                self._connections[ws]["active_stream_id"] = stream_id
                self._connections[ws]["stream_subscription_mode"] = (
                    SUBSCRIPTION_MODE_SOURCE_WIDE
                    if stream_id is None
                    else SUBSCRIPTION_MODE_EXPLICIT_STREAM
                )
                self._connections[ws]["channel_detail"].add(name)

    async def unsubscribe_channel(self, ws: WebSocket, name: str) -> None:
        """Unsubscribe from channel detail."""
        async with self._lock:
            if ws in self._connections:
                self._connections[ws]["channel_detail"].discard(name)

    def connection_count(self) -> int:
        """Return number of connected clients."""
        return len(self._connections)

    async def stop(self) -> None:
        """Cancel broadcast drain task."""
        if self._drain_task is not None:
            self._drain_task.cancel()
            try:
                await self._drain_task
            except asyncio.CancelledError:
                pass
            self._drain_task = None


_hub: RealtimeWsHub | None = None


def get_ws_hub() -> RealtimeWsHub:
    """Get singleton WebSocket hub."""
    global _hub
    if _hub is None:
        _hub = RealtimeWsHub()
    return _hub
