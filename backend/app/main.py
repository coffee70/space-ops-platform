"""Platform API gateway entrypoint."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from platform_common.messaging import Subjects, get_messaging
from platform_common.web import create_service_app
from app.realtime.ws_hub import get_ws_hub
from app.routes import gateway_http, realtime_gateway


@asynccontextmanager
async def lifespan(app):
    hub = get_ws_hub()
    hub.set_loop(asyncio.get_running_loop())
    messaging = get_messaging()
    await messaging.connect()

    async def on_telemetry_update(envelope) -> None:
        from app.models.schemas import RealtimeChannelUpdate

        hub.schedule_telemetry_update(RealtimeChannelUpdate.model_validate(envelope.payload))

    async def on_alert(envelope) -> None:
        hub.schedule_alert_event(envelope.payload.get("type", ""), envelope.payload.get("alert", {}))

    async def on_feed_status(envelope) -> None:
        hub.schedule_feed_status(envelope.payload)

    async def on_orbit_status(envelope) -> None:
        payload = envelope.payload
        hub.schedule_orbit_status(payload.get("vehicle_id", ""), payload)

    await messaging.subscribe(Subjects.TELEMETRY_UPDATE, on_telemetry_update)
    await messaging.subscribe(Subjects.TELEMETRY_ALERT, on_alert)
    await messaging.subscribe(Subjects.FEED_HEALTH, on_feed_status)
    await messaging.subscribe(Subjects.ORBIT_STATUS, on_orbit_status)

    yield

    await hub.stop()
    await messaging.close()


app = create_service_app(
    title="Platform API Gateway",
    description="Stable Mission Control API gateway over managed platform services.",
    lifespan=lifespan,
)
app.include_router(gateway_http.router, tags=["gateway"])
app.include_router(realtime_gateway.router, prefix="/telemetry/realtime", tags=["realtime"])
