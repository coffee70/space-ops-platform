"""Telemetry ingest service entrypoint."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from platform_common.messaging import Subjects, get_messaging
from platform_common.web import create_service_app
from app.routes import feed_health, realtime_ingest, telemetry_ingest_admin

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app):
    from app.orbit import register_on_status_change, reset_source
    from app.database import get_session_factory
    from app.realtime import get_realtime_processor
    from app.services.feed_health_service import refresh_feed_health_states, serialize_feed_health
    from app.services.ops_events_service import write_event as write_ops_event

    messaging = get_messaging()
    await messaging.connect()
    processor = get_realtime_processor()

    def publish_orbit_status(vehicle_id: str, payload: dict) -> None:
        messaging.publish_nowait(
            Subjects.ORBIT_STATUS,
            event_type="telemetry.orbit.status",
            payload={"vehicle_id": vehicle_id, **payload},
        )

    async def on_orbit_reset(envelope) -> None:
        source_id = envelope.payload.get("source_id")
        if isinstance(source_id, str) and source_id:
            reset_source(source_id)

    async def refresh_feed_health_periodically() -> None:
        session_factory = get_session_factory()
        while True:
            await asyncio.sleep(5)
            session = session_factory()
            try:
                changed = refresh_feed_health_states(session)
                for row in changed:
                    payload = serialize_feed_health(row)
                    messaging.publish_nowait(
                        Subjects.FEED_HEALTH,
                        event_type="telemetry.feed_health.updated",
                        payload=payload,
                    )
                    write_ops_event(
                        session,
                        source_id=row.source_id,
                        event_time=datetime.now(timezone.utc),
                        event_type="system.feed_status",
                        severity="info" if row.state == "connected" else "warning",
                        summary=f"Feed {row.source_id}: {row.state}",
                        entity_type="system",
                        entity_id=row.source_id,
                        payload=payload,
                    )
                session.commit()
            except Exception as exc:
                logger.exception("Feed health refresh failed: %s", exc)
                session.rollback()
            finally:
                session.close()

    register_on_status_change(publish_orbit_status)
    await messaging.subscribe(Subjects.ORBIT_RESET, on_orbit_reset)
    feed_health_task = asyncio.create_task(refresh_feed_health_periodically())
    yield
    feed_health_task.cancel()
    try:
        await feed_health_task
    except asyncio.CancelledError:
        pass
    processor.stop()
    await messaging.close()


app = create_service_app(
    title="Telemetry Ingest Service",
    description="Realtime telemetry ingest and feed-health service.",
    lifespan=lifespan,
)
app.include_router(realtime_ingest.router, prefix="/telemetry/realtime", tags=["realtime-ingest"])
app.include_router(telemetry_ingest_admin.router, prefix="/telemetry", tags=["telemetry-ingest"])
app.include_router(feed_health.router, prefix="/telemetry", tags=["feed-health"])
