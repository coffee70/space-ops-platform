"""FastAPI application entry point."""

import asyncio
import logging
import re
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config import get_settings
from app.lib.audit import audit_log
from app.lib.logging_setup import configure_logging
from app.routes import ops, orbit as orbit_routes, position, realtime, simulator, telemetry, vehicle_configs

configure_logging()
logger = logging.getLogger(__name__)

# CORS: explicit origins plus optional regex (e.g. LAN access to UI on port 3000 in Docker).
_settings_for_cors = get_settings()
CORS_ORIGINS = _settings_for_cors.get_cors_origins_list()
_CORS_ORIGIN_REGEX_RAW = (_settings_for_cors.cors_origin_regex or "").strip()
CORS_ORIGIN_PATTERN = re.compile(_CORS_ORIGIN_REGEX_RAW) if _CORS_ORIGIN_REGEX_RAW else None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    logger.info("Starting Telemetry Operations Platform")
    from app.realtime import get_realtime_processor
    from app.realtime.feed_health import get_feed_health_tracker
    from app.realtime.ws_hub import get_ws_hub

    hub = get_ws_hub()
    hub.set_loop(asyncio.get_running_loop())
    proc = get_realtime_processor()
    proc.register_telemetry_update_handler(hub.schedule_telemetry_update)
    bus = proc._bus

    from app.database import get_session_factory
    from app.orbit import register_on_status_change
    from app.services.ops_events_service import write_event as write_ops_event
    from app.services.realtime_service import (
        auto_register_sources_from_configs,
        repair_registered_sources_on_startup,
        refresh_source_embeddings,
    )

    register_on_status_change(hub.schedule_orbit_status)

    def on_alert(ev: dict):
        hub.schedule_alert_event(ev.get("type", ""), ev.get("alert", {}))

    def on_feed_transition(source_id: str, old_state: str, new_state: str):
        session_factory = get_session_factory()
        session = session_factory()
        try:
            from datetime import datetime, timezone

            write_ops_event(
                session,
                source_id=source_id,
                event_time=datetime.now(timezone.utc),
                event_type="system.feed_status",
                severity="info" if new_state == "connected" else "warning",
                summary=f"Feed {source_id}: {old_state} -> {new_state}",
                entity_type="system",
                entity_id=source_id,
                payload={"old_state": old_state, "new_state": new_state},
            )
            session.commit()
        except Exception as e:
            logger.exception("Failed to write feed transition ops_event: %s", e)
            session.rollback()
        finally:
            session.close()
        status = get_feed_health_tracker().get_status(source_id)
        hub.schedule_feed_status(status)

    get_feed_health_tracker().set_on_transition(on_feed_transition)
    bus.subscribe_alerts(on_alert)

    async def run_startup_reconciliation():
        def run_sync() -> None:
            session = get_session_factory()()
            try:
                repaired_source_ids = repair_registered_sources_on_startup(session)
                from app.services.embedding_service import SentenceTransformerEmbeddingProvider

                provider = SentenceTransformerEmbeddingProvider()
                auto_register_sources_from_configs(
                    session,
                    embedding_provider=provider,
                )
                if repaired_source_ids:
                    refresh_source_embeddings(
                        session,
                        source_ids=repaired_source_ids,
                        embedding_provider=provider,
                    )
            except Exception as e:
                logger.exception("Startup source reconciliation failed: %s", e)
                session.rollback()
            finally:
                session.close()

        await asyncio.to_thread(run_sync)

    startup_reconciliation_task = asyncio.create_task(run_startup_reconciliation())

    async def broadcast_feed_status_periodically():
        while True:
            await asyncio.sleep(5)
            tracker = get_feed_health_tracker()
            for st in tracker.get_all_statuses():
                if st:
                    hub.schedule_feed_status(st)

    feed_task = asyncio.create_task(broadcast_feed_status_periodically())

    yield

    feed_task.cancel()
    try:
        await feed_task
    except asyncio.CancelledError:
        pass
    if not startup_reconciliation_task.done():
        startup_reconciliation_task.cancel()
        try:
            await startup_reconciliation_task
        except asyncio.CancelledError:
            pass
    await hub.stop()
    bus.unsubscribe_alerts(on_alert)
    proc.unregister_telemetry_update_handler(hub.schedule_telemetry_update)
    proc.stop()
    logger.info("Shutting down")


app = FastAPI(
    title="Telemetry Operations Platform",
    description="Ingest telemetry, compute stats, semantic search, and LLM explanations",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_origin_regex=_CORS_ORIGIN_REGEX_RAW or None,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)


def _allowed_request_origin(origin: str | None) -> str:
    """Return Origin header value to echo on responses (must match browser Origin for credentialed CORS)."""
    if not origin:
        return CORS_ORIGINS[0] if CORS_ORIGINS else ""
    if origin in CORS_ORIGINS:
        return origin
    if CORS_ORIGIN_PATTERN and CORS_ORIGIN_PATTERN.fullmatch(origin):
        return origin
    return CORS_ORIGINS[0] if CORS_ORIGINS else ""


def _cors_headers(request: Request) -> dict:
    origin = request.headers.get("origin")
    allowed = _allowed_request_origin(origin)
    return {
        "Access-Control-Allow-Origin": allowed,
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Allow-Methods": "*",
        "Access-Control-Allow-Headers": "*",
    }


@app.exception_handler(StarletteHTTPException)
async def http_exception_with_cors(request: Request, exc: StarletteHTTPException):
    """Add CORS headers to HTTPException responses."""
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
        headers=_cors_headers(request),
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_with_cors(request: Request, exc: RequestValidationError):
    """Add CORS headers to validation error responses."""
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors()},
        headers=_cors_headers(request),
    )


@app.exception_handler(Exception)
async def add_cors_to_exception_response(request: Request, exc: Exception):
    """Ensure CORS headers are present on exception responses so the client can read the error."""
    logger.exception("Unhandled exception: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
        headers=_cors_headers(request),
    )


@app.middleware("http")
async def audit_request_middleware(request: Request, call_next):
    """Log HTTP requests for audit and debugging."""
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    request.state.request_id = request_id
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000
    audit_log(
        "http.request",
        method=request.method,
        path=request.url.path,
        status_code=response.status_code,
        duration_ms=round(duration_ms, 2),
        request_id=request_id,
    )
    try:
        response.headers["X-Request-ID"] = request_id
    except (TypeError, ValueError):
        pass
    return response


app.include_router(telemetry.router, prefix="/telemetry", tags=["telemetry"])
app.include_router(position.router, prefix="/telemetry", tags=["position"])
app.include_router(orbit_routes.router, prefix="/telemetry", tags=["orbit"])
app.include_router(ops.router, prefix="/ops", tags=["ops"])
app.include_router(realtime.router, prefix="/telemetry/realtime", tags=["realtime"])
app.include_router(simulator.router, prefix="/simulator", tags=["simulator"])
app.include_router(vehicle_configs.router, prefix="/vehicle-configs", tags=["vehicle-configs"])


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok"}
