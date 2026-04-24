"""Composed telemetry router grouped by service ownership."""

from fastapi import APIRouter

from app.routes import source_registry, telemetry_ingest_admin, telemetry_intelligence, telemetry_query

router = APIRouter()
router.include_router(source_registry.router)
router.include_router(telemetry_query.router)
router.include_router(telemetry_intelligence.router)
router.include_router(telemetry_ingest_admin.router)
