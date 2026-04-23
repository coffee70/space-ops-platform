"""Telemetry intelligence service entrypoint."""

from platform_common.web import create_service_app
from app.routes import telemetry_intelligence

app = create_service_app(
    title="Telemetry Intelligence Service",
    description="Semantic search and telemetry explanation service.",
)
app.include_router(telemetry_intelligence.router, prefix="/telemetry", tags=["telemetry-intelligence"])
