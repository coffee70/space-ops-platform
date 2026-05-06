"""Telemetry simulator managed service entrypoint."""

from __future__ import annotations

from app.simulator.routes import router as simulator_router
from platform_common.web import create_service_app

app = create_service_app(
    title="Telemetry Simulator Service",
    description="Layer 2 managed telemetry simulator service.",
)
app.include_router(simulator_router, tags=["simulator"])
