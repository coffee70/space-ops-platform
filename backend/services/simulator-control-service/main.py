"""Simulator control service entrypoint."""

from platform_common.web import create_service_app
from app.routes import simulator

app = create_service_app(
    title="Simulator Control Service",
    description="Simulator control proxy service.",
)
app.include_router(simulator.router, prefix="/simulator", tags=["simulator"])
