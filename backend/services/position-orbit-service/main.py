"""Position and orbit service entrypoint."""

from platform_common.web import create_service_app
from app.routes import orbit, position

app = create_service_app(
    title="Position Orbit Service",
    description="Position mapping and orbit status service.",
)
app.include_router(position.router, prefix="/telemetry", tags=["position"])
app.include_router(orbit.router, prefix="/telemetry", tags=["orbit"])
