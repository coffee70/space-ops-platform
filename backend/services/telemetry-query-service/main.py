"""Telemetry query service entrypoint."""

from platform_common.web import create_service_app
from app.routes import telemetry_query

app = create_service_app(
    title="Telemetry Query Service",
    description="Telemetry query and dashboard read service.",
)
app.include_router(telemetry_query.router, prefix="/telemetry", tags=["telemetry-query"])
