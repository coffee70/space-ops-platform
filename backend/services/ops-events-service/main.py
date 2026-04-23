"""Ops events service entrypoint."""

from platform_common.web import create_service_app
from app.routes import ops_events

app = create_service_app(
    title="Ops Events Service",
    description="Operational timeline service.",
)
app.include_router(ops_events.router, prefix="/ops", tags=["ops-events"])
