"""Tool registry service entrypoint."""

from platform_common.web import create_service_app
from app.routes import tool_registry

app = create_service_app(title="Tool Registry Service", description="Tool metadata registry service.")
app.include_router(tool_registry.router, prefix="", tags=["tool-registry"])
