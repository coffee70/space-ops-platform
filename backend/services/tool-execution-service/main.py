"""Tool execution service entrypoint."""

from platform_common.web import create_service_app
from app.routes import tool_execution

app = create_service_app(title="Tool Execution Service", description="Controlled tool execution service.")
app.include_router(tool_execution.router, prefix="", tags=["tool-execution"])
