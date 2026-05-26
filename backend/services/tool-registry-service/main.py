"""Tool registry service entrypoint."""

import logging
from contextlib import asynccontextmanager

from app.database import get_db_context
from platform_common.web import create_service_app
from app.routes import tool_registry
from app.routes.handlers.tool_registry import reconcile_tool_definitions

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app):
    try:
        with get_db_context() as db:
            reconcile_tool_definitions(db)
    except Exception:
        logger.exception("failed to reconcile tool definitions during startup")
        raise
    yield


app = create_service_app(title="Tool Registry Service", description="Tool metadata registry service.", lifespan=lifespan)
app.include_router(tool_registry.router, prefix="", tags=["tool-registry"])
