"""Context retrieval service entrypoint."""

from platform_common.web import create_service_app
from app.routes import context_retrieval

app = create_service_app(title="Context Retrieval Service", description="Bounded context packet assembly service.")
app.include_router(context_retrieval.router, prefix="", tags=["context-retrieval"])
