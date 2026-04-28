"""Code intelligence service entrypoint."""

from platform_common.web import create_service_app
from app.routes import code_intelligence

app = create_service_app(title="Code Intelligence Service", description="Managed-fork indexing and semantic code retrieval service.")
app.include_router(code_intelligence.router, prefix="", tags=["code-intelligence"])
