"""Document knowledge service entrypoint."""

from platform_common.web import create_service_app
from app.routes import document_knowledge

app = create_service_app(title="Document Knowledge Service", description="Mission and vehicle document ingestion and retrieval service.")
app.include_router(document_knowledge.router, prefix="", tags=["document-knowledge"])
