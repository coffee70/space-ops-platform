"""Document knowledge service entrypoint."""

from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI

from app.intelligence.embedding import get_embedding_provider
from platform_common.web import create_service_app
from app.routes import document_knowledge

logger = logging.getLogger(__name__)


@asynccontextmanager
async def _lifespan(_: FastAPI):
    try:
        get_embedding_provider()
    except Exception:
        logger.exception("document knowledge embedding provider prewarm failed")
    yield


app = create_service_app(title="Document Knowledge Service", description="Mission and vehicle document ingestion and retrieval service.", lifespan=_lifespan)
app.include_router(document_knowledge.router, prefix="", tags=["document-knowledge"])
