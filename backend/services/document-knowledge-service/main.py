"""Document knowledge service entrypoint."""

from contextlib import asynccontextmanager
import logging
import threading

from fastapi import FastAPI

from app.config import get_settings
from app.database import get_db_context
from app.intelligence.embedding import get_embedding_provider
from app.intelligence.platform_docs_indexing import enqueue_platform_docs_index_on_startup
from platform_common.web import create_service_app
from app.routes import document_knowledge

logger = logging.getLogger(__name__)


def _prewarm_embedding_provider() -> None:
    try:
        get_embedding_provider()
    except Exception:
        logger.exception("document knowledge embedding provider prewarm failed")


def _enqueue_platform_docs_index_startup() -> None:
    settings = get_settings()
    if not settings.platform_docs_index_on_startup:
        return
    try:
        with get_db_context() as db:
            enqueue_platform_docs_index_on_startup(db, strict=settings.platform_docs_index_startup_strict)
    except Exception:
        if settings.platform_docs_index_startup_strict:
            raise
        logger.exception("platform docs startup index enqueue failed")


@asynccontextmanager
async def _lifespan(_: FastAPI):
    _enqueue_platform_docs_index_startup()
    thread = threading.Thread(target=_prewarm_embedding_provider, name="document-knowledge-prewarm", daemon=True)
    thread.start()
    yield


app = create_service_app(title="Document Knowledge Service", description="Mission and vehicle document ingestion and retrieval service.", lifespan=_lifespan)
app.include_router(document_knowledge.router, prefix="", tags=["document-knowledge"])
