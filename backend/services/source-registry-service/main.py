"""Source registry service entrypoint."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from platform_common.web import create_service_app
from app.routes import source_registry

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app):
    from app.database import get_session_factory
    from app.services.realtime_service import (
        auto_register_sources_from_configs,
        repair_registered_sources_on_startup,
        refresh_source_embeddings,
    )

    async def run_startup_reconciliation() -> None:
        def run_sync() -> None:
            session = get_session_factory()()
            try:
                repaired_source_ids = repair_registered_sources_on_startup(session)
                from app.services.embedding_service import SentenceTransformerEmbeddingProvider

                provider = SentenceTransformerEmbeddingProvider()
                auto_register_sources_from_configs(session, embedding_provider=provider)
                if repaired_source_ids:
                    refresh_source_embeddings(
                        session,
                        source_ids=repaired_source_ids,
                        embedding_provider=provider,
                    )
            except Exception as exc:
                logger.exception("Startup source reconciliation failed: %s", exc)
                session.rollback()
            finally:
                session.close()

        await asyncio.to_thread(run_sync)

    reconciliation_task = asyncio.create_task(run_startup_reconciliation())
    yield
    if not reconciliation_task.done():
        reconciliation_task.cancel()
        try:
            await reconciliation_task
        except asyncio.CancelledError:
            pass


app = create_service_app(
    title="Source Registry Service",
    description="Canonical source and stream registry service.",
    lifespan=lifespan,
)
app.include_router(source_registry.router, prefix="/telemetry", tags=["source-registry"])
