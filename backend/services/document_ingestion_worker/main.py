"""Polls `ai_document_ingestion_jobs` and ingests queued documents."""

from __future__ import annotations

import logging
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI

from app.config import get_settings
from app.database import get_db_context
from app.intelligence.document_ingestion import ingest_document_now
from app.intelligence.embedding import DEFAULT_EMBEDDING_MODEL, get_embedding_provider
from app.intelligence.events import emit_event
from app.intelligence.platform_docs_indexing import run_platform_docs_index_job
from app.models.intelligence import Document, DocumentIngestionJob, PlatformDocsIndexJob
from platform_common.web import create_service_app

logger = logging.getLogger(__name__)


def _has_trace_metadata(job: DocumentIngestionJob) -> bool:
    return bool(job.conversation_id and job.agent_run_id and job.request_id)


def _claim_next_job(db) -> DocumentIngestionJob | None:
    return (
        db.query(DocumentIngestionJob)
        .filter(DocumentIngestionJob.status == "queued")
        .order_by(DocumentIngestionJob.requested_at.asc())
        .with_for_update(skip_locked=True)
        .first()
    )


def _claim_next_platform_docs_job(db) -> PlatformDocsIndexJob | None:
    return (
        db.query(PlatformDocsIndexJob)
        .filter(PlatformDocsIndexJob.status == "queued")
        .order_by(PlatformDocsIndexJob.requested_at.asc())
        .with_for_update(skip_locked=True)
        .first()
    )


def process_one_platform_docs_job() -> bool:
    with get_db_context() as db:
        job = _claim_next_platform_docs_job(db)
        if not job:
            return False
        job.status = "running"
        job.started_at = datetime.now(timezone.utc)
        job_id = job.id

    try:
        with get_db_context() as db:
            job = db.query(PlatformDocsIndexJob).filter(PlatformDocsIndexJob.id == job_id).one()
            run_platform_docs_index_job(db, job)
    except Exception as exc:
        settings = get_settings()
        logger.exception("platform docs index job failed job_id=%s", job_id)
        with get_db_context() as db:
            job = db.query(PlatformDocsIndexJob).filter(PlatformDocsIndexJob.id == job_id).one_or_none()
            if job:
                job.status = "failed"
                job.completed_at = datetime.now(timezone.utc)
                job.error = str(exc)[: max(1, int(settings.document_ingestion_max_error_length))]
    return True


def _claim_and_mark_running() -> uuid.UUID | None:
    with get_db_context() as db:
        job = _claim_next_job(db)
        if not job:
            return None
        document = db.query(Document).filter(Document.id == job.document_id).one()
        started = datetime.now(timezone.utc)
        job.status = "running"
        job.started_at = started
        document.ingestion_status = "pending"
        document.ingestion_error = None
        document.updated_at = started
        if _has_trace_metadata(job):
            emit_event(
                db,
                event_type="document.ingestion_started",
                payload={"document_id": str(document.id), "chunking_strategy": "fixed_1200_overlap_120", "embedding_model": DEFAULT_EMBEDDING_MODEL},
                conversation_id=str(job.conversation_id) if job.conversation_id else None,
                agent_run_id=str(job.agent_run_id),
                request_id=str(job.request_id),
                sequence=2,
                emitted_by="document-ingestion-worker",
            )
        job_id = job.id
    return job_id


def _mark_job_failed(job_id: uuid.UUID, exc: BaseException) -> None:
    settings = get_settings()
    msg = str(exc)[: max(1, int(settings.document_ingestion_max_error_length))]
    with get_db_context() as db:
        job = db.query(DocumentIngestionJob).filter(DocumentIngestionJob.id == job_id).one_or_none()
        if not job:
            logger.info("document ingestion job disappeared before failure persistence job_id=%s", job_id)
            return
        document = db.query(Document).filter(Document.id == job.document_id).one_or_none()
        if not document:
            logger.info("document disappeared before failure persistence job_id=%s document_id=%s", job_id, job.document_id)
            return
        failed_at = datetime.now(timezone.utc)
        job.status = "failed"
        job.completed_at = failed_at
        job.error = msg
        document.ingestion_status = "failed"
        document.ingestion_error = msg
        document.updated_at = failed_at
        if _has_trace_metadata(job):
            emit_event(
                db,
                event_type="document.ingestion_failed",
                payload={"document_id": str(document.id), "error_code": "ingestion_failed", "message": msg},
                conversation_id=str(job.conversation_id) if job.conversation_id else None,
                agent_run_id=str(job.agent_run_id),
                request_id=str(job.request_id),
                sequence=3,
                emitted_by="document-ingestion-worker",
            )


def process_one_job() -> None:
    if process_one_platform_docs_job():
        return
    job_id = _claim_and_mark_running()
    if not job_id:
        return
    try:
        with get_db_context() as db:
            job = db.query(DocumentIngestionJob).filter(DocumentIngestionJob.id == job_id).one()
            document = db.query(Document).filter(Document.id == job.document_id).one()
            result = ingest_document_now(
                db=db,
                document=document,
                conversation_id=job.conversation_id,
                agent_run_id=job.agent_run_id,
                request_id=job.request_id,
            )
            completed_at = datetime.now(timezone.utc)
            job.status = "completed"
            job.completed_at = completed_at
            job.error = None
            logger.info("document ingestion completed job_id=%s document_id=%s chunks=%s duration_ms=%s", job.id, document.id, result.chunk_count, result.duration_ms)
    except Exception as exc:
        logger.exception("document ingestion job failed job_id=%s", job_id)
        _mark_job_failed(job_id, exc)


def _prewarm_embedding_provider() -> None:
    try:
        get_embedding_provider()
    except Exception:
        logger.exception("document ingestion worker embedding provider prewarm failed")


def _worker_thread(stop: threading.Event) -> None:
    settings = get_settings()
    interval = max(1, int(settings.document_ingestion_poll_interval_seconds))
    _prewarm_embedding_provider()
    while not stop.is_set():
        try:
            process_one_job()
        except Exception:
            logger.exception("document ingestion worker iteration error")
        stop.wait(timeout=interval)


@asynccontextmanager
async def _lifespan(_: FastAPI):
    stop = threading.Event()
    thread = threading.Thread(target=_worker_thread, args=(stop,), name="document-ingestion-worker", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=15.0)


app = create_service_app(
    title="Document Ingestion Worker",
    description="Background document ingestion worker with health check.",
    lifespan=_lifespan,
)
