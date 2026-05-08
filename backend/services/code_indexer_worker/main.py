"""Polls `ai_code_index_jobs`, runs indexing, and performs startup root checks."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI

from app.config import get_settings
from app.database import get_db_context
from app.intelligence.code_index_job_query import find_active_index_job
from app.intelligence.indexing import get_current_commit_for_root, index_repository_now
from app.models.intelligence import CodeIndexJob, CodeRepository
from platform_common.web import create_service_app

logger = logging.getLogger(__name__)


def _ensure_repository_for_root(db, root: str, branch: str) -> CodeRepository:
    repo = db.query(CodeRepository).filter(CodeRepository.source_uri == root, CodeRepository.default_branch == branch).one_or_none()
    if repo:
        return repo
    repo = CodeRepository(
        name=root.split("/")[-1],
        source_uri=root,
        layer="layer2",
        default_branch=branch,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    db.add(repo)
    db.flush()
    return repo


def _enqueue_if_needed(db, root: str, branch: str) -> bool:
    try:
        sha = asyncio.run(get_current_commit_for_root(root, branch))
    except Exception:
        logger.exception("startup commit check failed for root=%s branch=%s", root, branch)
        return False

    repo = _ensure_repository_for_root(db, root, branch)
    repo.current_commit_sha = sha or repo.current_commit_sha
    now = datetime.now(timezone.utc)

    if sha and repo.indexed_commit_sha == sha and repo.index_status == "ready":
        return True

    if sha and repo.indexed_commit_sha and repo.indexed_commit_sha != sha:
        repo.index_status = "stale"

    if find_active_index_job(db, repo.id, sha or ""):
        return True

    job = CodeIndexJob(
        id=uuid.uuid4(),
        repository_id=repo.id,
        root=root,
        branch=branch,
        target_commit_sha=sha or "",
        status="queued",
        requested_at=now,
    )
    repo.index_status = "queued"
    repo.index_requested_at = now
    db.add(job)
    return True


def startup_enqueue_roots(max_attempts: int | None = None, delay_seconds: float | None = None) -> None:
    settings = get_settings()
    branch = settings.code_indexer_default_branch
    pending_roots = settings.get_code_indexer_startup_roots()
    if not pending_roots:
        return
    attempts = max(1, max_attempts or int(settings.code_indexer_startup_enqueue_attempts))
    delay = max(0.0, delay_seconds if delay_seconds is not None else float(settings.code_indexer_poll_interval_seconds))
    for attempt in range(1, attempts + 1):
        failed_roots: list[str] = []
        try:
            with get_db_context() as db:
                for root in pending_roots:
                    if not _enqueue_if_needed(db, root, branch):
                        failed_roots.append(root)
        except Exception:
            logger.exception("startup enqueue failed")
            failed_roots = pending_roots
        if not failed_roots:
            return
        pending_roots = failed_roots
        if attempt < attempts:
            logger.warning(
                "startup enqueue will retry for roots=%s attempt=%s/%s",
                ",".join(pending_roots),
                attempt + 1,
                attempts,
            )
            time.sleep(delay)


def _claim_next_job(db) -> CodeIndexJob | None:
    return (
        db.query(CodeIndexJob)
        .filter(CodeIndexJob.status == "queued")
        .order_by(CodeIndexJob.requested_at.asc())
        .with_for_update(skip_locked=True)
        .first()
    )


def _claim_and_mark_running() -> uuid.UUID | None:
    """Transaction A: claim job and mark running + repo indexing; commit before indexing work."""
    with get_db_context() as db:
        job = _claim_next_job(db)
        if not job:
            return None
        repo = db.query(CodeRepository).filter(CodeRepository.id == job.repository_id).one()
        started = datetime.now(timezone.utc)
        job.status = "running"
        job.started_at = started
        repo.index_status = "indexing"
        repo.index_started_at = started
        repo.last_error = None
        job_id = job.id
    return job_id


def _mark_job_failed(job_id: uuid.UUID, exc: BaseException) -> None:
    """Transaction C: persist failure without depending on rolled-back indexing writes."""
    msg = str(exc)[:2000]
    with get_db_context() as db:
        job = db.query(CodeIndexJob).filter(CodeIndexJob.id == job_id).one()
        repo = db.query(CodeRepository).filter(CodeRepository.id == job.repository_id).one()
        failed_at = datetime.now(timezone.utc)
        job.status = "failed"
        job.completed_at = failed_at
        job.error = msg
        repo.index_status = "failed"
        repo.last_error = msg


def process_one_job() -> None:
    job_id = _claim_and_mark_running()
    if not job_id:
        return
    settings = get_settings()
    max_preview = settings.code_indexer_max_failed_file_preview
    try:
        with get_db_context() as db:
            job = db.query(CodeIndexJob).filter(CodeIndexJob.id == job_id).one()
            result = asyncio.run(
                index_repository_now(
                    repository_id=job.repository_id,
                    root=job.root,
                    branch=job.branch,
                    target_commit_sha=job.target_commit_sha,
                    db=db,
                    max_failed_file_preview=max_preview,
                )
            )
            completed_at = datetime.now(timezone.utc)
            job.status = "completed"
            job.completed_at = completed_at
            job.file_count = result.file_count
            job.chunk_count = result.chunk_count
            job.skipped_file_count = result.skipped_file_count
            job.failed_file_count = result.failed_file_count
            job.failed_files_preview_json = result.failed_files_preview
            repo = db.query(CodeRepository).filter(CodeRepository.id == job.repository_id).one()
            repo.file_count = result.file_count
            repo.chunk_count = result.chunk_count
            repo.skipped_file_count = result.skipped_file_count
            repo.failed_file_count = result.failed_file_count
            repo.indexed_commit_sha = result.target_commit_sha
            repo.current_commit_sha = result.target_commit_sha
            repo.index_status = "ready"
            repo.index_completed_at = completed_at
            repo.last_error = None
    except Exception as exc:
        logger.exception("index job failed")
        _mark_job_failed(job_id, exc)


def _worker_thread(stop: threading.Event) -> None:
    settings = get_settings()
    interval = max(1, int(settings.code_indexer_poll_interval_seconds))
    startup_enqueue_roots()
    while not stop.is_set():
        try:
            process_one_job()
        except Exception:
            logger.exception("worker iteration error")
        stop.wait(timeout=interval)


@asynccontextmanager
async def _lifespan(_: FastAPI):
    stop = threading.Event()
    thread = threading.Thread(target=_worker_thread, args=(stop,), name="code-indexer-worker", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=15.0)


app = create_service_app(
    title="Code Indexer Worker",
    description="Background code indexing worker with health check.",
    lifespan=_lifespan,
)
