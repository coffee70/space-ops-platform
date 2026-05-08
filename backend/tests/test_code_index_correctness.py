"""Targeted correctness tests for async indexing (transactions, branch, dedupe)."""

from __future__ import annotations

from datetime import datetime, timezone
import uuid
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from app.intelligence.code_index_job_query import find_active_index_job
from app.models.intelligence import CodeIndexJob, CodeRepository
from app.routes.handlers import code_intelligence

from tests.test_code_intelligence_hardening import _SessionDouble


def _job(
    repo_id: uuid.UUID,
    *,
    status: str = "queued",
    target_sha: str = "abc",
    requested_at: datetime | None = None,
) -> CodeIndexJob:
    ts = requested_at or datetime.now(timezone.utc)
    return CodeIndexJob(
        id=uuid.uuid4(),
        repository_id=repo_id,
        root="project/space-ops-platform",
        branch="main",
        target_commit_sha=target_sha,
        status=status,
        requested_at=ts,
    )


def test_find_active_index_job_returns_oldest() -> None:
    session = _SessionDouble()
    repo = CodeRepository(
        name="space-ops-platform",
        source_uri="project/space-ops-platform",
        layer="layer2",
        default_branch="main",
    )
    session.add(repo)
    old = _job(repo.id, requested_at=datetime(2020, 1, 1, tzinfo=timezone.utc))
    new = _job(repo.id, requested_at=datetime(2021, 6, 1, tzinfo=timezone.utc))
    session.add(old)
    session.add(new)

    got = find_active_index_job(session, repo.id, "abc")
    assert got is not None
    assert got.id == old.id


def test_search_wrong_branch_returns_503_not_indexed() -> None:
    session = _SessionDouble()
    session.add(
        CodeRepository(
            name="space-ops-platform",
            source_uri="project/space-ops-platform",
            layer="layer2",
            default_branch="main",
            index_status="ready",
            indexed_commit_sha="s",
            current_commit_sha="s",
        )
    )
    with pytest.raises(HTTPException) as exc:
        code_intelligence.search_code(
            {"query": "telemetry", "repository": "space-ops-platform", "branch": "feature/foo"},
            db=session,
        )
    assert exc.value.status_code == 503
    d = exc.value.detail
    assert d["error_code"] == "code_index_not_ready"
    assert d["branch"] == "feature/foo"
    assert d["root"] is None


@pytest.mark.anyio
async def test_enqueue_reuses_oldest_when_duplicate_active_jobs_exist(monkeypatch) -> None:
    session = _SessionDouble()
    monkeypatch.setattr(code_intelligence, "get_current_commit_for_root", AsyncMock(return_value="deadbeef"))

    repo = CodeRepository(
        name="space-ops-platform",
        source_uri="project/space-ops-platform",
        layer="layer2",
        default_branch="main",
    )
    session.add(repo)
    older = _job(repo.id, target_sha="deadbeef", requested_at=datetime(2019, 1, 1, tzinfo=timezone.utc))
    newer = _job(repo.id, target_sha="deadbeef", requested_at=datetime(2025, 1, 1, tzinfo=timezone.utc))
    session.add(older)
    session.add(newer)

    out = await code_intelligence.index_repository(
        {"root": "project/space-ops-platform", "branch": "main"},
        db=session,
    )
    assert out["job_id"] == str(older.id)
    assert out["index_status"] in ("queued", "indexing")


def test_process_one_job_marks_failed_without_calling_mark_failed_on_success(monkeypatch) -> None:
    from services.code_indexer_worker import main as worker_main

    mark_calls: list[uuid.UUID] = []
    real_mark = worker_main._mark_job_failed

    def wrap_mark(jid: uuid.UUID, exc: Exception) -> None:
        mark_calls.append(jid)
        return real_mark(jid, exc)

    monkeypatch.setattr(worker_main, "_mark_job_failed", wrap_mark)

    monkeypatch.setattr(worker_main, "_claim_and_mark_running", lambda: None)
    worker_main.process_one_job()
    assert mark_calls == []


def test_process_one_job_invokes_mark_failed_when_index_raises(monkeypatch) -> None:
    from contextlib import contextmanager

    from services.code_indexer_worker import main as worker_main

    job_uuid = uuid.uuid4()
    monkeypatch.setattr(worker_main, "_claim_and_mark_running", lambda: job_uuid)

    async def boom(**kwargs) -> object:
        raise RuntimeError("simulated index failure")

    monkeypatch.setattr(worker_main, "index_repository_now", boom)

    mark_calls: list[tuple[uuid.UUID, Exception]] = []

    def capture_mark(id_: uuid.UUID, exc: Exception) -> None:
        mark_calls.append((id_, exc))

    monkeypatch.setattr(worker_main, "_mark_job_failed", capture_mark)

    session = _SessionDouble()
    repo = CodeRepository(
        name="space-ops-platform",
        source_uri="project/space-ops-platform",
        layer="layer2",
        default_branch="main",
    )
    session.add(repo)
    job = CodeIndexJob(
        id=job_uuid,
        repository_id=repo.id,
        root="project/space-ops-platform",
        branch="main",
        target_commit_sha="x",
        status="running",
        requested_at=datetime.now(timezone.utc),
    )
    session.add(job)

    @contextmanager
    def fake_db():
        yield session

    monkeypatch.setattr(worker_main, "get_db_context", fake_db)

    worker_main.process_one_job()
    assert len(mark_calls) == 1
    assert mark_calls[0][0] == job_uuid
    assert "simulated index failure" in str(mark_calls[0][1])


def test_process_one_job_completes_marks_repo_ready(monkeypatch) -> None:
    from contextlib import contextmanager

    from app.intelligence.indexing import IndexRepositoryResult
    from services.code_indexer_worker import main as worker_main

    job_uuid = uuid.uuid4()

    async def ok(**kwargs) -> IndexRepositoryResult:
        rid = kwargs["repository_id"]
        return IndexRepositoryResult(
            repository_id=rid,
            root="project/space-ops-platform",
            branch="main",
            target_commit_sha="sha-complete",
            file_count=3,
            chunk_count=10,
            skipped_file_count=1,
            failed_file_count=0,
            failed_files_preview=[],
        )

    monkeypatch.setattr(worker_main, "_claim_and_mark_running", lambda: job_uuid)
    monkeypatch.setattr(worker_main, "index_repository_now", ok)

    session = _SessionDouble()
    repo = CodeRepository(
        name="space-ops-platform",
        source_uri="project/space-ops-platform",
        layer="layer2",
        default_branch="main",
    )
    session.add(repo)
    job = CodeIndexJob(
        id=job_uuid,
        repository_id=repo.id,
        root="project/space-ops-platform",
        branch="main",
        target_commit_sha="x",
        status="running",
        requested_at=datetime.now(timezone.utc),
    )
    session.add(job)

    @contextmanager
    def fake_db():
        yield session

    monkeypatch.setattr(worker_main, "get_db_context", fake_db)

    worker_main.process_one_job()

    assert job.status == "completed"
    assert repo.index_status == "ready"
    assert repo.file_count == 3
    assert repo.chunk_count == 10
    assert repo.indexed_commit_sha == "sha-complete"


def test_find_active_includes_queued_status() -> None:
    session = _SessionDouble()
    repo = CodeRepository(
        name="x",
        source_uri="project/x",
        layer="layer2",
        default_branch="main",
    )
    session.add(repo)
    j = _job(repo.id, status="queued", target_sha="abc")
    session.add(j)
    got = find_active_index_job(session, repo.id, "abc")
    assert got is j


@pytest.mark.anyio
async def test_index_repository_now_inserts_no_chunks_when_second_embed_fails(monkeypatch) -> None:
    from app.intelligence import indexing
    from app.intelligence.chunking import CodeChunkResult

    root = "project/space-ops-platform"
    tree = {
        "commit_sha": "s",
        "data": {"entries": [{"path": f"{root}/f.py", "name": "f.py", "is_dir": False}]},
    }

    async def fake_json(path: str, params: dict | None = None):
        if path == "code/tree" and params and params.get("path") == root:
            return tree
        raise AssertionError((path, params))

    monkeypatch.setattr(indexing, "cp_get_json", fake_json)
    monkeypatch.setattr(
        indexing,
        "cp_get_file_payload",
        AsyncMock(return_value={"commit_sha": "s", "data": {"content": "ignored"}}),
    )

    def fake_chunks(_text: str, **kwargs) -> list[CodeChunkResult]:
        return [
            CodeChunkResult("a", 1, 1, "A", "class", {}),
            CodeChunkResult("b", 3, 3, "B", "class", {}),
        ]

    monkeypatch.setattr(indexing, "chunk_code_with_metadata", fake_chunks)

    calls = {"n": 0}

    class FlakyProvider:
        def embed(self, text: str) -> list[float]:
            calls["n"] += 1
            if calls["n"] >= 2:
                raise RuntimeError("embed failed on second chunk")
            return [1.0]

    monkeypatch.setattr("app.intelligence.embedding.get_embedding_provider", lambda: FlakyProvider())

    session = _SessionDouble()
    repo = CodeRepository(
        name="p",
        source_uri=root,
        layer="layer2",
        default_branch="main",
    )
    session.add(repo)
    await indexing.index_repository_now(
        repository_id=repo.id,
        root=root,
        branch="main",
        target_commit_sha="s",
        db=session,
    )
    assert len(session.code_chunks) == 0
