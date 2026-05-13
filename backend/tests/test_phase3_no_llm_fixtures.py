from __future__ import annotations

from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
import sys
import types
import uuid

import pytest
from fastapi import HTTPException, UploadFile

if "sentence_transformers" not in sys.modules:
    _st = types.ModuleType("sentence_transformers")

    class _SentenceTransformer:  # noqa: N801
        def __init__(self, *_args, **_kwargs):
            pass

        def encode(self, *_args, **_kwargs):
            return [0.0]

    _st.SentenceTransformer = _SentenceTransformer
    sys.modules["sentence_transformers"] = _st

from app.intelligence.chunking import chunk_text
from app.intelligence import document_ingestion
from app.intelligence import indexing
from app.models.intelligence import AgentEvent, CodeChunk, CodeIndexJob, CodeRepository, Document, DocumentChunk, DocumentIngestionJob
from app.routes.handlers import code_intelligence, document_knowledge
from services.document_ingestion_worker import main as document_ingestion_worker

FIXTURES_ROOT = Path(__file__).resolve().parent / "fixtures"


class _Provider:
    def embed(self, text: str) -> list[float]:
        return [float(len(text))]


class _TupleQuery:
    def __init__(self, rows):
        self._rows = rows

    def join(self, *_args, **_kwargs):
        return self

    def filter(self, *_args, **_kwargs):
        return self

    def all(self):
        return list(self._rows)


class _ModelQuery:
    def __init__(self, session: "_SessionDouble", model):
        self._session = session
        self._model = model
        self._filters = []

    def filter(self, *args, **_kwargs):
        self._filters.extend(args)
        return self

    def order_by(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def with_for_update(self, *_args, **_kwargs):
        return self

    def one_or_none(self):
        rows = self.all()
        return rows[0] if rows else None

    def one(self):
        rows = self.all()
        if len(rows) != 1:
            raise ValueError("Expected exactly one row")
        return rows[0]

    def first(self):
        rows = self.all()
        return rows[0] if rows else None

    def count(self):
        return len(self.all())

    def delete(self, **_kwargs):
        if self._model is DocumentChunk:
            document_id = self._document_id_filter()
            if document_id is None:
                count = len(self._session.document_chunks)
                self._session.document_chunks.clear()
                return count
            before = len(self._session.document_chunks)
            self._session.document_chunks = [chunk for chunk in self._session.document_chunks if chunk.document_id != document_id]
            return before - len(self._session.document_chunks)
        if self._model is DocumentIngestionJob:
            document_id = self._document_id_filter()
            if document_id is None:
                count = len(self._session.document_ingestion_jobs)
                self._session.document_ingestion_jobs.clear()
                return count
            before = len(self._session.document_ingestion_jobs)
            self._session.document_ingestion_jobs = [job for job in self._session.document_ingestion_jobs if job.document_id != document_id]
            return before - len(self._session.document_ingestion_jobs)
        if self._model is Document:
            document_id = self._id_filter()
            if document_id is None:
                count = len(self._session.documents)
                self._session.documents.clear()
                return count
            before = len(self._session.documents)
            self._session.documents = [document for document in self._session.documents if document.id != document_id]
            return before - len(self._session.documents)
        if self._model is CodeChunk:
            self._session.code_chunks.clear()
        return 0

    def all(self):
        if self._model is Document:
            rows = list(self._session.documents)
            document_id = self._id_filter()
            return [document for document in rows if document.id == document_id] if document_id else rows
        if self._model is DocumentChunk:
            rows = list(self._session.document_chunks)
            document_id = self._document_id_filter()
            return [chunk for chunk in rows if chunk.document_id == document_id] if document_id else rows
        if self._model is DocumentIngestionJob:
            rows = list(self._session.document_ingestion_jobs)
            job_id = self._id_filter()
            document_id = self._document_id_filter()
            statuses = self._status_filter_values()
            if job_id:
                rows = [job for job in rows if job.id == job_id]
            if document_id:
                rows = [job for job in rows if job.document_id == document_id]
            if statuses:
                rows = [job for job in rows if job.status in statuses]
            return rows
        if self._model is CodeRepository:
            return list(self._session.code_repositories)
        if self._model is CodeChunk:
            return list(self._session.code_chunks)
        if self._model is CodeIndexJob:
            return list(self._session.code_index_jobs)
        return []

    def _uuid_filter_value(self, column_name: str):
        for expression in self._filters:
            left = getattr(expression, "left", None)
            right = getattr(expression, "right", None)
            if getattr(left, "name", None) == column_name:
                return getattr(right, "value", None)
        return None

    def _id_filter(self):
        return self._uuid_filter_value("id")

    def _document_id_filter(self):
        return self._uuid_filter_value("document_id")

    def _status_filter_values(self):
        for expression in self._filters:
            left = getattr(expression, "left", None)
            right = getattr(expression, "right", None)
            if getattr(left, "name", None) == "status":
                value = getattr(right, "value", None)
                if isinstance(value, (list, tuple, set)):
                    return set(value)
                if value:
                    return {value}
        return set()


class _SessionDouble:
    def __init__(self):
        self.documents: list[Document] = []
        self.document_chunks: list[DocumentChunk] = []
        self.document_ingestion_jobs: list[DocumentIngestionJob] = []
        self.code_repositories: list[CodeRepository] = []
        self.code_chunks: list[CodeChunk] = []
        self.code_index_jobs: list[CodeIndexJob] = []
        self.events: list[AgentEvent] = []

    def add(self, obj):
        if getattr(obj, "id", None) is None and hasattr(obj, "id"):
            obj.id = uuid.uuid4()
        if isinstance(obj, Document):
            self.documents.append(obj)
        elif isinstance(obj, DocumentChunk):
            self.document_chunks.append(obj)
        elif isinstance(obj, DocumentIngestionJob):
            self.document_ingestion_jobs.append(obj)
        elif isinstance(obj, CodeRepository):
            self.code_repositories.append(obj)
        elif isinstance(obj, CodeChunk):
            self.code_chunks.append(obj)
        elif isinstance(obj, CodeIndexJob):
            self.code_index_jobs.append(obj)
        elif isinstance(obj, AgentEvent):
            self.events.append(obj)

    def flush(self):
        return None

    def delete(self, obj):
        if isinstance(obj, Document):
            self.documents = [document for document in self.documents if document.id != obj.id]
            self.document_chunks = [chunk for chunk in self.document_chunks if chunk.document_id != obj.id]
            self.document_ingestion_jobs = [job for job in self.document_ingestion_jobs if job.document_id != obj.id]

    def query(self, *models):
        if len(models) == 2 and models == (DocumentChunk, Document):
            rows = []
            for chunk in self.document_chunks:
                document = next((doc for doc in self.documents if doc.id == chunk.document_id), None)
                if document is not None:
                    rows.append((chunk, document))
            return _TupleQuery(rows)
        if len(models) == 2 and models == (CodeChunk, CodeRepository):
            rows = []
            for chunk in self.code_chunks:
                repository = next((repo for repo in self.code_repositories if repo.id == chunk.repository_id), None)
                if repository is not None:
                    rows.append((chunk, repository))
            return _TupleQuery(rows)
        return _ModelQuery(self, models[0])


@contextmanager
def _session_context(session: _SessionDouble):
    yield session


async def _upload_document(
    session: _SessionDouble,
    *,
    filename: str,
    content: str,
    mission_id: str | None = None,
    vehicle_id: str | None = None,
    subsystem_id: str | None = None,
    tags: str | None = None,
) -> dict:
    return await document_knowledge.create_document(
        file=UploadFile(filename=filename, file=BytesIO(content.encode("utf-8"))),
        title=None,
        document_type=None,
        mission_id=mission_id,
        vehicle_id=vehicle_id,
        subsystem_id=subsystem_id,
        tags=tags,
        description=None,
        conversation_id=None,
        agent_run_id=None,
        request_id=None,
        db=session,
    )


@pytest.mark.anyio
async def test_phase3_fixture_document_upload_emits_lifecycle_events_and_searchable_chunks(monkeypatch) -> None:
    monkeypatch.setattr(document_knowledge, "get_embedding_provider", lambda: _Provider())
    monkeypatch.setattr(document_ingestion, "get_embedding_provider", lambda: _Provider())
    session = _SessionDouble()
    fixture_path = FIXTURES_ROOT / "phase3_documents" / "battery_efficiency_notes.md"
    content = fixture_path.read_text(encoding="utf-8")

    result = await document_knowledge.create_document(
        file=UploadFile(filename=fixture_path.name, file=BytesIO(content.encode("utf-8"))),
        title=None,
        document_type=None,
        mission_id=None,
        vehicle_id=None,
        subsystem_id=None,
        tags=None,
        description=None,
        conversation_id="11111111-1111-1111-1111-111111111111",
        agent_run_id="22222222-2222-2222-2222-222222222222",
        request_id="33333333-3333-3333-3333-333333333333",
        db=session,
    )

    assert result["ingestion_status"] == "pending"
    assert len(session.documents) == 1
    assert session.documents[0].raw_content == content
    assert len(session.document_ingestion_jobs) == 1
    assert session.document_ingestion_jobs[0].status == "queued"
    assert len(session.document_chunks) == 0
    assert [event.event_type for event in session.events] == ["document.uploaded"]

    ingest_result = document_ingestion.ingest_document_now(
        db=session,
        document=session.documents[0],
        conversation_id=session.document_ingestion_jobs[0].conversation_id,
        agent_run_id=session.document_ingestion_jobs[0].agent_run_id,
        request_id=session.document_ingestion_jobs[0].request_id,
    )
    session.document_ingestion_jobs[0].status = "completed"

    assert session.documents[0].ingestion_status == "ready"
    assert ingest_result.chunk_count == len(chunk_text(content, max_chars=1200, overlap=120))
    assert len(session.document_chunks) == len(chunk_text(content, max_chars=1200, overlap=120))
    assert [event.event_type for event in session.events] == [
        "document.uploaded",
        "document.ingestion_completed",
    ]

    search_results = document_knowledge.search_documents({"query": "battery efficiency", "limit": 2}, db=session)
    assert search_results
    assert any("battery efficiency" in item["content"].lower() for item in search_results)
    assert "ranking_signals" in search_results[0]["metadata"]


@pytest.mark.anyio
async def test_document_worker_emits_traced_started_and_completed_events(monkeypatch) -> None:
    monkeypatch.setattr(document_ingestion, "get_embedding_provider", lambda: _Provider())
    monkeypatch.setattr(document_ingestion_worker, "get_db_context", lambda: _session_context(session))
    session = _SessionDouble()

    await document_knowledge.create_document(
        file=UploadFile(filename="telemetry.md", file=BytesIO(b"battery voltage telemetry note")),
        title=None,
        document_type=None,
        mission_id=None,
        vehicle_id=None,
        subsystem_id=None,
        tags=None,
        description=None,
        conversation_id="11111111-1111-1111-1111-111111111111",
        agent_run_id="22222222-2222-2222-2222-222222222222",
        request_id="33333333-3333-3333-3333-333333333333",
        db=session,
    )

    document_ingestion_worker.process_one_job()

    assert session.documents[0].ingestion_status == "ready"
    assert session.document_ingestion_jobs[0].status == "completed"
    assert [event.event_type for event in session.events] == [
        "document.uploaded",
        "document.ingestion_started",
        "document.ingestion_completed",
    ]


@pytest.mark.anyio
async def test_document_search_hardening_ranking_filters_and_limits(monkeypatch) -> None:
    monkeypatch.setattr(document_knowledge, "get_embedding_provider", lambda: _Provider())
    monkeypatch.setattr(document_ingestion, "get_embedding_provider", lambda: _Provider())
    session = _SessionDouble()

    telemetry_dictionary = """# DemoSat Telemetry Dictionary

Battery voltage channels belong to the Electrical Power Subsystem.

Channel: EPS_BATT_VOLTAGE
Display Name: Battery Voltage
Unit: volts
Subsystem: EPS
Description: Main spacecraft battery bus voltage.
"""
    mission_notes = """# Mission Notes

The spacecraft has general operations procedures and scheduling notes.
"""

    await _upload_document(
        session,
        filename="demo-telemetry.md",
        content=telemetry_dictionary,
        mission_id="demo-1",
        vehicle_id="demosat-1",
        subsystem_id="eps",
        tags="telemetry,battery,voltage",
    )
    document_ingestion.ingest_document_now(db=session, document=session.documents[-1])
    await _upload_document(
        session,
        filename="mission-notes.md",
        content=mission_notes,
        mission_id="demo-2",
        vehicle_id="demosat-2",
        subsystem_id="adcs",
        tags="ops,notes",
    )
    document_ingestion.ingest_document_now(db=session, document=session.documents[-1])

    query = "battery voltage subsystem display name units"
    results = document_knowledge.search_documents({"query": query, "limit": 4}, db=session)
    assert results
    assert "demo-telemetry.md" == results[0]["metadata"]["filename"]
    assert "battery voltage" in results[0]["content"].lower()
    assert "ranking_signals" in results[0]["metadata"]

    top_titles = [item["title"] for item in results[:2]]
    assert top_titles[0].lower().startswith("demo-telemetry")

    mission_filtered = document_knowledge.search_documents({"query": query, "mission_id": "demo-1"}, db=session)
    assert mission_filtered
    assert all(item["metadata"].get("mission_id") == "demo-1" for item in mission_filtered)

    vehicle_filtered = document_knowledge.search_documents({"query": query, "vehicle_id": "demosat-1"}, db=session)
    assert vehicle_filtered
    assert all(item["metadata"].get("vehicle_id") == "demosat-1" for item in vehicle_filtered)

    subsystem_filtered = document_knowledge.search_documents({"query": "battery voltage", "subsystem_id": "eps"}, db=session)
    assert subsystem_filtered
    assert all(item["metadata"].get("subsystem_id") == "eps" for item in subsystem_filtered)
    assert all("mission-notes.md" != item["metadata"].get("filename") for item in subsystem_filtered)

    no_match = document_knowledge.search_documents(
        {"query": "reaction wheel thermal bearing lubricant"},
        db=session,
    )
    assert no_match == []

    clamped = document_knowledge.search_documents({"query": query, "limit": 999}, db=session)
    assert len(clamped) <= 8

    with pytest.raises(HTTPException) as exc_info:
        document_knowledge.search_documents({"query": "   "}, db=session)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "query is required"


@pytest.mark.anyio
async def test_document_reingest_queues_job_and_replaces_chunks(monkeypatch) -> None:
    monkeypatch.setattr(document_knowledge, "get_embedding_provider", lambda: _Provider())
    monkeypatch.setattr(document_ingestion, "get_embedding_provider", lambda: _Provider())
    monkeypatch.setattr(document_ingestion_worker, "get_db_context", lambda: _session_context(session))
    session = _SessionDouble()

    await _upload_document(session, filename="telemetry.md", content="old battery voltage note")
    document = session.documents[0]
    document_ingestion.ingest_document_now(db=session, document=document)
    original_hashes = {chunk.content_hash for chunk in session.document_chunks}
    session.document_ingestion_jobs[0].status = "completed"
    document.raw_content = "new reaction wheel thermal note"

    result = document_knowledge.reingest_document(str(document.id), db=session)

    assert result["ingestion_status"] == "pending"
    queued_jobs = [job for job in session.document_ingestion_jobs if job.status == "queued"]
    assert len(queued_jobs) == 1

    document_ingestion_worker.process_one_job()

    assert document.ingestion_status == "ready"
    assert queued_jobs[0].status == "completed"
    assert session.document_chunks
    assert {chunk.content_hash for chunk in session.document_chunks} != original_hashes
    assert all("new reaction wheel" in chunk.content for chunk in session.document_chunks)


@pytest.mark.anyio
async def test_document_worker_failure_marks_document_failed(monkeypatch) -> None:
    class _FailingProvider:
        def embed(self, _text: str) -> list[float]:
            raise RuntimeError("embedding unavailable")

    monkeypatch.setattr(document_ingestion, "get_embedding_provider", lambda: _FailingProvider())
    monkeypatch.setattr(document_ingestion_worker, "get_db_context", lambda: _session_context(session))
    session = _SessionDouble()

    await _upload_document(session, filename="telemetry.md", content="battery voltage note")

    document_ingestion_worker.process_one_job()

    assert session.documents[0].ingestion_status == "failed"
    assert "embedding unavailable" in (session.documents[0].ingestion_error or "")
    assert session.document_ingestion_jobs[0].status == "failed"
    assert "embedding unavailable" in (session.document_ingestion_jobs[0].error or "")


@pytest.mark.anyio
async def test_delete_document_removes_associated_chunks_and_jobs(monkeypatch) -> None:
    monkeypatch.setattr(document_ingestion, "get_embedding_provider", lambda: _Provider())
    session = _SessionDouble()

    await _upload_document(session, filename="telemetry.md", content="battery voltage note")
    document = session.documents[0]
    document_ingestion.ingest_document_now(db=session, document=document)

    result = document_knowledge.delete_document(str(document.id), db=session)

    assert result == {"deleted": True, "document_id": str(document.id)}
    assert session.documents == []
    assert session.document_chunks == []
    assert session.document_ingestion_jobs == []


@pytest.mark.anyio
async def test_phase3_fixture_code_indexing_emits_started_and_completed_events(monkeypatch) -> None:
    monkeypatch.setattr("app.intelligence.embedding.get_embedding_provider", lambda: _Provider())
    session = _SessionDouble()
    fixture_root = FIXTURES_ROOT / "phase3_code" / "phase3-test-fixture-service"
    fixture_file = fixture_root / "app" / "main.py"
    service_root = "project/space-ops-platform/backend/services/phase3-test-fixture-service"

    async def fake_commit(_root: str, _branch: str) -> str:
        return "abc1234"

    monkeypatch.setattr(code_intelligence, "get_current_commit_for_root", fake_commit)

    trees: dict[str, dict] = {
        service_root: {
            "commit_sha": "abc1234",
            "data": {
                "entries": [
                    {"path": f"{service_root}/app", "name": "app", "is_dir": True},
                ]
            },
        },
        f"{service_root}/app": {
            "commit_sha": "abc1234",
            "data": {
                "entries": [
                    {
                        "path": f"{service_root}/app/main.py",
                        "name": "main.py",
                        "is_dir": False,
                    },
                ]
            },
        },
    }
    file_text = fixture_file.read_text(encoding="utf-8")

    async def fake_cp_json(path: str, params: dict | None = None):
        if path != "code/tree":
            raise AssertionError(path)
        req = params.get("path") if params else None
        assert req in trees, req
        return trees[req]

    async def fake_cp_file(branch: str, path: str):
        assert path == f"{service_root}/app/main.py"
        return {"commit_sha": "abc1234", "data": {"content": file_text}}

    result = await code_intelligence.index_repository(
        {
            "root": service_root,
            "branch": "main",
            "conversation_id": "11111111-1111-1111-1111-111111111111",
            "agent_run_id": "22222222-2222-2222-2222-222222222222",
            "request_id": "33333333-3333-3333-3333-333333333333",
        },
        db=session,
    )

    assert result["index_status"] == "queued"
    assert result.get("job_id")
    assert [event.event_type for event in session.events] == ["code.index_started"]

    monkeypatch.setattr(indexing, "cp_get_json", fake_cp_json)
    monkeypatch.setattr(indexing, "cp_get_file_payload", fake_cp_file)

    repo = session.code_repositories[0]
    index_result = await indexing.index_repository_now(
        repository_id=repo.id,
        root=service_root,
        branch="main",
        target_commit_sha="abc1234",
        db=session,
    )
    assert index_result.file_count == 1
    assert index_result.chunk_count >= 1
    repo.index_status = "ready"
    repo.indexed_commit_sha = "abc1234"
    repo.current_commit_sha = "abc1234"

    search_results = code_intelligence.search_code({"query": "metadata endpoint", "branch": "main", "limit": 2}, db=session)
    assert search_results
    assert search_results[0]["file_path"].endswith("app/main.py")
    assert "metadata" in search_results[0]["content"]


@pytest.mark.anyio
async def test_phase3_source_lookup_surfaces_control_plane_safe_path_rejection(monkeypatch) -> None:
    async def fake_cp_get(path: str, params: dict | None = None):
        raise HTTPException(status_code=400, detail="path traversal")

    monkeypatch.setattr(code_intelligence, "_cp_get", fake_cp_get)

    with pytest.raises(HTTPException) as exc_info:
        await code_intelligence.read_source_file(branch="main", path="../secrets.txt")

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "path traversal"
