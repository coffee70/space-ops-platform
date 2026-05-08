from __future__ import annotations

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
from app.intelligence import indexing
from app.models.intelligence import AgentEvent, CodeChunk, CodeIndexJob, CodeRepository, Document, DocumentChunk
from app.routes.handlers import code_intelligence, document_knowledge

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

    def filter(self, *_args, **_kwargs):
        return self

    def order_by(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
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
        if self._model is CodeChunk:
            self._session.code_chunks.clear()
        return 0

    def all(self):
        if self._model is Document:
            return list(self._session.documents)
        if self._model is DocumentChunk:
            return list(self._session.document_chunks)
        if self._model is CodeRepository:
            return list(self._session.code_repositories)
        if self._model is CodeChunk:
            return list(self._session.code_chunks)
        if self._model is CodeIndexJob:
            return list(self._session.code_index_jobs)
        return []


class _SessionDouble:
    def __init__(self):
        self.documents: list[Document] = []
        self.document_chunks: list[DocumentChunk] = []
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

    assert result["ingestion_status"] == "ready"
    assert len(session.documents) == 1
    assert len(session.document_chunks) == len(chunk_text(content, max_chars=1200, overlap=120))
    assert [event.event_type for event in session.events] == [
        "document.uploaded",
        "document.ingestion_started",
        "document.ingestion_completed",
    ]

    search_results = document_knowledge.search_documents({"query": "battery efficiency", "limit": 2}, db=session)
    assert search_results
    assert any("battery efficiency" in item["content"].lower() for item in search_results)
    assert "ranking_signals" in search_results[0]["metadata"]


@pytest.mark.anyio
async def test_document_search_hardening_ranking_filters_and_limits(monkeypatch) -> None:
    monkeypatch.setattr(document_knowledge, "get_embedding_provider", lambda: _Provider())
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
    await _upload_document(
        session,
        filename="mission-notes.md",
        content=mission_notes,
        mission_id="demo-2",
        vehicle_id="demosat-2",
        subsystem_id="adcs",
        tags="ops,notes",
    )

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

    clamped = document_knowledge.search_documents({"query": query, "limit": 999}, db=session)
    assert len(clamped) <= 8

    with pytest.raises(HTTPException) as exc_info:
        document_knowledge.search_documents({"query": "   "}, db=session)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "query is required"


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
