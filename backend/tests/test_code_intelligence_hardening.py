from __future__ import annotations

from dataclasses import dataclass
import sys
import types
import uuid

import pytest

if "sentence_transformers" not in sys.modules:
    _st = types.ModuleType("sentence_transformers")

    class _SentenceTransformer:  # noqa: N801
        def __init__(self, *_args, **_kwargs):
            pass

        def encode(self, *_args, **_kwargs):
            return [0.0]

    _st.SentenceTransformer = _SentenceTransformer
    sys.modules["sentence_transformers"] = _st

from app.intelligence.chunking import chunk_code_with_metadata
from app.models.intelligence import CodeChunk, CodeRepository
from app.routes.handlers import code_intelligence, tool_execution


class _Provider:
    def embed(self, text: str) -> list[float]:
        return [float(len(text))]


@dataclass
class _CodePairQuery:
    rows: list[tuple[CodeChunk, CodeRepository]]

    def join(self, *_args, **_kwargs):
        return self

    def filter(self, *_args, **_kwargs):
        return self

    def all(self):
        return list(self.rows)


class _ModelQuery:
    def __init__(self, session: "_SessionDouble", model):
        self._session = session
        self._model = model

    def filter(self, *_args, **_kwargs):
        return self

    def order_by(self, *_args, **_kwargs):
        return self

    def one_or_none(self):
        rows = self.all()
        return rows[0] if rows else None

    def all(self):
        if self._model is CodeRepository:
            return list(self._session.code_repositories)
        if self._model is CodeChunk:
            return list(self._session.code_chunks)
        return []

    def delete(self):
        if self._model is CodeChunk:
            self._session.code_chunks.clear()
        return 0


class _SessionDouble:
    def __init__(self):
        self.code_repositories: list[CodeRepository] = []
        self.code_chunks: list[CodeChunk] = []

    def add(self, obj):
        if getattr(obj, "id", None) is None and hasattr(obj, "id"):
            obj.id = uuid.uuid4()
        if isinstance(obj, CodeRepository):
            self.code_repositories.append(obj)
        if isinstance(obj, CodeChunk):
            self.code_chunks.append(obj)

    def flush(self):
        return None

    def query(self, *models):
        if len(models) == 2 and models == (CodeChunk, CodeRepository):
            rows: list[tuple[CodeChunk, CodeRepository]] = []
            for chunk in self.code_chunks:
                repo = next((item for item in self.code_repositories if item.id == chunk.repository_id), None)
                if repo:
                    rows.append((chunk, repo))
            return _CodePairQuery(rows)
        return _ModelQuery(self, models[0])


def _chunk(
    repo_id: uuid.UUID,
    file_path: str,
    content: str,
    *,
    symbol_name: str | None = None,
    symbol_type: str | None = None,
    start_line: int = 1,
    end_line: int = 20,
) -> CodeChunk:
    return CodeChunk(
        repository_id=repo_id,
        branch="main",
        commit_sha="abc1234",
        file_path=file_path,
        language=file_path.rsplit(".", 1)[-1],
        symbol_name=symbol_name,
        symbol_type=symbol_type,
        start_line=start_line,
        end_line=end_line,
        content=content,
        content_hash=f"hash-{file_path}",
        embedding=None,
        embedding_model="test-model",
        metadata_json={"chunk_index": 0, "chunk_strategy": "test"},
    )


def test_chunk_code_with_metadata_detects_python_js_and_markdown_symbols() -> None:
    py_chunks = chunk_code_with_metadata(
        "class TelemetryService:\n    pass\n\nasync def get_value():\n    return 1\n",
        language="py",
    )
    assert py_chunks[0].symbol_name == "TelemetryService"
    assert py_chunks[0].symbol_type == "class"
    assert any(chunk.symbol_name == "get_value" and chunk.symbol_type == "function" for chunk in py_chunks)
    assert all(chunk.start_line is not None and chunk.end_line is not None for chunk in py_chunks)

    ts_chunks = chunk_code_with_metadata("export const ActionTimeline = () => <div />\n", language="tsx")
    assert ts_chunks[0].symbol_name == "ActionTimeline"
    assert ts_chunks[0].symbol_type == "component_candidate"

    md_chunks = chunk_code_with_metadata("# Overview\nBody\n## Details\nValue\n", language="md")
    assert md_chunks[0].symbol_type == "section"
    assert md_chunks[0].symbol_name == "Overview"


def test_chunk_code_with_metadata_fallback_windows_and_no_empty_chunks() -> None:
    long_text = "\n".join([f"line-{idx}" for idx in range(1, 190)])
    chunks = chunk_code_with_metadata(long_text, language="txt", fallback_window_lines=80, overlap_lines=10)
    assert len(chunks) >= 3
    assert chunks[0].start_line == 1
    assert chunks[1].start_line == 71
    assert all(chunk.start_line <= chunk.end_line for chunk in chunks)
    assert all(chunk.content.strip() for chunk in chunks)


def test_search_scoring_prefers_phrase_path_and_symbol_matches() -> None:
    session = _SessionDouble()
    repo = CodeRepository(name="space-ops-apps", source_uri="/tmp/space-ops-apps", layer="layer2", default_branch="main")
    session.add(repo)

    session.add(
        _chunk(
            repo.id,
            "mission-control-ui/src/applications/ai-engineer/components/ActionTimeline.tsx",
            "Action timeline panel for AI Engineer events",
            symbol_name="ActionTimeline",
            symbol_type="component_candidate",
        )
    )
    session.add(_chunk(repo.id, "backend/misc/random.txt", "unrelated logging content"))

    results = code_intelligence.search_code({"query": "AI Engineer action timeline", "branch": "main", "limit": 5}, db=session)
    assert results
    assert results[0]["file_path"].endswith("ActionTimeline.tsx")
    assert results[0]["score"] > 0
    assert "ranking_signals" in results[0]["metadata"]


def test_search_smoke_queries_return_plausible_files() -> None:
    session = _SessionDouble()
    repo = CodeRepository(name="space-ops-platform", source_uri="/tmp/space-ops-platform", layer="layer2", default_branch="main")
    session.add(repo)
    session.add(_chunk(repo.id, "backend/services/source_registry.py", "Source registry service index and health"))
    session.add(_chunk(repo.id, "backend/services/telemetry_detail_view.py", "Telemetry detail table and view service"))

    source = code_intelligence.search_code({"query": "source registry", "branch": "main", "limit": 3}, db=session)
    telemetry = code_intelligence.search_code({"query": "telemetry detail", "branch": "main", "limit": 3}, db=session)

    assert source and "source_registry" in source[0]["file_path"]
    assert telemetry and "telemetry_detail" in telemetry[0]["file_path"]


@pytest.mark.anyio
async def test_index_repository_persists_line_ranges_and_tool_path_reads_file(monkeypatch) -> None:
    session = _SessionDouble()
    monkeypatch.setattr(code_intelligence, "get_embedding_provider", lambda: _Provider())

    source = (
        "class SourceRegistry:\n"
        "    pass\n\n"
        "def telemetry_detail(channel):\n"
        "    return channel\n"
    )

    async def fake_cp_get(path: str, params: dict | None = None):
        if path == "code/tree":
            return {"commit_sha": "abc1234", "data": {"entries": [{"type": "file", "path": "backend/services/source_registry.py"}]}}
        if path == "code/file":
            return {"commit_sha": "abc1234", "data": {"content": source}}
        raise AssertionError(path)

    monkeypatch.setattr(code_intelligence, "_cp_get", fake_cp_get)

    await code_intelligence.index_repository({"root": "/tmp/space-ops-platform", "branch": "main"}, db=session)
    assert session.code_chunks
    assert all(chunk.start_line is not None and chunk.end_line is not None for chunk in session.code_chunks)
    assert all(chunk.start_line <= chunk.end_line for chunk in session.code_chunks)

    async def fake_cp_get_tool(path: str, params: dict | None = None):
        assert path == "code/file"
        assert params and params["path"] == "backend/services/source_registry.py"
        return {"commit_sha": "abc1234", "data": {"content": source}}

    monkeypatch.setattr(tool_execution, "_cp_get", fake_cp_get_tool)
    tool_result = await tool_execution._execute_mapped_tool(
        "read_source_file",
        {"branch": "main", "path": "backend/services/source_registry.py"},
        db=session,
    )
    assert tool_result["data"]["content"].startswith("class SourceRegistry")
