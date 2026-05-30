from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import inspect
import sys
import types
import uuid

import pytest
from fastapi import HTTPException

if "sentence_transformers" not in sys.modules:
    _st = types.ModuleType("sentence_transformers")

    class _SentenceTransformer:  # noqa: N801
        def __init__(self, *_args, **_kwargs):
            pass

        def encode(self, *_args, **_kwargs):
            return [0.0]

    _st.SentenceTransformer = _SentenceTransformer
    sys.modules["sentence_transformers"] = _st

from app.intelligence import indexing
from app.intelligence.chunking import chunk_code_with_metadata
from app.intelligence.managed_code_paths import canonicalize_managed_code_path
from app.models.intelligence import CodeChunk, CodeIndexJob, CodeRepository
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
        self._filter_criteria: tuple = ()
        self._order_start_line = False
        self._order_jobs_by_requested_at = False

    def filter(self, *args, **_kwargs):
        self._filter_criteria = self._filter_criteria + args
        return self

    def order_by(self, *args, **_kwargs):
        if any("start_line" in str(a) for a in args):
            self._order_start_line = True
        if any("requested_at" in str(a) for a in args):
            self._order_jobs_by_requested_at = True
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
        if not rows:
            return None
        if self._model is CodeIndexJob and self._order_jobs_by_requested_at:
            rows = sorted(rows, key=lambda j: j.requested_at or datetime.min.replace(tzinfo=timezone.utc))
        return rows[0]

    def all(self):
        if self._model is CodeRepository:
            rows = list(self._session.code_repositories)
            if self._filter_criteria:
                rows = [r for r in rows if self._obj_matches_all_filters(r)]
            return rows
        if self._model is CodeChunk:
            rows = list(self._session.code_chunks)
            if self._filter_criteria:
                rows = [c for c in rows if self._chunk_matches_filters(c)]
            if self._order_start_line:
                rows = sorted(rows, key=lambda c: (c.start_line or 0, c.end_line or 0))
            return rows
        if self._model is CodeIndexJob:
            rows = list(self._session.code_index_jobs)
            if self._filter_criteria:
                rows = [j for j in rows if self._obj_matches_all_filters(j)]
            return rows
        return []

    @staticmethod
    def _expand_filter(c):
        if c is None:
            return []
        if getattr(c, "left", None) is not None:
            return [c]
        if hasattr(c, "clauses"):
            parts: list = []
            for sub in c.clauses:
                parts.extend(_ModelQuery._expand_filter(sub))
            return parts
        return [c]

    def _obj_matches_all_filters(self, obj) -> bool:
        for fc in self._filter_criteria:
            for leaf in self._expand_filter(fc):
                if not self._leaf_matches_object(obj, leaf):
                    return False
        return True

    def _leaf_matches_object(self, obj, leaf) -> bool:
        from sqlalchemy.sql import operators

        key, val = self._clause_field_value(leaf)
        op = getattr(leaf, "operator", None)
        if key is None:
            return True
        cur = getattr(obj, key, None)
        if op is operators.in_op:
            coll = val
            if hasattr(leaf, "right") and coll is None:
                ev = getattr(leaf.right, "effective_value", None)
                if ev is not None:
                    coll = ev
            if coll is None:
                return False
            return cur in list(coll)
        return cur == val

    @staticmethod
    def _clause_field_value(clause) -> tuple[str | None, object | None]:
        left = getattr(clause, "left", None)
        right = getattr(clause, "right", None)
        if left is None or right is None:
            return None, None
        key = getattr(left, "key", None)
        if key is None and hasattr(left, "property"):
            key = getattr(left.property, "key", None)
        val = getattr(right, "value", None)
        return key, val

    def _chunk_matches_filters(self, chunk: CodeChunk) -> bool:
        for clause in self._filter_criteria:
            for leaf in self._expand_filter(clause):
                key, val = self._clause_field_value(leaf)
                if not key:
                    continue
                if getattr(chunk, key, object()) != val:
                    return False
        return True

    def delete(self, **_kwargs):
        if self._model is CodeChunk and self._filter_criteria:
            self._session.code_chunks = [c for c in self._session.code_chunks if not self._chunk_matches_filters(c)]
            self._filter_criteria = ()
        elif self._model is CodeChunk:
            self._session.code_chunks.clear()
        return 0


class _SessionDouble:
    def __init__(self):
        self.code_repositories: list[CodeRepository] = []
        self.code_chunks: list[CodeChunk] = []
        self.code_index_jobs: list[CodeIndexJob] = []

    def add(self, obj):
        if getattr(obj, "id", None) is None and hasattr(obj, "id"):
            obj.id = uuid.uuid4()
        if isinstance(obj, CodeRepository):
            self.code_repositories.append(obj)
        if isinstance(obj, CodeChunk):
            self.code_chunks.append(obj)
        if isinstance(obj, CodeIndexJob):
            self.code_index_jobs.append(obj)

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


def test_chunk_code_with_metadata_includes_preamble_before_first_symbol() -> None:
    chunks = chunk_code_with_metadata("# Module doc\n\nclass Foo:\n    pass\n", language="py")
    assert chunks[0].metadata.get("chunk_strategy") == "preamble"
    assert "Module doc" in chunks[0].content
    assert any(c.symbol_name == "Foo" for c in chunks)


def test_search_scoring_prefers_phrase_path_and_symbol_matches() -> None:
    session = _SessionDouble()
    repo = CodeRepository(
        name="space-ops-apps",
        source_uri="/tmp/space-ops-apps",
        layer="layer2",
        default_branch="main",
        index_status="ready",
        indexed_commit_sha="abc",
        current_commit_sha="abc",
    )
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
    repo = CodeRepository(
        name="space-ops-platform",
        source_uri="/tmp/space-ops-platform",
        layer="layer2",
        default_branch="main",
        index_status="ready",
        indexed_commit_sha="abc",
        current_commit_sha="abc",
    )
    session.add(repo)
    session.add(_chunk(repo.id, "backend/services/source_registry.py", "Source registry service index and health"))
    session.add(_chunk(repo.id, "backend/services/telemetry_detail_view.py", "Telemetry detail table and view service"))

    source = code_intelligence.search_code({"query": "source registry", "branch": "main", "limit": 3}, db=session)
    telemetry = code_intelligence.search_code({"query": "telemetry detail", "branch": "main", "limit": 3}, db=session)

    assert source and "source_registry" in source[0]["file_path"]
    assert telemetry and "telemetry_detail" in telemetry[0]["file_path"]


@pytest.mark.anyio
async def test_index_repository_walks_nested_tree_control_plane_shape(monkeypatch) -> None:
    session = _SessionDouble()
    monkeypatch.setattr("app.intelligence.embedding.get_embedding_provider", lambda: _Provider())

    root = "project/space-ops-platform"
    nested_py = "class Nested:\n    pass\n"
    shallow_py = "x = 1\n"

    trees: dict[str, dict] = {
        root: {
            "commit_sha": "abc1234",
            "data": {
                "entries": [
                    {"path": f"{root}/backend", "name": "backend", "is_dir": True},
                    {"path": f"{root}/shallow.py", "name": "shallow.py", "is_dir": False},
                ]
            },
        },
        f"{root}/backend": {
            "commit_sha": "abc1234",
            "data": {
                "entries": [
                    {"path": f"{root}/backend/services", "name": "services", "is_dir": True},
                ]
            },
        },
        f"{root}/backend/services": {
            "commit_sha": "abc1234",
            "data": {
                "entries": [
                    {
                        "path": f"{root}/backend/services/source_registry.py",
                        "name": "source_registry.py",
                        "is_dir": False,
                    },
                ]
            },
        },
    }
    files: dict[str, str] = {
        f"{root}/shallow.py": shallow_py,
        f"{root}/backend/services/source_registry.py": nested_py,
    }

    async def fake_cp_json(path: str, params: dict | None = None):
        if path == "code/tree":
            req_path = params.get("path") if params else None
            assert req_path in trees, req_path
            return trees[req_path]
        raise AssertionError((path, params))

    async def fake_cp_file(branch: str, path: str):
        assert path in files, path
        return {"commit_sha": "abc1234", "data": {"content": files[path]}}

    monkeypatch.setattr(indexing, "cp_get_json", fake_cp_json)
    monkeypatch.setattr(indexing, "cp_get_file_payload", fake_cp_file)

    repo = CodeRepository(
        name=root.split("/")[-1],
        source_uri=root,
        layer="layer2",
        default_branch="main",
    )
    session.add(repo)
    session.flush()

    result = await indexing.index_repository_now(
        repository_id=repo.id,
        root=root,
        branch="main",
        target_commit_sha="abc1234",
        db=session,
    )
    assert result.file_count == 2
    assert result.chunk_count >= 2
    indexed_paths = {c.file_path for c in session.code_chunks}
    assert f"{root}/backend/services/source_registry.py" in indexed_paths
    assert f"{root}/shallow.py" in indexed_paths
    assert all(c.start_line is not None and c.end_line is not None for c in session.code_chunks)
    assert all(c.start_line <= c.end_line for c in session.code_chunks)

    async def fake_cp_get_tool(p: str, params: dict | None = None):
        assert p == "code/file"
        assert params and params["path"] == f"{root}/backend/services/source_registry.py"
        return {"commit_sha": "abc1234", "data": {"content": nested_py}}

    monkeypatch.setattr(tool_execution, "_cp_get", fake_cp_get_tool)
    tool_result = await tool_execution._execute_mapped_tool(
        "read_source_file",
        {"branch": "main", "path": f"{root}/backend/services/source_registry.py"},
        db=session,
    )
    assert tool_result["data"]["content"].startswith("class Nested")


def test_related_context_returns_same_file_chunks_for_canonical_path() -> None:
    session = _SessionDouble()
    repo = CodeRepository(
        name="space-ops-platform",
        source_uri="project/space-ops-platform",
        layer="layer2",
        default_branch="main",
        index_status="ready",
        indexed_commit_sha="abc",
        current_commit_sha="abc",
    )
    session.add(repo)
    fp = "project/space-ops-platform/backend/z.py"
    session.add(_chunk(repo.id, fp, "a = 1\n", start_line=1, end_line=1, symbol_name=None, symbol_type=None))
    session.add(_chunk(repo.id, fp, "class Z:\n    pass\n", start_line=3, end_line=4, symbol_name="Z", symbol_type="class"))

    out = code_intelligence.related_context({"file_path": fp, "branch": "main"}, db=session)
    assert len(out) == 2
    assert {row["file_path"] for row in out} == {fp}
    assert all(row["start_line"] is not None for row in out)


def test_get_code_index_status_tool_is_registered_and_schema_backed() -> None:
    from app.routes.handlers import tool_registry

    assert "get_code_index_status" in tool_registry.SUPPORTED_TOOL_NAMES

    schema = tool_registry.TOOL_INPUT_SCHEMAS["get_code_index_status"]
    assert schema["type"] == "object"
    assert set(schema["properties"]) == {"repository", "root", "branch"}
    assert schema["properties"]["repository"]["maxLength"] == 256
    assert schema["properties"]["root"]["maxLength"] == 512
    assert schema["properties"]["branch"]["maxLength"] == 256
    assert schema["additionalProperties"] is False
    assert "anyOf" not in schema

    reconcile_source = inspect.getsource(tool_registry.reconcile_tool_definitions)
    assert "('get_code_index_status', 'Inspect managed repository code index lifecycle readiness before or after indexed search.', 'code_intelligence', 'layer2', 'read_only')" in reconcile_source
    assert (
        "'get_code_index_status': ('code-intelligence-service', 'GET /intelligence/code/repositories/status')"
        in reconcile_source
    )


@pytest.mark.anyio
async def test_get_code_index_status_tool_rejects_missing_target() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await tool_execution._execute_mapped_tool(
            "get_code_index_status",
            {},
            db=_SessionDouble(),
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == {
        "error_code": "missing_code_index_target",
        "message": "repository or root is required",
    }


@pytest.mark.anyio
async def test_get_code_index_status_tool_calls_status_endpoint_and_normalizes_queued(monkeypatch) -> None:
    called: dict = {}

    async def fake_runtime_get(slug: str, path: str, params: dict | None = None):
        called["slug"] = slug
        called["path"] = path
        called["params"] = params or {}
        return {
            "repository": "space-ops-platform",
            "root": "project/space-ops-platform",
            "branch": "main",
            "index_status": "queued",
            "indexed_commit_sha": None,
            "current_commit_sha": "abc1234",
        }

    monkeypatch.setattr(tool_execution, "_runtime_get", fake_runtime_get)

    out = await tool_execution._execute_mapped_tool(
        "get_code_index_status",
        {"repository": "space-ops-platform"},
        db=_SessionDouble(),
    )

    assert called == {
        "slug": "code-intelligence-service",
        "path": "repositories/status",
        "params": {"root": "project/space-ops-platform", "branch": "main"},
    }
    repo = out["repositories"][0]
    assert repo["raw_index_status"] == "queued"
    assert repo["index_status"] == "indexing"
    assert repo["temporary"] is True
    assert repo["retry_after_seconds"] == 20


@pytest.mark.anyio
async def test_get_code_index_status_tool_marks_ready_mismatched_commit_as_stale(monkeypatch) -> None:
    async def fake_runtime_get(slug: str, path: str, params: dict | None = None):
        assert slug == "code-intelligence-service"
        assert path == "repositories/status"
        assert params == {"root": "project/space-ops-platform", "branch": "main"}
        return {
            "repository": "space-ops-platform",
            "root": "project/space-ops-platform",
            "branch": "main",
            "index_status": "ready",
            "indexed_commit_sha": "old1234",
            "current_commit_sha": "new5678",
        }

    monkeypatch.setattr(tool_execution, "_runtime_get", fake_runtime_get)

    out = await tool_execution._execute_mapped_tool(
        "get_code_index_status",
        {"repository": "space-ops-platform"},
        db=_SessionDouble(),
    )

    repo = out["repositories"][0]
    assert repo["raw_index_status"] == "ready"
    assert repo["index_status"] == "stale"
    assert repo["indexed_commit_sha"] == "old1234"
    assert repo["current_commit_sha"] == "new5678"
    assert repo["temporary"] is True
    assert repo["retry_after_seconds"] == 30


@pytest.mark.anyio
async def test_get_code_index_status_tool_returns_not_indexed_on_missing_repository(monkeypatch) -> None:
    async def fake_runtime_get(_slug: str, _path: str, params: dict | None = None):
        raise HTTPException(status_code=404, detail="repository not found")

    monkeypatch.setattr(tool_execution, "_runtime_get", fake_runtime_get)

    out = await tool_execution._execute_mapped_tool(
        "get_code_index_status",
        {"root": "project/space-ops-apps", "branch": "main"},
        db=_SessionDouble(),
    )

    repo = out["repositories"][0]
    assert repo["root"] == "project/space-ops-apps"
    assert repo["branch"] == "main"
    assert repo["index_status"] == "not_indexed"
    assert repo["temporary"] is True
    assert repo["retry_after_seconds"] == 30


@pytest.mark.anyio
async def test_get_related_code_context_tool_keeps_canonical_path(monkeypatch) -> None:
    posted: dict = {}

    async def fake_runtime_post(slug: str, path: str, json_body: dict | None = None):
        posted["slug"] = slug
        posted["path"] = path
        posted["json"] = json_body or {}
        return []

    monkeypatch.setattr(tool_execution, "_runtime_post", fake_runtime_post)
    session = _SessionDouble()

    canonical = "project/space-ops-platform/backend/services/x.py"
    await tool_execution._execute_mapped_tool(
        "get_related_code_context",
        {"repository": "space-ops-platform", "path": canonical, "branch": "main"},
        db=session,
    )
    assert posted["slug"] == "code-intelligence-service"
    assert posted["path"] == "related-context"
    assert posted["json"]["file_path"] == canonical


@pytest.mark.anyio
async def test_get_related_code_context_tool_canonicalizes_relative_path(monkeypatch) -> None:
    posted: dict = {}

    async def fake_runtime_post(slug: str, path: str, json_body: dict | None = None):
        posted["json"] = json_body or {}
        return []

    monkeypatch.setattr(tool_execution, "_runtime_post", fake_runtime_post)
    session = _SessionDouble()
    await tool_execution._execute_mapped_tool(
        "get_related_code_context",
        {"repository": "space-ops-platform", "path": "backend/app/foo.py", "branch": "main"},
        db=session,
    )
    assert posted["json"]["file_path"] == "project/space-ops-platform/backend/app/foo.py"


def test_canonicalize_managed_code_path_passes_through_when_already_canonical() -> None:
    p = "project/space-ops-platform/backend/app/foo.py"
    assert canonicalize_managed_code_path("space-ops-platform", p) == p


def test_canonicalize_managed_code_path_relative_platform() -> None:
    assert (
        canonicalize_managed_code_path("space-ops-platform", "backend/app/routes/handlers/code_intelligence.py")
        == "project/space-ops-platform/backend/app/routes/handlers/code_intelligence.py"
    )


def test_canonicalize_managed_code_path_relative_apps() -> None:
    assert (
        canonicalize_managed_code_path("space-ops-apps", "mission-control-ui/src/foo.tsx")
        == "project/space-ops-apps/mission-control-ui/src/foo.tsx"
    )


def test_canonicalize_managed_code_path_manifests() -> None:
    assert (
        canonicalize_managed_code_path("manifests/units", "code-intelligence-service.yaml")
        == "manifests/units/code-intelligence-service.yaml"
    )


def test_canonicalize_managed_code_path_no_duplicate_prefix() -> None:
    p = "project/space-ops-platform/backend/app/foo.py"
    assert canonicalize_managed_code_path("project/space-ops-platform", p) == p


def test_canonicalize_unknown_repository_still_allows_canonical_path() -> None:
    p = "project/space-ops-platform/lib/x.py"
    assert canonicalize_managed_code_path("unknown-repo", p) == p


def test_canonicalize_unknown_repository_non_canonical_raises() -> None:
    with pytest.raises(ValueError, match="unknown repository"):
        canonicalize_managed_code_path("my-unit", "src/handler.py")


def test_canonicalize_path_traversal_raises() -> None:
    with pytest.raises(ValueError, match="path traversal"):
        canonicalize_managed_code_path("space-ops-platform", "../secret.txt")


def test_canonicalize_absolute_path_raises() -> None:
    with pytest.raises(ValueError, match="absolute"):
        canonicalize_managed_code_path("space-ops-platform", "/etc/passwd")


@pytest.mark.anyio
async def test_get_related_code_context_rejects_traversal_with_http_exception() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await tool_execution._execute_mapped_tool(
            "get_related_code_context",
            {"repository": "space-ops-platform", "path": "../secret.txt", "branch": "main"},
            db=_SessionDouble(),
        )
    assert exc_info.value.status_code == 400
    assert isinstance(exc_info.value.detail, dict)
    assert exc_info.value.detail.get("error_code") == "invalid_managed_code_path"
