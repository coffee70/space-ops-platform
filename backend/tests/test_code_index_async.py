"""Async indexing readiness and ranking guardrails (no real control plane)."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from app.models.intelligence import CodeRepository
from app.routes.handlers import code_intelligence

from tests.test_code_intelligence_hardening import _SessionDouble, _chunk


def _ready_repo(**kwargs) -> CodeRepository:
    defaults = dict(
        name="space-ops-platform",
        source_uri="project/space-ops-platform",
        layer="layer2",
        default_branch="main",
        index_status="ready",
        indexed_commit_sha="sha1",
        current_commit_sha="sha1",
    )
    defaults.update(kwargs)
    return CodeRepository(**defaults)


def test_search_rejects_not_ready_repository_with_503() -> None:
    session = _SessionDouble()
    repo = _ready_repo(
        name="space-ops-apps",
        source_uri="project/space-ops-apps",
        index_status="indexing",
        indexed_commit_sha=None,
        current_commit_sha="sha1",
    )
    session.add(repo)
    with pytest.raises(HTTPException) as exc:
        code_intelligence.search_code({"query": "foo", "repository": "space-ops-apps", "branch": "main"}, db=session)
    assert exc.value.status_code == 503
    assert isinstance(exc.value.detail, dict)
    assert exc.value.detail.get("error_code") == "code_index_not_ready"


def test_search_global_rejects_when_no_ready_indexes() -> None:
    session = _SessionDouble()
    session.add(
        _ready_repo(
            index_status="queued",
            indexed_commit_sha=None,
            current_commit_sha="x",
        )
    )
    with pytest.raises(HTTPException) as exc:
        code_intelligence.search_code({"query": "foo", "branch": "main"}, db=session)
    assert exc.value.status_code == 503
    assert exc.value.detail.get("error_code") == "code_index_not_ready"


def test_related_context_rejects_when_index_not_ready() -> None:
    session = _SessionDouble()
    session.add(
        _ready_repo(
            index_status="queued",
            indexed_commit_sha=None,
            current_commit_sha="x",
        )
    )
    with pytest.raises(HTTPException) as exc:
        code_intelligence.related_context(
            {"file_path": "project/space-ops-platform/backend/x.py", "branch": "main"},
            db=session,
        )
    assert exc.value.status_code == 503


def test_implementation_file_scores_above_test_path_for_same_query() -> None:
    session = _SessionDouble()
    repo = _ready_repo()
    session.add(repo)
    rid = repo.id
    session.add(
        _chunk(
            rid,
            "backend/app/telemetry_service.py",
            "class TelemetryService:\n    def recent(self):\n        return []\n",
        )
    )
    session.add(
        _chunk(
            rid,
            "backend/tests/test_telemetry_service.py",
            "def test_recent():\n    pass\n",
        )
    )
    results = code_intelligence.search_code({"query": "telemetry service recent", "repository": "space-ops-platform", "branch": "main"}, db=session)
    assert results
    assert results[0]["file_path"].endswith("telemetry_service.py")
