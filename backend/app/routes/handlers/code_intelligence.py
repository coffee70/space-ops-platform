from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
import subprocess
import uuid

import httpx
from fastapi import Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.config import get_settings
from app.database import get_db
from app.intelligence.events import emit_event
from app.intelligence.indexing import get_current_commit_for_root
from app.intelligence.managed_code_paths import longest_managed_code_root_for_path
from app.models.intelligence import CodeChunk, CodeIndexJob, CodeRepository


def _cp_url(path: str) -> str:
    base = get_settings().control_plane_url.rstrip("/")
    return f"{base}/{path.lstrip('/')}"


async def _cp_get(path: str, params: dict | None = None) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(_cp_url(path), params=params)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


def _repo_summary(repo: CodeRepository) -> dict:
    return {
        "id": str(repo.id),
        "name": repo.name,
        "source_uri": repo.source_uri,
        "layer": repo.layer,
        "default_branch": repo.default_branch,
        "created_at": repo.created_at,
        "updated_at": repo.updated_at,
    }


def _repository_is_searchable(repo: CodeRepository) -> bool:
    return (
        repo.index_status == "ready"
        and bool(repo.indexed_commit_sha)
        and bool(repo.current_commit_sha)
        and repo.indexed_commit_sha == repo.current_commit_sha
    )


_TOKEN_SPLIT_PATTERN = re.compile(r"[^a-z0-9]+")
_CAMEL_SPLIT_PATTERN = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_STOP_WORDS = {"the", "and", "for", "with", "from", "this", "that", "into", "code"}
_UI_HINTS = {"ui", "component", "page", "view", "timeline", "button", "table", "detail"}
_SERVICE_HINTS = {"api", "route", "handler", "service", "registry", "ingest", "query"}
_UI_EXTENSIONS = {"tsx", "ts", "jsx", "js"}
_SERVICE_EXTENSIONS = {"py"}


def _tokenize(value: str) -> list[str]:
    if not value:
        return []
    normalized = _CAMEL_SPLIT_PATTERN.sub(" ", value).lower()
    bits = _TOKEN_SPLIT_PATTERN.split(normalized)
    return [bit for bit in bits if len(bit) >= 2 and bit not in _STOP_WORDS]


def _is_test_or_spec_path(path: str) -> bool:
    p = (path or "").lower().replace("\\", "/")
    if "/test/" in p or "/tests/" in p or "__tests__" in p:
        return True
    base = p.rsplit("/", 1)[-1]
    return ".test." in base or ".spec." in base


def _score_code_chunk(query: str, chunk: CodeChunk) -> tuple[float, dict]:
    query_tokens = _tokenize(query)
    path_text = (chunk.file_path or "").lower()
    symbol_text = (chunk.symbol_name or "").lower()
    content_text = (chunk.content or "").lower()
    query_phrase = query.strip().lower()
    path_tokens = set(_tokenize(chunk.file_path or ""))
    symbol_tokens = set(_tokenize(chunk.symbol_name or ""))
    content_tokens = set(_tokenize(chunk.content or ""))

    path_hits = sorted(token for token in query_tokens if token in path_tokens)
    symbol_hits = sorted(token for token in query_tokens if token in symbol_tokens)
    content_hits = sorted(token for token in query_tokens if token in content_tokens)
    all_hits = set(path_hits) | set(symbol_hits) | set(content_hits)

    score = 0.0
    if query_phrase and query_phrase in content_text:
        score += 10.0
    if query_phrase and query_phrase in path_text:
        score += 8.0
    if query_phrase and query_phrase in symbol_text:
        score += 6.0
    score += float(3 * len(path_hits))
    score += float(2 * len(symbol_hits))
    score += float(len(content_hits))
    if query_tokens and all(token in all_hits for token in query_tokens):
        score += 2.0

    query_token_set = set(query_tokens)
    extension = chunk.file_path.rsplit(".", 1)[-1].lower() if "." in chunk.file_path else ""
    if query_token_set.intersection(_UI_HINTS) and extension in _UI_EXTENSIONS:
        score += 1.0
    if query_token_set.intersection(_SERVICE_HINTS) and extension in _SERVICE_EXTENSIONS:
        score += 1.0

    if _is_test_or_spec_path(chunk.file_path or ""):
        score -= 2.0

    debug = {
        "path_token_hits": path_hits,
        "symbol_token_hits": symbol_hits,
        "content_token_hits": content_hits,
        "exact_phrase_in_path": bool(query_phrase and query_phrase in path_text),
        "exact_phrase_in_symbol": bool(query_phrase and query_phrase in symbol_text),
        "exact_phrase_in_content": bool(query_phrase and query_phrase in content_text),
        "test_spec_path_penalty": _is_test_or_spec_path(chunk.file_path or ""),
    }
    return score, debug


def _is_logical_managed_source_uri(uri: str) -> bool:
    u = uri.strip().lstrip("/")
    if not u:
        return False
    return u.startswith("project/") or u.startswith("manifests/")


def _ripgrep_search_root_path(uri: str) -> Path | None:
    if _is_logical_managed_source_uri(uri):
        return None
    try:
        candidate = Path(uri).expanduser().resolve()
    except OSError:
        return None
    if not candidate.exists():
        return None
    for base in (Path("/workspace"), Path("/repos"), Path("/tmp")):
        try:
            candidate.relative_to(base.resolve())
            return candidate if candidate.is_dir() else None
        except ValueError:
            continue
    return None


def _run_ripgrep_search(repository_root: str, query: str, limit: int) -> list[dict]:
    search_root = _ripgrep_search_root_path(repository_root)
    if not query.strip() or search_root is None:
        return []
    try:
        completed = subprocess.run(
            [
                "rg",
                "--line-number",
                "--column",
                "--ignore-case",
                "--fixed-strings",
                "--glob",
                "!node_modules",
                "--glob",
                "!.next",
                "--glob",
                "!dist",
                "--glob",
                "!build",
                query,
                str(search_root),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    if completed.returncode not in (0, 1):
        return []
    matches: list[dict] = []
    for line in completed.stdout.splitlines():
        if len(matches) >= limit:
            break
        parts = line.split(":", 3)
        if len(parts) != 4:
            continue
        file_path, line_no, _column, matched_line = parts
        try:
            line_num = int(line_no)
        except ValueError:
            continue
        matches.append({"file_path": file_path, "line": line_num, "line_text": matched_line.strip()})
    return matches


def list_repositories(db: Session = Depends(get_db)):
    repos = db.query(CodeRepository).order_by(CodeRepository.created_at.desc()).all()
    return [_repo_summary(repo) for repo in repos]


async def index_repository(body: dict, db: Session = Depends(get_db)):
    root = body.get("root")
    branch = body.get("branch", "main")
    force = bool(body.get("force"))
    if not root:
        raise HTTPException(status_code=400, detail="root is required")

    try:
        current_sha = await get_current_commit_for_root(root, branch)
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail={"error_code": "control_plane_unavailable", "message": f"could not resolve current commit: {exc}"},
        ) from exc

    repository = db.query(CodeRepository).filter(CodeRepository.source_uri == root, CodeRepository.default_branch == branch).one_or_none()
    if not repository:
        repository = CodeRepository(
            name=root.split("/")[-1],
            source_uri=root,
            layer="layer2",
            default_branch=branch,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add(repository)
        db.flush()

    repository.current_commit_sha = current_sha or repository.current_commit_sha

    if (
        not force
        and repository.index_status == "ready"
        and repository.indexed_commit_sha
        and current_sha
        and repository.indexed_commit_sha == current_sha
    ):
        return {
            "repository_id": str(repository.id),
            "job_id": None,
            "root": root,
            "branch": branch,
            "target_commit_sha": current_sha,
            "index_status": "ready",
            "message": "index already ready for current commit",
        }

    existing_job = (
        db.query(CodeIndexJob)
        .filter(
            CodeIndexJob.repository_id == repository.id,
            CodeIndexJob.target_commit_sha == (current_sha or ""),
            CodeIndexJob.status.in_(("queued", "running")),
        )
        .one_or_none()
    )
    if existing_job:
        return {
            "repository_id": str(repository.id),
            "job_id": str(existing_job.id),
            "root": root,
            "branch": branch,
            "target_commit_sha": current_sha,
            "index_status": repository.index_status if repository.index_status in ("queued", "indexing") else "queued",
        }

    now = datetime.now(timezone.utc)
    job = CodeIndexJob(
        id=uuid.uuid4(),
        repository_id=repository.id,
        root=root,
        branch=branch,
        target_commit_sha=current_sha or "",
        status="queued",
        requested_at=now,
    )
    repository.index_status = "queued"
    repository.index_requested_at = now
    repository.last_error = None
    if repository.indexed_commit_sha and current_sha and repository.indexed_commit_sha != current_sha:
        repository.index_status = "queued"
    db.add(job)
    db.flush()

    if body.get("conversation_id") and body.get("agent_run_id") and body.get("request_id"):
        emit_event(
            db,
            event_type="code.index_started",
            payload={"repository": repository.name, "branch": branch, "commit_sha": current_sha or ""},
            conversation_id=body.get("conversation_id"),
            agent_run_id=body.get("agent_run_id"),
            request_id=body.get("request_id"),
            sequence=1,
            emitted_by="code-intelligence-service",
        )

    return {
        "repository_id": str(repository.id),
        "job_id": str(job.id),
        "root": root,
        "branch": branch,
        "target_commit_sha": current_sha,
        "index_status": "queued",
    }


def _status_payload(repo: CodeRepository, db: Session) -> dict:
    live_chunk_total = db.query(CodeChunk).filter(CodeChunk.repository_id == repo.id).count()
    return {
        **_repo_summary(repo),
        "chunk_count": live_chunk_total,
        "index_status": repo.index_status,
        "indexed_commit_sha": repo.indexed_commit_sha,
        "current_commit_sha": repo.current_commit_sha,
        "file_count": repo.file_count,
        "skipped_file_count": repo.skipped_file_count,
        "failed_file_count": repo.failed_file_count,
        "last_error": repo.last_error,
        "index_requested_at": repo.index_requested_at,
        "index_started_at": repo.index_started_at,
        "index_completed_at": repo.index_completed_at,
    }


def get_repository_status(repository_id: str, db: Session = Depends(get_db)):
    try:
        rid = uuid.UUID(repository_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid repository id") from exc
    repo = db.query(CodeRepository).filter(CodeRepository.id == rid).one_or_none()
    if not repo:
        raise HTTPException(status_code=404, detail="repository not found")
    return _status_payload(repo, db)


def get_repository_status_lookup(root: str = Query(...), branch: str = Query("main"), db: Session = Depends(get_db)):
    repo = db.query(CodeRepository).filter(CodeRepository.source_uri == root, CodeRepository.default_branch == branch).one_or_none()
    if not repo:
        raise HTTPException(status_code=404, detail="repository not found")
    return _status_payload(repo, db)


def search_code(body: dict, db: Session = Depends(get_db)):
    query = (body.get("query") or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required")
    limit = min(max(int(body.get("limit", 6)), 1), 8)
    branch = body.get("branch", "main")
    if body.get("repository"):
        repo = db.query(CodeRepository).filter(CodeRepository.name == body["repository"]).one_or_none()
        if not repo:
            raise HTTPException(status_code=404, detail="repository not found")
        if repo.default_branch != branch:
            pass
        if repo.index_status == "failed":
            raise HTTPException(
                status_code=503,
                detail={
                    "error_code": "code_index_failed",
                    "message": f"Code index failed for {repo.source_uri} on {branch}.",
                    "repository": repo.name,
                    "root": repo.source_uri,
                    "branch": branch,
                    "index_status": "failed",
                    "last_error": repo.last_error or "",
                },
            )
        if not _repository_is_searchable(repo):
            raise HTTPException(
                status_code=503,
                detail={
                    "error_code": "code_index_not_ready",
                    "message": f"Code index is not ready for {repo.source_uri} on {branch}.",
                    "repository": repo.name,
                    "root": repo.source_uri,
                    "branch": branch,
                    "index_status": repo.index_status,
                },
            )
        target_repos = {repo.id}
    else:
        candidates = db.query(CodeRepository).filter(CodeRepository.default_branch == branch).all()
        if not any(_repository_is_searchable(r) for r in candidates):
            raise HTTPException(
                status_code=503,
                detail={
                    "error_code": "code_index_not_ready",
                    "message": f"No ready code index for branch {branch}.",
                    "repository": None,
                    "root": None,
                    "branch": branch,
                    "index_status": "not_indexed",
                },
            )
        target_repos = {r.id for r in candidates if _repository_is_searchable(r)}

    rows = db.query(CodeChunk, CodeRepository).join(CodeRepository, CodeRepository.id == CodeChunk.repository_id).filter(CodeChunk.branch == branch).all()
    rg_boosts: dict[tuple[str, int], float] = {}
    if body.get("repository"):
        repo = db.query(CodeRepository).filter(CodeRepository.name == body["repository"]).one_or_none()
        if repo and not _is_logical_managed_source_uri(repo.source_uri):
            for match in _run_ripgrep_search(repo.source_uri, query, limit=max(limit * 4, 24)):
                rg_boosts[(match["file_path"], match["line"])] = 1.5

    scored: list[dict] = []
    for chunk, repository in rows:
        if repository.id not in target_repos:
            continue
        if body.get("repository") and repository.name != body["repository"]:
            continue
        score, ranking_signals = _score_code_chunk(query, chunk)
        if (chunk.file_path, chunk.start_line or -1) in rg_boosts:
            score += 1.5
            ranking_signals["ripgrep_match"] = True
        elif any(path == chunk.file_path for path, _line in rg_boosts):
            score += 0.5
            ranking_signals["ripgrep_match"] = "file_only"
        if score <= 0:
            continue
        scored.append(
            {
                "repository": repository.name,
                "branch": chunk.branch,
                "commit_sha": chunk.commit_sha,
                "file_path": chunk.file_path,
                "symbol_name": chunk.symbol_name,
                "symbol_type": chunk.symbol_type,
                "start_line": chunk.start_line,
                "end_line": chunk.end_line,
                "content": chunk.content[:1500],
                "score": float(score),
                "metadata": {
                    **(chunk.metadata_json or {}),
                    "ranking_signals": ranking_signals,
                },
            }
        )
    scored.sort(key=lambda item: item["score"], reverse=True)
    deduped: list[dict] = []
    seen_files: set[str] = set()
    overflow: list[dict] = []
    for item in scored:
        if item["file_path"] not in seen_files:
            deduped.append(item)
            seen_files.add(item["file_path"])
        else:
            overflow.append(item)
    deduped.extend(overflow)
    return deduped[:limit]


async def read_source_file(branch: str = Query("main"), path: str = Query(...)):
    if len(path) > 512:
        raise HTTPException(status_code=400, detail="path too long")
    payload = await _cp_get("code/file", params={"branch": branch, "path": path})
    content = payload.get("data", {}).get("content", "")
    if len(content) > 100_000:
        raise HTTPException(status_code=400, detail="file too large")
    return {"branch": branch, "path": path, "commit_sha": payload.get("commit_sha"), "content": content[:20000], "truncated": len(content) > 20000}


def related_context(body: dict, db: Session = Depends(get_db)):
    path = body.get("file_path")
    if not path:
        raise HTTPException(status_code=400, detail="file_path is required")
    branch = body.get("branch", "main")
    line = body.get("line")
    limit = min(max(int(body.get("limit", 6)), 1), 25)

    root_prefix = longest_managed_code_root_for_path(path)
    if not root_prefix:
        raise HTTPException(
            status_code=400,
            detail={"error_code": "invalid_managed_code_path", "message": "file_path is not under a managed code root"},
        )

    repo = db.query(CodeRepository).filter(CodeRepository.source_uri == root_prefix, CodeRepository.default_branch == branch).one_or_none()
    if not repo:
        short = root_prefix.split("/")[-1] if "/" in root_prefix else root_prefix
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "code_index_not_ready",
                "message": f"Code index is not ready for {root_prefix} on {branch}.",
                "repository": short,
                "root": root_prefix,
                "branch": branch,
                "index_status": "not_indexed",
            },
        )

    if repo.index_status == "failed":
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "code_index_failed",
                "message": f"Code index failed for {root_prefix} on {branch}.",
                "repository": repo.name,
                "root": root_prefix,
                "branch": branch,
                "index_status": "failed",
                "last_error": repo.last_error or "",
            },
        )
    if not _repository_is_searchable(repo):
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "code_index_not_ready",
                "message": f"Code index is not ready for {root_prefix} on {branch}.",
                "repository": repo.name,
                "root": root_prefix,
                "branch": branch,
                "index_status": repo.index_status,
            },
        )

    rows = db.query(CodeChunk).filter(CodeChunk.file_path == path, CodeChunk.branch == branch).order_by(CodeChunk.start_line.asc()).all()
    if line:
        rows.sort(key=lambda row: min(abs((row.start_line or line) - line), abs((row.end_line or line) - line)))
    rows = rows[:limit]
    return [
        {
            "file_path": row.file_path,
            "content": row.content,
            "start_line": row.start_line,
            "end_line": row.end_line,
            "symbol_name": row.symbol_name,
            "symbol_type": row.symbol_type,
            "metadata": row.metadata_json,
        }
        for row in rows
    ]
