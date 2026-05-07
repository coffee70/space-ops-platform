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
from app.intelligence.chunking import chunk_code_with_metadata
from app.intelligence.embedding import DEFAULT_EMBEDDING_MODEL, get_embedding_provider
from app.intelligence.events import emit_event
from app.intelligence.hashing import sha256_text
from app.models.intelligence import CodeChunk, CodeRepository


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
    extension = (chunk.file_path.rsplit(".", 1)[-1].lower() if "." in chunk.file_path else "")
    if query_token_set.intersection(_UI_HINTS) and extension in _UI_EXTENSIONS:
        score += 1.0
    if query_token_set.intersection(_SERVICE_HINTS) and extension in _SERVICE_EXTENSIONS:
        score += 1.0

    debug = {
        "path_token_hits": path_hits,
        "symbol_token_hits": symbol_hits,
        "content_token_hits": content_hits,
        "exact_phrase_in_path": bool(query_phrase and query_phrase in path_text),
        "exact_phrase_in_symbol": bool(query_phrase and query_phrase in symbol_text),
        "exact_phrase_in_content": bool(query_phrase and query_phrase in content_text),
    }
    return score, debug


def _is_safe_repo_root(root: str) -> bool:
    try:
        resolved = Path(root).resolve()
    except OSError:
        return False
    allowed = {Path("/workspace").resolve(), Path("/repos").resolve(), Path("/tmp").resolve()}
    return any(str(resolved).startswith(str(prefix)) for prefix in allowed)


def _run_ripgrep_search(repository_root: str, query: str, limit: int) -> list[dict]:
    if not query.strip() or not _is_safe_repo_root(repository_root):
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
                repository_root,
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
    if not root:
        raise HTTPException(status_code=400, detail="root is required")

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

    if body.get("conversation_id") and body.get("agent_run_id") and body.get("request_id"):
        emit_event(
            db,
            event_type="code.index_started",
            payload={"repository": repository.name, "branch": branch, "commit_sha": ""},
            conversation_id=body.get("conversation_id"),
            agent_run_id=body.get("agent_run_id"),
            request_id=body.get("request_id"),
            sequence=1,
            emitted_by="code-intelligence-service",
        )

    try:
        tree = await _cp_get("code/tree", params={"branch": branch, "path": root})
        entries = tree.get("data", {}).get("entries", [])
        files = [entry["path"] for entry in entries if entry.get("type") == "file"]
        provider = get_embedding_provider()
        file_count = 0
        chunk_count = 0
        for path in files:
            if any(skip in path for skip in ["node_modules", ".next", "/dist/", "/build/", "/coverage/", "/.git/"]):
                continue
            file_data = await _cp_get("code/file", params={"branch": branch, "path": path})
            content = file_data.get("data", {}).get("content", "")
            if not content or len(content) > 100_000:
                continue
            language = path.split(".")[-1] if "." in path else None
            chunks = chunk_code_with_metadata(content, language=language)
            commit_sha = file_data.get("commit_sha") or tree.get("commit_sha") or ""
            db.query(CodeChunk).filter(CodeChunk.repository_id == repository.id, CodeChunk.branch == branch, CodeChunk.file_path == path).delete()
            for chunk in chunks:
                db.add(
                    CodeChunk(
                        repository_id=repository.id,
                        branch=branch,
                        commit_sha=commit_sha,
                        file_path=path,
                        language=language,
                        symbol_name=chunk.symbol_name,
                        symbol_type=chunk.symbol_type,
                        start_line=chunk.start_line,
                        end_line=chunk.end_line,
                        content=chunk.content,
                        content_hash=sha256_text(chunk.content),
                        embedding=provider.embed(chunk.content),
                        embedding_model=DEFAULT_EMBEDDING_MODEL,
                        metadata_json=chunk.metadata,
                        indexed_at=datetime.now(timezone.utc),
                    )
                )
                chunk_count += 1
            file_count += 1

        repository.updated_at = datetime.now(timezone.utc)
        if body.get("conversation_id") and body.get("agent_run_id") and body.get("request_id"):
            emit_event(
                db,
                event_type="code.index_completed",
                payload={"repository": repository.name, "branch": branch, "commit_sha": "", "file_count": file_count, "chunk_count": chunk_count, "duration_ms": 0},
                conversation_id=body.get("conversation_id"),
                agent_run_id=body.get("agent_run_id"),
                request_id=body.get("request_id"),
                sequence=2,
                emitted_by="code-intelligence-service",
            )
        return {"repository_id": str(repository.id), "file_count": file_count, "chunk_count": chunk_count}
    except Exception as exc:
        if body.get("conversation_id") and body.get("agent_run_id") and body.get("request_id"):
            emit_event(
                db,
                event_type="code.index_failed",
                payload={"repository": repository.name, "branch": branch, "error_code": "code_index_failed", "message": str(exc)},
                conversation_id=body.get("conversation_id"),
                agent_run_id=body.get("agent_run_id"),
                request_id=body.get("request_id"),
                sequence=2,
                emitted_by="code-intelligence-service",
            )
        raise


def get_repository_status(repository_id: str, db: Session = Depends(get_db)):
    repo = db.query(CodeRepository).filter(CodeRepository.id == uuid.UUID(repository_id)).one_or_none()
    if not repo:
        raise HTTPException(status_code=404, detail="repository not found")
    chunk_count = db.query(CodeChunk).filter(CodeChunk.repository_id == repo.id).count()
    latest = db.query(CodeChunk).filter(CodeChunk.repository_id == repo.id).order_by(CodeChunk.indexed_at.desc()).first()
    return {**_repo_summary(repo), "chunk_count": chunk_count, "latest_commit_sha": latest.commit_sha if latest else None, "indexed_at": latest.indexed_at if latest else None}


def search_code(body: dict, db: Session = Depends(get_db)):
    query = (body.get("query") or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required")
    limit = min(max(int(body.get("limit", 6)), 1), 8)
    rows = db.query(CodeChunk, CodeRepository).join(CodeRepository, CodeRepository.id == CodeChunk.repository_id).filter(CodeChunk.branch == body.get("branch", "main")).all()
    rg_boosts: dict[tuple[str, int], float] = {}
    if body.get("repository"):
        repo = db.query(CodeRepository).filter(CodeRepository.name == body["repository"]).one_or_none()
        if repo and _is_safe_repo_root(repo.source_uri):
            for match in _run_ripgrep_search(repo.source_uri, query, limit=max(limit * 4, 24)):
                rg_boosts[(match["file_path"], match["line"])] = 1.5

    scored: list[dict] = []
    for chunk, repository in rows:
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
