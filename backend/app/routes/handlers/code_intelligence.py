from __future__ import annotations

from datetime import datetime, timezone
import uuid

import httpx
from fastapi import Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.config import get_settings
from app.database import get_db
from app.intelligence.chunking import chunk_code
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
        chunks = chunk_code(content, max_chars=1500)
        commit_sha = file_data.get("commit_sha") or tree.get("commit_sha") or ""
        db.query(CodeChunk).filter(CodeChunk.repository_id == repository.id, CodeChunk.branch == branch, CodeChunk.file_path == path).delete()
        for idx, chunk in enumerate(chunks):
            db.add(
                CodeChunk(
                    repository_id=repository.id,
                    branch=branch,
                    commit_sha=commit_sha,
                    file_path=path,
                    language=path.split(".")[-1] if "." in path else None,
                    symbol_name=None,
                    symbol_type="chunk",
                    start_line=None,
                    end_line=None,
                    content=chunk,
                    content_hash=sha256_text(chunk),
                    embedding=provider.embed(chunk),
                    embedding_model=DEFAULT_EMBEDDING_MODEL,
                    metadata_json={"chunk_index": idx},
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
            sequence=1,
            emitted_by="code-intelligence-service",
        )
    return {"repository_id": str(repository.id), "file_count": file_count, "chunk_count": chunk_count}


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
    scored: list[dict] = []
    for chunk, repository in rows:
        if body.get("repository") and repository.name != body["repository"]:
            continue
        score = 1.0 / (1.0 + abs(len(chunk.content) - len(query)))
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
            }
        )
    scored.sort(key=lambda item: item["score"], reverse=True)
    return scored[:limit]


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
    rows = db.query(CodeChunk).filter(CodeChunk.file_path == path, CodeChunk.branch == branch).order_by(CodeChunk.indexed_at.desc()).limit(6).all()
    return [{"file_path": row.file_path, "content": row.content, "metadata": row.metadata_json} for row in rows]
