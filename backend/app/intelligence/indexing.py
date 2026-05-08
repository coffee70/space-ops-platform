"""Shared control-plane code indexing engine for the background worker and tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import uuid

import httpx
from sqlalchemy.orm import Session

from app.config import get_settings
from app.intelligence.chunking import chunk_code_with_metadata
from app.intelligence.hashing import sha256_text
from app.models.intelligence import CodeChunk, CodeRepository


def _cp_url(path: str) -> str:
    base = get_settings().control_plane_url.rstrip("/")
    return f"{base}/{path.lstrip('/')}"


async def cp_get_json(path: str, params: dict | None = None) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(_cp_url(path), params=params)
    if resp.status_code >= 400:
        raise RuntimeError(f"control plane {path} failed: {resp.status_code} {resp.text[:500]}")
    return resp.json()


async def get_current_commit_for_root(root: str, branch: str) -> str:
    tree = await cp_get_json("code/tree", params={"branch": branch, "path": root})
    return (tree.get("commit_sha") or "").strip()


def _should_skip_index_path(path: str) -> bool:
    normalized = path.replace("\\", "/")
    return any(skip in normalized for skip in ["node_modules", ".next", "/dist/", "/build/", "/coverage/", "/.git/"])


_BINARY_CODE_INDEX_EXTENSIONS = frozenset(
    {
        "png",
        "jpg",
        "jpeg",
        "gif",
        "webp",
        "bmp",
        "ico",
        "tif",
        "tiff",
        "pdf",
        "zip",
        "gz",
        "tgz",
        "bz2",
        "xz",
        "7z",
        "rar",
        "mp3",
        "mp4",
        "m4a",
        "wav",
        "ogg",
        "webm",
        "mov",
        "avi",
        "mkv",
        "woff",
        "woff2",
        "ttf",
        "otf",
        "eot",
        "wasm",
        "so",
        "dylib",
        "dll",
        "exe",
        "bin",
        "sqlite",
        "jar",
        "apk",
        "dmg",
        "iso",
        "ppt",
        "pptx",
        "xls",
        "xlsx",
        "doc",
        "docx",
        "npz",
        "npy",
        "parquet",
        "pickle",
        "pkl",
    }
)


def _should_skip_binary_index_path(path: str) -> bool:
    if "." not in path:
        return False
    ext = path.rsplit(".", 1)[-1].lower()
    return ext in _BINARY_CODE_INDEX_EXTENSIONS


def _tree_entry_is_dir(entry: dict) -> bool:
    if "is_dir" in entry and entry["is_dir"] is not None:
        return bool(entry["is_dir"])
    etype = entry.get("type")
    if etype == "file":
        return False
    if etype in {"dir", "directory", "folder"}:
        return True
    return False


MAX_MANAGED_CODE_FILE_BYTES = 100_000


async def collect_code_file_paths(root: str, branch: str) -> tuple[list[str], str]:
    """Walk control-plane code/tree recursively."""
    paths: list[str] = []
    head_sha = ""

    async def walk(current_path: str) -> None:
        nonlocal head_sha
        tree = await cp_get_json("code/tree", params={"branch": branch, "path": current_path})
        head_sha = tree.get("commit_sha") or head_sha
        entries = tree.get("data", {}).get("entries", [])
        for entry in entries:
            entry_path = entry.get("path")
            if not entry_path or _should_skip_index_path(entry_path):
                continue
            if _tree_entry_is_dir(entry):
                await walk(entry_path)
            else:
                if _should_skip_binary_index_path(entry_path):
                    continue
                paths.append(entry_path)

    await walk(root)
    return paths, head_sha


async def cp_get_file_payload(branch: str, path: str) -> dict | None:
    """Return JSON payload for code/file or None on transport/HTTP failure."""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(_cp_url("code/file"), params={"branch": branch, "path": path})
        if resp.status_code >= 400:
            return None
        return resp.json()
    except (httpx.HTTPError, OSError, ValueError, TypeError):
        return None


@dataclass
class IndexRepositoryResult:
    repository_id: uuid.UUID
    root: str
    branch: str
    target_commit_sha: str
    file_count: int
    chunk_count: int
    skipped_file_count: int
    failed_file_count: int
    failed_files_preview: list[str]


async def index_repository_now(
    *,
    repository_id: uuid.UUID,
    root: str,
    branch: str,
    target_commit_sha: str,
    db: Session,
    max_failed_file_preview: int = 10,
) -> IndexRepositoryResult:
    """Replace branch chunks with a fresh index for target_commit_sha. Caller manages transactions."""
    repo = db.query(CodeRepository).filter(CodeRepository.id == repository_id).one()
    file_count = 0
    chunk_count = 0
    skipped_file_count = 0
    failed_file_count = 0
    failed_files_preview: list[str] = []

    file_paths, _tree_head = await collect_code_file_paths(root, branch)
    db.query(CodeChunk).filter(CodeChunk.repository_id == repository_id, CodeChunk.branch == branch).delete(synchronize_session=False)

    from app.intelligence.embedding import DEFAULT_EMBEDDING_MODEL, get_embedding_provider

    provider = get_embedding_provider()
    commit_for_rows = target_commit_sha.strip() or ""

    for path in file_paths:
        if _should_skip_binary_index_path(path):
            skipped_file_count += 1
            continue

        file_data = await cp_get_file_payload(branch, path)
        if file_data is None:
            failed_file_count += 1
            if len(failed_files_preview) < max_failed_file_preview:
                failed_files_preview.append(path)
            continue

        content = file_data.get("data", {}).get("content", "")
        if not content:
            skipped_file_count += 1
            continue
        if len(content) > MAX_MANAGED_CODE_FILE_BYTES:
            skipped_file_count += 1
            continue

        commit_sha = (file_data.get("commit_sha") or "").strip() or commit_for_rows
        language = path.rsplit(".", 1)[-1] if "." in path else None
        try:
            chunks = chunk_code_with_metadata(content, language=language)
        except Exception:
            failed_file_count += 1
            if len(failed_files_preview) < max_failed_file_preview:
                failed_files_preview.append(path)
            continue

        if not chunks:
            skipped_file_count += 1
            continue

        file_chunks_ok = True
        for chunk in chunks:
            try:
                embedding = provider.embed(chunk.content)
            except Exception:
                failed_file_count += 1
                if len(failed_files_preview) < max_failed_file_preview:
                    failed_files_preview.append(path)
                file_chunks_ok = False
                break
            db.add(
                CodeChunk(
                    repository_id=repository_id,
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
                    embedding=embedding,
                    embedding_model=DEFAULT_EMBEDDING_MODEL,
                    metadata_json=chunk.metadata,
                    indexed_at=datetime.now(timezone.utc),
                )
            )
            chunk_count += 1
        if file_chunks_ok:
            file_count += 1

    repo.updated_at = datetime.now(timezone.utc)
    return IndexRepositoryResult(
        repository_id=repository_id,
        root=root,
        branch=branch,
        target_commit_sha=target_commit_sha,
        file_count=file_count,
        chunk_count=chunk_count,
        skipped_file_count=skipped_file_count,
        failed_file_count=failed_file_count,
        failed_files_preview=failed_files_preview,
    )
