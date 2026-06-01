"""Startup-driven platform documentation indexing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
import re
import subprocess
import uuid

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.intelligence.hashing import sha256_text
from app.models.intelligence import Document, DocumentChunk, DocumentIngestionJob, PlatformDocsIndexJob

ALLOWED_PLATFORM_DOC_ROOTS = {
    "space-ops-kernel": "../space-ops-kernel/docs",
    "space-ops-platform": "./docs",
    "space-ops-apps": "../space-ops-apps/docs",
}
DEFAULT_PLATFORM_DOC_REPOSITORIES = list(ALLOWED_PLATFORM_DOC_ROOTS)
PLATFORM_DOC_DOCUMENT_TYPE = "platform_doc"

_FRONTMATTER_PATTERN = re.compile(r"\A---\s*\n(.*?)\n---\s*(?:\n|\Z)", re.DOTALL)
_H1_PATTERN = re.compile(r"^\s{0,3}#\s+(.+?)\s*$", re.MULTILINE)
_ACTIVE_JOB_STATUSES = ("queued", "running")


@dataclass(frozen=True)
class PlatformDocUpsertResult:
    repository: str
    status: str
    indexed_commit_sha: str | None
    current_commit_sha: str | None
    document_count: int
    queued_document_count: int
    skipped_document_count: int
    stale_document_count: int
    errors: list[str]


def platform_root() -> Path:
    return Path(__file__).resolve().parents[3]


def resolve_docs_root(repository: str) -> Path:
    if repository not in ALLOWED_PLATFORM_DOC_ROOTS:
        raise ValueError(f"unknown platform docs repository: {repository}")
    return (platform_root() / ALLOWED_PLATFORM_DOC_ROOTS[repository]).resolve()


def normalize_repositories(repositories: list[str] | None) -> list[str]:
    selected = repositories or DEFAULT_PLATFORM_DOC_REPOSITORIES
    unknown = [repo for repo in selected if repo not in ALLOWED_PLATFORM_DOC_ROOTS]
    if unknown:
        raise HTTPException(status_code=400, detail=f"unknown platform docs repository: {unknown[0]}")
    return list(dict.fromkeys(selected))


def enqueue_platform_docs_index_job(
    db: Session,
    *,
    repositories: list[str] | None = None,
    force: bool = False,
    trigger: str = "manual",
    requested_at: datetime | None = None,
) -> PlatformDocsIndexJob:
    repos = normalize_repositories(repositories)
    now = requested_at or datetime.now(timezone.utc)
    active = (
        db.query(PlatformDocsIndexJob)
        .filter(PlatformDocsIndexJob.status.in_(_ACTIVE_JOB_STATUSES))
        .order_by(PlatformDocsIndexJob.requested_at.asc())
        .first()
    )
    if active and not force and set(active.repositories_json or []) == set(repos):
        return active
    job = PlatformDocsIndexJob(
        id=uuid.uuid4(),
        job_type="platform_docs_index",
        status="queued",
        repositories_json=repos,
        force=force,
        trigger=trigger,
        requested_at=now,
        result_json={},
    )
    db.add(job)
    return job


def enqueue_platform_docs_index_on_startup(db: Session, *, strict: bool = False) -> PlatformDocsIndexJob | None:
    try:
        return enqueue_platform_docs_index_job(db, repositories=None, force=False, trigger="startup")
    except Exception:
        if strict:
            raise
        return None


def run_platform_docs_index_job(db: Session, job: PlatformDocsIndexJob) -> dict:
    started = datetime.now(timezone.utc)
    job.status = "running"
    job.started_at = started
    job.error = None
    db.flush()

    results: list[dict] = []
    for repository in normalize_repositories(job.repositories_json):
        result = index_platform_docs_repository(db, repository=repository, force=job.force)
        results.append(result.__dict__)

    failures = [result for result in results if result["status"] in {"error", "degraded"}]
    successes = [result for result in results if result["status"] == "ready"]
    if failures and successes:
        status = "degraded"
    elif failures:
        status = "failed"
    else:
        status = "completed"

    completed = datetime.now(timezone.utc)
    job.status = status
    job.completed_at = completed
    job.error = "; ".join(error for result in results for error in result.get("errors", [])) or None
    job.result_json = {"repositories": results, "completed_at": completed.isoformat()}
    return job.result_json


def index_platform_docs_repository(db: Session, *, repository: str, force: bool = False) -> PlatformDocUpsertResult:
    errors: list[str] = []
    queued = 0
    skipped = 0
    current_source_uris: set[str] = set()
    commit_sha: str | None = None

    try:
        docs_root = resolve_docs_root(repository)
        repo_root = docs_root.parent
        if not docs_root.is_dir():
            raise FileNotFoundError(f"docs root not found: {docs_root}")
        commit_sha = _current_commit_sha(repo_root)
        for path in sorted(docs_root.rglob("*.md")):
            if not path.is_file():
                continue
            resolved = path.resolve()
            _assert_inside_root(resolved, docs_root)
            raw = path.read_text(encoding="utf-8")
            source_uri = _source_uri(repository, docs_root, resolved)
            current_source_uris.add(source_uri)
            changed = _upsert_platform_doc(
                db,
                repository=repository,
                docs_root=docs_root,
                path=resolved,
                raw_content=raw,
                source_uri=source_uri,
                commit_sha=commit_sha,
            )
            if force or changed:
                document = db.query(Document).filter(Document.source_uri == source_uri).one()
                _enqueue_document_ingestion(db, document=document)
                queued += 1
            else:
                skipped += 1
        stale = _mark_missing_docs_stale(db, repository=repository, current_source_uris=current_source_uris)
        return PlatformDocUpsertResult(
            repository=repository,
            status="ready",
            indexed_commit_sha=commit_sha,
            current_commit_sha=commit_sha,
            document_count=len(current_source_uris),
            queued_document_count=queued,
            skipped_document_count=skipped,
            stale_document_count=stale,
            errors=[],
        )
    except Exception as exc:
        errors.append(str(exc))
        return PlatformDocUpsertResult(
            repository=repository,
            status="error",
            indexed_commit_sha=None,
            current_commit_sha=commit_sha,
            document_count=0,
            queued_document_count=queued,
            skipped_document_count=skipped,
            stale_document_count=0,
            errors=errors,
        )


def _current_commit_sha(repo_root: Path) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return result.stdout.strip()


def _assert_inside_root(path: Path, docs_root: Path) -> None:
    try:
        path.resolve().relative_to(docs_root.resolve())
    except ValueError as exc:
        raise ValueError(f"docs path escapes allowed root: {path}") from exc


def _source_uri(repository: str, docs_root: Path, path: Path) -> str:
    repo_path = Path("docs") / path.relative_to(docs_root)
    return f"repo://{repository}/{repo_path.as_posix()}"


def _repo_path(docs_root: Path, path: Path) -> str:
    return (Path("docs") / path.relative_to(docs_root)).as_posix()


def _derive_layer(repository: str) -> str:
    return repository.removeprefix("space-ops-")


def _parse_frontmatter(raw: str) -> tuple[dict, str]:
    match = _FRONTMATTER_PATTERN.match(raw)
    if not match:
        return {}, raw
    return _parse_simple_yaml(match.group(1)), raw[match.end() :]


def _parse_simple_yaml(text: str) -> dict:
    data: dict[str, object] = {}
    current_key: str | None = None
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("- ") and current_key:
            data.setdefault(current_key, [])
            if isinstance(data[current_key], list):
                data[current_key].append(stripped[2:].strip().strip("\"'"))
            continue
        if ":" not in stripped:
            current_key = None
            continue
        key, value = stripped.split(":", 1)
        current_key = key.strip()
        value = value.strip()
        if value == "":
            data[current_key] = []
        else:
            data[current_key] = _coerce_frontmatter_scalar(value)
    return data


def _coerce_frontmatter_scalar(value: str) -> object:
    unquoted = value.strip().strip("\"'")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", unquoted):
        return unquoted
    if unquoted.startswith("[") and unquoted.endswith("]"):
        return [item.strip().strip("\"'") for item in unquoted[1:-1].split(",") if item.strip()]
    return unquoted


def _first_h1(markdown_body: str) -> str | None:
    match = _H1_PATTERN.search(markdown_body)
    return match.group(1).strip() if match else None


def _json_safe(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return value


def _upsert_platform_doc(
    db: Session,
    *,
    repository: str,
    docs_root: Path,
    path: Path,
    raw_content: str,
    source_uri: str,
    commit_sha: str,
) -> bool:
    frontmatter, markdown_body = _parse_frontmatter(raw_content)
    repo_path = _repo_path(docs_root, path)
    title = str(frontmatter.get("title") or _first_h1(markdown_body) or path.stem.replace("-", " ").title())
    layer = str(frontmatter.get("layer") or _derive_layer(repository))
    topics = frontmatter.get("topics") if isinstance(frontmatter.get("topics"), list) else []
    metadata = {
        "source_type": PLATFORM_DOC_DOCUMENT_TYPE,
        "repository": repository,
        "repo_path": repo_path,
        "indexed_commit_sha": commit_sha,
        "layer": layer,
        "audience": frontmatter.get("audience"),
        "topics": topics,
        "status": frontmatter.get("status"),
        "last_verified": _json_safe(frontmatter.get("last_verified")),
        "stale": False,
    }
    content_hash = sha256_text(raw_content)
    tags = ["platform-doc", layer, *[str(topic) for topic in topics]]
    now = datetime.now(timezone.utc)
    document = db.query(Document).filter(Document.source_uri == source_uri).one_or_none()
    if document is None:
        document = Document(
            id=uuid.uuid4(),
            title=title,
            document_type=PLATFORM_DOC_DOCUMENT_TYPE,
            source_uri=source_uri,
            mission_id=None,
            vehicle_id=None,
            subsystem_id=None,
            tags_json=tags,
            metadata_json=metadata,
            description=None,
            raw_content=raw_content,
            content_hash=content_hash,
            ingestion_status="pending",
            ingestion_error=None,
            created_at=now,
            updated_at=now,
        )
        db.add(document)
        db.flush()
        return True

    changed = document.content_hash != content_hash
    document.title = title
    document.document_type = PLATFORM_DOC_DOCUMENT_TYPE
    document.tags_json = tags
    document.metadata_json = metadata
    document.raw_content = raw_content
    document.content_hash = content_hash
    document.ingestion_error = None
    document.updated_at = now
    if changed:
        document.ingestion_status = "pending"
    return changed


def _enqueue_document_ingestion(db: Session, *, document: Document) -> DocumentIngestionJob:
    active = (
        db.query(DocumentIngestionJob)
        .filter(DocumentIngestionJob.document_id == document.id, DocumentIngestionJob.status.in_(_ACTIVE_JOB_STATUSES))
        .order_by(DocumentIngestionJob.requested_at.asc())
        .first()
    )
    if active:
        return active
    job = DocumentIngestionJob(id=uuid.uuid4(), document_id=document.id, status="queued", requested_at=datetime.now(timezone.utc))
    db.add(job)
    return job


def _mark_missing_docs_stale(db: Session, *, repository: str, current_source_uris: set[str]) -> int:
    stale_count = 0
    documents = db.query(Document).filter(Document.document_type == PLATFORM_DOC_DOCUMENT_TYPE).all()
    now = datetime.now(timezone.utc)
    for document in documents:
        metadata = document.metadata_json or {}
        if metadata.get("repository") != repository or document.source_uri in current_source_uris:
            continue
        if metadata.get("stale") is True:
            continue
        document.metadata_json = {**metadata, "stale": True}
        document.updated_at = now
        stale_count += 1
    return stale_count


def latest_platform_index_jobs_by_repo(db: Session) -> dict[str, PlatformDocsIndexJob]:
    jobs = db.query(PlatformDocsIndexJob).order_by(PlatformDocsIndexJob.requested_at.desc()).all()
    latest: dict[str, PlatformDocsIndexJob] = {}
    for job in jobs:
        for repository in job.repositories_json or []:
            latest.setdefault(repository, job)
    return latest


def get_platform_doc_metadata(document: Document) -> dict:
    return document.metadata_json or {}


def is_platform_doc(document: Document) -> bool:
    metadata = get_platform_doc_metadata(document)
    return document.document_type == PLATFORM_DOC_DOCUMENT_TYPE or metadata.get("source_type") == PLATFORM_DOC_DOCUMENT_TYPE
