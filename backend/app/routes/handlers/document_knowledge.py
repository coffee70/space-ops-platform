from __future__ import annotations

from datetime import datetime, timezone
import re
import uuid

from fastapi import Depends, File, Form, HTTPException, UploadFile
from sqlalchemy.orm import Session

from app.database import get_db
from app.intelligence.events import emit_event
from app.intelligence.hashing import sha256_text
from app.models.intelligence import Document, DocumentChunk, DocumentIngestionJob

_TOKEN_SPLIT_PATTERN = re.compile(r"[^a-z0-9]+")
_CAMEL_SPLIT_PATTERN = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_STOP_WORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "this",
    "that",
    "into",
    "code",
    "document",
    "documents",
    "mission",
    "vehicle",
}
_DOCUMENT_DOMAIN_HINTS = {
    "telemetry",
    "channel",
    "channels",
    "battery",
    "voltage",
    "subsystem",
    "unit",
    "units",
    "display",
    "name",
    "dictionary",
    "metadata",
    "spacecraft",
    "sensor",
    "current",
    "power",
}


def _tokenize(value: str) -> list[str]:
    if not value:
        return []
    normalized = _CAMEL_SPLIT_PATTERN.sub(" ", value).lower()
    bits = _TOKEN_SPLIT_PATTERN.split(normalized)
    return [bit for bit in bits if len(bit) >= 2 and bit not in _STOP_WORDS]


def _document_metadata_text(document: Document, chunk: DocumentChunk) -> str:
    metadata = chunk.metadata_json or {}
    tags = metadata.get("tags") or document.tags_json or []
    if not isinstance(tags, list):
        tags = []
    metadata_values = [
        str(document.mission_id or ""),
        str(document.vehicle_id or ""),
        str(document.subsystem_id or ""),
        str(document.document_type or ""),
        str(metadata.get("filename") or ""),
        " ".join(str(tag) for tag in tags if tag is not None),
    ]
    return " ".join(metadata_values)


def _score_document_chunk(query: str, chunk: DocumentChunk, document: Document) -> tuple[float, dict]:
    query_lower = query.lower()
    title = document.title or ""
    content = chunk.content or ""
    metadata_text = _document_metadata_text(document, chunk)

    query_tokens = _tokenize(query)
    title_tokens = set(_tokenize(title))
    content_tokens = set(_tokenize(content))
    metadata_tokens = set(_tokenize(metadata_text))
    combined_tokens = title_tokens | content_tokens | metadata_tokens

    title_hits = len([token for token in query_tokens if token in title_tokens])
    content_hits = len([token for token in query_tokens if token in content_tokens])
    metadata_hits = len([token for token in query_tokens if token in metadata_tokens])
    all_query_tokens_present = bool(query_tokens) and all(token in combined_tokens for token in query_tokens)

    exact_phrase_in_content = query_lower in content.lower()
    exact_phrase_in_title = query_lower in title.lower()

    query_domain_hints = {token for token in query_tokens if token in _DOCUMENT_DOMAIN_HINTS}
    domain_hint_hits = len([token for token in query_domain_hints if token in combined_tokens])

    score = 0.0
    if exact_phrase_in_content:
        score += 10.0
    if exact_phrase_in_title:
        score += 6.0
    score += title_hits * 2.0
    score += content_hits * 1.0
    score += metadata_hits * 1.5
    if all_query_tokens_present:
        score += 2.0
    score += domain_hint_hits * 0.75

    ranking_signals = {
        "title_token_hits": title_hits,
        "content_token_hits": content_hits,
        "metadata_token_hits": metadata_hits,
        "domain_hint_hits": domain_hint_hits,
        "exact_phrase_in_title": exact_phrase_in_title,
        "exact_phrase_in_content": exact_phrase_in_content,
        "all_query_tokens_present": all_query_tokens_present,
    }
    return score, ranking_signals


def _serialize_document(doc: Document) -> dict:
    return {
        "id": str(doc.id),
        "title": doc.title,
        "document_type": doc.document_type,
        "mission_id": doc.mission_id,
        "vehicle_id": doc.vehicle_id,
        "subsystem_id": doc.subsystem_id,
        "tags": doc.tags_json,
        "description": doc.description,
        "ingestion_status": doc.ingestion_status,
        "ingestion_error": doc.ingestion_error,
        "created_at": doc.created_at,
        "updated_at": doc.updated_at,
    }


def _parse_trace_id(value: str | None) -> uuid.UUID | None:
    if not value:
        return None
    try:
        return uuid.UUID(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"invalid trace id: {value}") from exc


def _find_active_ingestion_job(db: Session, document_id: uuid.UUID) -> DocumentIngestionJob | None:
    return (
        db.query(DocumentIngestionJob)
        .filter(
            DocumentIngestionJob.document_id == document_id,
            DocumentIngestionJob.status.in_(("queued", "running")),
        )
        .order_by(DocumentIngestionJob.requested_at.asc())
        .first()
    )


def _enqueue_ingestion_job(
    db: Session,
    *,
    document_id: uuid.UUID,
    conversation_id: uuid.UUID | None,
    agent_run_id: uuid.UUID | None,
    request_id: uuid.UUID | None,
    requested_at: datetime,
) -> DocumentIngestionJob:
    active_job = _find_active_ingestion_job(db, document_id)
    if active_job:
        return active_job
    job = DocumentIngestionJob(
        id=uuid.uuid4(),
        document_id=document_id,
        status="queued",
        requested_at=requested_at,
        conversation_id=conversation_id,
        agent_run_id=agent_run_id,
        request_id=request_id,
    )
    db.add(job)
    return job


def list_documents(db: Session = Depends(get_db)):
    docs = db.query(Document).order_by(Document.created_at.desc()).all()
    return [_serialize_document(doc) for doc in docs]


async def create_document(
    file: UploadFile = File(...),
    title: str | None = Form(None),
    document_type: str | None = Form(None),
    mission_id: str | None = Form(None),
    vehicle_id: str | None = Form(None),
    subsystem_id: str | None = Form(None),
    tags: str | None = Form(None),
    description: str | None = Form(None),
    conversation_id: str | None = Form(None),
    agent_run_id: str | None = Form(None),
    request_id: str | None = Form(None),
    db: Session = Depends(get_db),
):
    raw = (await file.read()).decode("utf-8", errors="ignore")
    if not raw.strip():
        raise HTTPException(status_code=400, detail="empty document")
    if len(raw) > 500_000:
        raise HTTPException(status_code=400, detail="document too large")

    now = datetime.now(timezone.utc)
    trace_conversation_id = _parse_trace_id(conversation_id)
    trace_agent_run_id = _parse_trace_id(agent_run_id)
    trace_request_id = _parse_trace_id(request_id)
    doc = Document(
        title=title or file.filename or "uploaded-document",
        document_type=document_type or (file.filename.split(".")[-1].lower() if file.filename and "." in file.filename else "text"),
        source_uri=f"upload://{file.filename}",
        mission_id=mission_id,
        vehicle_id=vehicle_id,
        subsystem_id=subsystem_id,
        tags_json=[t.strip() for t in tags.split(",")] if tags else [],
        description=description,
        raw_content=raw,
        content_hash=sha256_text(raw),
        ingestion_status="pending",
        ingestion_error=None,
        created_at=now,
        updated_at=now,
    )
    db.add(doc)
    db.flush()
    _enqueue_ingestion_job(
        db,
        document_id=doc.id,
        conversation_id=trace_conversation_id,
        agent_run_id=trace_agent_run_id,
        request_id=trace_request_id,
        requested_at=now,
    )

    if trace_conversation_id and trace_agent_run_id and trace_request_id:
        emit_event(
            db,
            event_type="document.uploaded",
            payload={"document_id": str(doc.id), "title": doc.title, "document_type": doc.document_type, "content_hash": doc.content_hash},
            conversation_id=str(trace_conversation_id),
            agent_run_id=str(trace_agent_run_id),
            request_id=str(trace_request_id),
            sequence=1,
            emitted_by="document-knowledge-service",
        )

    return {"document_id": str(doc.id), "title": doc.title, "ingestion_status": doc.ingestion_status}


def get_document(document_id: str, db: Session = Depends(get_db)):
    doc = db.query(Document).filter(Document.id == uuid.UUID(document_id)).one_or_none()
    if not doc:
        raise HTTPException(status_code=404, detail="document not found")
    return _serialize_document(doc)


def delete_document(document_id: str, db: Session = Depends(get_db)):
    doc = db.query(Document).filter(Document.id == uuid.UUID(document_id)).one_or_none()
    if not doc:
        raise HTTPException(status_code=404, detail="document not found")
    db.delete(doc)
    return {"deleted": True, "document_id": document_id}


def reingest_document(document_id: str, db: Session = Depends(get_db)):
    doc = db.query(Document).filter(Document.id == uuid.UUID(document_id)).one_or_none()
    if not doc:
        raise HTTPException(status_code=404, detail="document not found")
    now = datetime.now(timezone.utc)
    doc.ingestion_status = "pending"
    doc.ingestion_error = None
    doc.updated_at = now
    _enqueue_ingestion_job(
        db,
        document_id=doc.id,
        conversation_id=None,
        agent_run_id=None,
        request_id=None,
        requested_at=now,
    )
    return {"document_id": document_id, "ingestion_status": doc.ingestion_status}


def search_documents(body: dict, db: Session = Depends(get_db)):
    query = (body.get("query") or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required")
    limit = min(max(int(body.get("limit", 6)), 1), 8)

    docs = db.query(DocumentChunk, Document).join(Document, Document.id == DocumentChunk.document_id).filter(Document.ingestion_status == "ready").all()
    scored: list[dict] = []
    for chunk, document in docs:
        if body.get("mission_id") and document.mission_id != body["mission_id"]:
            continue
        if body.get("vehicle_id") and document.vehicle_id != body["vehicle_id"]:
            continue
        if body.get("subsystem_id") and document.subsystem_id != body["subsystem_id"]:
            continue
        if chunk.embedding is None:
            continue
        score, ranking_signals = _score_document_chunk(query, chunk, document)
        if score <= 0:
            continue
        scored.append(
            {
                "document_id": str(document.id),
                "title": document.title,
                "chunk_index": chunk.chunk_index,
                "content": chunk.content[:1500],
                "score": float(score),
                "metadata": {
                    **(chunk.metadata_json or {}),
                    "ranking_signals": ranking_signals,
                },
            }
        )
    scored.sort(key=lambda item: item["score"], reverse=True)
    return scored[:limit]


def list_document_chunks(document_id: str, db: Session = Depends(get_db)):
    chunks = db.query(DocumentChunk).filter(DocumentChunk.document_id == uuid.UUID(document_id)).order_by(DocumentChunk.chunk_index.asc()).all()
    return [{"id": str(chunk.id), "chunk_index": chunk.chunk_index, "content": chunk.content, "metadata": chunk.metadata_json} for chunk in chunks]
