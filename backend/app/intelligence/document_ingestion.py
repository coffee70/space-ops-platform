"""Document ingestion helpers shared by the API and worker."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import time
from uuid import UUID

from sqlalchemy.orm import Session

from app.intelligence.chunking import chunk_text
from app.intelligence.embedding import DEFAULT_EMBEDDING_MODEL, get_embedding_provider
from app.intelligence.events import emit_event
from app.intelligence.hashing import sha256_text
from app.models.intelligence import Document, DocumentChunk


@dataclass(frozen=True)
class DocumentIngestionResult:
    chunk_count: int
    duration_ms: int


def _has_trace_metadata(conversation_id: UUID | None, agent_run_id: UUID | None, request_id: UUID | None) -> bool:
    return bool(conversation_id and agent_run_id and request_id)


def ingest_document_now(
    *,
    db: Session,
    document: Document,
    conversation_id: UUID | None = None,
    agent_run_id: UUID | None = None,
    request_id: UUID | None = None,
) -> DocumentIngestionResult:
    raw_content = (document.raw_content or "").strip()
    if not raw_content:
        raise ValueError("document source content unavailable")

    provider = get_embedding_provider()
    started = time.monotonic()

    db.query(DocumentChunk).filter(DocumentChunk.document_id == document.id).delete(synchronize_session=False)

    chunks = chunk_text(raw_content, max_chars=1200, overlap=120)
    for idx, chunk in enumerate(chunks):
        db.add(
            DocumentChunk(
                document_id=document.id,
                chunk_index=idx,
                content=chunk,
                metadata_json={
                    "filename": (document.source_uri or "").removeprefix("upload://"),
                    "document_type": document.document_type,
                    "mission_id": document.mission_id,
                    "vehicle_id": document.vehicle_id,
                    "subsystem_id": document.subsystem_id,
                    "tags": document.tags_json,
                },
                embedding=provider.embed(chunk),
                embedding_model=DEFAULT_EMBEDDING_MODEL,
                content_hash=sha256_text(chunk),
                created_at=datetime.now(timezone.utc),
            )
        )

    duration_ms = int((time.monotonic() - started) * 1000)
    document.ingestion_status = "ready"
    document.ingestion_error = None
    document.updated_at = datetime.now(timezone.utc)

    if _has_trace_metadata(conversation_id, agent_run_id, request_id):
        emit_event(
            db,
            event_type="document.ingestion_completed",
            payload={"document_id": str(document.id), "chunk_count": len(chunks), "embedding_model": DEFAULT_EMBEDDING_MODEL, "duration_ms": duration_ms},
            conversation_id=str(conversation_id) if conversation_id else None,
            agent_run_id=str(agent_run_id),
            request_id=str(request_id),
            sequence=3,
            emitted_by="document-ingestion-worker",
        )

    return DocumentIngestionResult(chunk_count=len(chunks), duration_ms=duration_ms)
