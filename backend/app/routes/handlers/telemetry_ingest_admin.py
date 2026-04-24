from datetime import datetime

from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.lib.audit import audit_log
from app.models.schemas import TelemetryDataIngest, TelemetryDataResponse
from app.routes.handlers.providers import get_embedding_provider, get_llm_provider
from app.services.embedding_service import SentenceTransformerEmbeddingProvider
from app.services.source_stream_service import StreamIdConflictError
from app.services.telemetry_service import TelemetryService


def ingest_data(
    body: TelemetryDataIngest,
    db: Session = Depends(get_db),
    embedding: SentenceTransformerEmbeddingProvider = Depends(get_embedding_provider),
    llm: object = Depends(get_llm_provider),
):
    """Ingest batch of telemetry data scoped by the source_id in the body."""
    service = TelemetryService(db, embedding, llm)
    try:
        data = []
        for pt in body.data:
            ts = datetime.fromisoformat(pt.timestamp.replace("Z", "+00:00"))
            data.append((ts, pt.value))
        rows = service.insert_data(
            body.stream_id,
            body.telemetry_name,
            data,
            source_id=body.source_id,
            packet_source=body.packet_source,
            receiver_id=body.receiver_id,
        )
    except StreamIdConflictError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    audit_log(
        "ingest.batch",
        telemetry_name=body.telemetry_name,
        count=rows,
        source_id=body.source_id,
        stream_id=body.stream_id,
    )
    return TelemetryDataResponse(rows_inserted=rows)
