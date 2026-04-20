"""Statistics computation service."""

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.telemetry import TelemetryData, TelemetryMetadata, TelemetryStatistics
from app.services.source_stream_service import get_stream_source_id, normalize_source_id, resolve_latest_stream_id

logger = logging.getLogger(__name__)


class StatisticsService:
    """Service for computing and storing telemetry statistics."""

    def __init__(self, db: Session) -> None:
        self._db = db

    def recompute_all(
        self,
        source_id: Optional[str] = None,
        all_sources: bool = False,
    ) -> int:
        """Recompute statistics. source_id filters to one source; all_sources recomputes per source."""
        sources_to_process: list[str] = []
        if all_sources:
            stmt = select(TelemetryData.stream_id).distinct()
            sources_to_process = [r[0] for r in self._db.execute(stmt).fetchall()]
        else:
            if source_id is None:
                raise ValueError("source_id is required unless all_sources=True")
            sources_to_process = [resolve_latest_stream_id(self._db, source_id)]

        count = 0
        for sid in sources_to_process:
            logical_source_id = get_stream_source_id(self._db, sid) or normalize_source_id(source_id or sid)
            meta_ids = [
                row[0]
                for row in self._db.execute(
                    select(TelemetryMetadata.id).where(TelemetryMetadata.source_id == logical_source_id)
                ).fetchall()
            ]
            for tid in meta_ids:
                try:
                    self._recompute_one(tid, source_id=sid)
                    count += 1
                except Exception as e:
                    logger.warning(
                        "Failed to compute stats for %s/%s: %s", sid, tid, e
                    )
        logger.info("Recomputed statistics for %d telemetry points", count)
        return count

    def _recompute_one(
        self, telemetry_id, source_id: str
    ) -> None:
        """Compute and upsert statistics for a single telemetry point. source_id filters when telemetry_data is source-aware."""
        data_source_id = normalize_source_id(source_id)
        data_source_id = resolve_latest_stream_id(self._db, data_source_id)
        stmt = select(TelemetryData.value).where(
            TelemetryData.telemetry_id == telemetry_id,
            TelemetryData.stream_id == data_source_id,
        )
        rows = self._db.execute(stmt).fetchall()
        values = np.array([float(r[0]) for r in rows])

        if len(values) == 0:
            logger.warning("No data for telemetry %s, skipping", telemetry_id)
            return

        mean = float(np.mean(values))
        std_dev = float(np.std(values))
        if np.isnan(std_dev):
            std_dev = 0.0
        min_val = float(np.min(values))
        max_val = float(np.max(values))
        p5 = float(np.percentile(values, 5))
        p50 = float(np.percentile(values, 50))
        p95 = float(np.percentile(values, 95))

        n_samples = len(values)
        pk = (data_source_id, telemetry_id)
        existing = self._db.get(TelemetryStatistics, pk)
        now = datetime.now(timezone.utc)
        if existing:
            existing.mean = Decimal(str(mean))
            existing.std_dev = Decimal(str(std_dev))
            existing.min_value = Decimal(str(min_val))
            existing.max_value = Decimal(str(max_val))
            existing.p5 = Decimal(str(p5))
            existing.p50 = Decimal(str(p50))
            existing.p95 = Decimal(str(p95))
            existing.n_samples = n_samples
            existing.last_computed_at = now
        else:
            stats = TelemetryStatistics(
                stream_id=data_source_id,
                telemetry_id=telemetry_id,
                mean=Decimal(str(mean)),
                std_dev=Decimal(str(std_dev)),
                min_value=Decimal(str(min_val)),
                max_value=Decimal(str(max_val)),
                p5=Decimal(str(p5)),
                p50=Decimal(str(p50)),
                p95=Decimal(str(p95)),
                n_samples=n_samples,
                last_computed_at=now,
            )
            self._db.add(stats)
