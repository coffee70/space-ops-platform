"""Source-scoped observation window storage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models.schemas import SourceObservationBatchUpsert
from app.models.telemetry import SourceObservation, TelemetrySource

UPCOMING_STATUSES = ("scheduled", "in_progress")


class SourceObservationNotFoundError(ValueError):
    """Raised when an observation request targets an unknown source."""


@dataclass(frozen=True)
class SourceObservationUpsertResult:
    inserted: int
    deleted: int


def _ensure_source_exists(db: Session, source_id: str) -> None:
    if db.get(TelemetrySource, source_id) is None:
        raise SourceObservationNotFoundError(f"Source not found: {source_id}")


def upsert_source_observations(
    db: Session,
    *,
    source_id: str,
    batch: SourceObservationBatchUpsert,
    now: datetime,
) -> SourceObservationUpsertResult:
    """Write a provider observation snapshot for a source."""
    _ensure_source_exists(db, source_id)

    deleted = 0
    if batch.replace_future_scheduled:
        result = db.execute(
            delete(SourceObservation).where(
                SourceObservation.source_id == source_id,
                SourceObservation.provider == batch.provider,
                SourceObservation.status == "scheduled",
                SourceObservation.end_time >= now,
            )
        )
        deleted = int(result.rowcount or 0)

    inserted = 0
    for item in batch.observations:
        if not item.external_id:
            raise ValueError("external_id is required for source observations")
        observation = None
        if not batch.replace_future_scheduled and item.external_id:
            observation = (
                db.execute(
                    select(SourceObservation).where(
                        SourceObservation.source_id == source_id,
                        SourceObservation.external_id == item.external_id,
                    )
                )
                .scalars()
                .first()
            )

        if observation is None:
            observation = SourceObservation(
                source_id=source_id,
                external_id=item.external_id,
                provider=batch.provider,
            )
            db.add(observation)
            inserted += 1

        observation.status = item.status
        observation.start_time = item.start_time
        observation.end_time = item.end_time
        observation.station_name = item.station_name
        observation.station_id = item.station_id
        observation.receiver_id = item.receiver_id
        observation.max_elevation_deg = item.max_elevation_deg
        observation.details_json = item.details

    db.commit()
    return SourceObservationUpsertResult(inserted=inserted, deleted=deleted)


def list_upcoming_observations(
    db: Session,
    *,
    source_id: str,
    now: datetime,
    limit: int = 5,
    provider: str | None = None,
) -> list[SourceObservation]:
    """Return non-ended scheduled or active observation windows ordered by start."""
    _ensure_source_exists(db, source_id)
    stmt = select(SourceObservation).where(
        SourceObservation.source_id == source_id,
        SourceObservation.status.in_(UPCOMING_STATUSES),
        SourceObservation.end_time >= now,
    )
    if provider is not None:
        stmt = stmt.where(SourceObservation.provider == provider)
    stmt = stmt.order_by(SourceObservation.start_time.asc(), SourceObservation.id.asc()).limit(limit)
    return list(db.execute(stmt).scalars().all())


def get_next_observation(
    db: Session,
    *,
    source_id: str,
    now: datetime,
    provider: str | None = None,
) -> SourceObservation | None:
    """Return the earliest upcoming observation window for a source."""
    observations = list_upcoming_observations(
        db,
        source_id=source_id,
        now=now,
        limit=1,
        provider=provider,
    )
    return observations[0] if observations else None
