"""Durable feed-health persistence and reads."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import case, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.models.telemetry import TelemetryFeedHealth
from app.realtime.feed_health import DEGRADED_SEC, DISCONNECTED_SEC


def _coerce_timestamp(value: float | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(float(value), tz=timezone.utc)


def _compute_state(last_reception_time: datetime | None, now: datetime | None = None) -> tuple[bool, str]:
    if last_reception_time is None:
        return False, "disconnected"
    now = now or datetime.now(timezone.utc)
    age = (now - last_reception_time).total_seconds()
    if age <= DEGRADED_SEC:
        return True, "connected"
    if age <= DISCONNECTED_SEC:
        return False, "degraded"
    return False, "disconnected"


def upsert_feed_health_snapshot(
    db: Session,
    *,
    source_id: str,
    status: dict,
    now: datetime | None = None,
) -> TelemetryFeedHealth:
    """Persist the latest feed-health snapshot for a source."""

    now = now or datetime.now(timezone.utc)
    last_reception_time = _coerce_timestamp(status.get("last_reception_time"))
    connected, state = _compute_state(last_reception_time, now)
    approx_rate_hz = status.get("approx_rate_hz")
    approx_decimal = Decimal(str(approx_rate_hz)) if approx_rate_hz is not None else None
    drop_count = int(status.get("drop_count") or 0)

    insert_stmt = pg_insert(TelemetryFeedHealth).values(
        source_id=source_id,
        connected=connected,
        state=state,
        last_reception_time=last_reception_time,
        approx_rate_hz=approx_decimal,
        drop_count=drop_count,
        last_transition_at=now,
        updated_at=now,
    )

    excluded = insert_stmt.excluded

    stmt = insert_stmt.on_conflict_do_update(
        index_elements=[TelemetryFeedHealth.source_id],
        set_={
            "connected": excluded.connected,
            "state": excluded.state,
            "last_reception_time": excluded.last_reception_time,
            "approx_rate_hz": excluded.approx_rate_hz,
            "drop_count": excluded.drop_count,
            "updated_at": excluded.updated_at,
            "last_transition_at": case(
                (
                    TelemetryFeedHealth.state.is_distinct_from(excluded.state),
                    excluded.last_transition_at,
                ),
                else_=TelemetryFeedHealth.last_transition_at,
            ),
        },
    )
    db.execute(stmt)
    db.flush()
    record = db.get(TelemetryFeedHealth, source_id)
    if record is not None:
        return record
    # Core INSERT paths (or stubs like MagicMock DBs in unit tests) may not populate ORM identity; return snapshot fields.
    return TelemetryFeedHealth(
        source_id=source_id,
        connected=connected,
        state=state,
        last_reception_time=last_reception_time,
        approx_rate_hz=approx_decimal,
        drop_count=drop_count,
        last_transition_at=now,
        updated_at=now,
    )


def refresh_feed_health_states(db: Session, *, now: datetime | None = None) -> list[TelemetryFeedHealth]:
    """Refresh persisted state transitions based on elapsed time."""

    now = now or datetime.now(timezone.utc)
    rows = list(db.execute(select(TelemetryFeedHealth)).scalars().all())
    changed: list[TelemetryFeedHealth] = []
    for row in rows:
        connected, state = _compute_state(row.last_reception_time, now)
        if row.connected != connected or row.state != state:
            row.connected = connected
            row.state = state
            row.last_transition_at = now
            row.updated_at = now
            changed.append(row)
    if changed:
        db.flush()
    return changed


def serialize_feed_health(record: TelemetryFeedHealth | None, *, source_id: str | None = None) -> dict:
    """Return the external feed-health payload (computes connected/state vs age on read)."""

    now = datetime.now(timezone.utc)
    if record is None:
        return {
            "source_id": source_id or "",
            "connected": False,
            "state": "disconnected",
            "last_reception_time": None,
            "approx_rate_hz": None,
            "drop_count": 0,
        }
    connected, state = _compute_state(record.last_reception_time, now)
    return {
        "source_id": record.source_id,
        "connected": connected,
        "state": state,
        "last_reception_time": record.last_reception_time.isoformat() if record.last_reception_time else None,
        "approx_rate_hz": float(record.approx_rate_hz) if record.approx_rate_hz is not None else None,
        "drop_count": record.drop_count,
    }


def get_feed_health_status(db: Session, source_id: str) -> dict:
    """Read one durable feed-health snapshot."""

    record = db.get(TelemetryFeedHealth, source_id)
    return serialize_feed_health(record, source_id=source_id)


def list_feed_health_statuses(db: Session) -> list[dict]:
    """Read all durable feed-health snapshots."""

    return [serialize_feed_health(record) for record in db.execute(select(TelemetryFeedHealth)).scalars().all()]
