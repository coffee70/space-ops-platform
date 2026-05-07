"""PostgreSQL-backed tests for concurrent feed-health upserts."""

from __future__ import annotations

import os
from collections.abc import Iterator
from concurrent import futures
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import delete

from app.database import get_session_factory
from app.models.telemetry import TelemetryFeedHealth, TelemetrySource
from app.services.feed_health_service import get_feed_health_status, upsert_feed_health_snapshot


@pytest.fixture()
def postgresql_required() -> None:
    """Skip when DATABASE_URL targets a dialect without Postgres upsert."""

    url = os.environ.get("DATABASE_URL") or ""
    if "postgresql" not in url.lower() and "postgres" not in url.lower():
        pytest.skip("feed-health concurrent upserts require PostgreSQL INSERT ... ON CONFLICT")


@pytest.fixture()
def feed_health_logical_source(postgresql_required) -> Iterator[str]:
    """Real telemetry_sources row backing telemetry_feed_health FK."""

    source_id = str(uuid4())
    cfg_token = str(uuid4())
    sf = get_session_factory()
    session = sf()
    try:
        session.add(
            TelemetrySource(
                id=source_id,
                name="feed-health test source",
                description=None,
                source_type="vehicle",
                base_url=None,
                vehicle_config_path=f"vehicles/fh-{cfg_token}.yaml",
            )
        )
        session.commit()
        yield source_id
    finally:
        session.close()
        cleanup = sf()
        try:
            cleanup.execute(delete(TelemetryFeedHealth).where(TelemetryFeedHealth.source_id == source_id))
            cleanup.execute(delete(TelemetrySource).where(TelemetrySource.id == source_id))
            cleanup.commit()
        finally:
            cleanup.close()


def test_concurrent_feed_health_upserts_raise_no_integrity_error(feed_health_logical_source: str) -> None:
    sf = get_session_factory()
    concurrency = 16
    now = datetime.now(timezone.utc).timestamp()

    def _attempt(offset: float) -> None:
        sess = sf()
        try:
            upsert_feed_health_snapshot(
                sess,
                source_id=feed_health_logical_source,
                status={
                    "last_reception_time": now + offset,
                    "approx_rate_hz": 1.0 + offset,
                    "drop_count": 0,
                },
            )
            sess.commit()
        finally:
            sess.close()

    with futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(_attempt, (i * 1e-6 for i in range(concurrency))))

    reader = sf()
    try:
        snapshot = get_feed_health_status(reader, feed_health_logical_source)
        assert snapshot["source_id"] == feed_health_logical_source
        assert snapshot["approx_rate_hz"] is not None
    finally:
        reader.close()


def test_feed_health_upsert_and_read(feed_health_logical_source: str) -> None:
    sf = get_session_factory()
    sess = sf()
    try:
        upsert_feed_health_snapshot(
            sess,
            source_id=feed_health_logical_source,
            status={
                "last_reception_time": datetime.now(timezone.utc).timestamp(),
                "approx_rate_hz": 5.5,
                "drop_count": 2,
            },
        )
        sess.commit()
    finally:
        sess.close()

    read = sf()
    try:
        body = get_feed_health_status(read, feed_health_logical_source)
        assert body["connected"] is True
        assert body["drop_count"] == 2
        assert body["approx_rate_hz"] == pytest.approx(5.5)
    finally:
        read.close()
