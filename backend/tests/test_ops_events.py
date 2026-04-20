"""Tests for ops_events service and feed health."""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from app.realtime.feed_health import FeedHealthTracker, SourceHealth
from app.services.ops_events_service import query_events, write_event


class TestSourceHealth:
    """Unit tests for SourceHealth."""

    def test_record_reception(self) -> None:
        sh = SourceHealth(source_id="test")
        sh.record_reception()
        assert sh.last_reception_time > 0

    def test_current_state_connected(self) -> None:
        sh = SourceHealth(source_id="test")
        sh.record_reception()
        assert sh.current_state() == "connected"

    def test_approx_rate_hz(self) -> None:
        sh = SourceHealth(source_id="test")
        for _ in range(5):
            sh.record_reception()
        rate = sh.approx_rate_hz()
        assert rate is None or rate >= 0


class TestFeedHealthTracker:
    """Unit tests for FeedHealthTracker."""

    def test_record_reception(self) -> None:
        tracker = FeedHealthTracker()
        tracker.record_reception("source_a")
        status = tracker.get_status("source_a")
        assert status["source_id"] == "source_a"
        assert status["connected"] is True

    def test_unknown_source_returns_disconnected(self) -> None:
        tracker = FeedHealthTracker()
        status = tracker.get_status("unknown")
        assert status["source_id"] == "unknown"
        assert status["connected"] is False


def test_write_event_uses_source_and_stream_fields() -> None:
    db = MagicMock()
    event_time = datetime.now(timezone.utc)

    event = write_event(
        db,
        source_id="source-a",
        stream_id="vehicle-a-2026-03-28T12-00-00Z",
        event_time=event_time,
        event_type="alert.opened",
        severity="warning",
        summary="VBAT out of family/limits",
        entity_type="alert",
        entity_id="VBAT",
        payload={"alert_id": "a1"},
    )

    assert event.source_id == "source-a"
    assert event.stream_id == "vehicle-a-2026-03-28T12-00-00Z"
    db.add.assert_called_once()
    db.flush.assert_called_once()


def test_query_events_with_stream_scope_keeps_feed_status_events() -> None:
    db = MagicMock()
    statements: list[str] = []

    class _CountResult:
        def scalar(self):
            return 0

    class _RowsResult:
        def scalars(self):
            return self

        def all(self):
            return []

    def fake_execute(statement):
        sql = str(statement.compile(compile_kwargs={"literal_binds": True}))
        statements.append(sql)
        if "count(" in sql.lower():
            return _CountResult()
        return _RowsResult()

    db.execute.side_effect = fake_execute

    query_events(
        db,
        source_id="source-a",
        stream_ids=["vehicle-a-2026-03-28T12-00-00Z"],
        since=datetime(2026, 3, 28, 11, 0, tzinfo=timezone.utc),
    )

    assert any("ops_events.stream_id IS NULL" in sql for sql in statements)
    assert any("ops_events.event_type = 'system.feed_status'" in sql for sql in statements)
