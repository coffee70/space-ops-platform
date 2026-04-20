"""Tests for source-aware telemetry (multi-source correctness)."""

import pytest

from app.services import overview_service as overview_service_module
from app.services.overview_service import get_overview, get_anomalies


class TestSourceAwareOverview:
    """Verify overview/anomalies accept and use source_id."""

    def test_get_overview_accepts_source_id(self, monkeypatch) -> None:
        """get_overview accepts source_id parameter (default: default)."""
        from unittest.mock import MagicMock

        db = MagicMock()
        monkeypatch.setattr(
            overview_service_module,
            "resolve_latest_stream_id",
            lambda _db, source_id: source_id,
        )
        monkeypatch.setattr(
            overview_service_module,
            "_resolve_logical_source_id",
            lambda _db, source_id: source_id,
        )
        monkeypatch.setattr(overview_service_module, "get_watchlist", lambda *_args, **_kwargs: [])
        db.execute = MagicMock(return_value=MagicMock(fetchall=MagicMock(return_value=[])))
        result = get_overview(db, source_id="simulator")
        assert result == []

    def test_get_overview_falls_back_to_latest_row_without_current(self, monkeypatch) -> None:
        """get_overview should read latest telemetry rows when TelemetryCurrent is absent."""
        from datetime import datetime, timezone
        from types import SimpleNamespace
        from uuid import uuid4
        from unittest.mock import MagicMock

        db = MagicMock()
        logical_source_id = "vehicle-a"
        telemetry_id = uuid4()
        meta = SimpleNamespace(
            id=telemetry_id,
            name="VBAT",
            units="V",
            description=None,
            channel_origin="catalog",
            discovery_namespace=None,
            red_low=None,
            red_high=4.5,
        )

        call_count = {"count": 0}

        class _Result:
            def __init__(self, rows):
                self._rows = rows

            def scalars(self):
                return self

            def first(self):
                return meta

            def fetchall(self):
                return self._rows

            def fetchone(self):
                return self._rows[0] if self._rows else None

        def fake_execute(_statement):
            call_count["count"] += 1
            if call_count["count"] == 1:
                return _Result([(meta,)])
            return _Result([(datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc), 4.2)])

        db.execute.side_effect = fake_execute
        db.get.return_value = None
        monkeypatch.setattr(
            overview_service_module,
            "resolve_latest_stream_id",
            lambda _db, source_id: source_id,
        )
        monkeypatch.setattr(
            overview_service_module,
            "_resolve_logical_source_id",
            lambda _db, source_id: logical_source_id,
        )
        monkeypatch.setattr(
            overview_service_module,
            "get_watchlist",
            lambda *_args, **_kwargs: [{"name": "VBAT"}],
        )
        monkeypatch.setattr(
            overview_service_module,
            "get_all_telemetry_channels_for_source",
            lambda *_args, **_kwargs: [
                {"name": "VBAT", "aliases": [], "channel_origin": "catalog", "discovery_namespace": None}
            ],
        )
        monkeypatch.setattr(overview_service_module, "infer_subsystem", lambda _name, _meta: "power")
        monkeypatch.setattr(overview_service_module, "_get_recent_for_sparkline", lambda *_args, **_kwargs: [])
        monkeypatch.setattr(overview_service_module, "_compute_state", lambda value, *_args: ("normal", None))

        result = get_overview(db, source_id="simulator")

        assert result[0]["name"] == "VBAT"
        assert result[0]["current_value"] == 4.2
        assert result[0]["last_timestamp"] == "2026-03-28T12:00:00+00:00"

    def test_get_anomalies_accepts_source_id(self, monkeypatch) -> None:
        """get_anomalies accepts source_id parameter."""
        from unittest.mock import MagicMock

        db = MagicMock()
        monkeypatch.setattr(
            overview_service_module,
            "resolve_latest_stream_id",
            lambda _db, source_id: source_id,
        )
        monkeypatch.setattr(
            overview_service_module,
            "_resolve_logical_source_id",
            lambda _db, source_id: source_id,
        )
        db.execute = MagicMock(return_value=MagicMock(fetchall=MagicMock(return_value=[])))
        result = get_anomalies(db, source_id="mock_vehicle")
        assert "power" in result
        assert "thermal" in result
        assert "adcs" in result
        assert "comms" in result
        assert "other" in result
