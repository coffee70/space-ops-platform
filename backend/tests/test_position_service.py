"""Tests for source/stream helpers and position service boundaries."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from app.models.schemas import PositionChannelMappingUpsert, PositionSample
from app.models.telemetry import PositionChannelMapping, TelemetrySource, TelemetryStream
from app.services import position_service
from app.services.source_stream_service import (
    StreamIdConflictError,
    _get_cached_active_stream_entry,
    clear_active_stream,
    normalize_source_id,
    register_stream,
    resolve_active_stream_id,
    resolve_latest_stream_id,
)


DROGONSAT_SOURCE_ID = "test-drogonsat-source"


class _EmptyResult:
    def scalars(self):
        return self

    def first(self):
        return None

    def all(self):
        return []


class _ScalarResult:
    def __init__(self, value):
        self._value = value

    def scalars(self):
        return self

    def first(self):
        return self._value

    def all(self):
        return self._value


class _HttpxResponse:
    def __init__(self, payload: dict[str, object], status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict[str, object]:
        return self._payload


class _HttpxClient:
    def __init__(self, response: _HttpxResponse):
        self._response = response

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, _url: str):
        return self._response


def test_normalize_source_id_keeps_source_id_exact() -> None:
    assert normalize_source_id("simulator") == "simulator"


def test_resolve_active_stream_id_uses_simulator_status(monkeypatch) -> None:
    clear_active_stream(DROGONSAT_SOURCE_ID)
    stream_id = f"{DROGONSAT_SOURCE_ID}-2026-03-13T17-12-34Z"
    source = TelemetrySource(
        id=DROGONSAT_SOURCE_ID,
        name="DrogonSat",
        source_type="simulator",
        base_url="http://simulator:8001",
        vehicle_config_path="defs/drogonsat.yaml",
    )
    stream = SimpleNamespace(
        id=stream_id,
        source_id=DROGONSAT_SOURCE_ID,
        status="idle",
        packet_source=None,
        receiver_id=None,
        last_seen_at=None,
        started_at=None,
    )
    db = MagicMock()

    def fake_get(model, key):
        if model is TelemetrySource and key == DROGONSAT_SOURCE_ID:
            return source
        if model.__name__ == "TelemetryStream" and key == stream_id:
            return stream
        return None

    db.get.side_effect = fake_get
    db.execute.side_effect = [_EmptyResult(), _EmptyResult(), _EmptyResult()]
    monkeypatch.setattr(
        "app.services.source_stream_service.httpx.Client",
        lambda timeout=2.0: _HttpxClient(
            _HttpxResponse({"state": "running", "config": {"stream_id": stream_id}})
        ),
    )

    try:
        assert resolve_active_stream_id(db, DROGONSAT_SOURCE_ID) == stream_id
        assert _get_cached_active_stream_entry(DROGONSAT_SOURCE_ID)[0] == stream_id
    finally:
        clear_active_stream(DROGONSAT_SOURCE_ID)


def test_resolve_active_stream_id_returns_logical_source_when_simulator_is_idle(monkeypatch) -> None:
    clear_active_stream(DROGONSAT_SOURCE_ID)
    source = TelemetrySource(
        id=DROGONSAT_SOURCE_ID,
        name="DrogonSat",
        source_type="simulator",
        base_url="http://simulator:8001",
        vehicle_config_path="defs/drogonsat.yaml",
    )
    db = MagicMock()
    db.get.side_effect = lambda model, key: source if model is TelemetrySource and key == DROGONSAT_SOURCE_ID else None
    db.execute.side_effect = [_EmptyResult(), _EmptyResult(), _EmptyResult()]

    monkeypatch.setattr(
        "app.services.source_stream_service.httpx.Client",
        lambda timeout=2.0: _HttpxClient(
            _HttpxResponse({"state": "idle", "config": {"stream_id": "stale-stream"}})
        ),
    )

    assert resolve_active_stream_id(db, DROGONSAT_SOURCE_ID) == DROGONSAT_SOURCE_ID


def test_resolve_latest_stream_id_preserves_explicit_stream_id(monkeypatch) -> None:
    stream_id = f"{DROGONSAT_SOURCE_ID}-2026-03-13T17-12-34Z"
    db = MagicMock()
    monkeypatch.setattr(
        "app.services.source_stream_service.get_stream_source_id",
        lambda _db, value: DROGONSAT_SOURCE_ID if value == stream_id else None,
    )

    assert resolve_latest_stream_id(db, stream_id) == stream_id


def test_resolve_latest_stream_id_recovers_most_recent_stream_when_idle(monkeypatch) -> None:
    latest_stream_id = f"{DROGONSAT_SOURCE_ID}-2026-03-13T17-12-34Z"
    latest_seen_at = datetime(2026, 3, 13, 17, 12, 34, tzinfo=timezone.utc)
    db = MagicMock()
    monkeypatch.setattr(
        "app.services.source_stream_service.get_stream_source_id",
        lambda _db, _source_id: None,
    )
    monkeypatch.setattr(
        "app.services.source_stream_service.resolve_active_stream_id",
        lambda _db, source_id, timeout=2.0: source_id,
    )
    db.execute.side_effect = [
        _ScalarResult((latest_stream_id, latest_seen_at)),
        _EmptyResult(),
    ]

    assert resolve_latest_stream_id(db, DROGONSAT_SOURCE_ID) == latest_stream_id


def test_resolve_latest_stream_id_uses_history_only_stream_when_registry_missing(monkeypatch) -> None:
    history_stream_id = f"{DROGONSAT_SOURCE_ID}-2026-03-14T17-12-34Z"
    history_seen_at = datetime(2026, 3, 14, 17, 12, 34, tzinfo=timezone.utc)
    db = MagicMock()
    monkeypatch.setattr(
        "app.services.source_stream_service.get_stream_source_id",
        lambda _db, _source_id: None,
    )
    monkeypatch.setattr(
        "app.services.source_stream_service.resolve_active_stream_id",
        lambda _db, source_id, timeout=2.0: source_id,
    )
    db.execute.side_effect = [
        _EmptyResult(),
        _ScalarResult((history_stream_id, history_seen_at)),
    ]

    assert resolve_latest_stream_id(db, DROGONSAT_SOURCE_ID) == history_stream_id


def test_resolve_latest_stream_id_prefers_newer_history_stream_over_registry(monkeypatch) -> None:
    registry_stream_id = f"{DROGONSAT_SOURCE_ID}-2026-03-13T17-12-34Z"
    history_stream_id = f"{DROGONSAT_SOURCE_ID}-2026-03-14T17-12-34Z"
    registry_seen_at = datetime(2026, 3, 13, 17, 12, 34, tzinfo=timezone.utc)
    history_seen_at = datetime(2026, 3, 14, 17, 12, 34, tzinfo=timezone.utc)
    db = MagicMock()
    monkeypatch.setattr(
        "app.services.source_stream_service.get_stream_source_id",
        lambda _db, _source_id: None,
    )
    monkeypatch.setattr(
        "app.services.source_stream_service.resolve_active_stream_id",
        lambda _db, source_id, timeout=2.0: source_id,
    )
    db.execute.side_effect = [
        _ScalarResult((registry_stream_id, registry_seen_at)),
        _ScalarResult((history_stream_id, history_seen_at)),
    ]

    assert resolve_latest_stream_id(db, DROGONSAT_SOURCE_ID) == history_stream_id


def test_register_stream_rejects_reserved_source_collision() -> None:
    source_id = "source-a"
    db = MagicMock()

    def fake_get(model, key):
        if model is TelemetrySource and key == source_id:
            return TelemetrySource(
                id=source_id,
                name="Source A",
                source_type="vehicle",
                vehicle_config_path="defs/source-a.yaml",
            )
        if model is TelemetrySource and key == "source-b":
            return TelemetrySource(
                id="source-b",
                name="Source B",
                source_type="vehicle",
                vehicle_config_path="defs/source-b.yaml",
            )
        return None

    db.get.side_effect = fake_get

    with pytest.raises(StreamIdConflictError):
        register_stream(db, source_id=source_id, stream_id="source-b")


def test_clear_active_stream_marks_persisted_active_streams_idle() -> None:
    source_id = DROGONSAT_SOURCE_ID
    stream = TelemetryStream(id="stream-a", source_id=source_id, status="active")
    db = MagicMock()

    class _AllResult:
        def scalars(self):
            return self

        def all(self):
            return [stream]

    db.execute.return_value = _AllResult()

    clear_active_stream(source_id, db=db)

    assert stream.status == "idle"


def test_upsert_mapping_keeps_vehicle_surface_and_persists_source_id(monkeypatch) -> None:
    db = MagicMock()
    source = TelemetrySource(
        id="vehicle-a",
        name="Vehicle A",
        source_type="vehicle",
        vehicle_config_path="defs/vehicle-a.yaml",
    )

    class _LockResult:
        def scalars(self):
            return self

        def first(self):
            return source

    class _ExistingResult:
        def scalars(self):
            return self

        def first(self):
            return None

    db.execute.side_effect = [_LockResult(), _ExistingResult()]
    monkeypatch.setattr(
        position_service,
        "_resolve_mapping_channel_name",
        lambda _db, _source_id, channel_name: channel_name,
    )

    mapping = position_service.upsert_mapping(
        db,
        PositionChannelMappingUpsert(
            vehicle_id="vehicle-a",
            frame_type="gps_lla",
            lat_channel_name="GPS_LAT",
            lon_channel_name="GPS_LON",
            alt_channel_name="GPS_ALT",
            active=True,
        ),
    )

    assert mapping.source_id == "vehicle-a"
    db.add.assert_called_once()
    db.flush.assert_called_once()


def test_get_latest_positions_uses_vehicle_ids_filter_and_latest_stream(monkeypatch) -> None:
    mapping = PositionChannelMapping(
        id=uuid4(),
        source_id="vehicle-a",
        frame_type="gps_lla",
        lat_channel_name="GPS_LAT",
        lon_channel_name="GPS_LON",
        alt_channel_name="GPS_ALT",
        active=True,
    )
    source = TelemetrySource(
        id="vehicle-a",
        name="Vehicle A",
        source_type="vehicle",
        vehicle_config_path="defs/vehicle-a.yaml",
    )
    db = MagicMock()

    class _MappingsResult:
        def scalars(self):
            return self

        def all(self):
            return [mapping]

    class _SourcesResult:
        def scalars(self):
            return self

        def all(self):
            return [source]

    db.execute.side_effect = [_MappingsResult(), _SourcesResult()]
    monkeypatch.setattr(
        position_service,
        "resolve_latest_stream_id",
        lambda _db, source_id: f"{source_id}-stream",
    )
    seen_stream_ids: list[str] = []
    monkeypatch.setattr(
        position_service,
        "_build_sample_for_mapping",
        lambda _db, mapping_arg, source_arg, *, data_source_id, now, staleness: (
            seen_stream_ids.append(data_source_id)
            or PositionSample(
                vehicle_id=source_arg.id,
                vehicle_name=source_arg.name,
                vehicle_type=source_arg.source_type,
                lat_deg=1.0,
                lon_deg=2.0,
                alt_m=3.0,
                timestamp=now.isoformat(),
                valid=True,
                frame_type=mapping_arg.frame_type,
                raw_channels={"lat": 1.0, "lon": 2.0, "alt": 3.0},
            )
        ),
    )

    samples = position_service.get_latest_positions(db, vehicle_ids=["vehicle-a"])

    assert len(samples) == 1
    assert samples[0].vehicle_id == "vehicle-a"
    assert seen_stream_ids == ["vehicle-a-stream"]
