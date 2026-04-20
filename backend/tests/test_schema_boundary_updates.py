from __future__ import annotations

import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

sys.modules.setdefault(
    "app.services.embedding_service",
    SimpleNamespace(SentenceTransformerEmbeddingProvider=object),
)
sys.modules.setdefault(
    "app.services.llm_service",
    SimpleNamespace(
        MockLLMProvider=object,
        OpenAICompatibleLLMProvider=object,
    ),
)

from app.models.schemas import (
    ActiveStreamUpdate,
    MeasurementEvent,
    SourceObservationBatchUpsert,
    SourceObservationUpsert,
    TelemetryDataIngest,
    TelemetrySchemaCreate,
    WatchlistAddRequest,
)
from app.models.telemetry import TelemetrySource
from app.routes import ops as ops_routes
from app.routes import realtime as realtime_routes
from app.routes import telemetry as telemetry_routes
from app.services import overview_service as overview_service_module
from app.services import realtime_service
from app.services.source_stream_service import (
    SourceNotFoundError,
    StreamIdConflictError,
    ensure_stream_belongs_to_source,
)


class _ScalarResult:
    def __init__(self, value):
        self._value = value

    def scalars(self):
        return self

    def first(self):
        return self._value

    def all(self):
        return self._value


class _FetchAllResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


def test_telemetry_routes_use_source_id_request_fields(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeService:
        def __init__(self, *_args):
            pass

        def create_schema(self, **kwargs):
            captured["create_schema"] = kwargs
            return uuid4()

        def insert_data(
            self,
            stream_id: str,
            telemetry_name: str,
            data,
            *,
            source_id: str | None = None,
            packet_source: str | None = None,
            receiver_id: str | None = None,
        ):
            captured["insert_data"] = {
                "stream_id": stream_id,
                "source_id": source_id,
                "telemetry_name": telemetry_name,
                "rows": len(data),
                "packet_source": packet_source,
                "receiver_id": receiver_id,
            }
            return len(data)

    monkeypatch.setattr(telemetry_routes, "TelemetryService", FakeService)
    monkeypatch.setattr(telemetry_routes, "audit_log", lambda *_args, **_kwargs: None)

    add_calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        telemetry_routes,
        "add_to_watchlist",
        lambda _db, source_id, telemetry_name: add_calls.append((source_id, telemetry_name)),
    )

    telemetry_routes.create_schema(
        body=TelemetrySchemaCreate(source_id="source-a", name="VBAT", units="V"),
        db=MagicMock(),
        embedding=object(),
        llm=object(),
    )

    telemetry_routes.ingest_data(
        body=TelemetryDataIngest(
            telemetry_name="VBAT",
            data=[{"timestamp": "2026-03-28T12:00:00Z", "value": 4.2}],
            source_id="source-a",
            stream_id="source-a-2026-03-28T12-00-00Z",
            packet_source="ground-station-a",
            receiver_id="rx-7",
        ),
        db=MagicMock(),
        embedding=object(),
        llm=object(),
    )

    telemetry_routes.add_watchlist(
        body=WatchlistAddRequest(source_id="source-a", telemetry_name="VBAT"),
        db=MagicMock(),
    )

    assert captured["create_schema"] == {
        "source_id": "source-a",
        "name": "VBAT",
        "units": "V",
        "description": None,
        "subsystem_tag": None,
        "red_low": None,
        "red_high": None,
    }
    assert captured["insert_data"] == {
        "stream_id": "source-a-2026-03-28T12-00-00Z",
        "source_id": "source-a",
        "telemetry_name": "VBAT",
        "rows": 1,
        "packet_source": "ground-station-a",
        "receiver_id": "rx-7",
    }
    assert add_calls == [("source-a", "VBAT")]


def test_set_active_stream_registers_new_stream_ids(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(telemetry_routes, "audit_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(telemetry_routes, "get_stream_source_id", lambda *_args: None)
    monkeypatch.setattr(
        telemetry_routes,
        "register_stream",
        lambda _db, *, source_id, stream_id, **_kwargs: captured.update(
            source_id=source_id,
            stream_id=stream_id,
        ),
    )

    response = telemetry_routes.set_active_stream(
        body=ActiveStreamUpdate(
            source_id="source-a",
            stream_id="2d2cc0c2-5a5a-4ac6-8f2d-7d04d6c35b0e",
            state="active",
        ),
        db=MagicMock(),
    )

    assert response == {
        "status": "active",
        "source_id": "source-a",
        "stream_id": "2d2cc0c2-5a5a-4ac6-8f2d-7d04d6c35b0e",
    }
    assert captured == {
        "source_id": "source-a",
        "stream_id": "2d2cc0c2-5a5a-4ac6-8f2d-7d04d6c35b0e",
    }


def test_set_active_stream_idle_clears_active_stream(monkeypatch) -> None:
    cleared: list[str] = []
    monkeypatch.setattr(telemetry_routes, "audit_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        telemetry_routes,
        "clear_active_stream",
        lambda source_id, *, db=None: cleared.append(source_id),
    )

    response = telemetry_routes.set_active_stream(
        body=ActiveStreamUpdate(source_id="source-a", state="idle"),
        db=MagicMock(),
    )

    assert response == {"status": "idle", "source_id": "source-a"}
    assert cleared == ["source-a"]


def test_active_run_route_is_removed() -> None:
    paths = {route.path for route in telemetry_routes.router.routes}
    assert "/sources/active-run" not in paths
    assert "/sources/active-stream" in paths
    assert "/sources/{source_id}/observations/upcoming" in paths
    assert "/sources/{source_id}/observations/next" in paths
    assert "/sources/{source_id}/observations:batch-upsert" in paths


def test_source_observation_request_rejects_invalid_time_range() -> None:
    with pytest.raises(ValueError, match="end_time must be after start_time"):
        SourceObservationUpsert(
            external_id="obs-invalid",
            start_time=datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc),
        )


def test_source_observation_routes_normalize_source_and_call_service(monkeypatch) -> None:
    calls: dict[str, object] = {}

    monkeypatch.setattr(telemetry_routes, "normalize_source_id", lambda source_id: f"normalized-{source_id}")
    monkeypatch.setattr(
        telemetry_routes,
        "upsert_source_observations",
        lambda db, *, source_id, batch, now: calls.update(
            {"source_id": source_id, "provider": batch.provider, "now": now}
        )
        or SimpleNamespace(inserted=1, deleted=2),
    )

    response = telemetry_routes.batch_upsert_source_observations(
        source_id="source-a",
        body=SourceObservationBatchUpsert(
            provider="satnogs",
            observations=[
                SourceObservationUpsert(
                    external_id="obs-1",
                    start_time=datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc),
                    end_time=datetime(2026, 4, 7, 12, 10, tzinfo=timezone.utc),
                )
            ],
        ),
        db=MagicMock(),
    )

    assert response.inserted == 1
    assert response.deleted == 2
    assert calls["source_id"] == "normalized-source-a"
    assert calls["provider"] == "satnogs"


def test_runtime_route_signature_no_longer_accepts_run_id() -> None:
    with pytest.raises(TypeError):
        telemetry_routes.get_summary_for_source(
            source_id="source-a",
            name="VBAT",
            run_id="legacy-run",
            db=MagicMock(),
        )


def test_resolve_scoped_stream_id_defaults_to_latest_stream(monkeypatch) -> None:
    db = MagicMock()
    monkeypatch.setattr(telemetry_routes, "normalize_source_id", lambda source_id: source_id)
    monkeypatch.setattr(telemetry_routes, "resolve_active_stream_id", lambda _db, source_id: source_id)
    db.execute.return_value = _ScalarResult("source-a-2026-03-28T12-00-00Z")

    assert (
        telemetry_routes._resolve_scoped_stream_id(db, "source-a")
        == "source-a-2026-03-28T12-00-00Z"
    )


def test_resolve_scoped_stream_id_rejects_invalid_explicit_stream(monkeypatch) -> None:
    monkeypatch.setattr(
        telemetry_routes,
        "ensure_stream_belongs_to_source",
        lambda _db, _source_id, _stream_id: (_ for _ in ()).throw(ValueError("Stream not found for source")),
    )

    with pytest.raises(HTTPException) as exc_info:
        telemetry_routes._resolve_scoped_stream_id(MagicMock(), "source-a", "bad-stream")

    assert exc_info.value.status_code == 404


def test_overview_service_resolves_stream_inputs_to_logical_source(monkeypatch) -> None:
    from app.services.overview_service import get_all_telemetry_channels_for_source, get_watchlist

    logical_source_id = "source-a"
    opaque_source_id = "opaque-stream-id"
    telemetry_id = uuid4()
    db = MagicMock()
    captured_sql: list[str] = []
    alias_source_ids: list[str] = []

    first_rows = [(telemetry_id, "VBAT", "catalog", None)]
    second_rows = [(logical_source_id, "VBAT", 0, telemetry_id, "catalog", None)]

    def fake_execute(statement):
        captured_sql.append(str(statement.compile(compile_kwargs={"literal_binds": True})))
        if len(captured_sql) == 1:
            return _FetchAllResult(first_rows)
        return _FetchAllResult(second_rows)

    db.execute.side_effect = fake_execute
    monkeypatch.setattr(
        overview_service_module,
        "_resolve_logical_source_id",
        lambda _db, _source_id: logical_source_id,
    )
    monkeypatch.setattr(
        overview_service_module,
        "get_aliases_by_telemetry_ids",
        lambda _db, *, source_id, telemetry_ids: (
            alias_source_ids.append(source_id)
            or ({telemetry_ids[0]: ["BAT"]} if telemetry_ids else {})
        ),
    )

    channels = get_all_telemetry_channels_for_source(db, opaque_source_id)
    watchlist_rows = get_watchlist(db, opaque_source_id)

    assert channels == [
        {
            "name": "VBAT",
            "aliases": ["BAT"],
            "channel_origin": "catalog",
            "discovery_namespace": None,
        }
    ]
    assert watchlist_rows == [
        {
            "source_id": logical_source_id,
            "name": "VBAT",
            "aliases": ["BAT"],
            "display_order": 0,
            "channel_origin": "catalog",
            "discovery_namespace": None,
        }
    ]
    assert alias_source_ids == [logical_source_id, logical_source_id]
    assert any(f"telemetry_metadata.source_id = '{logical_source_id}'" in sql for sql in captured_sql)
    assert any(f"watchlist.source_id = '{logical_source_id}'" in sql for sql in captured_sql)


def test_resolve_requested_stream_id_uses_explicit_stream_or_latest(monkeypatch) -> None:
    session = MagicMock()
    session_factory = MagicMock(return_value=session)
    monkeypatch.setattr(
        realtime_routes,
        "ensure_stream_belongs_to_source",
        lambda _db, _source_id, stream_id: stream_id,
    )

    explicit = realtime_routes._resolve_requested_stream_id(
        session_factory,
        "source-a",
        "stream-a",
    )
    assert explicit == "stream-a"

    monkeypatch.setattr(realtime_routes, "resolve_latest_stream_id", lambda _db, source_id: f"{source_id}-latest")
    latest = realtime_routes._resolve_requested_stream_id(session_factory, "source-a", None)
    assert latest == "source-a-latest"


def test_resolve_requested_stream_id_rejects_invalid_explicit_stream(monkeypatch) -> None:
    session_factory = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(
        realtime_routes,
        "ensure_stream_belongs_to_source",
        lambda _db, _source_id, _stream_id: (_ for _ in ()).throw(ValueError("Stream not found for source")),
    )

    with pytest.raises(HTTPException) as exc_info:
        realtime_routes._resolve_requested_stream_id(session_factory, "source-a", "bad-stream")

    assert exc_info.value.status_code == 404


def test_get_recent_values_db_only_preserves_explicit_stream_scope(monkeypatch) -> None:
    timestamp = datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc)
    telemetry_id = uuid4()
    captured_params: list[dict[str, object]] = []
    db = MagicMock()

    monkeypatch.setattr(
        telemetry_routes,
        "resolve_latest_stream_id",
        lambda _db, source_id: source_id,
    )
    monkeypatch.setattr(
        telemetry_routes,
        "_get_channel_meta",
        lambda _db, _source_id, _name: SimpleNamespace(id=telemetry_id),
    )

    def fake_execute(statement):
        compiled = statement.compile()
        captured_params.append(dict(compiled.params))
        return _FetchAllResult([(timestamp, 1.0)])

    db.execute.side_effect = fake_execute

    rows = telemetry_routes._get_recent_values_db_only(
        db,
        "VBAT",
        limit=5,
        source_id="stream-a",
    )

    assert rows == [(timestamp, 1.0)]
    assert any("stream-a" in params.values() for params in captured_params)


def test_summary_for_registered_channel_without_samples_returns_no_data(monkeypatch) -> None:
    telemetry_id = uuid4()
    db = MagicMock()
    db.get.return_value = None
    meta = SimpleNamespace(
        id=telemetry_id,
        name="ISS_POS_LON_DEG",
        description="Longitude",
        units="deg",
        channel_origin="catalog",
        discovery_namespace=None,
        red_low=None,
        red_high=None,
    )

    class FakeStatisticsService:
        def __init__(self, _db):
            pass

        def _recompute_one(self, _telemetry_id, *, source_id):
            return None

    monkeypatch.setattr(telemetry_routes, "_get_channel_meta", lambda _db, _source_id, _name: meta)
    monkeypatch.setattr(telemetry_routes, "get_aliases_by_telemetry_ids", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(telemetry_routes, "StatisticsService", FakeStatisticsService)

    result = telemetry_routes._get_explanation_summary_db_only(
        db,
        "ISS_POS_LON_DEG",
        source_id="iss-source",
    )

    assert result.name == "ISS_POS_LON_DEG"
    assert result.recent_value is None
    assert result.statistics.n_samples == 0
    assert result.statistics.mean is None
    assert result.statistics.p50 is None
    assert result.state == "no_data"
    assert result.state_reason == "no_samples"


def test_summary_for_unknown_channel_still_fails(monkeypatch) -> None:
    monkeypatch.setattr(telemetry_routes, "_get_channel_meta", lambda _db, _source_id, _name: None)

    with pytest.raises(ValueError, match="Telemetry not found"):
        telemetry_routes._get_explanation_summary_db_only(
            MagicMock(),
            "UNKNOWN",
            source_id="iss-source",
        )


def test_validate_stream_batch_identities_rejects_unknown_source() -> None:
    db = MagicMock()
    db.get.return_value = None

    with pytest.raises(SourceNotFoundError):
        realtime_routes._validate_stream_batch_identities(
            db,
            [
                MeasurementEvent(
                    source_id="missing-source",
                    stream_id="stream-a",
                    channel_name="VBAT",
                    generation_time="2026-03-28T12:00:00+00:00",
                    value=1.0,
                )
            ],
        )


def test_validate_stream_batch_identities_rejects_mixed_stream_owners(monkeypatch) -> None:
    db = MagicMock()

    def fake_get(model, key):
        if model is TelemetrySource and key in {"source-a", "source-b"}:
            return TelemetrySource(
                id=key,
                name=key,
                source_type="vehicle",
                vehicle_config_path=f"defs/{key}.yaml",
            )
        return None

    db.get.side_effect = fake_get
    monkeypatch.setattr(realtime_routes, "get_stream_source_id", lambda _db, _stream_id: None)

    with pytest.raises(HTTPException) as exc_info:
        realtime_routes._validate_stream_batch_identities(
            db,
            [
                MeasurementEvent(
                    source_id="source-a",
                    stream_id="shared-stream",
                    channel_name="VBAT",
                    generation_time="2026-03-28T12:00:00+00:00",
                    value=1.0,
                ),
                MeasurementEvent(
                    source_id="source-b",
                    stream_id="shared-stream",
                    channel_name="IBAT",
                    generation_time="2026-03-28T12:00:01+00:00",
                    value=2.0,
                ),
            ],
        )

    assert exc_info.value.status_code == 400
    assert "multiple sources" in str(exc_info.value.detail)


def test_resolve_stream_source_id_returns_404_for_unknown_stream(monkeypatch) -> None:
    monkeypatch.setattr(realtime_routes, "get_stream_source_id", lambda _db, _stream_id: None)

    with pytest.raises(HTTPException) as exc_info:
        realtime_routes._resolve_stream_source_id(MagicMock(), "unknown-stream")

    assert exc_info.value.status_code == 404


def test_ops_routes_use_source_id_contract(monkeypatch) -> None:
    now = datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc)
    event = SimpleNamespace(
        id=uuid4(),
        source_id="source-a",
        stream_id="stream-a",
        event_time=now,
        event_type="system.feed_status",
        severity="info",
        summary="Connected",
        entity_type="feed",
        entity_id="source-a",
        payload={"connected": True},
        created_at=now,
    )

    monkeypatch.setattr(
        ops_routes,
        "query_events",
        lambda *args, **kwargs: ([event], 1),
    )

    response = ops_routes.get_timeline_events(
        source_id="source-a",
        scope="streams",
        stream_ids=["stream-a"],
        db=MagicMock(),
    )

    assert response.total == 1
    assert response.events[0].source_id == "source-a"
    assert response.events[0].stream_id == "stream-a"


def test_feed_status_route_returns_source_id(monkeypatch) -> None:
    tracker = MagicMock()
    tracker.get_status.return_value = {
        "source_id": "source-a",
        "connected": True,
        "state": "connected",
        "last_reception_time": None,
        "approx_rate_hz": 2.5,
        "drop_count": 0,
    }
    monkeypatch.setattr(ops_routes, "get_feed_health_tracker", lambda: tracker)

    payload = ops_routes.get_feed_status(source_id="source-a")

    assert payload["source_id"] == "source-a"
    assert payload["connected"] is True


def test_ensure_stream_belongs_to_source_rejects_unknown_explicit_source_stream() -> None:
    db = MagicMock()
    db.get.return_value = None
    db.execute.return_value = _ScalarResult(None)

    with pytest.raises(ValueError):
        ensure_stream_belongs_to_source(db, "source-a", "source-a")


def test_parse_detail_scope_latest_ignores_stray_since_without_error() -> None:
    scope = telemetry_routes._parse_detail_scope_params(
        scope="latest",
        stream_ids=["should-be-ignored"],
        since="not-an-iso8601-timestamp",
        until="also-bad",
    )
    assert scope.mode == "latest"
    assert scope.stream_ids == ()


def test_detail_page_scope_payload_streams() -> None:
    since = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    until = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
    detail = telemetry_routes.DetailDataScope(
        mode="streams",
        stream_ids=("a", "b"),
        since=since,
        until=until,
    )
    payload = telemetry_routes._detail_page_scope_payload(detail)
    assert payload.mode == "streams"
    assert payload.stream_count == 2
    assert payload.stream_ids == ["a", "b"]
    assert payload.window is not None
    assert payload.window.since is not None
    assert "Z" in (payload.window.since or "")
