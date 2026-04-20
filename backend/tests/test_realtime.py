"""Tests for realtime telemetry processing."""

import asyncio
import json
import time
from decimal import Decimal
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from starlette.websockets import WebSocketDisconnect

from app.models.schemas import MeasurementEvent, MeasurementEventBatch, RealtimeChannelUpdate
from app.models.telemetry import TelemetryAlert, TelemetryData, TelemetryMetadata, TelemetrySource
from app.realtime.bus import InProcessEventBus
from app.realtime.processor import (
    RealtimeProcessor,
    _build_channel_name_from_tags,
    _resolve_measurement_channel,
)
from app.routes.realtime import (
    _normalize_event_times,
    _validate_required_sequences,
    _validate_stream_batch_identities,
    websocket_realtime,
)
from app.services.source_stream_service import SourceNotFoundError, StreamIdConflictError
from app.services.telemetry_service import _compute_state


class TestComputeState:
    """Unit tests for _compute_state (alert transition logic)."""

    def test_normal_in_family(self) -> None:
        state, reason = _compute_state(28.0, 0.5, 26.0, 30.0, 0.5)
        assert state == "normal"
        assert reason is None

    def test_warning_out_of_limits_high(self) -> None:
        state, reason = _compute_state(31.0, 2.0, 26.0, 30.0, 0.5)
        assert state == "warning"
        assert reason == "out_of_limits"

    def test_warning_out_of_limits_low(self) -> None:
        state, reason = _compute_state(25.0, -2.0, 26.0, 30.0, 0.5)
        assert state == "warning"
        assert reason == "out_of_limits"

    def test_warning_out_of_family_z_score(self) -> None:
        state, reason = _compute_state(29.0, 2.5, None, None, 0.5)
        assert state == "warning"
        assert reason == "out_of_family"

    def test_caution_near_limits(self) -> None:
        # Within 1 sigma of red_high
        state, reason = _compute_state(29.6, 1.0, 26.0, 30.0, 0.5)
        assert state == "caution"
        assert reason is not None

    def test_caution_z_score_1_5_to_2(self) -> None:
        state, reason = _compute_state(28.9, 1.8, None, None, 0.5)
        assert state == "caution"
        assert reason == "out_of_family"

    def test_no_limits_normal(self) -> None:
        state, reason = _compute_state(28.0, 0.0, None, None, 0.5)
        assert state == "normal"
        assert reason is None

    def test_debounce_consecutive_warnings(self) -> None:
        """Two consecutive warning samples should trigger alert (logic in processor)."""
        v1, _ = _compute_state(31.0, 2.5, 26.0, 30.0, 0.5)
        v2, _ = _compute_state(31.0, 2.5, 26.0, 30.0, 0.5)
        assert v1 == "warning"
        assert v2 == "warning"


@pytest.mark.anyio
async def test_realtime_bus_processes_measurements_in_parallel() -> None:
    bus = InProcessEventBus()
    seen: list[str] = []

    def handler(event: MeasurementEvent) -> None:
        time.sleep(0.2)
        seen.append(event.channel_name)

    bus.subscribe_measurements(handler)
    bus.start()
    started = time.perf_counter()
    for idx in range(4):
        bus.publish_measurement(
            MeasurementEvent(
                source_id="test",

                stream_id="test",
                channel_name=f"CHAN_{idx}",
                generation_time="2026-03-13T00:00:00+00:00",
                reception_time="2026-03-13T00:00:00+00:00",
                value=float(idx),
                quality="valid",
                sequence=idx,
            )
        )

    await asyncio.wait_for(bus._measurement_queue.join(), timeout=2.0)
    elapsed = time.perf_counter() - started
    bus.stop()

    assert sorted(seen) == ["CHAN_0", "CHAN_1", "CHAN_2", "CHAN_3"]
    assert elapsed < 0.5


def test_build_channel_name_from_tags_uses_decoder_namespace() -> None:
    channel_name, namespace = _build_channel_name_from_tags(
        {"decoder": "APRS", "field_name": "Payload Temp"}
    )

    assert channel_name == "decoder.aprs.payload_temp"
    assert namespace == "decoder.aprs"


def test_normalize_event_times_uses_ingest_time_when_reception_missing(monkeypatch) -> None:
    fixed_now = datetime(2026, 3, 27, 16, 30, tzinfo=timezone.utc)

    class _FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr("app.routes.realtime.datetime", _FakeDatetime)

    events = _normalize_event_times(
        [
            MeasurementEvent(
                source_id="source-a",

                stream_id="source-a",
                channel_name="PWR_MAIN_BUS_VOLT",
                generation_time="2026-03-20T12:00:00+00:00",
                value=28.0,
            )
        ]
    )

    assert events[0].reception_time == fixed_now.isoformat()
    assert events[0].reception_time != events[0].generation_time


def test_build_channel_name_from_tags_normalizes_explicit_dynamic_name() -> None:
    channel_name, namespace = _build_channel_name_from_tags(
        {"dynamic_channel_name": "Decoder/APRS/Payload Temp"}
    )

    assert channel_name == "decoder.aprs.payload_temp"
    assert namespace == "decoder.aprs"


def test_resolve_measurement_channel_prefers_dynamic_tags_over_raw_channel_name() -> None:
    channel_name, namespace, allow_dynamic = _resolve_measurement_channel(
        MeasurementEvent(
            source_id="source-a",

            stream_id="source-a",
            channel_name="PayloadTemp",
            generation_time="2026-03-26T12:00:00+00:00",
            value=1.0,
            tags={"decoder": "APRS", "field_name": "Payload Temp"},
        )
    )

    assert channel_name == "decoder.aprs.payload_temp"
    assert namespace == "decoder.aprs"
    assert allow_dynamic is True


def test_resolve_measurement_channel_keeps_strict_explicit_name_without_dynamic_context() -> None:
    channel_name, namespace, allow_dynamic = _resolve_measurement_channel(
        MeasurementEvent(
            source_id="source-a",

            stream_id="source-a",
            channel_name="PWR_MAIN_BUS_VOLT",
            generation_time="2026-03-26T12:00:00+00:00",
            value=1.0,
        )
    )

    assert channel_name == "PWR_MAIN_BUS_VOLT"
    assert namespace is None
    assert allow_dynamic is False


def test_normalize_event_times_synthesizes_generation_from_reception() -> None:
    normalized = _normalize_event_times(
        [
            MeasurementEvent(
                source_id="source-a",

                stream_id="source-a",
                channel_name="PWR_MAIN_BUS_VOLT",
                reception_time="2026-03-26T12:00:01+00:00",
                value=1.0,
            )
        ]
    )

    assert len(normalized) == 1
    assert normalized[0].generation_time == "2026-03-26T12:00:01+00:00"
    assert normalized[0].reception_time == "2026-03-26T12:00:01+00:00"


def test_normalize_event_times_preserves_server_arrival_when_reception_missing() -> None:
    normalized = _normalize_event_times(
        [
            MeasurementEvent(
                source_id="source-a",

                stream_id="source-a",
                channel_name="PWR_MAIN_BUS_VOLT",
                generation_time="2026-03-26T12:00:01+00:00",
                value=1.0,
            )
        ]
    )

    assert len(normalized) == 1
    assert normalized[0].generation_time == "2026-03-26T12:00:01+00:00"
    assert normalized[0].reception_time is not None
    assert normalized[0].reception_time != normalized[0].generation_time


def test_realtime_ingest_requires_sequence() -> None:
    with pytest.raises(HTTPException) as exc:
        _validate_required_sequences(
            [
                MeasurementEvent(
                    source_id="source-a",
                    stream_id="source-a",
                    channel_name="PWR_MAIN_BUS_VOLT",
                    generation_time="2026-03-26T12:00:01+00:00",
                    value=1.0,
                )
            ]
        )

    assert exc.value.status_code == 400
    assert "sequence" in exc.value.detail


def test_validate_stream_batch_identities_allows_reserved_source_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = MagicMock()
    db.get.side_effect = lambda model, key: (
        TelemetrySource(
            id="vehicle-a",
            name="Vehicle A",
            source_type="vehicle",
            vehicle_config_path="defs/vehicle-a.yaml",
        )
        if model is TelemetrySource and key == "vehicle-a"
        else None
    )
    monkeypatch.setattr("app.routes.realtime.get_stream_source_id", lambda *_args, **_kwargs: None)

    _validate_stream_batch_identities(
        db,
        [
            MeasurementEvent(
                source_id="vehicle-a",
                stream_id="vehicle-a",
                channel_name="PWR_MAIN_BUS_VOLT",
                generation_time="2026-03-26T12:00:00+00:00",
                value=1.0,
            )
        ],
    )


@pytest.mark.anyio
@pytest.mark.parametrize("message_type", ["subscribe_watchlist", "subscribe_channel", "subscribe_alerts"])
async def test_websocket_realtime_resolves_latest_stream_without_explicit_stream_id(
    monkeypatch: pytest.MonkeyPatch,
    message_type: str,
) -> None:
    resolve_calls: list[tuple[object, str]] = []
    created_sessions: list[object] = []

    class FakeSession:
        def __init__(self) -> None:
            self.close = MagicMock()

    def session_factory() -> FakeSession:
        session = FakeSession()
        created_sessions.append(session)
        return session

    class FakeHub:
        def __init__(self) -> None:
            self.subscriptions: list[tuple[object, ...]] = []
            self.disconnected = False

        async def connect(self, websocket) -> None:
            await websocket.accept()

        async def disconnect(self, websocket) -> None:
            self.disconnected = True

        async def subscribe_watchlist(self, websocket, channels, source_id="default", stream_id=None) -> None:
            self.subscriptions.append(("watchlist", list(channels), source_id, stream_id))

        async def subscribe_channel(self, websocket, name, source_id="default", stream_id=None) -> None:
            self.subscriptions.append(("channel", name, source_id, stream_id))

        async def subscribe_alerts(self, websocket, source_id="default", stream_id=None) -> None:
            self.subscriptions.append(("alerts", source_id, stream_id))

    class FakeWebSocket:
        def __init__(self, messages: list[str]) -> None:
            self._messages = list(messages)
            self.sent_texts: list[str] = []

        async def accept(self) -> None:
            return None

        async def receive_text(self) -> str:
            if self._messages:
                return self._messages.pop(0)
            raise WebSocketDisconnect(code=1000)

        async def send_text(self, text: str) -> None:
            self.sent_texts.append(text)

    fake_hub = FakeHub()
    snapshot_update = RealtimeChannelUpdate(
        source_id="vehicle-a",
        stream_id="stream-1",
        name="VBAT",
        subsystem_tag="power",
        current_value=28.4,
        generation_time="2026-03-26T12:00:00+00:00",
        reception_time="2026-03-26T12:00:01+00:00",
        state="normal",
    )

    def fake_resolve_latest_stream_id(session, source_id: str) -> str:
        resolve_calls.append((session, source_id))
        return "stream-1"

    monkeypatch.setattr("app.routes.realtime.get_ws_hub", lambda: fake_hub)
    monkeypatch.setattr("app.routes.realtime.get_session_factory", lambda: session_factory)
    monkeypatch.setattr("app.routes.realtime.resolve_latest_stream_id", fake_resolve_latest_stream_id)
    monkeypatch.setattr("app.routes.realtime.get_watchlist_channel_names", lambda *args, **kwargs: ["VBAT"])
    monkeypatch.setattr(
        "app.routes.realtime.get_realtime_snapshot_for_channels",
        lambda *args, **kwargs: [snapshot_update],
    )
    monkeypatch.setattr("app.routes.realtime.get_active_alerts", lambda *args, **kwargs: [])

    message_by_type = {
        "subscribe_watchlist": {"type": "subscribe_watchlist", "source_id": "vehicle-a"},
        "subscribe_channel": {"type": "subscribe_channel", "source_id": "vehicle-a", "name": "VBAT"},
        "subscribe_alerts": {"type": "subscribe_alerts", "source_id": "vehicle-a"},
    }
    expected_subscription = {
        "subscribe_watchlist": ("watchlist", ["VBAT"], "vehicle-a", None),
        "subscribe_channel": ("channel", "VBAT", "vehicle-a", None),
        "subscribe_alerts": ("alerts", "vehicle-a", None),
    }

    ws = FakeWebSocket([json.dumps(message_by_type[message_type])])

    await websocket_realtime(ws)

    assert resolve_calls == [(created_sessions[0], "vehicle-a")]
    created_sessions[0].close.assert_called_once()
    assert fake_hub.subscriptions == [expected_subscription[message_type]]
    assert fake_hub.disconnected is True
    assert ws.sent_texts
    if message_type == "subscribe_alerts":
        assert "snapshot_alerts" in ws.sent_texts[0]
    else:
        assert "stream-1" in ws.sent_texts[0]


@pytest.mark.anyio
@pytest.mark.parametrize("message_type", ["subscribe_watchlist", "subscribe_channel", "subscribe_alerts"])
async def test_websocket_realtime_rejects_explicit_stream_outside_source_scope(
    monkeypatch: pytest.MonkeyPatch,
    message_type: str,
) -> None:
    class FakeSession:
        def __init__(self) -> None:
            self.close = MagicMock()

    class FakeHub:
        def __init__(self) -> None:
            self.subscriptions: list[tuple[object, ...]] = []
            self.disconnected = False

        async def connect(self, websocket) -> None:
            await websocket.accept()

        async def disconnect(self, websocket) -> None:
            self.disconnected = True

        async def subscribe_watchlist(self, websocket, channels, source_id="default", stream_id=None) -> None:
            self.subscriptions.append(("watchlist", list(channels), source_id, stream_id))

        async def subscribe_channel(self, websocket, name, source_id="default", stream_id=None) -> None:
            self.subscriptions.append(("channel", name, source_id, stream_id))

        async def subscribe_alerts(self, websocket, source_id="default", stream_id=None) -> None:
            self.subscriptions.append(("alerts", source_id, stream_id))

    class FakeWebSocket:
        def __init__(self, messages: list[str]) -> None:
            self._messages = list(messages)
            self.sent_texts: list[str] = []

        async def accept(self) -> None:
            return None

        async def receive_text(self) -> str:
            if self._messages:
                return self._messages.pop(0)
            raise WebSocketDisconnect(code=1000)

        async def send_text(self, text: str) -> None:
            self.sent_texts.append(text)

    fake_hub = FakeHub()
    monkeypatch.setattr("app.routes.realtime.get_ws_hub", lambda: fake_hub)
    monkeypatch.setattr("app.routes.realtime.get_session_factory", lambda: lambda: FakeSession())
    monkeypatch.setattr(
        "app.routes.realtime.ensure_stream_belongs_to_source",
        lambda _db, _source_id, _stream_id: (_ for _ in ()).throw(ValueError("Stream not found for source")),
    )

    message_by_type = {
        "subscribe_watchlist": {
            "type": "subscribe_watchlist",
            "source_id": "source-a",
            "stream_id": "source-b-stream",
            "channels": ["VBAT"],
        },
        "subscribe_channel": {
            "type": "subscribe_channel",
            "source_id": "source-a",
            "stream_id": "source-b-stream",
            "name": "VBAT",
        },
        "subscribe_alerts": {
            "type": "subscribe_alerts",
            "source_id": "source-a",
            "stream_id": "source-b-stream",
        },
    }

    ws = FakeWebSocket([json.dumps(message_by_type[message_type])])

    await websocket_realtime(ws)

    assert fake_hub.subscriptions == []
    assert fake_hub.disconnected is True
    assert ws.sent_texts == []


@pytest.mark.anyio
async def test_ingest_realtime_validates_stream_batch_before_ack(monkeypatch: pytest.MonkeyPatch) -> None:
    call_order: list[str] = []

    class FakeBus:
        def measurement_queue_size(self) -> int:
            return 0

        def measurement_queue_maxsize(self) -> int:
            return 100

        def publish_measurement(self, event: MeasurementEvent) -> bool:
            call_order.append(f"publish:{event.channel_name}")
            return True

    class FakeSession:
        def rollback(self) -> None:
            call_order.append("rollback")

        def close(self) -> None:
            call_order.append("close")

    monkeypatch.setattr("app.routes.realtime.get_session_factory", lambda: lambda: FakeSession())
    monkeypatch.setattr("app.routes.realtime.get_realtime_bus", lambda: FakeBus())
    monkeypatch.setattr(
        "app.routes.realtime._validate_stream_batch_identities",
        lambda _db, _events: call_order.append("validated"),
    )
    monkeypatch.setattr("app.routes.realtime.audit_log", lambda *args, **kwargs: None)

    request = SimpleNamespace(
        state=SimpleNamespace(request_id=None),
        headers={},
        method="POST",
        url=SimpleNamespace(path="/telemetry/realtime/ingest"),
    )
    body = MeasurementEventBatch(
        events=[
            MeasurementEvent(
                source_id="source-a",
                stream_id="stream-a",
                channel_name="VBAT",
                generation_time="2026-03-30T12:00:00+00:00",
                value=1.0,
                sequence=1,
            )
        ]
    )

    response = await __import__("app.routes.realtime", fromlist=["ingest_realtime"]).ingest_realtime(body, request)

    assert response == {"accepted": 1}
    assert call_order[:2] == ["validated", "publish:VBAT"]


@pytest.mark.anyio
async def test_ingest_realtime_does_not_ack_failed_stream_batch_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[str] = []

    class FakeBus:
        def measurement_queue_size(self) -> int:
            return 0

        def measurement_queue_maxsize(self) -> int:
            return 100

        def publish_measurement(self, event: MeasurementEvent) -> bool:
            published.append(event.channel_name)
            return True

    class FakeSession:
        def __init__(self) -> None:
            self.rollback_calls = 0
            self.close_calls = 0

        def rollback(self) -> None:
            self.rollback_calls += 1

        def close(self) -> None:
            self.close_calls += 1

    session = FakeSession()
    monkeypatch.setattr("app.routes.realtime.get_session_factory", lambda: lambda: session)
    monkeypatch.setattr("app.routes.realtime.get_realtime_bus", lambda: FakeBus())
    monkeypatch.setattr(
        "app.routes.realtime._validate_stream_batch_identities",
        lambda _db, _events: (_ for _ in ()).throw(SourceNotFoundError("Source not found: source-a")),
    )
    monkeypatch.setattr("app.routes.realtime.audit_log", lambda *args, **kwargs: None)

    request = SimpleNamespace(
        state=SimpleNamespace(request_id=None),
        headers={},
        method="POST",
        url=SimpleNamespace(path="/telemetry/realtime/ingest"),
    )
    body = MeasurementEventBatch(
        events=[
            MeasurementEvent(
                source_id="source-a",
                stream_id="stream-a",
                channel_name="VBAT",
                generation_time="2026-03-30T12:00:00+00:00",
                value=1.0,
                sequence=1,
            )
        ]
    )

    realtime_module = __import__("app.routes.realtime", fromlist=["ingest_realtime"])
    with pytest.raises(HTTPException) as exc_info:
        await realtime_module.ingest_realtime(body, request)

    assert exc_info.value.status_code == 404
    assert published == []
    assert session.rollback_calls == 1
    assert session.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    "message_type",
    ["ack_alert", "resolve_alert"],
)
async def test_websocket_realtime_audit_logs_logical_source_id_for_alert_actions(
    monkeypatch: pytest.MonkeyPatch,
    message_type: str,
) -> None:
    audit_calls: list[tuple[str, dict[str, object]]] = []

    class FakeSession:
        def __init__(self) -> None:
            self.close = MagicMock()
            self.commit = MagicMock()
            self.rollback = MagicMock()
            telemetry_id = uuid4()
            self._alert = SimpleNamespace(
                id=uuid4(),
                telemetry_id=telemetry_id,
                stream_id="stream-1",
                status="new",
                severity="warning",
                reason="out_of_family",
                current_value_at_open=Decimal("4.2"),
                opened_at=datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
                opened_reception_at=datetime(2026, 3, 26, 12, 0, 1, tzinfo=timezone.utc),
                cleared_at=None,
                resolved_at=None,
                acked_at=None,
                acked_by=None,
                resolved_by=None,
                resolution_text=None,
                resolution_code=None,
            )
            self._meta = TelemetryMetadata(
                id=telemetry_id,
                source_id="vehicle-a",
                name="VBAT",
                units="V",
                description=None,
                subsystem_tag="power",
                channel_origin="catalog",
                discovery_namespace=None,
                discovered_at=datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
                last_seen_at=datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
            )

        def get(self, model, key):
            if model is TelemetryAlert:
                return self._alert
            if model is TelemetryMetadata:
                return self._meta
            return None

    def session_factory() -> FakeSession:
        return FakeSession()

    class FakeHub:
        async def connect(self, websocket) -> None:
            await websocket.accept()

        async def disconnect(self, websocket) -> None:
            return None

        async def subscribe_watchlist(self, *args, **kwargs) -> None:
            return None

        async def subscribe_channel(self, *args, **kwargs) -> None:
            return None

        async def subscribe_alerts(self, *args, **kwargs) -> None:
            return None

        async def broadcast_alert_event(self, *args, **kwargs) -> None:
            return None

    class FakeWebSocket:
        def __init__(self, messages: list[str]) -> None:
            self._messages = list(messages)
            self.sent_texts: list[str] = []

        async def accept(self) -> None:
            return None

        async def receive_text(self) -> str:
            if self._messages:
                return self._messages.pop(0)
            raise WebSocketDisconnect(code=1000)

        async def send_text(self, text: str) -> None:
            self.sent_texts.append(text)

    def fake_write_ops_event(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr("app.routes.realtime.get_ws_hub", lambda: FakeHub())
    monkeypatch.setattr("app.routes.realtime.get_session_factory", lambda: session_factory)
    monkeypatch.setattr("app.routes.realtime.audit_log", lambda event, **kwargs: audit_calls.append((event, kwargs)))
    monkeypatch.setattr("app.routes.realtime.write_ops_event", fake_write_ops_event)

    if message_type == "ack_alert":
        ws = FakeWebSocket([json.dumps({"type": "ack_alert", "alert_id": "00000000-0000-0000-0000-000000000001"})])
    else:
        ws = FakeWebSocket(
            [
                json.dumps(
                    {
                        "type": "resolve_alert",
                        "alert_id": "00000000-0000-0000-0000-000000000001",
                        "resolution_text": "fixed",
                        "resolution_code": "manual",
                    }
                )
            ]
        )

    await websocket_realtime(ws)

    audit_event = "alert.acked" if message_type == "ack_alert" else "alert.resolved"
    matching = [kwargs for event, kwargs in audit_calls if event == audit_event]
    assert matching
    assert matching[0]["source_id"] == "vehicle-a"
    assert matching[0]["channel_name"] == "VBAT"


def test_process_measurement_creates_discovered_channel_for_unknown_input(monkeypatch) -> None:
    monkeypatch.setattr("app.realtime.processor.get_realtime_bus", lambda: MagicMock())
    monkeypatch.setattr("app.realtime.processor.register_stream", lambda *args, **kwargs: None)
    processor = RealtimeProcessor()
    db = MagicMock()
    added: list[object] = []
    updates = []
    orbit_submissions = []

    class _ScalarResult:
        def __init__(self, row):
            self._row = row

        def scalars(self):
            return self

        def first(self):
            return self._row

    meta = TelemetryMetadata(
        id=uuid4(),
        source_id="source-a",
        name="decoder.aprs.payload_temp",
        units="",
        description=None,
        subsystem_tag="dynamic",
        channel_origin="discovered",
        discovery_namespace="decoder.aprs",
        discovered_at=datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
    )

    db.execute.return_value = _ScalarResult(None)
    db.get.return_value = None
    db.add.side_effect = added.append

    create_mock = MagicMock(return_value=meta)
    monkeypatch.setattr(
        "app.realtime.processor.create_discovered_channel_metadata",
        create_mock,
    )
    monkeypatch.setattr(processor, "_broadcast_telemetry_update", updates.append)
    monkeypatch.setattr(
        processor,
        "_maybe_submit_orbit_sample",
        lambda *args, **kwargs: orbit_submissions.append(kwargs or args),
    )

    event = _normalize_event_times(
        [
            MeasurementEvent(
                source_id="source-a",

                stream_id="source-a",
                channel_name=None,
                reception_time="2026-03-26T12:00:01+00:00",
                value=42.5,
                sequence=1,
                tags={"decoder": "APRS", "field_name": "Payload Temp"},
            )
        ]
    )[0]

    processor._process_measurement(db, event)

    assert create_mock.call_args.kwargs["source_id"] == "source-a"
    assert any(getattr(obj, "telemetry_id", None) == meta.id for obj in added)
    assert any(getattr(obj, "state", None) == "normal" for obj in added)
    assert len(updates) == 1
    assert updates[0].name == "decoder.aprs.payload_temp"
    assert updates[0].channel_origin == "discovered"
    assert updates[0].discovery_namespace == "decoder.aprs"
    assert orbit_submissions


def test_process_measurement_skips_unknown_explicit_channel_without_dynamic_context(monkeypatch) -> None:
    monkeypatch.setattr("app.realtime.processor.get_realtime_bus", lambda: MagicMock())
    processor = RealtimeProcessor()
    db = MagicMock()
    updates = []

    class _ScalarResult:
        def __init__(self, row):
            self._row = row

        def scalars(self):
            return self

        def first(self):
            return self._row

    db.execute.return_value = _ScalarResult(None)
    db.get.return_value = None

    create_mock = MagicMock()
    monkeypatch.setattr(
        "app.realtime.processor.create_discovered_channel_metadata",
        create_mock,
    )
    monkeypatch.setattr(processor, "_broadcast_telemetry_update", updates.append)

    processor._process_measurement(
        db,
        MeasurementEvent(
            source_id="source-a",

            stream_id="source-a",
            channel_name="PAYLOAD_TEMP_TYPO",
            generation_time="2026-03-26T12:00:00+00:00",
            reception_time="2026-03-26T12:00:01+00:00",
            value=42.5,
            sequence=1,
        ),
    )

    create_mock.assert_not_called()
    db.add.assert_not_called()
    assert updates == []


def test_process_measurement_does_not_record_feed_health_for_rejected_stream(monkeypatch) -> None:
    monkeypatch.setattr("app.realtime.processor.get_realtime_bus", lambda: MagicMock())
    processor = RealtimeProcessor()
    db = MagicMock()
    recorded: list[str] = []

    class _ScalarResult:
        def __init__(self, row):
            self._row = row

        def scalars(self):
            return self

        def first(self):
            return self._row

    class _FeedHealthTracker:
        def record_reception(self, source_id: str) -> None:
            recorded.append(source_id)

    meta = TelemetryMetadata(
        id=uuid4(),
        source_id="vehicle-a",
        name="VBAT",
        units="V",
        description=None,
        subsystem_tag="power",
        channel_origin="catalog",
        discovery_namespace=None,
        discovered_at=datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
    )

    def fake_execute(statement, *args, **kwargs):
        if "telemetry_metadata" in str(statement):
            return _ScalarResult(meta)
        return _ScalarResult(None)

    db.execute.side_effect = fake_execute
    db.get.return_value = None

    monkeypatch.setattr(
        "app.realtime.processor.get_feed_health_tracker",
        lambda: _FeedHealthTracker(),
    )
    monkeypatch.setattr(
        "app.realtime.processor.resolve_channel_name",
        lambda *_args, **_kwargs: "VBAT",
    )
    monkeypatch.setattr(
        "app.realtime.processor.register_stream",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            StreamIdConflictError("stream_id does not belong to source")
        ),
    )

    event = MeasurementEvent(
        source_id="vehicle-a",
        stream_id="vehicle-b-2026-03-26T12-00-00Z",
        channel_name="VBAT",
        generation_time="2026-03-26T12:00:01+00:00",
        reception_time="2026-03-26T12:00:02+00:00",
        value=28.0,
        quality="valid",
        sequence=1,
    )

    with pytest.raises(ValueError):
        processor._process_measurement(db, event)

    assert recorded == []


def test_process_measurement_duplicate_insert_refreshes_stream_and_feed_health(monkeypatch) -> None:
    monkeypatch.setattr("app.realtime.processor.get_realtime_bus", lambda: MagicMock())
    processor = RealtimeProcessor()
    db = MagicMock()
    registered: list[tuple[str, str]] = []
    recorded: list[str] = []

    class _ScalarResult:
        def __init__(self, row):
            self._row = row

        def scalars(self):
            return self

        def first(self):
            return self._row

    class _FeedHealthTracker:
        def record_reception(self, source_id: str) -> None:
            recorded.append(source_id)

    meta = TelemetryMetadata(
        id=uuid4(),
        source_id="vehicle-a",
        name="VBAT",
        units="V",
        description=None,
        subsystem_tag="power",
        channel_origin="catalog",
        discovery_namespace=None,
        discovered_at=datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
    )
    current = SimpleNamespace(
        generation_time=datetime(2026, 3, 26, 12, 0, 1, tzinfo=timezone.utc),
        reception_time=datetime(2026, 3, 26, 12, 0, 2, tzinfo=timezone.utc),
        value=Decimal("28.1"),
        state="normal",
        state_reason=None,
        z_score=None,
        quality="valid",
        sequence=1,
        packet_source="old-source",
        receiver_id="old-receiver",
    )
    savepoint = MagicMock()

    def fake_execute(statement, *args, **kwargs):
        if "telemetry_metadata" in str(statement):
            return _ScalarResult(meta)
        return _ScalarResult(None)

    db.execute.side_effect = fake_execute
    db.get.side_effect = lambda model, key: (
        current
        if model.__name__ == "TelemetryCurrent"
        else None
        if model.__name__ == "TelemetryStatistics"
        else None
    )
    db.begin_nested.return_value = savepoint
    db.flush.side_effect = IntegrityError("insert", {}, Exception("duplicate key"))

    monkeypatch.setattr(
        "app.realtime.processor.get_feed_health_tracker",
        lambda: _FeedHealthTracker(),
    )
    monkeypatch.setattr(
        "app.realtime.processor.resolve_channel_name",
        lambda *_args, **_kwargs: "VBAT",
    )
    monkeypatch.setattr(
        "app.realtime.processor.register_stream",
        lambda *args, **kwargs: registered.append(
            (kwargs["source_id"], kwargs["stream_id"])
        ),
    )
    monkeypatch.setattr(
        processor,
        "_maybe_submit_orbit_sample",
        lambda *args, **kwargs: None,
    )

    processor._process_measurement(
        db,
        MeasurementEvent(
            source_id="vehicle-a",
            stream_id="vehicle-a-2026-03-26T12-00-00Z",
            channel_name="VBAT",
            generation_time="2026-03-26T12:00:01+00:00",
            reception_time="2026-03-26T12:00:02+00:00",
            value=28.0,
            quality="valid",
            sequence=1,
        ),
    )

    savepoint.rollback.assert_called_once()
    assert registered == [("vehicle-a", "vehicle-a-2026-03-26T12-00-00Z")]
    assert recorded == ["vehicle-a"]
    assert current.generation_time == datetime(2026, 3, 26, 12, 0, 1, tzinfo=timezone.utc)


def test_process_measurement_persists_sequence_as_history_identity(monkeypatch) -> None:
    monkeypatch.setattr("app.realtime.processor.get_realtime_bus", lambda: MagicMock())
    monkeypatch.setattr("app.realtime.processor.register_stream", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.realtime.processor.resolve_channel_name", lambda *args, **kwargs: "VBAT")
    processor = RealtimeProcessor()
    db = MagicMock()
    added: list[object] = []

    class _ScalarResult:
        def __init__(self, row):
            self._row = row

        def scalars(self):
            return self

        def first(self):
            return self._row

    meta = TelemetryMetadata(
        id=uuid4(),
        source_id="source-a",
        name="VBAT",
        units="V",
        description="Main bus voltage",
        subsystem_tag="power",
        channel_origin="catalog",
    )
    def fake_execute(statement, *args, **kwargs):
        if "telemetry_metadata" in str(statement):
            return _ScalarResult(meta)
        return _ScalarResult(None)

    db.execute.side_effect = fake_execute
    db.get.return_value = None
    db.add.side_effect = added.append
    monkeypatch.setattr(processor, "_broadcast_telemetry_update", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(processor, "_maybe_submit_orbit_sample", lambda *args, **kwargs: None)

    for sequence, value in [(7, 28.1), (19, 28.4)]:
        processor._process_measurement(
            db,
            MeasurementEvent(
                source_id="source-a",
                stream_id="satnogs-obs-13787109",
                channel_name="VBAT",
                generation_time="2026-04-11T18:14:53+00:00",
                reception_time="2026-04-11T18:14:53+00:00",
                value=value,
                quality="valid",
                sequence=sequence,
            ),
        )

    history_rows = [item for item in added if isinstance(item, TelemetryData)]
    assert [(row.sequence, float(row.value)) for row in history_rows] == [(7, 28.1), (19, 28.4)]


def test_process_measurement_same_time_current_tie_breaks_by_sequence(monkeypatch) -> None:
    monkeypatch.setattr("app.realtime.processor.get_realtime_bus", lambda: MagicMock())
    monkeypatch.setattr("app.realtime.processor.register_stream", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.realtime.processor.resolve_channel_name", lambda *args, **kwargs: "VBAT")
    processor = RealtimeProcessor()
    db = MagicMock()
    updates = []

    class _ScalarResult:
        def __init__(self, row):
            self._row = row

        def scalars(self):
            return self

        def first(self):
            return self._row

    meta = TelemetryMetadata(
        id=uuid4(),
        source_id="source-a",
        name="VBAT",
        units="V",
        description="Main bus voltage",
        subsystem_tag="power",
        channel_origin="catalog",
    )
    current = SimpleNamespace(
        generation_time=datetime(2026, 4, 11, 18, 14, 53, tzinfo=timezone.utc),
        reception_time=datetime(2026, 4, 11, 18, 14, 53, tzinfo=timezone.utc),
        value=Decimal("28.1"),
        state="normal",
        state_reason=None,
        z_score=None,
        quality="valid",
        sequence=7,
        packet_source="OK0LSR",
        receiver_id="satnogs-station-42",
    )

    db.execute.return_value = _ScalarResult(meta)
    db.get.side_effect = lambda model, key: (
        current
        if model.__name__ == "TelemetryCurrent"
        else None
        if model.__name__ == "TelemetryStatistics"
        else None
    )
    monkeypatch.setattr(processor, "_broadcast_telemetry_update", updates.append)
    monkeypatch.setattr(processor, "_maybe_submit_orbit_sample", lambda *args, **kwargs: None)

    processor._process_measurement(
        db,
        MeasurementEvent(
            source_id="source-a",
            stream_id="satnogs-obs-13787109",
            channel_name="VBAT",
            generation_time="2026-04-11T18:14:53+00:00",
            reception_time="2026-04-11T18:14:53+00:00",
            value=28.4,
            quality="valid",
            sequence=19,
        ),
    )

    assert current.sequence == 19
    assert current.value == Decimal("28.4")
    assert len(updates) == 1

    processor._process_measurement(
        db,
        MeasurementEvent(
            source_id="source-a",
            stream_id="satnogs-obs-13787109",
            channel_name="VBAT",
            generation_time="2026-04-11T18:14:53+00:00",
            reception_time="2026-04-11T18:14:53+00:00",
            value=27.9,
            quality="valid",
            sequence=18,
        ),
    )

    assert current.sequence == 19
    assert current.value == Decimal("28.4")
    assert len(updates) == 1


def test_process_measurement_resolves_explicit_channel_alias_to_canonical(monkeypatch) -> None:
    monkeypatch.setattr("app.realtime.processor.get_realtime_bus", lambda: MagicMock())
    monkeypatch.setattr(
        "app.realtime.processor.resolve_channel_name",
        lambda *_args, **_kwargs: "PWR_MAIN_BUS_VOLT",
    )
    monkeypatch.setattr("app.realtime.processor.register_stream", lambda *args, **kwargs: None)
    processor = RealtimeProcessor()
    db = MagicMock()
    updates = []

    class _ScalarResult:
        def __init__(self, row):
            self._row = row

        def scalars(self):
            return self

        def first(self):
            return self._row

    meta = TelemetryMetadata(
        id=uuid4(),
        source_id="source-a",
        name="PWR_MAIN_BUS_VOLT",
        units="V",
        description="Main bus voltage",
        subsystem_tag="power",
        channel_origin="catalog",
    )
    db.execute.return_value = _ScalarResult(meta)
    db.get.return_value = None
    monkeypatch.setattr(processor, "_broadcast_telemetry_update", updates.append)
    monkeypatch.setattr(processor, "_maybe_submit_orbit_sample", lambda *args, **kwargs: None)

    processor._process_measurement(
        db,
        MeasurementEvent(
            source_id="source-a",

            stream_id="source-a",
            channel_name="VBAT",
            generation_time="2026-03-26T12:00:00+00:00",
            reception_time="2026-03-26T12:00:01+00:00",
            value=28.1,
            sequence=1,
        ),
    )

    assert len(updates) == 1
    assert updates[0].name == "PWR_MAIN_BUS_VOLT"


def test_process_measurement_uses_dynamic_tags_even_when_raw_channel_name_is_present(monkeypatch) -> None:
    monkeypatch.setattr("app.realtime.processor.get_realtime_bus", lambda: MagicMock())
    monkeypatch.setattr("app.realtime.processor.register_stream", lambda *args, **kwargs: None)
    processor = RealtimeProcessor()
    db = MagicMock()
    added: list[object] = []
    updates = []

    class _ScalarResult:
        def __init__(self, row):
            self._row = row

        def scalars(self):
            return self

        def first(self):
            return self._row

    meta = TelemetryMetadata(
        id=uuid4(),
        source_id="source-a",
        name="decoder.aprs.payload_temp",
        units="",
        description=None,
        subsystem_tag="dynamic",
        channel_origin="discovered",
        discovery_namespace="decoder.aprs",
        discovered_at=datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
    )
    db.execute.return_value = _ScalarResult(None)
    db.get.return_value = None
    db.add.side_effect = added.append

    create_mock = MagicMock(return_value=meta)
    monkeypatch.setattr(
        "app.realtime.processor.create_discovered_channel_metadata",
        create_mock,
    )
    monkeypatch.setattr(processor, "_broadcast_telemetry_update", updates.append)
    monkeypatch.setattr(processor, "_maybe_submit_orbit_sample", lambda *args, **kwargs: None)

    processor._process_measurement(
        db,
        MeasurementEvent(
            source_id="source-a",

            stream_id="source-a",
            channel_name="PayloadTemp",
            generation_time="2026-03-26T12:00:00+00:00",
            reception_time="2026-03-26T12:00:01+00:00",
            value=42.5,
            sequence=1,
            tags={"decoder": "APRS", "field_name": "Payload Temp"},
        ),
    )

    create_mock.assert_called_once()
    assert create_mock.call_args.kwargs["channel_name"] == "decoder.aprs.payload_temp"
    assert create_mock.call_args.kwargs["discovery_namespace"] == "decoder.aprs"
    assert len(updates) == 1
    assert updates[0].name == "decoder.aprs.payload_temp"


def test_process_measurement_duplicate_first_dynamic_sample_keeps_discovered_metadata(monkeypatch) -> None:
    monkeypatch.setattr("app.realtime.processor.get_realtime_bus", lambda: MagicMock())
    monkeypatch.setattr("app.realtime.processor.register_stream", lambda *args, **kwargs: None)
    processor = RealtimeProcessor()
    db = MagicMock()
    added: list[object] = []
    updates = []
    orbit_submissions = []

    class _ScalarResult:
        def __init__(self, row):
            self._row = row

        def scalars(self):
            return self

        def first(self):
            return self._row

    meta = TelemetryMetadata(
        id=uuid4(),
        source_id="source-a",
        name="decoder.aprs.payload_temp",
        units="",
        description=None,
        subsystem_tag="dynamic",
        channel_origin="discovered",
        discovery_namespace="decoder.aprs",
        discovered_at=datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
    )
    savepoint = MagicMock()

    db.execute.return_value = _ScalarResult(None)
    db.get.return_value = None
    db.add.side_effect = added.append
    db.begin_nested.return_value = savepoint
    db.flush.side_effect = IntegrityError("insert", {}, Exception("duplicate key"))

    monkeypatch.setattr(
        "app.realtime.processor.create_discovered_channel_metadata",
        lambda *args, **kwargs: meta,
    )
    monkeypatch.setattr(processor, "_broadcast_telemetry_update", updates.append)
    monkeypatch.setattr(
        processor,
        "_maybe_submit_orbit_sample",
        lambda *args, **kwargs: orbit_submissions.append(kwargs or args),
    )

    processor._process_measurement(
        db,
        MeasurementEvent(
            source_id="source-a",

            stream_id="source-a",
            channel_name=None,
            generation_time="2026-03-26T12:00:00+00:00",
            reception_time="2026-03-26T12:00:01+00:00",
            value=42.5,
            sequence=1,
            tags={"decoder": "APRS", "field_name": "Payload Temp"},
        ),
    )

    savepoint.rollback.assert_called_once()
    db.rollback.assert_not_called()
    assert any(getattr(obj, "state", None) == "normal" for obj in added)
    assert len(updates) == 1
    assert updates[0].name == "decoder.aprs.payload_temp"
    assert orbit_submissions


def test_process_measurement_refreshes_current_packet_identity(monkeypatch) -> None:
    monkeypatch.setattr("app.realtime.processor.get_realtime_bus", lambda: MagicMock())
    monkeypatch.setattr("app.realtime.processor.register_stream", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.realtime.processor.resolve_channel_name", lambda *args, **kwargs: "VBAT")
    processor = RealtimeProcessor()
    db = MagicMock()
    updates = []

    class _ScalarResult:
        def __init__(self, row):
            self._row = row

        def scalars(self):
            return self

        def first(self):
            return self._row

    meta = TelemetryMetadata(
        id=uuid4(),
        source_id="source-a",
        name="VBAT",
        units="V",
        description="Main bus voltage",
        subsystem_tag="power",
        channel_origin="catalog",
    )
    current = SimpleNamespace(
        generation_time=datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
        reception_time=datetime(2026, 3, 26, 12, 0, 1, tzinfo=timezone.utc),
        value=Decimal("28.1"),
        state="normal",
        state_reason=None,
        z_score=None,
        quality="valid",
        sequence=1,
        packet_source="old-source",
        receiver_id="old-receiver",
    )
    savepoint = MagicMock()

    db.execute.return_value = _ScalarResult(meta)

    def fake_get(model, key):
        if model.__name__ == "TelemetryCurrent":
            return current
        if model.__name__ == "TelemetryStatistics":
            return None
        return None

    db.get.side_effect = fake_get
    db.begin_nested.return_value = savepoint
    db.flush.return_value = None
    monkeypatch.setattr(processor, "_broadcast_telemetry_update", updates.append)
    monkeypatch.setattr(processor, "_maybe_submit_orbit_sample", lambda *args, **kwargs: None)

    processor._process_measurement(
        db,
        MeasurementEvent(
            source_id="source-a",
            stream_id="source-a",
            channel_name="VBAT",
            generation_time="2026-03-26T12:00:02+00:00",
            reception_time="2026-03-26T12:00:03+00:00",
            value=28.4,
            packet_source="new-source",
            receiver_id="new-receiver",
            quality="valid",
            sequence=2,
        ),
    )

    assert current.packet_source == "new-source"
    assert current.receiver_id == "new-receiver"
    assert current.generation_time == datetime(2026, 3, 26, 12, 0, 2, tzinfo=timezone.utc)
    assert current.reception_time == datetime(2026, 3, 26, 12, 0, 3, tzinfo=timezone.utc)
    assert len(updates) == 1
