"""Regression tests for stream-registry-backed source resolution."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

from app.models.telemetry import TelemetryMetadata, TelemetrySource, TelemetryStream
from app.services.channel_alias_service import (
    get_aliases_by_telemetry_ids,
    resolve_channel_metadata,
)
from app.services.source_stream_service import (
    _get_cached_active_stream_entry,
    clear_active_stream,
    get_stream_source_id,
    register_stream,
)


class _ScalarResult:
    def __init__(self, row):
        self._row = row

    def scalars(self):
        return self

    def first(self):
        return self._row


def test_resolve_channel_metadata_uses_registered_stream_owner() -> None:
    source_id = "source-a"
    stream_id = "stream-uuid-owned"
    meta = TelemetryMetadata(
        id=uuid4(),
        source_id=source_id,
        name="battery.voltage",
        units="V",
        description=None,
        subsystem_tag="power",
        channel_origin="catalog",
        discovery_namespace=None,
        discovered_at=datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc),
    )
    db = MagicMock()
    db.get.side_effect = lambda model, key: (
        TelemetryStream(id=stream_id, source_id=source_id, status="active")
        if model is TelemetryStream and key == stream_id
        else None
    )
    db.execute.side_effect = [_ScalarResult(meta)]

    resolved = resolve_channel_metadata(db, source_id=stream_id, channel_name="battery.voltage")

    assert resolved is meta
    statement = db.execute.call_args.args[0]
    assert source_id in statement.compile().params.values()


def test_resolve_channel_metadata_prefers_exact_source_lookup_for_source_ids() -> None:
    source_id = "source-a"
    meta = TelemetryMetadata(
        id=uuid4(),
        source_id=source_id,
        name="battery.voltage",
        units="V",
        description=None,
        subsystem_tag="power",
        channel_origin="catalog",
        discovery_namespace=None,
        discovered_at=datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc),
    )
    db = MagicMock()
    db.get.side_effect = lambda model, key: (
        TelemetrySource(
            id=source_id,
            name="Source A",
            source_type="vehicle",
            vehicle_config_path="defs/source-a.yaml",
        )
        if model is TelemetrySource and key == source_id
        else None
    )
    from app.services import channel_alias_service as alias_service

    original = alias_service.get_stream_source_id
    alias_service.get_stream_source_id = lambda _db, _source_id: None
    db.execute.side_effect = [_ScalarResult(meta)]

    try:
        resolved = resolve_channel_metadata(db, source_id=source_id, channel_name="battery.voltage")
        assert resolved is meta
        statement = db.execute.call_args.args[0]
        assert source_id in statement.compile().params.values()
    finally:
        alias_service.get_stream_source_id = original


def test_resolve_channel_metadata_keeps_source_lookup_for_non_stream_ids(monkeypatch) -> None:
    source_id = "source-a"
    meta = TelemetryMetadata(
        id=uuid4(),
        source_id=source_id,
        name="battery.voltage",
        units="V",
        description=None,
        subsystem_tag="power",
        channel_origin="catalog",
        discovery_namespace=None,
        discovered_at=datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc),
    )
    monkeypatch.setattr("app.services.channel_alias_service.get_stream_source_id", lambda _db, _source_id: None)
    db = MagicMock()
    db.get.return_value = None
    db.execute.side_effect = [_ScalarResult(meta)]

    resolved = resolve_channel_metadata(db, source_id=source_id, channel_name="battery.voltage")

    assert resolved is meta
    statement = db.execute.call_args.args[0]
    assert source_id in statement.compile().params.values()


def test_resolve_channel_metadata_uses_persisted_stream_owner_without_registry() -> None:
    source_id = "source-a"
    stream_id = "source-a-2026-03-28T12-00-00Z"
    meta = TelemetryMetadata(
        id=uuid4(),
        source_id=source_id,
        name="battery.voltage",
        units="V",
        description=None,
        subsystem_tag="power",
        channel_origin="catalog",
        discovery_namespace=None,
        discovered_at=datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc),
    )
    db = MagicMock()
    db.get.return_value = None
    db.execute.side_effect = [_ScalarResult(source_id), _ScalarResult(meta)]

    resolved = resolve_channel_metadata(db, source_id=stream_id, channel_name="battery.voltage")

    assert resolved is meta


def test_get_stream_source_id_uses_current_rows_after_restart() -> None:
    source_id = "source-a"
    stream_id = "stream-uuid-restart"
    db = MagicMock()
    db.get.return_value = None
    db.execute.return_value = _ScalarResult(source_id)

    resolved = get_stream_source_id(db, stream_id)

    assert resolved == source_id
    statement = db.execute.call_args.args[0]
    assert "telemetry_current" in str(statement.compile())
    assert stream_id in statement.compile().params.values()


def test_get_stream_source_id_does_not_overwrite_active_cache() -> None:
    source_id = "source-a"
    active_stream_id = "source-a-2026-03-28T12-00-00Z"
    historical_stream_id = "source-a-2026-03-28T10-00-00Z"
    db = MagicMock()
    db.get.side_effect = lambda model, key: (
        MagicMock(id=source_id)
        if key == source_id
        else TelemetryStream(id=active_stream_id, source_id=source_id, status="active")
        if model is TelemetryStream and key == active_stream_id
        else TelemetryStream(id=historical_stream_id, source_id=source_id, status="idle")
        if model is TelemetryStream and key == historical_stream_id
        else None
    )

    clear_active_stream(source_id)
    try:
        register_stream(db, source_id=source_id, stream_id=active_stream_id)
        resolved = get_stream_source_id(db, historical_stream_id)

        assert resolved == source_id
        assert _get_cached_active_stream_entry(source_id)[0] == active_stream_id
        db.execute.assert_not_called()
    finally:
        clear_active_stream(source_id)


def test_get_aliases_by_telemetry_ids_uses_registered_stream_owner() -> None:
    source_id = "source-a"
    stream_id = "stream-uuid-alias"
    telemetry_id = uuid4()
    db = MagicMock()
    db.get.side_effect = lambda model, key: (
        TelemetryStream(id=stream_id, source_id=source_id, status="active")
        if model is TelemetryStream and key == stream_id
        else None
    )
    db.execute.return_value.fetchall.return_value = [(telemetry_id, "VBAT")]

    aliases = get_aliases_by_telemetry_ids(
        db,
        source_id=stream_id,
        telemetry_ids=[telemetry_id],
    )

    assert aliases == {telemetry_id: ["VBAT"]}
    statement = db.execute.call_args.args[0]
    assert source_id in statement.compile().params.values()
