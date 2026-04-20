"""Tests for telemetry source registration and bootstrap flows."""

from __future__ import annotations

import pytest
import sys
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock
from types import SimpleNamespace
from sqlalchemy.exc import IntegrityError

from app.services.realtime_service import _seed_metadata_for_source
from app.services.realtime_service import _source_to_dict
from app.services.realtime_service import auto_register_sources_from_configs
from app.services.realtime_service import create_discovered_channel_metadata
from app.services.realtime_service import create_source
from app.services.realtime_service import get_source_by_vehicle_config_path
from app.services.realtime_service import infer_auto_registration_fields
from app.services.realtime_service import refresh_source_embeddings
from app.services.realtime_service import repair_registered_sources_on_startup
from app.services.realtime_service import register_source_if_missing
from app.services.realtime_service import resolve_source
from app.services.realtime_service import source_has_telemetry_history
from app.services.realtime_service import update_backfill_progress
from app.services.realtime_service import update_live_state
from app.services.realtime_service import update_source
from app.models.telemetry import TelemetryChannelAlias
from app.models.telemetry import TelemetryMetadata
from app.models.telemetry import TelemetrySource
from app.models.schemas import SourceResolveRequest


DROGONSAT_SOURCE_ID = "test-drogonsat-source"
MOCK_VEHICLE_SOURCE_ID = "test-mock-vehicle-source"


def test_source_api_payload_uses_only_vehicle_config_path() -> None:
    source = MagicMock()
    source.id = "source-1"
    source.name = "ISS"
    source.description = "Low Earth orbit"
    source.source_type = "vehicle"
    source.base_url = None
    source.vehicle_config_path = "vehicles/iss.yaml"

    payload = _source_to_dict(source)

    assert payload["vehicle_config_path"] == "vehicles/iss.yaml"
    assert "telemetry_definition_path" not in payload


def test_create_source_flushes_before_seeding_metadata(monkeypatch) -> None:
    """New sources must exist in-session before FK-backed metadata/mappings are seeded."""

    db = MagicMock()
    embedding_provider = MagicMock()
    call_order: list[str] = []

    def flush() -> None:
        call_order.append("flush")

    def add(_obj) -> None:
        call_order.append("add")

    def commit() -> None:
        call_order.append("commit")

    def refresh(_obj) -> None:
        call_order.append("refresh")

    def fake_seed_metadata_for_source(*args, **kwargs) -> None:
        assert "flush" in call_order
        call_order.append("seed")

    db.add.side_effect = add
    db.flush.side_effect = flush
    db.commit.side_effect = commit
    db.refresh.side_effect = refresh

    monkeypatch.setattr(
        "app.services.realtime_service._seed_metadata_for_source",
        fake_seed_metadata_for_source,
    )
    monkeypatch.setattr(
        "app.services.realtime_service.uuid.uuid4",
        lambda: uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
    )

    create_source(
        db,
        embedding_provider=embedding_provider,
        source_type="vehicle",
        name="Test Vehicle",
        vehicle_config_path="vehicles/aegon-relay.yaml",
    )

    assert call_order == ["add", "flush", "seed", "commit", "refresh"]


def test_get_source_by_vehicle_config_path_canonicalizes_before_lookup(monkeypatch) -> None:
    db = MagicMock()
    source = MagicMock()
    captured_params: dict[str, object] = {}

    class ScalarResult:
        def scalars(self):
            return self

        def first(self):
            return source

    def fake_execute(stmt):
        captured_params.update(stmt.compile().params)
        return ScalarResult()

    db.execute.side_effect = fake_execute
    monkeypatch.setattr(
        "app.services.realtime_service.canonical_vehicle_config_path",
        lambda path: "vehicles/iss.yaml",
    )

    result = get_source_by_vehicle_config_path(db, "./vehicles/../vehicles/iss.yaml")

    assert result is source
    assert "vehicles/iss.yaml" in captured_params.values()


def test_register_source_if_missing_returns_existing_source_without_creating_duplicate(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    existing = MagicMock()
    existing.id = "source-1"
    existing.name = "Existing"
    existing.description = "Already present"
    existing.source_type = "vehicle"
    existing.base_url = None
    existing.vehicle_config_path = "vehicles/iss.yaml"

    monkeypatch.setattr(
        "app.services.realtime_service.get_source_by_vehicle_config_path",
        lambda _db, _path: existing,
    )
    create_calls: list[dict] = []
    monkeypatch.setattr(
        "app.services.realtime_service.create_source",
        lambda *args, **kwargs: create_calls.append(kwargs),
    )

    result, created = register_source_if_missing(
        db,
        embedding_provider=embedding_provider,
        source_type="vehicle",
        name="ISS",
        description="desc",
        vehicle_config_path="vehicles/iss.yaml",
    )

    assert created is False
    assert result["id"] == "source-1"
    assert create_calls == []


def test_register_source_if_missing_creates_source_when_missing(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    monkeypatch.setattr(
        "app.services.realtime_service.get_source_by_vehicle_config_path",
        lambda _db, _path: None,
    )
    create_calls: list[dict] = []

    def fake_create_source(_db, **kwargs):
        create_calls.append(kwargs)
        return {"id": "created-1", **kwargs}

    monkeypatch.setattr(
        "app.services.realtime_service.create_source",
        fake_create_source,
    )

    result, created = register_source_if_missing(
        db,
        embedding_provider=embedding_provider,
        source_type="vehicle",
        name="ISS",
        description="desc",
        vehicle_config_path="vehicles/iss.yaml",
    )

    assert created is True
    assert result["id"] == "created-1"
    assert create_calls == [
        {
            "embedding_provider": embedding_provider,
            "source_type": "vehicle",
            "name": "ISS",
            "description": "desc",
            "base_url": None,
            "vehicle_config_path": "vehicles/iss.yaml",
        }
    ]


def test_resolve_source_is_vehicle_only() -> None:
    with pytest.raises(ValueError, match="source_type must be 'vehicle'"):
        resolve_source(
            MagicMock(),
            embedding_provider=MagicMock(),
            source_type="simulator",
            name="Simulator",
            vehicle_config_path="simulators/drogonsat.yaml",
        )


def test_source_resolve_request_rejects_non_vehicle_type() -> None:
    with pytest.raises(ValueError):
        SourceResolveRequest(
            source_type="simulator",
            name="Simulator",
            vehicle_config_path="simulators/drogonsat.yaml",
        )


def test_resolve_source_returns_existing_auto_registered_vehicle_without_mutating(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    existing = MagicMock()
    existing.id = "source-iss"
    existing.name = "International Space Station"
    existing.description = "Auto-registered from vehicle configuration: vehicles/iss.yaml"
    existing.source_type = "vehicle"
    existing.base_url = None
    existing.vehicle_config_path = "vehicles/iss.yaml"

    monkeypatch.setattr(
        "app.services.realtime_service.get_source_by_vehicle_config_path",
        lambda _db, _path: existing,
    )
    create_calls: list[dict] = []
    monkeypatch.setattr(
        "app.services.realtime_service.create_source",
        lambda *args, **kwargs: create_calls.append(kwargs),
    )

    result, created = resolve_source(
        db,
        embedding_provider=embedding_provider,
        source_type="vehicle",
        name="Different Adapter Name",
        description="Different description",
        vehicle_config_path="vehicles/iss.yaml",
    )

    assert created is False
    assert result["id"] == "source-iss"
    assert result["name"] == "International Space Station"
    assert result["description"] == "Auto-registered from vehicle configuration: vehicles/iss.yaml"
    assert existing.name == "International Space Station"
    assert existing.description == "Auto-registered from vehicle configuration: vehicles/iss.yaml"
    assert create_calls == []


def test_resolve_source_applies_first_run_monitoring_start_to_existing_source(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    existing = MagicMock()
    existing.id = "source-lasarsat"
    existing.name = "LASARSAT"
    existing.description = "Auto-registered from vehicle configuration: vehicles/lasarsat.yaml"
    existing.source_type = "vehicle"
    existing.base_url = None
    existing.vehicle_config_path = "vehicles/lasarsat.yaml"
    existing.monitoring_start_time = datetime(2026, 4, 11, 20, tzinfo=timezone.utc)
    existing.last_reconciled_at = None
    existing.history_mode = "time_window_replay"
    existing.backfill_state = "complete"
    existing.live_state = "idle"
    existing.active_backfill_target_time = None
    monkeypatch.setattr(
        "app.services.realtime_service.get_source_by_vehicle_config_path",
        lambda _db, _path: existing,
    )

    start = datetime(2026, 4, 11, 18, tzinfo=timezone.utc)
    result, created = resolve_source(
        db,
        embedding_provider=embedding_provider,
        source_type="vehicle",
        name="LASARSAT",
        vehicle_config_path="vehicles/lasarsat.yaml",
        monitoring_start_time=start,
    )

    assert created is False
    assert existing.monitoring_start_time == start
    assert existing.backfill_state == "idle"
    db.commit.assert_called_once()
    db.refresh.assert_called_once_with(existing)
    assert result["monitoring_start_time"] == start


def test_register_source_if_missing_rereads_winner_after_unique_conflict(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    existing = MagicMock()
    existing.id = "winner-source"
    existing.name = "Winner"
    existing.description = None
    existing.source_type = "vehicle"
    existing.base_url = None
    existing.vehicle_config_path = "vehicles/iss.yaml"
    lookups = iter([None, existing])

    monkeypatch.setattr(
        "app.services.realtime_service.get_source_by_vehicle_config_path",
        lambda _db, _path: next(lookups),
    )

    def fake_create_source(*args, **kwargs):
        raise IntegrityError("insert", {}, Exception("duplicate key"))

    monkeypatch.setattr(
        "app.services.realtime_service.create_source",
        fake_create_source,
    )

    result, created = register_source_if_missing(
        db,
        embedding_provider=embedding_provider,
        source_type="vehicle",
        name="ISS",
        vehicle_config_path="vehicles/iss.yaml",
    )

    assert created is False
    assert result["id"] == "winner-source"
    db.rollback.assert_called_once()


def test_telemetry_source_vehicle_config_path_is_unique() -> None:
    table = TelemetrySource.__table__

    assert any(
        index.unique
        and index.name == "ix_telemetry_sources_vehicle_config_path"
        and [column.name for column in index.columns] == ["vehicle_config_path"]
        for index in table.indexes
    )


def test_infer_auto_registration_fields_prefers_parsed_name_and_configured_simulator_url() -> None:
    config_item = SimpleNamespace(category="simulators")
    loaded = SimpleNamespace(
        path="simulators/drogonsat.yaml",
        parsed=SimpleNamespace(name="DrogonSat", base_url="http://simulator:8001"),
    )

    result = infer_auto_registration_fields("simulators/drogonsat.yaml", config_item, loaded)

    assert result["source_type"] == "simulator"
    assert result["name"] == "DrogonSat"
    assert result["base_url"] == "http://simulator:8001"
    assert result["vehicle_config_path"] == "simulators/drogonsat.yaml"


@pytest.mark.parametrize(
    ("category", "expected_type"),
    [
        ("simulator", "simulator"),
        ("simulators", "simulator"),
        ("vehicles", "vehicle"),
        ("custom", "vehicle"),
    ],
)
def test_infer_auto_registration_fields_uses_category_and_filename_fallback(category: str, expected_type: str) -> None:
    config_item = SimpleNamespace(category=category)
    loaded = SimpleNamespace(
        path="custom/demo-config.yaml",
        parsed=SimpleNamespace(name=None),
    )

    result = infer_auto_registration_fields("custom/demo-config.yaml", config_item, loaded)

    assert result["source_type"] == expected_type
    assert result["name"] == "demo-config"
    assert result["base_url"] is None


def test_repair_registered_sources_on_startup_preserves_existing_operator_edits(monkeypatch) -> None:
    """Existing local-stack sources should keep operator-edited fields across restarts."""

    db = MagicMock()
    existing = MagicMock()
    existing.id = DROGONSAT_SOURCE_ID
    existing.name = "Mission Drogon"
    existing.description = "Operator override"
    existing.source_type = "simulator"
    existing.base_url = "http://custom-simulator:8010"
    existing.vehicle_config_path = "simulators/rhaegalsat.json"
    db.get.side_effect = lambda model, source_id: existing if source_id == DROGONSAT_SOURCE_ID else None

    seeded_calls: list[dict] = []

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

    def fake_seed_metadata_for_source(*args, **kwargs) -> None:
        seeded_calls.append(kwargs)

    monkeypatch.setattr(
        "app.services.realtime_service._seed_metadata_for_source",
        fake_seed_metadata_for_source,
    )
    db.execute.return_value = ScalarResult([existing])

    repair_registered_sources_on_startup(
        db,
    )

    assert existing.name == "Mission Drogon"
    assert existing.description == "Operator override"
    assert existing.base_url == "http://custom-simulator:8010"
    assert existing.vehicle_config_path == "simulators/rhaegalsat.json"
    assert any(
        call["source_id"] == DROGONSAT_SOURCE_ID
        and call["vehicle_config_path"] == "simulators/rhaegalsat.json"
        and call["refresh_embeddings"] is False
        and call["overwrite_position_mapping"] is False
        for call in seeded_calls
    )
    assert all("prune_missing" not in call for call in seeded_calls)
    assert all(call.get("refresh_embeddings") is False for call in seeded_calls)


def test_auto_register_sources_from_configs_skips_invalid_files(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.services.realtime_service.list_vehicle_configs",
        lambda: [SimpleNamespace(path="vehicles/bad.yaml", category="vehicles")],
    )
    monkeypatch.setattr(
        "app.services.realtime_service.load_vehicle_config",
        lambda _path: SimpleNamespace(
            path="vehicles/bad.yaml",
            parsed=SimpleNamespace(name="Bad"),
            validation_errors=[SimpleNamespace(model_dump=lambda: {"message": "invalid"})],
        ),
    )
    register_calls: list[dict] = []
    monkeypatch.setattr(
        "app.services.realtime_service.register_source_if_missing",
        lambda *args, **kwargs: register_calls.append(kwargs),
    )

    summary = auto_register_sources_from_configs(MagicMock(), embedding_provider=MagicMock())

    assert summary["examined"] == 1
    assert summary["created"] == []
    assert summary["invalid"] == [{"path": "vehicles/bad.yaml", "errors": [{"message": "invalid"}]}]
    assert register_calls == []


def test_auto_register_sources_from_configs_skips_simulator_without_base_url(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.services.realtime_service.list_vehicle_configs",
        lambda: [SimpleNamespace(path="simulators/custom.yaml", category="simulators")],
    )
    monkeypatch.setattr(
        "app.services.realtime_service.load_vehicle_config",
        lambda _path: SimpleNamespace(
            path="simulators/custom.yaml",
            parsed=SimpleNamespace(name="Custom Sim"),
            validation_errors=[],
        ),
    )
    register_calls: list[dict] = []
    monkeypatch.setattr(
        "app.services.realtime_service.register_source_if_missing",
        lambda *args, **kwargs: register_calls.append(kwargs),
    )

    summary = auto_register_sources_from_configs(MagicMock(), embedding_provider=MagicMock())

    assert summary["examined"] == 1
    assert summary["skipped"] == [
        {
            "path": "simulators/custom.yaml",
            "reason": "missing_base_url",
            "source_type": "simulator",
        }
    ]
    assert register_calls == []


def test_auto_register_sources_from_configs_continues_after_failure_and_audits_creates(monkeypatch) -> None:
    items = [
        SimpleNamespace(path="vehicles/bad.yaml", category="vehicles"),
        SimpleNamespace(path="vehicles/good.yaml", category="vehicles"),
        SimpleNamespace(path="vehicles/existing.yaml", category="vehicles"),
    ]
    monkeypatch.setattr("app.services.realtime_service.list_vehicle_configs", lambda: items)

    def fake_load_vehicle_config(path: str):
        if path == "vehicles/bad.yaml":
            raise RuntimeError("boom")
        return SimpleNamespace(
            path=path,
            parsed=SimpleNamespace(name=None),
            validation_errors=[],
        )

    monkeypatch.setattr(
        "app.services.realtime_service.load_vehicle_config",
        fake_load_vehicle_config,
    )
    register_calls: list[dict] = []

    def fake_register_source_if_missing(_db, embedding_provider=None, **kwargs):
        register_calls.append(kwargs)
        if kwargs["vehicle_config_path"] == "vehicles/good.yaml":
            return (
                {
                    "id": "created-1",
                    "name": kwargs["name"],
                    "description": kwargs["description"],
                    "source_type": kwargs["source_type"],
                    "base_url": kwargs["base_url"],
                    "vehicle_config_path": kwargs["vehicle_config_path"],
                },
                True,
            )
        return (
            {
                "id": "existing-1",
                "name": kwargs["name"],
                "description": kwargs["description"],
                "source_type": kwargs["source_type"],
                "base_url": kwargs["base_url"],
                "vehicle_config_path": kwargs["vehicle_config_path"],
            },
            False,
        )

    monkeypatch.setattr(
        "app.services.realtime_service.register_source_if_missing",
        fake_register_source_if_missing,
    )
    audit_calls: list[dict] = []
    monkeypatch.setattr(
        "app.services.realtime_service.audit_log",
        lambda action, **kwargs: audit_calls.append({"action": action, **kwargs}),
    )

    summary = auto_register_sources_from_configs(MagicMock(), embedding_provider=MagicMock())

    assert summary["examined"] == 3
    assert summary["invalid"] == [
        {
            "path": "vehicles/bad.yaml",
            "errors": [{"message": "Unexpected reconciliation failure", "type": "startup_error"}],
        }
    ]
    assert [item["vehicle_config_path"] for item in summary["created"]] == ["vehicles/good.yaml"]
    assert [item["vehicle_config_path"] for item in summary["existing"]] == ["vehicles/existing.yaml"]
    assert [call["vehicle_config_path"] for call in register_calls] == [
        "vehicles/good.yaml",
        "vehicles/existing.yaml",
    ]
    assert audit_calls == [
        {
            "action": "sources.auto_register",
            "source_id": "created-1",
            "vehicle_config_path": "vehicles/good.yaml",
            "source_type": "vehicle",
            "name": "good",
        }
    ]


def test_repair_registered_sources_on_startup_does_not_create_sources(monkeypatch) -> None:
    """Startup repair reseeds metadata for existing rows without creating source rows."""

    db = MagicMock()
    call_order: list[str] = []
    db.get.return_value = None

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

    def add(_obj) -> None:
        call_order.append("add")

    def flush() -> None:
        call_order.append("flush")

    def fake_seed_metadata_for_source(*args, **kwargs) -> None:
        call_order.append("seed")

    db.add.side_effect = add
    db.flush.side_effect = flush

    monkeypatch.setattr(
        "app.services.realtime_service._seed_metadata_for_source",
        fake_seed_metadata_for_source,
    )
    source_rows = [MagicMock(id=DROGONSAT_SOURCE_ID, vehicle_config_path="simulators/drogonsat.yaml")]
    db.execute.return_value = ScalarResult(source_rows)

    repair_registered_sources_on_startup(
        db,
    )

    assert "seed" in call_order
    assert "add" not in call_order
    assert "flush" not in call_order


def test_seed_metadata_for_source_creates_channel_alias_rows(tmp_path, monkeypatch) -> None:
    db = MagicMock()
    added: list[object] = []
    definition = SimpleNamespace(
        channels=[
            SimpleNamespace(
                name="PWR_MAIN_BUS_VOLT",
                aliases=["BAT_V", "VBAT"],
                units="V",
                description="Main bus voltage",
                subsystem="power",
                red_low=None,
                red_high=None,
            )
        ],
        position_mapping=None,
    )

    class _ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

        def first(self):
            return self._rows[0] if self._rows else None

    execute_results = [
        _ScalarResult([]),  # existing metadata
        _ScalarResult([]),  # existing aliases
        _ScalarResult([]),  # alias prune delete
        _ScalarResult([]),  # existing position mappings
    ]
    db.execute.side_effect = lambda *args, **kwargs: execute_results.pop(0)
    db.add.side_effect = added.append

    def fake_flush() -> None:
        for obj in added:
            if isinstance(obj, TelemetryMetadata) and obj.id is None:
                obj.id = uuid.uuid4()

    db.flush.side_effect = fake_flush
    embedding_provider = MagicMock()
    embedding_provider.embed.return_value = [0.1, 0.2, 0.3]
    monkeypatch.setattr(
        "app.services.realtime_service.load_vehicle_config_file",
        lambda _path: definition,
    )

    _seed_metadata_for_source(
        db,
        source_id="source-a",
        vehicle_config_path="ignored.yaml",
        embedding_provider=embedding_provider,
        refresh_embeddings=True,
    )

    aliases = [obj for obj in added if isinstance(obj, TelemetryChannelAlias)]
    assert sorted(alias.alias_name for alias in aliases) == ["BAT_V", "VBAT"]
    assert all(alias.telemetry_id is not None for alias in aliases)


def test_repair_registered_sources_on_startup_does_not_prune_existing_metadata(monkeypatch) -> None:
    """Startup repair must not delete historical telemetry via metadata pruning."""

    db = MagicMock()
    db.get.return_value = None
    seed_calls: list[dict] = []

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

    def fake_seed_metadata_for_source(*args, **kwargs) -> None:
        seed_calls.append(kwargs)

    monkeypatch.setattr(
        "app.services.realtime_service._seed_metadata_for_source",
        fake_seed_metadata_for_source,
    )
    db.execute.return_value = ScalarResult(
        [MagicMock(id=MOCK_VEHICLE_SOURCE_ID, vehicle_config_path="vehicles/balerion-surveyor.json")]
    )

    repair_registered_sources_on_startup(
        db,
    )

    assert seed_calls
    assert all("prune_missing" not in call for call in seed_calls)
    assert all(call.get("refresh_embeddings") is False for call in seed_calls)
    assert all(call.get("overwrite_position_mapping") is False for call in seed_calls)


def test_repair_registered_sources_on_startup_preserves_existing_position_mappings(monkeypatch) -> None:
    """Startup repair must not overwrite operator-managed active position mappings."""

    db = MagicMock()
    existing_source = MagicMock()
    existing_source.id = DROGONSAT_SOURCE_ID
    existing_source.vehicle_config_path = "simulators/drogonsat.yaml"
    db.get.side_effect = lambda model, source_id: existing_source if source_id == DROGONSAT_SOURCE_ID else None
    seed_calls: list[dict] = []

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

    monkeypatch.setattr(
        "app.services.realtime_service._seed_metadata_for_source",
        lambda *args, **kwargs: seed_calls.append(kwargs),
    )
    db.execute.return_value = ScalarResult([existing_source])

    repair_registered_sources_on_startup(db)

    assert any(call.get("overwrite_position_mapping") is False for call in seed_calls)


def test_repair_registered_sources_on_startup_seeds_existing_custom_sources(monkeypatch) -> None:
    """Startup repair must repair metadata for persisted custom sources, not just registered sources."""

    db = MagicMock()
    custom = MagicMock()
    custom.id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    custom.vehicle_config_path = "vehicles/aegon-relay.yaml"
    db.get.return_value = None
    seed_calls: list[dict] = []

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

    monkeypatch.setattr(
        "app.services.realtime_service._seed_metadata_for_source",
        lambda *args, **kwargs: seed_calls.append(kwargs),
    )
    db.execute.return_value = ScalarResult([custom])

    repair_registered_sources_on_startup(db)

    assert any(call["source_id"] == custom.id for call in seed_calls)
    assert any(
        call["source_id"] == custom.id and call["refresh_embeddings"] is False
        for call in seed_calls
    )


def test_repair_registered_sources_on_startup_returns_repaired_sources_for_embedding_backfill(monkeypatch) -> None:
    """Startup repair should backfill embeddings for every source it repaired."""

    db = MagicMock()
    built_in = MagicMock()
    built_in.id = DROGONSAT_SOURCE_ID
    built_in.vehicle_config_path = "simulators/drogonsat.yaml"
    custom = MagicMock()
    custom.id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    custom.vehicle_config_path = "vehicles/aegon-relay.yaml"
    db.get.side_effect = lambda model, source_id: built_in if source_id == DROGONSAT_SOURCE_ID else None

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

    monkeypatch.setattr(
        "app.services.realtime_service._seed_metadata_for_source",
        lambda *args, **kwargs: None,
    )
    db.execute.return_value = ScalarResult([built_in, custom])

    repaired_source_ids = repair_registered_sources_on_startup(db)

    expected_ids = {built_in.id, custom.id}

    assert set(repaired_source_ids) == expected_ids


def test_refresh_source_embeddings_backfills_real_embeddings(monkeypatch) -> None:
    """Post-startup embedding refresh should use the real provider for specified sources."""

    db = MagicMock()
    source = MagicMock()
    source.id = DROGONSAT_SOURCE_ID
    source.vehicle_config_path = "simulators/drogonsat.yaml"
    provider = MagicMock()
    seed_calls: list[dict] = []

    db.get.side_effect = lambda model, source_id: source if source_id == DROGONSAT_SOURCE_ID else None

    monkeypatch.setattr(
        "app.services.realtime_service._seed_metadata_for_source",
        lambda *args, **kwargs: seed_calls.append(kwargs),
    )

    refresh_source_embeddings(
        db,
        source_ids=[DROGONSAT_SOURCE_ID],
        embedding_provider=provider,
    )

    assert seed_calls == [
        {
            "source_id": DROGONSAT_SOURCE_ID,
            "vehicle_config_path": "simulators/drogonsat.yaml",
            "embedding_provider": provider,
            "refresh_embeddings": True,
            "preserve_existing_embeddings": True,
            "overwrite_position_mapping": False,
        }
    ]
    db.commit.assert_called_once()


def test_repair_registered_sources_on_startup_skips_invalid_catalogs_without_aborting(monkeypatch) -> None:
    """One stale vehicle configuration path should not prevent startup repair for other sources."""

    db = MagicMock()
    bad = MagicMock()
    bad.id = "bad-source"
    bad.vehicle_config_path = "vehicles/missing.yaml"
    good = MagicMock()
    good.id = DROGONSAT_SOURCE_ID
    good.vehicle_config_path = "simulators/drogonsat.yaml"
    seed_calls: list[str] = []

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

    def fake_seed_metadata_for_source(*args, **kwargs):
        if kwargs["source_id"] == bad.id:
            raise ValueError("missing definition")
        seed_calls.append(kwargs["source_id"])

    monkeypatch.setattr(
        "app.services.realtime_service._seed_metadata_for_source",
        fake_seed_metadata_for_source,
    )
    db.execute.return_value = ScalarResult([bad, good])

    repair_registered_sources_on_startup(db)

    assert good.id in seed_calls
    db.commit.assert_called_once()


def test_seed_metadata_prunes_watchlist_entries_for_removed_channels(monkeypatch) -> None:
    """Pruning metadata should also prune watchlist entries for removed channels."""

    db = MagicMock()
    embedding_provider = MagicMock()
    obsolete_meta = MagicMock()
    obsolete_meta.name = "obsolete_channel"
    obsolete_meta.channel_origin = "catalog"
    stale_watchlist_delete = []

    retained_channel = MagicMock()
    retained_channel.name = "retained_channel"
    retained_channel.units = "V"
    retained_channel.description = "Retained"
    retained_channel.subsystem = "power"
    retained_channel.red_low = None
    retained_channel.red_high = None
    definition = MagicMock()
    definition.channels = [retained_channel]
    definition.position_mapping = None

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def all(self):
            return self._rows

        def first(self):
            return self._rows[0] if self._rows else None

    def fake_execute(statement):
        statement_sql = str(statement)
        if "DELETE FROM watchlist" in statement_sql:
            stale_watchlist_delete.append(statement_sql)
            return MagicMock()
        if "FROM telemetry_channel_aliases" in statement_sql:
            return MagicMock(scalars=lambda: ScalarResult([]))
        if "FROM telemetry_metadata" in statement_sql:
            return MagicMock(scalars=lambda: ScalarResult([obsolete_meta]))
        if "FROM position_channel_mappings" in statement_sql:
            return MagicMock(scalars=lambda: ScalarResult([]))
        raise AssertionError(f"Unexpected statement: {statement_sql}")

    db.execute.side_effect = fake_execute

    monkeypatch.setattr(
        "app.services.realtime_service.load_vehicle_config_file",
        lambda _path: definition,
    )

    _seed_metadata_for_source(
        db,
        source_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        vehicle_config_path="vehicles/balerion-surveyor.json",
        embedding_provider=embedding_provider,
        prune_missing=True,
    )

    assert stale_watchlist_delete
    db.delete.assert_called_once_with(obsolete_meta)


def test_seed_metadata_does_not_prune_discovered_channels(monkeypatch) -> None:
    """Discovered channels must survive definition reseeds even when prune_missing=True."""

    db = MagicMock()
    embedding_provider = MagicMock()
    discovered_meta = MagicMock()
    discovered_meta.name = "decoder.aprs.payload_temp"
    discovered_meta.channel_origin = "discovered"
    watchlist_delete_statements = []

    retained_channel = MagicMock()
    retained_channel.name = "retained_channel"
    retained_channel.units = "V"
    retained_channel.description = "Retained"
    retained_channel.subsystem = "power"
    retained_channel.red_low = None
    retained_channel.red_high = None
    definition = MagicMock()
    definition.channels = [retained_channel]
    definition.position_mapping = None

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def all(self):
            return self._rows

        def first(self):
            return self._rows[0] if self._rows else None

    def fake_execute(statement):
        statement_sql = str(statement)
        if "DELETE FROM watchlist" in statement_sql:
            watchlist_delete_statements.append(statement_sql)
            return MagicMock()
        if "FROM telemetry_channel_aliases" in statement_sql:
            return MagicMock(scalars=lambda: ScalarResult([]))
        if "FROM telemetry_metadata" in statement_sql:
            return MagicMock(scalars=lambda: ScalarResult([discovered_meta]))
        if "FROM position_channel_mappings" in statement_sql:
            return MagicMock(scalars=lambda: ScalarResult([]))
        raise AssertionError(f"Unexpected statement: {statement_sql}")

    db.execute.side_effect = fake_execute

    monkeypatch.setattr(
        "app.services.realtime_service.load_vehicle_config_file",
        lambda _path: definition,
    )

    _seed_metadata_for_source(
        db,
        source_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        vehicle_config_path="vehicles/balerion-surveyor.json",
        embedding_provider=embedding_provider,
        prune_missing=True,
    )

    assert watchlist_delete_statements == []
    db.delete.assert_not_called()


def test_seed_metadata_prunes_removed_aliases_even_without_metadata_pruning(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    delete_statements: list[str] = []

    retained_meta = MagicMock()
    retained_meta.name = "retained_channel"
    retained_meta.channel_origin = "catalog"

    retained_channel = MagicMock()
    retained_channel.name = "retained_channel"
    retained_channel.aliases = ["NEW_ALIAS"]
    retained_channel.units = "V"
    retained_channel.description = "Retained"
    retained_channel.subsystem = "power"
    retained_channel.red_low = None
    retained_channel.red_high = None
    definition = MagicMock()
    definition.channels = [retained_channel]
    definition.position_mapping = None

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def all(self):
            return self._rows

        def scalars(self):
            return self

        def first(self):
            return self._rows[0] if self._rows else None

    def fake_execute(statement):
        statement_sql = str(statement)
        if "DELETE FROM telemetry_channel_aliases" in statement_sql:
            delete_statements.append(statement_sql)
            return MagicMock()
        if "FROM telemetry_channel_aliases" in statement_sql:
            return MagicMock(scalars=lambda: ScalarResult([]))
        if "FROM telemetry_metadata" in statement_sql:
            return MagicMock(scalars=lambda: ScalarResult([retained_meta]))
        if "FROM position_channel_mappings" in statement_sql:
            return MagicMock(scalars=lambda: ScalarResult([]))
        raise AssertionError(f"Unexpected statement: {statement_sql}")

    db.execute.side_effect = fake_execute

    monkeypatch.setattr(
        "app.services.realtime_service.load_vehicle_config_file",
        lambda _path: definition,
    )

    _seed_metadata_for_source(
        db,
        source_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        vehicle_config_path="vehicles/balerion-surveyor.json",
        embedding_provider=embedding_provider,
        prune_missing=False,
    )

    assert delete_statements
    assert "DELETE FROM telemetry_channel_aliases" in delete_statements[0]


def test_seed_metadata_merges_discovered_alias_conflict_into_canonical_channel(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    statements: list[str] = []

    catalog_meta = MagicMock()
    catalog_meta.id = uuid.UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    catalog_meta.name = "PWR_MAIN_BUS_VOLT"
    catalog_meta.channel_origin = "catalog"
    discovered_meta = MagicMock()
    discovered_meta.id = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    discovered_meta.name = "VBAT"
    discovered_meta.channel_origin = "discovered"

    channel = MagicMock()
    channel.name = "PWR_MAIN_BUS_VOLT"
    channel.aliases = ["VBAT"]
    channel.units = "V"
    channel.description = "Main bus voltage"
    channel.subsystem = "power"
    channel.red_low = None
    channel.red_high = None
    definition = MagicMock()
    definition.channels = [channel]
    definition.position_mapping = None

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

        def first(self):
            return self._rows[0] if self._rows else None

    def fake_execute(statement, params=None):
        statement_sql = str(statement)
        statements.append(statement_sql)
        if "DELETE FROM telemetry_channel_aliases" in statement_sql:
            return MagicMock()
        if "FROM telemetry_channel_aliases" in statement_sql:
            return ScalarResult([])
        if "FROM telemetry_metadata" in statement_sql:
            return ScalarResult([catalog_meta, discovered_meta])
        if "FROM position_channel_mappings" in statement_sql:
            return ScalarResult([])
        return MagicMock()

    db.execute.side_effect = fake_execute

    monkeypatch.setattr(
        "app.services.realtime_service.load_vehicle_config_file",
        lambda _path: definition,
    )

    _seed_metadata_for_source(
        db,
        source_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        vehicle_config_path="vehicles/balerion-surveyor.json",
        embedding_provider=embedding_provider,
        prune_missing=False,
    )

    assert any("INSERT INTO telemetry_data" in stmt for stmt in statements)
    assert any("INSERT INTO telemetry_current" in stmt for stmt in statements)
    assert any("INSERT INTO telemetry_statistics" in stmt for stmt in statements)
    assert any("UPDATE telemetry_alerts" in stmt for stmt in statements)
    assert any("tmp_same_source_stream_scope" in stmt for stmt in statements)
    data_merge_sql = next(stmt for stmt in statements if "INSERT INTO telemetry_data" in stmt)
    current_merge_sql = next(stmt for stmt in statements if "INSERT INTO telemetry_current" in stmt)
    assert "sequence" in data_merge_sql
    assert "ON CONFLICT (source_id, telemetry_id, timestamp, sequence)" in data_merge_sql
    assert "packet_source" in data_merge_sql
    assert "receiver_id" in data_merge_sql
    assert "packet_source" in current_merge_sql
    assert "receiver_id" in current_merge_sql
    current_merge_sql = next(stmt for stmt in statements if "INSERT INTO telemetry_current" in stmt)
    assert "EXCLUDED.generation_time > telemetry_current.generation_time" in current_merge_sql
    assert "EXCLUDED.generation_time = telemetry_current.generation_time" in current_merge_sql
    assert "EXCLUDED.reception_time >= telemetry_current.reception_time" in current_merge_sql
    stats_recompute_sql = next(stmt for stmt in statements if "INSERT INTO telemetry_statistics" in stmt)
    assert "AVG(td.value)" in stats_recompute_sql
    assert "COALESCE(STDDEV_POP(td.value), 0)" in stats_recompute_sql
    assert "PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY td.value)" in stats_recompute_sql
    assert "GROUP BY td.source_id" in stats_recompute_sql
    stats_delete_sql = next(
        stmt for stmt in statements if "DELETE FROM telemetry_statistics" in stmt
    )
    assert "telemetry_id IN (:old_id, :new_id)" in stats_delete_sql
    db.delete.assert_called_once_with(discovered_meta)


def test_seed_metadata_allows_renamed_channel_to_keep_old_name_as_alias_when_pruning(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()

    old_meta = MagicMock()
    old_meta.id = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    old_meta.name = "VBAT"
    old_meta.channel_origin = "catalog"

    channel = MagicMock()
    channel.name = "PWR_MAIN_BUS_VOLT"
    channel.aliases = ["VBAT"]
    channel.units = "V"
    channel.description = "Main bus voltage"
    channel.subsystem = "power"
    channel.red_low = None
    channel.red_high = None
    definition = MagicMock()
    definition.channels = [channel]
    definition.position_mapping = None

    added: list[object] = []
    watchlist_statements: list[object] = []

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

        def first(self):
            return self._rows[0] if self._rows else None

    def fake_execute(statement, params=None):
        statement_sql = str(statement)
        if "DELETE FROM telemetry_channel_aliases" in statement_sql:
            return MagicMock()
        if "watchlist" in statement_sql:
            watchlist_statements.append((statement, params))
            return MagicMock()
        if "FROM telemetry_channel_aliases" in statement_sql:
            return ScalarResult([])
        if "FROM telemetry_metadata" in statement_sql:
            return ScalarResult([old_meta])
        if "FROM position_channel_mappings" in statement_sql:
            return ScalarResult([])
        raise AssertionError(f"Unexpected statement: {statement_sql}")

    db.execute.side_effect = fake_execute
    db.add.side_effect = added.append

    monkeypatch.setattr(
        "app.services.realtime_service.load_vehicle_config_file",
        lambda _path: definition,
    )

    _seed_metadata_for_source(
        db,
        source_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        vehicle_config_path="vehicles/balerion-surveyor.json",
        embedding_provider=embedding_provider,
        prune_missing=True,
    )

    db.delete.assert_not_called()
    assert old_meta.name == "PWR_MAIN_BUS_VOLT"
    assert len(watchlist_statements) == 2
    assert watchlist_statements[0][1] == {
        "source_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "old_name": "VBAT",
        "new_name": "PWR_MAIN_BUS_VOLT",
    }
    aliases = [obj for obj in added if isinstance(obj, TelemetryChannelAlias)]
    assert len(aliases) == 1
    assert aliases[0].alias_name == "VBAT"
    assert aliases[0].telemetry_id == old_meta.id


def test_seed_metadata_does_not_prune_watchlist_for_preserved_alias_name(monkeypatch) -> None:
    db = MagicMock()
    embedding_provider = MagicMock()

    old_meta = MagicMock()
    old_meta.id = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    old_meta.name = "VBAT"
    old_meta.channel_origin = "catalog"

    obsolete_meta = MagicMock()
    obsolete_meta.name = "OBSOLETE"
    obsolete_meta.channel_origin = "catalog"

    channel = MagicMock()
    channel.name = "PWR_MAIN_BUS_VOLT"
    channel.aliases = ["VBAT"]
    channel.units = "V"
    channel.description = "Main bus voltage"
    channel.subsystem = "power"
    channel.red_low = None
    channel.red_high = None
    definition = MagicMock()
    definition.channels = [channel]
    definition.position_mapping = None

    watchlist_statements: list[tuple[object, object]] = []

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

        def first(self):
            return self._rows[0] if self._rows else None

    def fake_execute(statement, params=None):
        statement_sql = str(statement)
        if "DELETE FROM telemetry_channel_aliases" in statement_sql:
            return MagicMock()
        if "watchlist" in statement_sql:
            watchlist_statements.append((statement, params))
            return MagicMock()
        if "FROM telemetry_channel_aliases" in statement_sql:
            return ScalarResult([])
        if "FROM telemetry_metadata" in statement_sql:
            return ScalarResult([old_meta, obsolete_meta])
        if "FROM position_channel_mappings" in statement_sql:
            return ScalarResult([])
        raise AssertionError(f"Unexpected statement: {statement_sql}")

    db.execute.side_effect = fake_execute

    monkeypatch.setattr(
        "app.services.realtime_service.load_vehicle_config_file",
        lambda _path: definition,
    )

    _seed_metadata_for_source(
        db,
        source_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        vehicle_config_path="vehicles/balerion-surveyor.json",
        embedding_provider=embedding_provider,
        prune_missing=True,
    )

    prune_deletes = [
        statement.compile().params
        for statement, _params in watchlist_statements
        if "telemetry_name_1" in statement.compile().params
    ]
    assert prune_deletes == [{"telemetry_name_1": ["OBSOLETE"], "source_id_1": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}]


def test_seed_metadata_promotes_discovered_channel_when_definition_catches_up(monkeypatch) -> None:
    """A discovered channel should become catalog-managed when it appears in the source definition."""

    db = MagicMock()
    existing_meta = MagicMock()
    existing_meta.name = "decoder.aprs.payload_temp"
    existing_meta.channel_origin = "discovered"
    existing_meta.discovery_namespace = "decoder.aprs"
    existing_meta.embedding = None

    channel = MagicMock()
    channel.name = "decoder.aprs.payload_temp"
    channel.units = "C"
    channel.description = "Payload temperature from APRS decoder"
    channel.subsystem = "payload"
    channel.red_low = -10.0
    channel.red_high = 60.0
    definition = MagicMock()
    definition.channels = [channel]
    definition.position_mapping = None

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

        def first(self):
            return self._rows[0] if self._rows else None

    def fake_execute(statement):
        statement_sql = str(statement)
        if "FROM telemetry_metadata" in statement_sql:
            return ScalarResult([existing_meta])
        if "FROM telemetry_channel_aliases" in statement_sql:
            return ScalarResult([])
        if "FROM position_channel_mappings" in statement_sql:
            return ScalarResult([])
        raise AssertionError(f"Unexpected statement: {statement_sql}")

    db.execute.side_effect = fake_execute

    monkeypatch.setattr(
        "app.services.realtime_service.load_vehicle_config_file",
        lambda _path: definition,
    )

    _seed_metadata_for_source(
        db,
        source_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        vehicle_config_path="vehicles/balerion-surveyor.json",
        embedding_provider=None,
        refresh_embeddings=False,
    )

    assert existing_meta.channel_origin == "catalog"
    assert existing_meta.discovery_namespace is None
    assert existing_meta.units == "C"
    assert existing_meta.description == "Payload temperature from APRS decoder"


def test_seed_metadata_flags_promoted_discovered_channel_without_embedding_for_backfill(monkeypatch) -> None:
    db = MagicMock()
    existing_meta = MagicMock()
    existing_meta.name = "decoder.aprs.payload_temp"
    existing_meta.channel_origin = "discovered"
    existing_meta.discovery_namespace = "decoder.aprs"
    existing_meta.embedding = None

    channel = MagicMock()
    channel.name = "decoder.aprs.payload_temp"
    channel.units = "C"
    channel.description = "Payload temperature from APRS decoder"
    channel.subsystem = "payload"
    channel.red_low = -10.0
    channel.red_high = 60.0
    definition = MagicMock()
    definition.channels = [channel]
    definition.position_mapping = None

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

        def first(self):
            return self._rows[0] if self._rows else None

    def fake_execute(statement):
        statement_sql = str(statement)
        if "FROM telemetry_metadata" in statement_sql:
            return ScalarResult([existing_meta])
        if "FROM telemetry_channel_aliases" in statement_sql:
            return ScalarResult([])
        if "FROM position_channel_mappings" in statement_sql:
            return ScalarResult([])
        raise AssertionError(f"Unexpected statement: {statement_sql}")

    db.execute.side_effect = fake_execute

    monkeypatch.setattr(
        "app.services.realtime_service.load_vehicle_config_file",
        lambda _path: definition,
    )

    needs_backfill = _seed_metadata_for_source(
        db,
        source_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        vehicle_config_path="vehicles/balerion-surveyor.json",
        refresh_embeddings=False,
        overwrite_position_mapping=False,
    )

    assert needs_backfill is True


def test_repair_registered_sources_on_startup_backfills_embeddings_for_promoted_channels(monkeypatch) -> None:
    db = MagicMock()
    source = MagicMock()
    source.id = DROGONSAT_SOURCE_ID
    source.vehicle_config_path = "simulators/drogonsat.yaml"
    db.get.side_effect = lambda model, source_id: source if source_id == DROGONSAT_SOURCE_ID else None

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

    seed_calls: list[dict] = []

    def fake_seed_metadata_for_source(*args, **kwargs):
        seed_calls.append(kwargs)
        return kwargs.get("refresh_embeddings", False) is False

    monkeypatch.setattr(
        "app.services.realtime_service._seed_metadata_for_source",
        fake_seed_metadata_for_source,
    )
    fake_provider = MagicMock()
    monkeypatch.setitem(
        sys.modules,
        "app.services.embedding_service",
        SimpleNamespace(SentenceTransformerEmbeddingProvider=lambda: fake_provider),
    )
    db.execute.return_value = ScalarResult([source])

    repair_registered_sources_on_startup(db)

    assert seed_calls[0]["refresh_embeddings"] is False
    assert seed_calls[1]["refresh_embeddings"] is True
    assert seed_calls[1]["embedding_provider"] is fake_provider
    db.commit.assert_called_once()


def test_repair_registered_sources_on_startup_ignores_embedding_provider_init_failures(monkeypatch) -> None:
    db = MagicMock()
    source = MagicMock()
    source.id = DROGONSAT_SOURCE_ID
    source.vehicle_config_path = "simulators/drogonsat.yaml"
    db.get.side_effect = lambda model, source_id: source if source_id == DROGONSAT_SOURCE_ID else None

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

    seed_calls: list[dict] = []

    def fake_seed_metadata_for_source(*args, **kwargs):
        seed_calls.append(kwargs)
        return kwargs.get("refresh_embeddings", False) is False

    monkeypatch.setattr(
        "app.services.realtime_service._seed_metadata_for_source",
        fake_seed_metadata_for_source,
    )

    class BrokenProvider:
        def __init__(self):
            raise RuntimeError("model unavailable")

    monkeypatch.setitem(
        sys.modules,
        "app.services.embedding_service",
        SimpleNamespace(SentenceTransformerEmbeddingProvider=BrokenProvider),
    )
    db.execute.return_value = ScalarResult([source])

    repair_registered_sources_on_startup(db)

    assert [call["refresh_embeddings"] for call in seed_calls] == [False]
    db.commit.assert_called_once()


def test_repair_registered_sources_on_startup_ignores_backfill_failures_per_source(monkeypatch) -> None:
    db = MagicMock()
    source = MagicMock()
    source.id = DROGONSAT_SOURCE_ID
    source.vehicle_config_path = "simulators/drogonsat.yaml"
    db.get.side_effect = lambda model, source_id: source if source_id == DROGONSAT_SOURCE_ID else None

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

    seed_calls: list[dict] = []

    def fake_seed_metadata_for_source(*args, **kwargs):
        seed_calls.append(kwargs)
        if kwargs.get("refresh_embeddings") is True:
            raise RuntimeError("embedding refresh failed")
        return True

    monkeypatch.setattr(
        "app.services.realtime_service._seed_metadata_for_source",
        fake_seed_metadata_for_source,
    )
    fake_provider = MagicMock()
    monkeypatch.setitem(
        sys.modules,
        "app.services.embedding_service",
        SimpleNamespace(SentenceTransformerEmbeddingProvider=lambda: fake_provider),
    )
    db.execute.return_value = ScalarResult([source])

    repair_registered_sources_on_startup(db)

    assert [call["refresh_embeddings"] for call in seed_calls] == [False, True]
    assert seed_calls[1]["embedding_provider"] is fake_provider
    db.commit.assert_called_once()


def test_update_source_prunes_missing_channels_when_vehicle_definition_changes(monkeypatch) -> None:
    """Changing a vehicle definition before ingest should drop channels not in the new catalog."""

    db = MagicMock()
    embedding_provider = MagicMock()
    existing = MagicMock()
    existing.id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    existing.vehicle_config_path = "vehicles/aegon-relay.yaml"
    existing.source_type = "vehicle"
    db.get.return_value = existing

    seed_calls: list[tuple[tuple, dict]] = []

    monkeypatch.setattr(
        "app.services.realtime_service.source_has_telemetry_history",
        lambda _db, _source_id: False,
    )

    def fake_seed_metadata_for_source(*args, **kwargs) -> None:
        seed_calls.append((args, kwargs))

    monkeypatch.setattr(
        "app.services.realtime_service._seed_metadata_for_source",
        fake_seed_metadata_for_source,
    )

    update_source(
        db,
        embedding_provider=embedding_provider,
        source_id=existing.id,
        vehicle_config_path="vehicles/balerion-surveyor.json",
    )

    assert existing.vehicle_config_path == "vehicles/balerion-surveyor.json"
    assert seed_calls == [
        (
            (db,),
            {
                "source_id": existing.id,
                "vehicle_config_path": "vehicles/balerion-surveyor.json",
                "embedding_provider": embedding_provider,
                "prune_missing": True,
            },
        )
    ]


def test_create_discovered_channel_metadata_recovers_from_concurrent_insert() -> None:
    """Concurrent first-discovery inserts should recover by re-reading the winning row."""

    db = MagicMock()
    existing = MagicMock()
    existing.channel_origin = "discovered"
    existing.discovery_namespace = None
    savepoint = MagicMock()

    class ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def first(self):
            return self._rows[0] if self._rows else None

    db.execute.side_effect = [
        ScalarResult([]),
        ScalarResult([existing]),
    ]
    db.begin_nested.return_value = savepoint
    db.flush.side_effect = IntegrityError("insert", {}, Exception("duplicate key"))

    meta = create_discovered_channel_metadata(
        db,
        source_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        channel_name="decoder.aprs.payload_temp",
        discovery_namespace="decoder.aprs",
    )

    assert meta is existing
    savepoint.rollback.assert_called_once()
    savepoint.commit.assert_not_called()
    db.rollback.assert_not_called()


def test_source_has_telemetry_history_uses_stream_registry() -> None:
    """Historical telemetry checks should include owned stream ids from the registry."""

    db = MagicMock()
    statements: list[str] = []

    class ScalarOneResult:
        def scalar_one(self):
            return 1

    def fake_execute(statement):
        statements.append(str(statement))
        return ScalarOneResult()

    db.execute.side_effect = fake_execute

    assert source_has_telemetry_history(db, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa") is True
    assert len(statements) == 1
    assert "telemetry_streams" in statements[0]
    assert "LIKE" not in statements[0]


def test_update_source_rejects_simulator_definition_path_changes() -> None:
    """Simulator definitions are fixed by the runtime deployment and cannot drift in DB."""

    db = MagicMock()
    embedding_provider = MagicMock()
    existing = MagicMock()
    existing.id = "27a7e3d4-bbcc-4fa1-9e14-8ebabbea1be6"
    existing.vehicle_config_path = "simulators/drogonsat.yaml"
    existing.source_type = "simulator"
    db.get.return_value = existing

    with pytest.raises(ValueError) as exc_info:
        update_source(
            db,
            embedding_provider=embedding_provider,
            source_id=existing.id,
            vehicle_config_path="simulators/rhaegalsat.json",
        )

    assert "Cannot change vehicle_config_path for simulator sources" in str(exc_info.value)


def test_infer_auto_registration_fields_reads_simulator_base_url_from_full_config() -> None:
    item = SimpleNamespace(category="simulators")
    loaded = SimpleNamespace(
        path="simulators/drogonsat.yaml",
        parsed=SimpleNamespace(name="DrogonSat"),
    )

    fields = infer_auto_registration_fields("simulators/drogonsat.yaml", item, loaded)

    assert fields["source_type"] == "simulator"
    assert fields["base_url"] == "http://simulator:8001"


def test_update_source_live_only_history_mode_marks_backfill_complete() -> None:
    db = MagicMock()
    embedding_provider = MagicMock()
    existing = MagicMock()
    existing.id = "source-1"
    existing.source_type = "vehicle"
    existing.history_mode = "time_window_replay"
    existing.backfill_state = "running"
    existing.last_reconciled_at = datetime.now(timezone.utc)
    db.get.return_value = existing

    update_source(
        db,
        embedding_provider=embedding_provider,
        source_id=existing.id,
        history_mode="live_only",
    )

    assert existing.history_mode == "live_only"
    assert existing.backfill_state == "complete"
    assert existing.active_backfill_target_time is None
    assert existing.last_backfill_error is None


def test_backfill_progress_started_supersedes_running_target(monkeypatch) -> None:
    db = MagicMock()
    source = MagicMock()
    source.backfill_state = "running"
    source.active_backfill_target_time = datetime(2026, 4, 10, 12, tzinfo=timezone.utc)
    source.last_reconciled_at = datetime(2026, 4, 10, 9, tzinfo=timezone.utc)
    db.get.return_value = source
    target = datetime(2026, 4, 10, 13, tzinfo=timezone.utc)
    audit_events = []
    monkeypatch.setattr(
        "app.services.realtime_service.audit_log",
        lambda action, **kwargs: audit_events.append((action, kwargs)),
    )

    update_backfill_progress(
        db,
        source_id="source-1",
        status="started",
        target_time=target,
    )

    assert source.backfill_state == "running"
    assert source.active_backfill_target_time == target
    assert source.last_reconciled_at == datetime(2026, 4, 10, 9, tzinfo=timezone.utc)
    assert source.last_backfill_error is None
    assert audit_events == [
        (
            "sources.backfill_superseded",
            {
                "level": "warning",
                "source_id": "source-1",
                "old_target_time": datetime(2026, 4, 10, 12, tzinfo=timezone.utc),
                "new_target_time": target,
            },
        )
    ]


def test_backfill_completed_rejects_mismatched_target() -> None:
    db = MagicMock()
    source = MagicMock()
    source.backfill_state = "running"
    source.active_backfill_target_time = datetime(2026, 4, 10, 12, tzinfo=timezone.utc)
    db.get.return_value = source

    with pytest.raises(ValueError, match="target_time"):
        update_backfill_progress(
            db,
            source_id="source-1",
            status="completed",
            target_time=datetime(2026, 4, 10, 13, tzinfo=timezone.utc),
            chunk_end=datetime(2026, 4, 10, 13, tzinfo=timezone.utc),
        )


def test_backfill_completed_rejects_superseded_target(monkeypatch) -> None:
    db = MagicMock()
    source = MagicMock()
    old_target = datetime(2026, 4, 10, 12, tzinfo=timezone.utc)
    new_target = datetime(2026, 4, 10, 13, tzinfo=timezone.utc)
    source.backfill_state = "running"
    source.active_backfill_target_time = old_target
    db.get.return_value = source
    monkeypatch.setattr("app.services.realtime_service.audit_log", lambda *args, **kwargs: None)

    update_backfill_progress(
        db,
        source_id="source-1",
        status="started",
        target_time=new_target,
    )

    with pytest.raises(ValueError, match="target_time"):
        update_backfill_progress(
            db,
            source_id="source-1",
            status="completed",
            target_time=old_target,
            chunk_end=old_target,
        )


def test_backfill_completed_advances_checkpoint_and_clears_target() -> None:
    db = MagicMock()
    source = MagicMock()
    source.backfill_state = "running"
    target = datetime(2026, 4, 10, 12, tzinfo=timezone.utc)
    chunk_end = datetime(2026, 4, 10, 10, tzinfo=timezone.utc)
    source.active_backfill_target_time = target
    db.get.return_value = source

    update_backfill_progress(
        db,
        source_id="source-1",
        status="completed",
        target_time=target,
        chunk_end=chunk_end,
        backlog_drained=True,
    )

    assert source.last_reconciled_at == chunk_end
    assert source.backfill_state == "complete"
    assert source.active_backfill_target_time is None
    assert source.last_backfill_error is None


def test_update_live_state_sets_durable_source_state() -> None:
    db = MagicMock()
    source = MagicMock()
    db.get.return_value = source

    update_live_state(db, source_id="source-1", state="active")

    assert source.live_state == "active"
