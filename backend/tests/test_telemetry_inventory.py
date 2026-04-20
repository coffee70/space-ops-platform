from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.database import get_db
from app.services import telemetry_inventory_service as inventory_service
from app.services.telemetry_inventory_service import get_telemetry_inventory_for_source


def _metadata(
    name: str,
    *,
    source_id: str = "vehicle-a",
    units: str = "V",
    description: str | None = None,
    subsystem_tag: str | None = None,
    channel_origin: str = "catalog",
    discovery_namespace: str | None = None,
    red_low: float | None = None,
    red_high: float | None = None,
):
    return SimpleNamespace(
        id=uuid4(),
        source_id=source_id,
        name=name,
        units=units,
        description=description,
        subsystem_tag=subsystem_tag,
        channel_origin=channel_origin,
        discovery_namespace=discovery_namespace,
        red_low=red_low,
        red_high=red_high,
    )


def _db_with_metadata(metadata_rows: list[SimpleNamespace]) -> MagicMock:
    db = MagicMock()
    db.execute.return_value = MagicMock(
        scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=metadata_rows)))
    )
    return db


def test_inventory_returns_all_metadata_channels_for_source(monkeypatch) -> None:
    meta_a = _metadata("A_TEMP")
    meta_b = _metadata("B_TEMP")
    db = _db_with_metadata([meta_a, meta_b])
    monkeypatch.setattr(inventory_service, "_resolve_logical_source_id", lambda *_args: "vehicle-a")
    monkeypatch.setattr(inventory_service, "get_aliases_by_telemetry_ids", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "_latest_current_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "_latest_data_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "_statistics_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "infer_subsystem", lambda *_args, **_kwargs: "thermal")

    result = get_telemetry_inventory_for_source(db, "vehicle-a")

    assert [entry["name"] for entry in result] == ["A_TEMP", "B_TEMP"]


def test_inventory_includes_channels_with_no_data(monkeypatch) -> None:
    meta = _metadata("VBAT")
    db = _db_with_metadata([meta])
    monkeypatch.setattr(inventory_service, "_resolve_logical_source_id", lambda *_args: "vehicle-a")
    monkeypatch.setattr(inventory_service, "get_aliases_by_telemetry_ids", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "_latest_current_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "_latest_data_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "_statistics_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "infer_subsystem", lambda *_args, **_kwargs: "power")

    result = get_telemetry_inventory_for_source(db, "vehicle-a")

    assert result[0]["has_data"] is False
    assert result[0]["state"] == "no_data"
    assert result[0]["state_reason"] == "waiting_for_data"


def test_inventory_includes_current_value_and_timestamp_when_present(monkeypatch) -> None:
    meta = _metadata("VBAT")
    timestamp = datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc)
    db = _db_with_metadata([meta])
    monkeypatch.setattr(inventory_service, "_resolve_logical_source_id", lambda *_args: "vehicle-a")
    monkeypatch.setattr(inventory_service, "get_aliases_by_telemetry_ids", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        inventory_service,
        "_latest_current_rows",
        lambda *_args, **_kwargs: {
            meta.id: {
                "telemetry_id": meta.id,
                "stream_id": "stream-1",
                "generation_time": timestamp,
                "value": 4.2,
            }
        },
    )
    monkeypatch.setattr(inventory_service, "_latest_data_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "_statistics_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "infer_subsystem", lambda *_args, **_kwargs: "power")
    monkeypatch.setattr(inventory_service, "_compute_state", lambda *_args: ("normal", None))

    result = get_telemetry_inventory_for_source(db, "vehicle-a")

    assert result[0]["current_value"] == 4.2
    assert result[0]["last_timestamp"] == "2026-04-09T12:00:00+00:00"


def test_inventory_includes_aliases_origin_and_namespace(monkeypatch) -> None:
    meta = _metadata(
        "RAD_TEMP",
        channel_origin="discovered",
        discovery_namespace="satnogs.iss",
    )
    db = _db_with_metadata([meta])
    monkeypatch.setattr(inventory_service, "_resolve_logical_source_id", lambda *_args: "vehicle-a")
    monkeypatch.setattr(
        inventory_service,
        "get_aliases_by_telemetry_ids",
        lambda *_args, **_kwargs: {meta.id: ["RAD_T", "THERM_RAD"]},
    )
    monkeypatch.setattr(inventory_service, "_latest_current_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "_latest_data_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "_statistics_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "infer_subsystem", lambda *_args, **_kwargs: "thermal")

    result = get_telemetry_inventory_for_source(db, "vehicle-a")

    assert result[0]["aliases"] == ["RAD_T", "THERM_RAD"]
    assert result[0]["channel_origin"] == "discovered"
    assert result[0]["discovery_namespace"] == "satnogs.iss"


def test_inventory_computes_anomaly_and_state_fields(monkeypatch) -> None:
    meta = _metadata("BUS_VOLT", red_low=26.0, red_high=30.0)
    db = _db_with_metadata([meta])
    timestamp = datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc)
    stats = SimpleNamespace(mean=28.0, std_dev=0.5, n_samples=12)
    monkeypatch.setattr(inventory_service, "_resolve_logical_source_id", lambda *_args: "vehicle-a")
    monkeypatch.setattr(inventory_service, "get_aliases_by_telemetry_ids", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        inventory_service,
        "_latest_current_rows",
        lambda *_args, **_kwargs: {
            meta.id: {
                "telemetry_id": meta.id,
                "stream_id": "stream-1",
                "generation_time": timestamp,
                "value": 31.0,
            }
        },
    )
    monkeypatch.setattr(inventory_service, "_latest_data_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        inventory_service,
        "_statistics_rows",
        lambda *_args, **_kwargs: {("stream-1", meta.id): stats},
    )
    monkeypatch.setattr(inventory_service, "infer_subsystem", lambda *_args, **_kwargs: "power")
    monkeypatch.setattr(inventory_service, "_compute_state", lambda *_args: ("warning", "out_of_limits"))

    result = get_telemetry_inventory_for_source(db, "vehicle-a")

    assert result[0]["state"] == "warning"
    assert result[0]["state_reason"] == "out_of_limits"
    assert result[0]["is_anomalous"] is True
    assert result[0]["z_score"] == 6.0
    assert result[0]["n_samples"] == 12


def test_inventory_is_scoped_to_requested_source(monkeypatch) -> None:
    meta = _metadata("VBAT", source_id="vehicle-b")
    db = _db_with_metadata([meta])
    seen: dict[str, object] = {}

    def fake_aliases(_db, *, source_id, telemetry_ids):
        seen["source_id"] = source_id
        seen["telemetry_ids"] = telemetry_ids
        return {}

    monkeypatch.setattr(inventory_service, "_resolve_logical_source_id", lambda *_args: "vehicle-b")
    monkeypatch.setattr(inventory_service, "get_aliases_by_telemetry_ids", fake_aliases)
    monkeypatch.setattr(inventory_service, "_latest_current_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "_latest_data_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "_statistics_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "infer_subsystem", lambda *_args, **_kwargs: "power")

    get_telemetry_inventory_for_source(db, "vehicle-b")

    assert seen["source_id"] == "vehicle-b"
    assert seen["telemetry_ids"] == [meta.id]


def test_inventory_order_is_deterministic(monkeypatch) -> None:
    meta_a = _metadata("ALPHA")
    meta_b = _metadata("BRAVO")
    db = _db_with_metadata([meta_a, meta_b])
    monkeypatch.setattr(inventory_service, "_resolve_logical_source_id", lambda *_args: "vehicle-a")
    monkeypatch.setattr(inventory_service, "get_aliases_by_telemetry_ids", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "_latest_current_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "_latest_data_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "_statistics_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(inventory_service, "infer_subsystem", lambda *_args, **_kwargs: "other")

    result = get_telemetry_inventory_for_source(db, "vehicle-a")

    assert [entry["name"] for entry in result] == ["ALPHA", "BRAVO"]


def test_inventory_route_returns_channels(monkeypatch) -> None:
    import sys

    monkeypatch.setitem(sys.modules, "sentence_transformers", SimpleNamespace(SentenceTransformer=object))
    from app.routes.telemetry import router

    app = FastAPI()
    app.include_router(router, prefix="/telemetry")
    db = MagicMock()
    app.dependency_overrides[get_db] = lambda: db
    monkeypatch.setattr(
        "app.routes.telemetry.get_telemetry_inventory_for_source",
        lambda *_args, **_kwargs: [
            {
                "name": "VBAT",
                "aliases": [],
                "description": None,
                "units": "V",
                "subsystem_tag": "power",
                "channel_origin": "catalog",
                "discovery_namespace": None,
                "current_value": 4.2,
                "last_timestamp": "2026-04-09T12:00:00+00:00",
                "state": "normal",
                "state_reason": None,
                "z_score": None,
                "is_anomalous": False,
                "has_data": True,
                "red_low": None,
                "red_high": None,
                "n_samples": 5,
            }
        ],
    )

    response = TestClient(app).get("/telemetry/inventory", params={"source_id": "vehicle-a"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["channels"][0]["name"] == "VBAT"
