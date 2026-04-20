from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routes.vehicle_configs import router


def _sample_yaml(name: str = "ISS") -> str:
    return "\n".join(
        [
            "version: 1",
            f"name: {name}",
            "channels:",
            "  - name: GPS_LAT",
            '    units: "deg"',
            '    description: "Latitude"',
            '    subsystem: "nav"',
            "    mean: 0.0",
            "    std_dev: 1.0",
            "",
        ]
    )


def _sample_yaml_with_comments(name: str = "ISS") -> str:
    return "\n".join(
        [
            "# Operator note",
            "version: 1",
            f"name: {name}",
            "channels:",
            "  # Primary latitude feed",
            "  - name: GPS_LAT",
            '    units: "deg"',
            '    description: "Latitude"',
            '    subsystem: "nav"',
            "    mean: 0.0",
            "    std_dev: 1.0",
            "",
        ]
    )


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/vehicle-configs")
    return TestClient(app)


def test_get_vehicle_configs_lists_files(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    (root / "vehicles").mkdir(parents=True)
    (root / "vehicles" / "iss.yaml").write_text(_sample_yaml(), encoding="utf-8")
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    response = _client().get("/vehicle-configs")

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["path"] == "vehicles/iss.yaml"
    assert payload[0]["category"] == "vehicles"


def test_get_vehicle_config_returns_content_and_validation(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    (root / "vehicles").mkdir(parents=True)
    (root / "vehicles" / "iss.yaml").write_text(_sample_yaml(), encoding="utf-8")
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    response = _client().get("/vehicle-configs/vehicles/iss.yaml")

    assert response.status_code == 200
    payload = response.json()
    assert payload["path"] == "vehicles/iss.yaml"
    assert payload["parsed"]["name"] == "ISS"
    assert payload["validation_errors"] == []


def test_validate_vehicle_config_route_reports_schema_errors() -> None:
    response = _client().post(
        "/vehicle-configs/validate",
        json={"path": "vehicles/iss.yaml", "content": "version: 1\nchannels:\n  - name: GPS_LAT\n"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["valid"] is False
    assert payload["errors"]


def test_create_vehicle_config_route_saves_file(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    root.mkdir()
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    response = _client().post(
        "/vehicle-configs",
        json={"path": "vehicles/new.yaml", "content": _sample_yaml_with_comments("Created")},
    )

    assert response.status_code == 200
    assert (root / "vehicles" / "new.yaml").exists()
    assert response.json()["parsed"]["name"] == "Created"
    saved = (root / "vehicles" / "new.yaml").read_text(encoding="utf-8")
    assert "# Operator note" in saved
    assert "# Primary latitude feed" in saved


def test_update_vehicle_config_route_updates_existing_file(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    (root / "vehicles").mkdir(parents=True)
    target = root / "vehicles" / "iss.yaml"
    target.write_text(_sample_yaml_with_comments("Old"), encoding="utf-8")
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    response = _client().put(
        "/vehicle-configs/vehicles/iss.yaml",
        json={
            "path": "vehicles/iss.yaml",
            "content": _sample_yaml_with_comments("Updated").replace(
                '    description: "Latitude"',
                '    description: "Latitude telemetry"',
            ),
        },
    )

    assert response.status_code == 200
    assert response.json()["parsed"]["name"] == "Updated"
    saved = target.read_text(encoding="utf-8")
    assert "# Operator note" in saved
    assert "# Primary latitude feed" in saved
    assert 'description: "Latitude telemetry"' in saved


def test_vehicle_config_route_rejects_traversal(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    root.mkdir()
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    response = _client().post(
        "/vehicle-configs",
        json={"path": "../outside.yaml", "content": _sample_yaml()},
    )

    assert response.status_code == 400


def test_vehicle_config_route_rejects_unsupported_extension(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    root.mkdir()
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    response = _client().post(
        "/vehicle-configs",
        json={"path": "vehicles/bad.txt", "content": _sample_yaml()},
    )

    assert response.status_code == 400
