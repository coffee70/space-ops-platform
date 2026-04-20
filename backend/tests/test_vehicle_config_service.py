from __future__ import annotations

import json
from pathlib import Path

from app.services.vehicle_config_service import (
    VehicleConfigServiceError,
    create_vehicle_config,
    list_vehicle_configs,
    load_vehicle_config,
    update_vehicle_config,
    validate_vehicle_config_content,
)


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
            "position_mapping:",
            "  frame_type: gps_lla",
            "  lat_channel_name: GPS_LAT",
            "  lon_channel_name: GPS_LAT",
            "",
        ]
    )


def _sample_yaml_with_comments(name: str = "ISS") -> str:
    return "\n".join(
        [
            "# Vehicle configuration for operators",
            "version: 1",
            f"name: {name}",
            "channels:",
            "  # Navigation latitude feed",
            "  - name: GPS_LAT",
            '    units: "deg"',
            '    description: "Latitude"',
            '    subsystem: "nav"',
            "    mean: 0.0",
            "    std_dev: 1.0",
            "",
        ]
    )


def test_list_vehicle_configs_reads_metadata(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    (root / "vehicles").mkdir(parents=True)
    (root / "simulators").mkdir(parents=True)
    (root / "vehicles" / "iss.yaml").write_text(_sample_yaml("Station"), encoding="utf-8")
    (root / "simulators" / "demo.json").write_text('{"version":1,"name":"Demo","channels":[]}', encoding="utf-8")
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    items = list_vehicle_configs()

    assert [item.path for item in items] == ["simulators/demo.json", "vehicles/iss.yaml"]
    assert items[0].category == "simulators"
    assert items[1].name == "Station"
    assert items[1].format == "yaml"


def test_load_vehicle_config_by_relative_path(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    (root / "vehicles").mkdir(parents=True)
    (root / "vehicles" / "iss.yaml").write_text(_sample_yaml(), encoding="utf-8")
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    result = load_vehicle_config("vehicles/iss.yaml")

    assert result.path == "vehicles/iss.yaml"
    assert result.format == "yaml"
    assert result.parsed is not None
    assert result.parsed.channel_count == 1
    assert result.validation_errors == []


def test_validate_vehicle_config_content_handles_valid_yaml() -> None:
    result = validate_vehicle_config_content(_sample_yaml(), path="vehicles/iss.yaml")

    assert result.valid is True
    assert result.parsed is not None
    assert result.parsed.name == "ISS"
    assert result.errors == []


def test_validate_vehicle_config_content_returns_structured_errors() -> None:
    result = validate_vehicle_config_content(
        "version: 1\nchannels:\n  - name: GPS_LAT\n",
        path="vehicles/iss.yaml",
    )

    assert result.valid is False
    assert result.parsed is None
    assert result.errors
    assert result.errors[0].message


def test_create_vehicle_config_preserves_yaml_comments(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    root.mkdir()
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    result = create_vehicle_config("vehicles/new.yaml", _sample_yaml_with_comments("New Vehicle"))

    saved = (root / "vehicles" / "new.yaml").read_text(encoding="utf-8")
    assert result.path == "vehicles/new.yaml"
    assert result.saved is True
    assert saved == _sample_yaml_with_comments("New Vehicle")
    assert "# Vehicle configuration for operators" in saved
    assert "# Navigation latitude feed" in saved


def test_create_vehicle_config_normalizes_yaml_line_endings(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    root.mkdir()
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    create_vehicle_config(
        "vehicles/new.yaml",
        _sample_yaml_with_comments("New Vehicle").replace("\n", "\r\n"),
    )

    saved = (root / "vehicles" / "new.yaml").read_text(encoding="utf-8")
    assert "\r" not in saved
    assert "# Vehicle configuration for operators" in saved


def test_create_vehicle_config_normalizes_json(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    root.mkdir()
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    create_vehicle_config(
        "vehicles/new.json",
        '{  "version": 1, "name": "JSON Vehicle", "channels": [] }',
    )

    saved = (root / "vehicles" / "new.json").read_text(encoding="utf-8")
    assert saved.endswith("\n")
    assert json.loads(saved) == {
        "version": 1,
        "name": "JSON Vehicle",
        "channels": [],
        "scenarios": {},
    }


def test_update_vehicle_config_preserves_comments_when_fields_change(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    (root / "vehicles").mkdir(parents=True)
    target = root / "vehicles" / "iss.yaml"
    target.write_text(_sample_yaml_with_comments("Old Name"), encoding="utf-8")
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    updated_content = _sample_yaml_with_comments("Updated Name").replace(
        '    description: "Latitude"',
        '    description: "Latitude telemetry"',
    )
    result = update_vehicle_config("vehicles/iss.yaml", updated_content)

    saved = target.read_text(encoding="utf-8")
    assert result.parsed.name == "Updated Name"
    assert 'description: "Latitude telemetry"' in saved
    assert "# Vehicle configuration for operators" in saved
    assert "# Navigation latitude feed" in saved


def test_update_vehicle_config_rejects_invalid_yaml_without_overwriting(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    (root / "vehicles").mkdir(parents=True)
    target = root / "vehicles" / "iss.yaml"
    original = _sample_yaml_with_comments("Original")
    target.write_text(original, encoding="utf-8")
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    try:
        update_vehicle_config("vehicles/iss.yaml", "version: 1\nchannels:\n  - name: GPS_LAT\n")
    except VehicleConfigServiceError as exc:
        assert exc.status_code == 400
        assert exc.errors
    else:
        raise AssertionError("Expected validation error")

    assert target.read_text(encoding="utf-8") == original


def test_update_vehicle_config_round_trips_yaml_with_comments(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    (root / "vehicles").mkdir(parents=True)
    target = root / "vehicles" / "iss.yaml"
    original = _sample_yaml_with_comments("Round Trip")
    target.write_text(original, encoding="utf-8")
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    loaded = load_vehicle_config("vehicles/iss.yaml")
    update_vehicle_config("vehicles/iss.yaml", loaded.content)

    assert target.read_text(encoding="utf-8") == original


def test_update_vehicle_config_overwrites_existing_json_file(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    (root / "vehicles").mkdir(parents=True)
    target = root / "vehicles" / "iss.json"
    target.write_text('{"version":1,"name":"Old","channels":[]}', encoding="utf-8")
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    result = update_vehicle_config(
        "vehicles/iss.json",
        '{ "version": 1, "name": "Updated", "channels": [] }',
    )

    assert result.parsed.name == "Updated"
    saved = target.read_text(encoding="utf-8")
    assert saved.endswith("\n")
    assert json.loads(saved) == {
        "version": 1,
        "name": "Updated",
        "channels": [],
        "scenarios": {},
    }


def test_update_vehicle_config_overwrites_existing_file(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    (root / "vehicles").mkdir(parents=True)
    target = root / "vehicles" / "iss.yaml"
    target.write_text(_sample_yaml("Old Name"), encoding="utf-8")
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    result = update_vehicle_config("vehicles/iss.yaml", _sample_yaml("Updated Name"))

    assert result.parsed.name == "Updated Name"
    assert "Updated Name" in target.read_text(encoding="utf-8")


def test_vehicle_config_service_rejects_traversal(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    root.mkdir()
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    try:
        create_vehicle_config("../outside.yaml", _sample_yaml())
    except VehicleConfigServiceError as exc:
        assert exc.status_code == 400
        assert "stay under" in str(exc)
    else:
        raise AssertionError("Expected traversal error")


def test_vehicle_config_service_rejects_unsupported_extension(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "vehicle-configurations"
    root.mkdir()
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))

    try:
        create_vehicle_config("vehicles/bad.txt", _sample_yaml())
    except VehicleConfigServiceError as exc:
        assert exc.status_code == 400
        assert ".json, .yaml, or .yml" in str(exc)
    else:
        raise AssertionError("Expected unsupported extension error")
