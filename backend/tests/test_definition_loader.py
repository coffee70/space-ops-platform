"""Tests for shared vehicle configuration loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from telemetry_catalog.definitions import (
    canonical_vehicle_config_path,
    load_vehicle_config_file,
)


def test_load_simulator_yaml_definition() -> None:
    definition = load_vehicle_config_file("simulators/drogonsat.yaml")

    assert definition.name == "DrogonSat"
    assert definition.base_url == "http://simulator:8001"
    assert definition.position_mapping is not None
    assert definition.position_mapping.frame_type == "gps_lla"
    assert any(channel.name == "PROP_MAIN_TANK_PRESS" for channel in definition.channels)


def test_load_simulator_json_definition() -> None:
    definition = load_vehicle_config_file("simulators/rhaegalsat.json")

    assert definition.name == "RhaegalSat"
    assert definition.base_url == "http://simulator2:8001"
    assert definition.position_mapping is not None
    assert definition.position_mapping.frame_type == "ecef"
    assert any(channel.name == "PROP_TANK_B_PRESS" for channel in definition.channels)
    assert any(channel.name == "OBC_C_CPU_LOAD" for channel in definition.channels)


def test_load_definition_with_channel_aliases(tmp_path: Path) -> None:
    path = tmp_path / "with-aliases.yaml"
    path.write_text(
        "\n".join(
            [
                "version: 1",
                "channels:",
                "  - name: PWR_MAIN_BUS_VOLT",
                "    aliases: [BAT_V, BATTERY_VOLT, VBAT]",
                '    units: "V"',
                '    description: "Main bus voltage"',
                '    subsystem: "power"',
                "    mean: 28.0",
                "    std_dev: 0.2",
            ]
        ),
        encoding="utf-8",
    )

    definition = load_vehicle_config_file(str(path), root=tmp_path)

    assert definition.channels[0].aliases == ["BAT_V", "BATTERY_VOLT", "VBAT"]


def test_load_definition_with_ingestion_mappings(tmp_path: Path) -> None:
    path = tmp_path / "with-ingestion.yaml"
    path.write_text(
        "\n".join(
            [
                "version: 1",
                "channels:",
                "  - name: GPS_LAT",
                '    units: "deg"',
                '    description: "Latitude"',
                '    subsystem: "nav"',
                "    mean: 0.0",
                "    std_dev: 1.0",
                "ingestion:",
                "  stable_field_mappings:",
                "    latitude: GPS_LAT",
            ]
        ),
        encoding="utf-8",
    )

    definition = load_vehicle_config_file(str(path), root=tmp_path)

    assert definition.ingestion is not None
    assert definition.ingestion.stable_field_mappings == {"latitude": "GPS_LAT"}


def test_load_definition_rejects_alias_colliding_with_other_canonical_name(tmp_path: Path) -> None:
    path = tmp_path / "bad-alias.yaml"
    path.write_text(
        "\n".join(
            [
                "version: 1",
                "channels:",
                "  - name: PWR_MAIN_BUS_VOLT",
                "    aliases: [GPS_LAT]",
                '    units: "V"',
                '    description: "Main bus voltage"',
                '    subsystem: "power"',
                "    mean: 28.0",
                "    std_dev: 0.2",
                "  - name: GPS_LAT",
                '    units: "deg"',
                '    description: "Latitude"',
                '    subsystem: "nav"',
                "    mean: 0.0",
                "    std_dev: 1.0",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="collides with canonical name"):
        load_vehicle_config_file(str(path), root=tmp_path)


def test_canonical_vehicle_config_path_rejects_traversal(tmp_path: Path) -> None:
    root = tmp_path / "defs"
    root.mkdir()
    (root / "ok.yaml").write_text("version: 1\nchannels: []\n", encoding="utf-8")

    assert canonical_vehicle_config_path("ok.yaml", root=root) == "ok.yaml"

    with pytest.raises(ValueError):
        canonical_vehicle_config_path("../outside.yaml", root=root)

