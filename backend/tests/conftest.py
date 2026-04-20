from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def default_vehicle_config_root(monkeypatch: pytest.MonkeyPatch) -> None:
    root = Path(__file__).resolve().parent / "fixtures" / "vehicle-configurations"
    monkeypatch.setenv("VEHICLE_CONFIG_ROOT", str(root))
