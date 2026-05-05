from __future__ import annotations

import importlib
import sys


def _reload_simulator_main():
    sys.modules.pop("app.simulator.routes", None)
    return importlib.import_module("app.simulator.routes")


def test_simulator_default_source_id_is_empty_without_env(monkeypatch) -> None:
    monkeypatch.delenv("SIMULATOR_SOURCE_ID", raising=False)

    module = _reload_simulator_main()

    assert module.DEFAULT_VEHICLE_ID == ""
    assert not module._generate_stream_id(None).startswith("simulator-")


def test_simulator_default_source_id_honors_env_override(monkeypatch) -> None:
    monkeypatch.setenv("SIMULATOR_SOURCE_ID", "custom-simulator")

    module = _reload_simulator_main()

    assert module.DEFAULT_VEHICLE_ID == "custom-simulator"
    assert module._generate_stream_id(None).startswith("custom-simulator-")
