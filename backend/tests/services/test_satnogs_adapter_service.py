from __future__ import annotations

import importlib.util
from pathlib import Path

from fastapi.testclient import TestClient


SERVICE_PATH = (
    Path(__file__).resolve().parents[2]
    / "services"
    / "satnogs-adapter-service"
    / "main.py"
)
CONFIG_EXAMPLE_PATH = Path(__file__).resolve().parents[2] / "app" / "adapters" / "satnogs" / "config.example.yaml"


def _load_service_module():
    spec = importlib.util.spec_from_file_location("satnogs_adapter_service_main", SERVICE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_status_is_healthy_without_live_or_satnogs_token(monkeypatch) -> None:
    monkeypatch.setenv("SATNOGS_ADAPTER_CONFIG", str(CONFIG_EXAMPLE_PATH))
    monkeypatch.setenv("SATNOGS_LIVE_ENABLED", "false")
    monkeypatch.delenv("SATNOGS_API_TOKEN", raising=False)
    module = _load_service_module()

    with TestClient(module.app) as client:
        assert client.get("/health").json() == {"status": "ok"}
        status = client.get("/status").json()

    assert status["live_enabled"] is False
    assert status["vehicle_name"] == "LASARSAT"
    assert status["norad_id"] == 62391
    assert status["transmitter_uuid_present"] is True
    assert status["source_resolved"] is False


def test_run_once_uses_runner_and_updates_status(monkeypatch) -> None:
    monkeypatch.setenv("SATNOGS_ADAPTER_CONFIG", str(CONFIG_EXAMPLE_PATH))
    monkeypatch.setenv("SATNOGS_LIVE_ENABLED", "false")
    module = _load_service_module()
    calls = {"run_once": 0}

    class Source:
        id = "resolved-source"

    class Runner:
        source_contract = Source()

        def run_live_once(self):
            calls["run_once"] += 1

    monkeypatch.setattr(module, "build_runner", lambda _path: Runner())

    with TestClient(module.app) as client:
        response = client.post("/run-once")
        status = client.get("/status").json()

    assert response.status_code == 200
    assert response.json() == {"status": "completed", "source_id": "resolved-source"}
    assert calls["run_once"] == 1
    assert status["source_resolved"] is True
    assert status["source_id"] == "resolved-source"
