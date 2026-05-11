"""Integration tests for model-config HTTP routes (with validation mocked)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient
import httpx
import pytest

from app.routes.model_config import router

_MIN_VALID = """version: 1
defaults:
  chatModel: m1
  codingModel: m1
  fastModel: m1
  restrictedModel: m1
providers:
  p1:
    type: openai
    displayName: OpenAI
    apiKeyEnv: OPENAI_API_KEY
models:
  - id: m1
    providerRef: p1
    providerModelId: gpt-4o-mini
    enabled: true
    defaultFor: [chat, coding, fast]
"""

_RUNTIME_OK_BODY = {
    "valid": True,
    "parsed": {
        "provider_count": 1,
        "model_count": 1,
        "enabled_model_count": 1,
        "default_model_id": "m1",
        "provider_types": ["openai"],
        "missing_api_key_envs": [],
        "warnings": [],
    },
    "errors": [],
}


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/model-config")
    return TestClient(app)


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONTROL_PLANE_URL", "http://control-plane:8100")
    monkeypatch.delenv("AGENT_RUNTIME_BASE_URL", raising=False)


def test_get_model_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "models.local.yaml"
    path.write_text(_MIN_VALID, encoding="utf-8")
    monkeypatch.setenv("MODEL_CONFIG_PATH", str(path))

    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = _RUNTIME_OK_BODY
    monkeypatch.setattr("httpx.post", lambda *a, **k: mock)

    response = _client().get("/model-config")
    assert response.status_code == 200
    payload = response.json()
    assert payload["format"] == "yaml"
    assert payload["parsed"]["provider_count"] == 1


def test_get_returns_503_when_runtime_down(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "models.local.yaml"
    path.write_text(_MIN_VALID, encoding="utf-8")
    monkeypatch.setenv("MODEL_CONFIG_PATH", str(path))

    def boom(*args, **kwargs):  # noqa: ANN002
        raise httpx.ConnectError("refused", request=MagicMock())

    monkeypatch.setattr("httpx.post", boom)

    response = _client().get("/model-config")
    assert response.status_code == 503


def test_validate_model_config(monkeypatch: pytest.MonkeyPatch) -> None:
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = _RUNTIME_OK_BODY
    monkeypatch.setattr("httpx.post", lambda *a, **k: mock)

    response = _client().post("/model-config/validate", json={"content": _MIN_VALID})
    assert response.status_code == 200
    assert response.json()["valid"] is True


def test_put_model_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "models.local.yaml"
    monkeypatch.setenv("MODEL_CONFIG_PATH", str(path))

    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = _RUNTIME_OK_BODY
    monkeypatch.setattr("httpx.post", lambda *a, **k: mock)

    response = _client().put("/model-config", json={"content": _MIN_VALID})
    assert response.status_code == 200
    assert path.read_text(encoding="utf-8") == _MIN_VALID.replace("\r\n", "\n")


def test_put_refuses_when_runtime_invalid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "models.local.yaml"
    monkeypatch.setenv("MODEL_CONFIG_PATH", str(path))

    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = {"valid": False, "parsed": None, "errors": [{"loc": [], "message": "bad", "type": "t"}]}
    monkeypatch.setattr("httpx.post", lambda *a, **k: mock)

    response = _client().put("/model-config", json={"content": "x"})
    assert response.status_code == 400
    assert not path.exists()
