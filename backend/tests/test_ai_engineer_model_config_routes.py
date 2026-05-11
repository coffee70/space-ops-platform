"""Tests for AI Engineer model registry routes."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routes.ai_engineer_model_config import router

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


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/model-config")
    return TestClient(app)


def test_get_model_config(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "models.local.yaml"
    path.write_text(_MIN_VALID, encoding="utf-8")
    monkeypatch.setenv("AI_ENGINEER_MODELS_CONFIG_PATH", str(path))

    response = _client().get("/model-config")
    assert response.status_code == 200
    payload = response.json()
    assert payload["format"] == "yaml"
    assert payload["parsed"]["provider_count"] == 1


def test_validate_model_config() -> None:
    response = _client().post("/model-config/validate", json={"content": _MIN_VALID})
    assert response.status_code == 200
    assert response.json()["valid"] is True


def test_put_model_config(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "models.local.yaml"
    monkeypatch.setenv("AI_ENGINEER_MODELS_CONFIG_PATH", str(path))

    response = _client().put("/model-config", json={"content": _MIN_VALID})
    assert response.status_code == 200
    assert path.read_text(encoding="utf-8") == _MIN_VALID.replace("\r\n", "\n")
