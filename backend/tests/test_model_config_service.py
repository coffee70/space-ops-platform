"""Tests for model-config-service (delegates validation to agent-runtime)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest

from app.services.model_config_service import (
    ModelConfigServiceError,
    delegate_validate_to_agent_runtime,
    load_model_registry_config,
    save_model_registry_config,
)

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

_RUNTIME_BAD_BODY = {
    "valid": False,
    "parsed": None,
    "errors": [{"loc": ["models"], "message": "invalid", "type": "semantic"}],
}


@pytest.fixture(autouse=True)
def _validation_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONTROL_PLANE_URL", "http://control-plane:8100")
    monkeypatch.delenv("AGENT_RUNTIME_BASE_URL", raising=False)


def test_delegate_validate_posts_to_control_plane_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    urls: list[str] = []

    def fake_post(url, json=None, timeout=None):  # noqa: ANN001
        urls.append(url)
        mock = MagicMock()
        mock.status_code = 200
        mock.json.return_value = _RUNTIME_OK_BODY
        return mock

    monkeypatch.setattr("httpx.post", fake_post)
    out = delegate_validate_to_agent_runtime("hello")
    assert out.valid is True
    assert out.parsed.default_model_id == "m1"
    assert urls[0] == "http://control-plane:8100/internal/runtime-services/agent-runtime-service/models/validate-config"


def test_delegate_validate_prefers_agent_runtime_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENT_RUNTIME_BASE_URL", "http://agent-runtime:8080")

    urls: list[str] = []

    def fake_post(url, json=None, timeout=None):  # noqa: ANN001
        urls.append(url)
        mock = MagicMock()
        mock.status_code = 200
        mock.json.return_value = _RUNTIME_OK_BODY
        return mock

    monkeypatch.setattr("httpx.post", fake_post)
    delegate_validate_to_agent_runtime("x")
    assert urls[0] == "http://agent-runtime:8080/models/validate-config"


def test_delegate_raises_503_on_connect_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(*args, **kwargs):  # noqa: ANN001
        raise httpx.ConnectError("refused", request=MagicMock())

    monkeypatch.setattr("httpx.post", fake_post)
    with pytest.raises(ModelConfigServiceError) as exc:
        delegate_validate_to_agent_runtime("x")
    assert exc.value.status_code == 503


def test_delegate_raises_503_on_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    mock = MagicMock()
    mock.status_code = 502
    mock.json.return_value = {}
    monkeypatch.setattr("httpx.post", lambda *a, **k: mock)
    with pytest.raises(ModelConfigServiceError) as exc:
        delegate_validate_to_agent_runtime("x")
    assert exc.value.status_code == 503


def test_save_refuses_when_runtime_invalid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MODEL_CONFIG_PATH", str(tmp_path / "models.local.yaml"))

    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = _RUNTIME_BAD_BODY
    monkeypatch.setattr("httpx.post", lambda *a, **k: mock)

    with pytest.raises(ModelConfigServiceError) as exc:
        save_model_registry_config(_MIN_VALID)
    assert exc.value.status_code == 400
    path = tmp_path / "models.local.yaml"
    assert not path.exists()


def test_save_writes_when_runtime_valid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "models.local.yaml"
    monkeypatch.setenv("MODEL_CONFIG_PATH", str(path))

    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = _RUNTIME_OK_BODY
    monkeypatch.setattr("httpx.post", lambda *a, **k: mock)

    resp = save_model_registry_config(_MIN_VALID.replace("\n", "\r\n"))
    assert resp.saved is True
    raw = path.read_bytes()
    assert b"\r\n" not in raw


def test_load_reads_file_and_attaches_validation_errors(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "models.local.yaml"
    path.write_text(_MIN_VALID, encoding="utf-8")
    monkeypatch.setenv("MODEL_CONFIG_PATH", str(path))

    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = _RUNTIME_BAD_BODY
    monkeypatch.setattr("httpx.post", lambda *a, **k: mock)

    out = load_model_registry_config()
    assert out.content == _MIN_VALID
    assert out.parsed is None
    assert len(out.validation_errors) == 1


def test_load_raises_503_when_validation_unavailable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "models.local.yaml"
    path.write_text(_MIN_VALID, encoding="utf-8")
    monkeypatch.setenv("MODEL_CONFIG_PATH", str(path))

    def fake_post(*args, **kwargs):  # noqa: ANN001
        raise httpx.ConnectError("down", request=MagicMock())

    monkeypatch.setattr("httpx.post", fake_post)
    with pytest.raises(ModelConfigServiceError) as exc:
        load_model_registry_config()
    assert exc.value.status_code == 503


def test_load_not_found_raises_404(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MODEL_CONFIG_PATH", str(tmp_path / "missing.yaml"))
    with pytest.raises(ModelConfigServiceError) as exc:
        load_model_registry_config()
    assert exc.value.status_code == 404


def test_model_config_path_takes_priority_over_legacy_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    a = tmp_path / "a.yaml"
    b = tmp_path / "b.yaml"
    a.write_text(_MIN_VALID, encoding="utf-8")
    b.write_text("oops: broken", encoding="utf-8")
    monkeypatch.setenv("MODEL_CONFIG_PATH", str(a))
    monkeypatch.setenv("AI_ENGINEER_MODELS_CONFIG_PATH", str(b))

    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = _RUNTIME_OK_BODY
    monkeypatch.setattr("httpx.post", lambda *a, **k: mock)

    out = load_model_registry_config()
    assert Path(out.path) == Path(a)


def test_load_requires_config_url_or_base(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "models.local.yaml"
    path.write_text(_MIN_VALID, encoding="utf-8")
    monkeypatch.setenv("MODEL_CONFIG_PATH", str(path))
    monkeypatch.delenv("CONTROL_PLANE_URL", raising=False)
    monkeypatch.delenv("AGENT_RUNTIME_BASE_URL", raising=False)
    with pytest.raises(ModelConfigServiceError) as exc:
        load_model_registry_config()
    assert exc.value.status_code == 500