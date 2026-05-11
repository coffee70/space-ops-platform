"""Tests for AI Engineer model registry config service."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.services.ai_engineer_model_config_service import (
    AiEngineerModelConfigServiceError,
    load_model_registry_config,
    save_model_registry_config,
    validate_model_registry_content,
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


def test_validate_accepts_valid_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AI_ENGINEER_MODELS_CONFIG_PATH", str(tmp_path / "models.local.yaml"))
    result = validate_model_registry_content(_MIN_VALID)
    assert result.valid is True
    assert result.parsed is not None
    assert result.parsed.provider_count == 1
    assert result.parsed.model_count == 1
    assert result.parsed.enabled_model_count == 1
    assert "openai" in result.parsed.provider_types


def test_validate_rejects_invalid_yaml_syntax() -> None:
    result = validate_model_registry_content("foo: [\n")
    assert result.valid is False
    assert result.parsed is None
    assert len(result.errors) >= 1


def test_validate_rejects_non_object_top_level() -> None:
    result = validate_model_registry_content("- not an object")
    assert result.valid is False


def test_validate_rejects_duplicate_model_ids() -> None:
    dup = (
        _MIN_VALID.rstrip()
        + """
  - id: m1
    providerRef: p1
    providerModelId: x
    enabled: false
"""
    )
    result = validate_model_registry_content(dup)
    assert result.valid is False


def test_validate_rejects_unknown_provider_ref() -> None:
    bad = _MIN_VALID.replace("providerRef: p1", "providerRef: missing")
    result = validate_model_registry_content(bad)
    assert result.valid is False


def test_validate_rejects_invalid_execution_mode() -> None:
    bad = _MIN_VALID.replace(
        "    defaultFor: [chat, coding, fast]",
        "    defaultFor: [chat, coding, fast]\n    governance:\n      allowedModes: [invalid]",
    )
    result = validate_model_registry_content(bad)
    assert result.valid is False


def test_validate_rejects_invalid_data_boundary() -> None:
    bad = _MIN_VALID.replace(
        "    defaultFor: [chat, coding, fast]",
        "    defaultFor: [chat, coding, fast]\n    governance:\n      dataBoundary: internet",
    )
    result = validate_model_registry_content(bad)
    assert result.valid is False


def test_save_preserves_line_endings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "models.local.yaml"
    monkeypatch.setenv("AI_ENGINEER_MODELS_CONFIG_PATH", str(path))
    content = _MIN_VALID.replace("\n", "\r\n")
    resp = save_model_registry_config(content)
    assert resp.saved is True
    raw = path.read_bytes()
    assert b"\r\n" not in raw


def test_load_reads_configured_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "models.local.yaml"
    path.write_text(_MIN_VALID, encoding="utf-8")
    monkeypatch.setenv("AI_ENGINEER_MODELS_CONFIG_PATH", str(path))
    resp = load_model_registry_config()
    assert resp.content == _MIN_VALID
    assert resp.parsed is not None


def test_load_not_found_returns_404(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AI_ENGINEER_MODELS_CONFIG_PATH", str(tmp_path / "missing.yaml"))
    with pytest.raises(AiEngineerModelConfigServiceError) as exc:
        load_model_registry_config()
    assert exc.value.status_code == 404
