"""Admin API for models.local.yaml: file I/O only; validation is delegated to agent-runtime."""

from __future__ import annotations

import os
from pathlib import Path

import httpx

from app.models.schemas import (
    ModelConfigFetchResponse,
    ModelConfigSaveResponse,
    ModelConfigValidationError,
    ModelConfigValidationResponse,
)


class ModelConfigServiceError(ValueError):
    """Raised when the model-config service cannot complete a request."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = 400,
        errors: list[ModelConfigValidationError] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.errors = errors or []


def _normalize_line_endings(content: str) -> str:
    return content.replace("\r\n", "\n").replace("\r", "\n")


def _config_file_path() -> Path:
    raw = (
        os.environ.get("MODEL_CONFIG_PATH")
        or os.environ.get("AI_ENGINEER_MODELS_CONFIG_PATH")
        or os.environ.get("AGENT_RUNTIME_MODELS_CONFIG_PATH")
        or ""
    ).strip()
    if not raw:
        raise ModelConfigServiceError(
            "Model config path is not configured (set MODEL_CONFIG_PATH or AI_ENGINEER_MODELS_CONFIG_PATH or AGENT_RUNTIME_MODELS_CONFIG_PATH).",
            status_code=500,
        )
    return Path(raw).expanduser().resolve()


def _runtime_validate_url() -> str:
    base = os.environ.get("AGENT_RUNTIME_BASE_URL", "").strip().rstrip("/")
    if base:
        return f"{base}/models/validate-config"
    cp = os.environ.get("CONTROL_PLANE_URL", "").strip().rstrip("/")
    if cp:
        return f"{cp}/internal/runtime-services/agent-runtime-service/models/validate-config"
    raise ModelConfigServiceError(
        "Cannot reach agent-runtime for validation (set AGENT_RUNTIME_BASE_URL or CONTROL_PLANE_URL).",
        status_code=500,
    )


def delegate_validate_to_agent_runtime(content: str) -> ModelConfigValidationResponse:
    """POST content to agent-runtime /models/validate-config (authoritative validation)."""

    url = _runtime_validate_url()
    try:
        response = httpx.post(url, json={"content": content}, timeout=30.0)
    except httpx.RequestError as exc:
        raise ModelConfigServiceError(
            "Agent runtime validation endpoint unavailable.",
            status_code=503,
        ) from exc
    if response.status_code >= 400:
        raise ModelConfigServiceError(
            "Agent runtime validation endpoint unavailable.",
            status_code=503,
        )
    try:
        return ModelConfigValidationResponse.model_validate(response.json())
    except Exception as exc:
        raise ModelConfigServiceError(
            "Invalid response from agent runtime validation.",
            status_code=502,
        ) from exc


def validate_model_registry_content(content: str) -> ModelConfigValidationResponse:
    return delegate_validate_to_agent_runtime(content)


def load_model_registry_config() -> ModelConfigFetchResponse:
    path = _config_file_path()
    if not path.is_file():
        hint = ""
        example = path.parent / "models.local.yaml.example"
        if example.is_file():
            hint = f" Copy or symlink from {example} if you are bootstrapping a new environment."
        raise ModelConfigServiceError(
            f"Model registry file not found at {path}.{hint}",
            status_code=404,
        )

    raw_content = path.read_text(encoding="utf-8")
    validation = delegate_validate_to_agent_runtime(raw_content)
    return ModelConfigFetchResponse(
        path=str(path),
        content=raw_content,
        format="yaml",
        parsed=validation.parsed if validation.valid else None,
        validation_errors=list(validation.errors),
    )


def save_model_registry_config(content: str) -> ModelConfigSaveResponse:
    path = _config_file_path()
    validation = delegate_validate_to_agent_runtime(content)
    if not validation.valid or validation.parsed is None:
        raise ModelConfigServiceError(
            "Model registry validation failed",
            status_code=400,
            errors=list(validation.errors),
        )

    normalized = _normalize_line_endings(content)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(normalized, encoding="utf-8")

    return ModelConfigSaveResponse(path=str(path), parsed=validation.parsed, saved=True)
