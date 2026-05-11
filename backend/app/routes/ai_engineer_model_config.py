"""AI Engineer model registry (models.local.yaml) management routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.models.schemas import (
    AiEngineerModelConfigFetchResponse,
    AiEngineerModelConfigSaveRequest,
    AiEngineerModelConfigSaveResponse,
    AiEngineerModelConfigValidationRequest,
    AiEngineerModelConfigValidationResponse,
)
from app.services.ai_engineer_model_config_service import (
    AiEngineerModelConfigServiceError,
    load_model_registry_config,
    save_model_registry_config,
    validate_model_registry_content,
)

router = APIRouter()


def _raise_http(exc: AiEngineerModelConfigServiceError) -> None:
    detail: object = str(exc)
    if exc.errors:
        detail = {"message": str(exc), "errors": [error.model_dump() for error in exc.errors]}
    raise HTTPException(status_code=exc.status_code, detail=detail)


@router.get("", response_model=AiEngineerModelConfigFetchResponse)
def get_model_registry_config_route():
    try:
        return load_model_registry_config()
    except AiEngineerModelConfigServiceError as exc:
        _raise_http(exc)


@router.post("/validate", response_model=AiEngineerModelConfigValidationResponse)
def validate_model_registry_route(
    body: AiEngineerModelConfigValidationRequest,
) -> AiEngineerModelConfigValidationResponse:
    return validate_model_registry_content(body.content)


@router.put("", response_model=AiEngineerModelConfigSaveResponse)
def save_model_registry_route(body: AiEngineerModelConfigSaveRequest) -> AiEngineerModelConfigSaveResponse:
    try:
        return save_model_registry_config(body.content)
    except AiEngineerModelConfigServiceError as exc:
        _raise_http(exc)
