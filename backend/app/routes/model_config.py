"""Model registry file (models.local.yaml) admin routes for the model-config service."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.models.schemas import (
    ModelConfigFetchResponse,
    ModelConfigSaveRequest,
    ModelConfigSaveResponse,
    ModelConfigValidationRequest,
    ModelConfigValidationResponse,
)
from app.services.model_config_service import (
    ModelConfigServiceError,
    load_model_registry_config,
    save_model_registry_config,
    validate_model_registry_content,
)

router = APIRouter()


def _raise_http(exc: ModelConfigServiceError) -> None:
    detail: object = str(exc)
    if exc.errors:
        detail = {"message": str(exc), "errors": [error.model_dump() for error in exc.errors]}
    raise HTTPException(status_code=exc.status_code, detail=detail)


@router.get("", response_model=ModelConfigFetchResponse)
def get_model_registry_config_route():
    try:
        return load_model_registry_config()
    except ModelConfigServiceError as exc:
        _raise_http(exc)


@router.post("/validate", response_model=ModelConfigValidationResponse)
def validate_model_registry_route(
    body: ModelConfigValidationRequest,
) -> ModelConfigValidationResponse:
    try:
        return validate_model_registry_content(body.content)
    except ModelConfigServiceError as exc:
        _raise_http(exc)


@router.put("", response_model=ModelConfigSaveResponse)
def save_model_registry_route(body: ModelConfigSaveRequest) -> ModelConfigSaveResponse:
    try:
        return save_model_registry_config(body.content)
    except ModelConfigServiceError as exc:
        _raise_http(exc)
