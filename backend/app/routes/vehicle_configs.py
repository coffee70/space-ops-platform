"""Vehicle configuration management routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.models.schemas import (
    VehicleConfigCreateRequest,
    VehicleConfigFetchResponse,
    VehicleConfigListItem,
    VehicleConfigSaveResponse,
    VehicleConfigValidationRequest,
    VehicleConfigValidationResponse,
)
from app.services.vehicle_config_service import (
    VehicleConfigServiceError,
    create_vehicle_config,
    list_vehicle_configs,
    load_vehicle_config,
    update_vehicle_config,
    validate_vehicle_config_content,
)

router = APIRouter()


def _raise_http(exc: VehicleConfigServiceError) -> None:
    detail: object = str(exc)
    if exc.errors:
        detail = {
            "message": str(exc),
            "errors": [error.model_dump() for error in exc.errors],
        }
    raise HTTPException(status_code=exc.status_code, detail=detail)


@router.get("", response_model=list[VehicleConfigListItem])
def list_vehicle_configs_route() -> list[VehicleConfigListItem]:
    return list_vehicle_configs()


@router.get("/{path:path}", response_model=VehicleConfigFetchResponse)
def get_vehicle_config_route(path: str) -> VehicleConfigFetchResponse:
    try:
        return load_vehicle_config(path)
    except VehicleConfigServiceError as exc:
        _raise_http(exc)


@router.post("/validate", response_model=VehicleConfigValidationResponse)
def validate_vehicle_config_route(
    body: VehicleConfigValidationRequest,
) -> VehicleConfigValidationResponse:
    return validate_vehicle_config_content(
        body.content,
        path=body.path,
        filename=body.filename,
        format_hint=body.format,
    )


@router.post("", response_model=VehicleConfigSaveResponse)
def create_vehicle_config_route(body: VehicleConfigCreateRequest) -> VehicleConfigSaveResponse:
    try:
        return create_vehicle_config(body.path, body.content)
    except VehicleConfigServiceError as exc:
        _raise_http(exc)


@router.put("/{path:path}", response_model=VehicleConfigSaveResponse)
def update_vehicle_config_route(path: str, body: VehicleConfigCreateRequest) -> VehicleConfigSaveResponse:
    if body.path != path:
        raise HTTPException(status_code=400, detail="Body path must match URL path")
    try:
        return update_vehicle_config(path, body.content)
    except VehicleConfigServiceError as exc:
        _raise_http(exc)
