"""Position mapping and latest position API routes."""

from __future__ import annotations

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.schemas import (
    PositionChannelMappingSchema,
    PositionChannelMappingUpsert,
    PositionSample,
)
from app.services.position_service import (
    delete_mapping,
    get_latest_positions,
    list_mappings,
    upsert_mapping,
)

router = APIRouter()


@router.get(
    "/position/config",
    response_model=List[PositionChannelMappingSchema],
)
def get_position_config(
    vehicle_id: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """List active position channel mappings, optionally filtered by vehicle."""
    mappings = list_mappings(db, vehicle_id=vehicle_id)
    return [PositionChannelMappingSchema.model_validate(m) for m in mappings]


@router.post(
    "/position/config",
    response_model=PositionChannelMappingSchema,
)
def upsert_position_config(
    body: PositionChannelMappingUpsert,
    db: Session = Depends(get_db),
):
    """Create or update a position channel mapping for a source."""
    try:
        mapping = upsert_mapping(db, body)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return PositionChannelMappingSchema.model_validate(mapping)


@router.delete("/position/config/{mapping_id}")
def delete_position_config(
    mapping_id: str,
    db: Session = Depends(get_db),
):
    """Delete a position mapping."""
    try:
        mapping_uuid = UUID(mapping_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid mapping id")
    deleted = delete_mapping(db, mapping_uuid)
    if not deleted:
        raise HTTPException(status_code=404, detail="Mapping not found")
    return {"status": "deleted"}


@router.get(
    "/position/latest",
    response_model=List[PositionSample],
)
def latest_positions(
    vehicle_ids: Optional[List[str]] = Query(
        default=None,
        description="Optional list of vehicle IDs to filter by.",
    ),
    db: Session = Depends(get_db),
):
    """Resolve latest positions for all mapped vehicles (or a filtered subset)."""
    positions = get_latest_positions(
        db,
        vehicle_ids=vehicle_ids,
    )
    return positions
