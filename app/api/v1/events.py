from datetime import date, datetime, time
from typing import List, Optional, Union
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.dependecies.auth import get_optional_current_user
from app.services.ppe_event_serialization import serialize_event as _serialize_event
from app.repositories.ppe_event_repository import PPEEventRepository
from app.schemas.ppe_event import PPEEventListResponse, PPEEventResponse

router = APIRouter(
    tags=["PPE Events"],
)



def _parse_start_date(val: Optional[str]) -> Optional[datetime]:
    if not val or not val.strip():
        return None
    s = val.strip()
    try:
        dt = datetime.fromisoformat(s)
        if len(s) <= 10:
            return datetime.combine(dt.date(), time.min)
        return dt
    except Exception:
        pass
    try:
        d = date.fromisoformat(s)
        return datetime.combine(d, time.min)
    except Exception:
        return None


def _parse_end_date(val: Optional[str]) -> Optional[datetime]:
    if not val or not val.strip():
        return None
    s = val.strip()
    try:
        dt = datetime.fromisoformat(s)
        if len(s) <= 10:
            return datetime.combine(dt.date(), time.max)
        return dt
    except Exception:
        pass
    try:
        d = date.fromisoformat(s)
        return datetime.combine(d, time.max)
    except Exception:
        return None


@router.get(
    "",
    response_model=PPEEventListResponse,
    summary="List safety events filtered by Date Range, Event Type, and Severity",
)
async def list_events(
    camera_id: Optional[UUID] = Query(
        None,
        description="Filter events by camera UUID",
    ),
    start_date: Optional[str] = Query(
        None,
        description="Start date (e.g. 2026-10-10 or 2026-10-10T00:00:00Z)",
    ),
    end_date: Optional[str] = Query(
        None,
        description="End date (e.g. 2026-10-10 or 2026-10-10T23:59:59Z)",
    ),
    event_type: Optional[str] = Query(
        None,
        description="Filter events by type/violation (matches 'All Event Types' dropdown)",
    ),
    severity: Optional[str] = Query(
        None,
        description="Filter events by severity (matches 'All Severities' dropdown, e.g. CRITICAL, HIGH)",
    ),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(50, ge=1, le=100, description="Max number of records to return"),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_optional_current_user),
) -> PPEEventListResponse:
    parsed_start = _parse_start_date(start_date)
    parsed_end = _parse_end_date(end_date)

    total = await PPEEventRepository.count_events(
        db=db,
        camera_id=camera_id,
        start_date=parsed_start,
        end_date=parsed_end,
        event_type=event_type,
        severity=severity,
    )

    events = await PPEEventRepository.get_events(
        db=db,
        camera_id=camera_id,
        start_date=parsed_start,
        end_date=parsed_end,
        event_type=event_type,
        severity=severity,
        skip=skip,
        limit=limit,
    )

    items = [_serialize_event(e) for e in events]

    return PPEEventListResponse(
        total=total,
        skip=skip,
        limit=limit,
        items=items,
    )


@router.get(
    "/camera/{camera_id}",
    response_model=List[PPEEventResponse],
    summary="Get all safety events for a specific camera ID",
)
async def get_camera_events(
    camera_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_optional_current_user),
) -> List[PPEEventResponse]:
    events = await PPEEventRepository.get_events_by_camera(db=db, camera_id=camera_id)
    return [_serialize_event(e) for e in events]


@router.get(
    "/{event_id}",
    response_model=PPEEventResponse,
    summary="Get single safety event details by ID",
)
async def get_event(
    event_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_optional_current_user),
) -> PPEEventResponse:
    event = await PPEEventRepository.get_by_id(db=db, event_id=event_id)
    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PPE Event with ID '{event_id}' not found",
        )
    return _serialize_event(event)
