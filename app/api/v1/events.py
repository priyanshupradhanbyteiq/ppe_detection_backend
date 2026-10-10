from datetime import date, datetime, time
from typing import Optional, Union
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.dependecies.auth import get_optional_current_user
from app.models.ppe_event import PPEEvent
from app.repositories.ppe_event_repository import PPEEventRepository
from app.schemas.ppe_event import PPEEventListResponse, PPEEventResponse

router = APIRouter(
    tags=["PPE Events"],
)


def _serialize_event(event: PPEEvent) -> PPEEventResponse:
    return PPEEventResponse(
        id=event.id,
        camera_id=event.camera_id,
        camera_name=event.camera.name if event.camera else None,
        location_name=event.camera.location_name if event.camera else None,
        session_id=event.session_id,
        tracking_session_id=event.tracking_session_id,
        person_track_id=event.person_track_id,
        ppe_type=event.ppe_type,
        violation_type=event.violation_type,
        event_type=event.event_type,
        status=event.status,
        severity=event.severity,
        confidence=event.confidence,
        violation_ratio=event.violation_ratio,
        missing_count=event.missing_count,
        present_count=event.present_count,
        unknown_count=event.unknown_count,
        first_observed_at=event.first_observed_at,
        confirmed_at=event.confirmed_at,
        last_observed_at=event.last_observed_at,
        resolved_at=event.resolved_at,
        resolution_reason=event.resolution_reason,
        snapshot_url=event.snapshot_url,
        snapshot_public_id=event.snapshot_public_id,
        snapshot_captured_at=event.snapshot_captured_at,
        clip_url=event.clip_url,
        created_at=event.created_at,
        updated_at=event.updated_at,
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
        start_date=parsed_start,
        end_date=parsed_end,
        event_type=event_type,
        severity=severity,
    )

    events = await PPEEventRepository.get_events(
        db=db,
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
