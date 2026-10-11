from datetime import date, datetime, time
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.dependecies.auth import get_optional_current_user
from app.repositories.session_repository import SessionRepository
from app.schemas.camera_session import (
    CameraSessionDetailResponse,
    CameraSessionResponse,
    SessionAnalyticsResponse,
    SessionLogsListResponse,
    SessionSummaryResponse,
)

router = APIRouter(
    tags=["Camera Sessions"],
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
    response_model=SessionLogsListResponse,
    summary="Get paginated camera session logs with KPI summary and filters",
)
async def get_camera_sessions(
    search: Optional[str] = Query(None, description="Search across camera, pipeline, session ID, or error message"),
    camera_id: Optional[UUID] = Query(None, description="Filter by camera UUID"),
    status: Optional[str] = Query(None, description="Filter by status (ERROR, STOPPED, ONLINE)"),
    pipeline_id: Optional[str] = Query(None, description="Filter by pipeline ID"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD or ISO timestamp)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD or ISO timestamp)"),
    skip: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(10, ge=1, le=200, description="Items per page"),
    db: AsyncSession = Depends(get_db),
    _user=Depends(get_optional_current_user),
):
    parsed_start = _parse_start_date(start_date)
    parsed_end = _parse_end_date(end_date)

    items_data = await SessionRepository.get_sessions(
        db=db,
        search=search,
        camera_id=camera_id,
        status=status,
        pipeline_id=pipeline_id,
        start_date=parsed_start,
        end_date=parsed_end,
        skip=skip,
        limit=limit,
    )

    total_count = await SessionRepository.count_sessions(
        db=db,
        search=search,
        camera_id=camera_id,
        status=status,
        pipeline_id=pipeline_id,
        start_date=parsed_start,
        end_date=parsed_end,
    )

    summary_data = await SessionRepository.get_summary(
        db=db,
        search=search,
        camera_id=camera_id,
        status=status,
        pipeline_id=pipeline_id,
        start_date=parsed_start,
        end_date=parsed_end,
    )

    items = [CameraSessionResponse(**item) for item in items_data]
    summary = SessionSummaryResponse(**summary_data)

    return SessionLogsListResponse(
        summary=summary,
        total=total_count,
        skip=skip,
        limit=limit,
        items=items,
    )


@router.get(
    "/analytics",
    response_model=SessionAnalyticsResponse,
    summary="Get session analytics: status trend, status distribution, and top error reasons",
)
async def get_session_analytics(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD or ISO timestamp)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD or ISO timestamp)"),
    camera_id: Optional[UUID] = Query(None, description="Optional camera UUID filter"),
    pipeline_id: Optional[str] = Query(None, description="Optional pipeline ID filter"),
    db: AsyncSession = Depends(get_db),
    _user=Depends(get_optional_current_user),
):
    parsed_start = _parse_start_date(start_date)
    parsed_end = _parse_end_date(end_date)

    analytics_data = await SessionRepository.get_analytics(
        db=db,
        start_date=parsed_start,
        end_date=parsed_end,
        camera_id=camera_id,
        pipeline_id=pipeline_id,
    )
    return SessionAnalyticsResponse(**analytics_data)


@router.get(
    "/{session_id}",
    response_model=CameraSessionDetailResponse,
    summary="Get single camera session details by UUID",
)
async def get_camera_session_by_id(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
    _user=Depends(get_optional_current_user),
):
    session_data = await SessionRepository.get_by_id(db=db, session_id=session_id)
    if not session_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Camera session with ID {session_id} not found",
        )
    return CameraSessionDetailResponse(**session_data)
