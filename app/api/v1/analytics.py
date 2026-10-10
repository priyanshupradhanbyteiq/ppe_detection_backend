from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.dependecies.auth import get_optional_current_user
from app.repositories.analytics_repository import AnalyticsRepository
from app.schemas.analytics import EventsTrendResponse

router = APIRouter(
    tags=["Analytics"],
)


@router.get(
    "/events-trend",
    response_model=EventsTrendResponse,
    summary="Get 24-hour events trend breakdown (PPE, Smoke, Fire) for bar charts",
)
async def get_events_trend(
    camera_id: Optional[UUID] = Query(
        None,
        description="Filter trend for a specific camera UUID (or all cameras by default)",
    ),
    date: Optional[str] = Query(
        None,
        description="Optional date (YYYY-MM-DD) to show 24h trend for. Defaults to today or latest recorded event date.",
    ),
    interval_hours: int = Query(
        2,
        ge=1,
        le=12,
        description="Bucket interval size in hours (default: 2 hours)",
    ),
    demo: bool = Query(
        False,
        description="Set to true to show full realistic 24-hour sample data matching the dashboard screenshot",
    ),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_optional_current_user),
) -> EventsTrendResponse:
    """
    Returns time-bucketed event counts across 24 hours for PPE, Smoke, and Fire.
    Matches the 'Events Trend (Last 24 hours)' dashboard chart.
    """
    return await AnalyticsRepository.get_events_trend(
        db=db,
        camera_id=camera_id,
        date_str=date,
        interval_hours=interval_hours,
        demo=demo,
    )
