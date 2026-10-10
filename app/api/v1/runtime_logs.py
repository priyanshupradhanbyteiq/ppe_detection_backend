from datetime import date, datetime, time
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.dependecies.auth import get_optional_current_user
from app.models.camera_runtime_log import CameraRuntimeLog
from app.repositories.runtime_log_repository import RuntimeLogRepository
from app.schemas.runtime_log import (
    RuntimeLogListResponse,
    RuntimeLogResponse,
    RuntimeLogSummaryResponse,
)

router = APIRouter(
    tags=["Runtime Logs"],
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


def _serialize_log(log: CameraRuntimeLog, camera_name: Optional[str]) -> RuntimeLogResponse:
    source_index = None
    if isinstance(log.metadata_json, dict):
        source_index = log.metadata_json.get("source_index")

    return RuntimeLogResponse(
        id=log.id,
        camera_id=log.camera_id,
        camera_name=camera_name,
        session_id=log.session_id,
        worker_id=log.worker_id,
        pipeline_id=log.pipeline_id,
        event_type=log.event_type,
        old_status=log.old_status,
        new_status=log.new_status,
        source_index=source_index,
        message=log.message,
        metadata=log.metadata_json,
        created_at=log.created_at,
    )


@router.get(
    "",
    response_model=RuntimeLogListResponse,
    summary="List pipeline runtime logs with search, status/event filters, date range, and KPI summary",
)
async def list_runtime_logs(
    search: Optional[str] = Query(
        None,
        description="Search logs across message, event type, pipeline, and camera name",
    ),
    event_type: Optional[str] = Query(
        None,
        description="Filter by event type (e.g. CAMERA_ONLINE, CAMERA_STOPPED, CAMERA_ERROR)",
    ),
    old_status: Optional[str] = Query(
        None,
        description="Filter by previous status (e.g. ONLINE, OFFLINE, CONNECTING, ERROR)",
    ),
    new_status: Optional[str] = Query(
        None,
        description="Filter by new status (e.g. ONLINE, OFFLINE, CONNECTING, ERROR)",
    ),
    pipeline_id: Optional[str] = Query(
        None,
        description="Filter by pipeline ID (e.g. default-pipeline)",
    ),
    camera_id: Optional[UUID] = Query(
        None,
        description="Filter by specific camera UUID",
    ),
    start_date: Optional[str] = Query(
        None,
        description="Start date (e.g. 2026-10-01 or 2026-10-01T00:00:00Z)",
    ),
    end_date: Optional[str] = Query(
        None,
        description="End date (e.g. 2026-10-10 or 2026-10-10T23:59:59Z)",
    ),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(50, ge=1, le=100, description="Max number of records to return"),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_optional_current_user),
) -> RuntimeLogListResponse:
    parsed_start = _parse_start_date(start_date)
    parsed_end = _parse_end_date(end_date)

    summary_data = await RuntimeLogRepository.get_summary(
        db=db,
        start_date=parsed_start,
        end_date=parsed_end,
        camera_id=camera_id,
    )

    total = await RuntimeLogRepository.count_logs(
        db=db,
        search=search,
        event_type=event_type,
        old_status=old_status,
        new_status=new_status,
        pipeline_id=pipeline_id,
        camera_id=camera_id,
        start_date=parsed_start,
        end_date=parsed_end,
    )

    logs = await RuntimeLogRepository.get_logs(
        db=db,
        search=search,
        event_type=event_type,
        old_status=old_status,
        new_status=new_status,
        pipeline_id=pipeline_id,
        camera_id=camera_id,
        start_date=parsed_start,
        end_date=parsed_end,
        skip=skip,
        limit=limit,
    )

    items = [_serialize_log(log, cam_name) for log, cam_name in logs]

    return RuntimeLogListResponse(
        summary=RuntimeLogSummaryResponse(**summary_data),
        total=total,
        skip=skip,
        limit=limit,
        items=items,
    )


@router.get(
    "/summary",
    response_model=RuntimeLogSummaryResponse,
    summary="Get top KPI summary counts for pipeline logs",
)
async def get_runtime_logs_summary(
    start_date: Optional[str] = Query(None, description="Start date filter"),
    end_date: Optional[str] = Query(None, description="End date filter"),
    camera_id: Optional[UUID] = Query(None, description="Camera filter"),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_optional_current_user),
) -> RuntimeLogSummaryResponse:
    parsed_start = _parse_start_date(start_date)
    parsed_end = _parse_end_date(end_date)

    summary_data = await RuntimeLogRepository.get_summary(
        db=db,
        start_date=parsed_start,
        end_date=parsed_end,
        camera_id=camera_id,
    )
    return RuntimeLogSummaryResponse(**summary_data)


@router.get(
    "/{log_id}",
    response_model=RuntimeLogResponse,
    summary="Get single pipeline runtime log details by ID",
)
async def get_runtime_log(
    log_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_optional_current_user),
) -> RuntimeLogResponse:
    result = await RuntimeLogRepository.get_by_id(db=db, log_id=log_id)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Camera runtime log with ID '{log_id}' not found",
        )
    log, cam_name = result
    return _serialize_log(log, cam_name)
