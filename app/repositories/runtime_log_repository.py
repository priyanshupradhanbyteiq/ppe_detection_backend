from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Camera, CameraRuntimeLog


class RuntimeLogRepository:
    @staticmethod
    def _apply_filters(
        query,
        search: Optional[str] = None,
        event_type: Optional[str] = None,
        old_status: Optional[str] = None,
        new_status: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        camera_id: Optional[UUID] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ):
        if search and search.strip():
            pat = f"%{search.strip()}%"
            query = query.where(
                or_(
                    CameraRuntimeLog.message.ilike(pat),
                    CameraRuntimeLog.event_type.ilike(pat),
                    CameraRuntimeLog.pipeline_id.ilike(pat),
                    Camera.name.ilike(pat),
                )
            )

        if event_type and event_type.strip() and event_type.strip().lower() != "all":
            query = query.where(func.upper(CameraRuntimeLog.event_type) == event_type.strip().upper())

        if old_status and old_status.strip() and old_status.strip().lower() != "all":
            query = query.where(func.upper(CameraRuntimeLog.old_status) == old_status.strip().upper())

        if new_status and new_status.strip() and new_status.strip().lower() != "all":
            query = query.where(func.upper(CameraRuntimeLog.new_status) == new_status.strip().upper())

        if pipeline_id and pipeline_id.strip() and pipeline_id.strip().lower() != "all":
            query = query.where(func.upper(CameraRuntimeLog.pipeline_id) == pipeline_id.strip().upper())

        if camera_id is not None:
            query = query.where(CameraRuntimeLog.camera_id == camera_id)

        if start_date is not None:
            query = query.where(CameraRuntimeLog.created_at >= start_date)

        if end_date is not None:
            query = query.where(CameraRuntimeLog.created_at <= end_date)

        return query

    @staticmethod
    async def get_logs(
        db: AsyncSession,
        search: Optional[str] = None,
        event_type: Optional[str] = None,
        old_status: Optional[str] = None,
        new_status: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        camera_id: Optional[UUID] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        skip: int = 0,
        limit: int = 50,
    ) -> List[Tuple[CameraRuntimeLog, Optional[str]]]:
        query = (
            select(CameraRuntimeLog, Camera.name.label("camera_name"))
            .outerjoin(Camera, CameraRuntimeLog.camera_id == Camera.id)
        )
        query = RuntimeLogRepository._apply_filters(
            query,
            search=search,
            event_type=event_type,
            old_status=old_status,
            new_status=new_status,
            pipeline_id=pipeline_id,
            camera_id=camera_id,
            start_date=start_date,
            end_date=end_date,
        )
        query = query.order_by(CameraRuntimeLog.created_at.desc()).offset(skip).limit(limit)
        result = await db.execute(query)
        return list(result.all())

    @staticmethod
    async def count_logs(
        db: AsyncSession,
        search: Optional[str] = None,
        event_type: Optional[str] = None,
        old_status: Optional[str] = None,
        new_status: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        camera_id: Optional[UUID] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> int:
        query = (
            select(func.count(CameraRuntimeLog.id))
            .outerjoin(Camera, CameraRuntimeLog.camera_id == Camera.id)
        )
        query = RuntimeLogRepository._apply_filters(
            query,
            search=search,
            event_type=event_type,
            old_status=old_status,
            new_status=new_status,
            pipeline_id=pipeline_id,
            camera_id=camera_id,
            start_date=start_date,
            end_date=end_date,
        )
        result = await db.execute(query)
        return result.scalar() or 0

    @staticmethod
    async def get_summary(
        db: AsyncSession,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        camera_id: Optional[UUID] = None,
    ) -> Dict[str, int]:
        query = select(
            func.count(CameraRuntimeLog.id).label("total_logs"),
            func.count().filter(
                or_(
                    CameraRuntimeLog.event_type == "CAMERA_ONLINE",
                    CameraRuntimeLog.new_status == "ONLINE",
                )
            ).label("camera_online"),
            func.count().filter(
                or_(
                    CameraRuntimeLog.event_type == "CAMERA_STOPPED",
                    CameraRuntimeLog.new_status == "OFFLINE",
                )
            ).label("camera_offline"),
            func.count().filter(
                or_(
                    CameraRuntimeLog.event_type == "CAMERA_ERROR",
                    CameraRuntimeLog.new_status == "ERROR",
                )
            ).label("errors"),
            func.count().filter(
                CameraRuntimeLog.event_type == "CAMERA_SOURCE_ASSIGNED"
            ).label("source_assigned"),
        )
        if camera_id is not None:
            query = query.where(CameraRuntimeLog.camera_id == camera_id)
        if start_date is not None:
            query = query.where(CameraRuntimeLog.created_at >= start_date)
        if end_date is not None:
            query = query.where(CameraRuntimeLog.created_at <= end_date)

        result = await db.execute(query)
        row = result.one()
        return {
            "total_logs": row[0] or 0,
            "camera_online": row[1] or 0,
            "camera_offline": row[2] or 0,
            "errors": row[3] or 0,
            "source_assigned": row[4] or 0,
        }

    @staticmethod
    async def get_by_id(
        db: AsyncSession,
        log_id: UUID,
    ) -> Optional[Tuple[CameraRuntimeLog, Optional[str]]]:
        query = (
            select(CameraRuntimeLog, Camera.name.label("camera_name"))
            .outerjoin(Camera, CameraRuntimeLog.camera_id == Camera.id)
            .where(CameraRuntimeLog.id == log_id)
        )
        result = await db.execute(query)
        return result.first()
