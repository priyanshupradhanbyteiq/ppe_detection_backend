from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from sqlalchemy import String, cast, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Camera, CameraSession


class SessionRepository:
    @staticmethod
    def _apply_filters(
        query,
        search: Optional[str] = None,
        camera_id: Optional[UUID] = None,
        status: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ):
        if search and search.strip():
            pat = f"%{search.strip()}%"
            query = query.where(
                or_(
                    CameraSession.pipeline_id.ilike(pat),
                    CameraSession.error_message.ilike(pat),
                    Camera.name.ilike(pat),
                    cast(CameraSession.id, String).ilike(pat),
                )
            )

        if camera_id is not None:
            query = query.where(CameraSession.camera_id == camera_id)

        if status and status.strip() and status.strip().lower() != "all":
            query = query.where(func.upper(CameraSession.status) == status.strip().upper())

        if pipeline_id and pipeline_id.strip() and pipeline_id.strip().lower() != "all":
            query = query.where(func.upper(CameraSession.pipeline_id) == pipeline_id.strip().upper())

        if start_date is not None:
            query = query.where(CameraSession.started_at >= start_date)

        if end_date is not None:
            query = query.where(CameraSession.started_at <= end_date)

        return query

    @classmethod
    async def get_sessions(
        cls,
        db: AsyncSession,
        search: Optional[str] = None,
        camera_id: Optional[UUID] = None,
        status: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        skip: int = 0,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        query = (
            select(
                CameraSession,
                Camera.name.label("camera_name"),
                Camera.thumbnail_url.label("camera_thumbnail_url"),
            )
            .outerjoin(Camera, CameraSession.camera_id == Camera.id)
        )
        query = cls._apply_filters(
            query,
            search=search,
            camera_id=camera_id,
            status=status,
            pipeline_id=pipeline_id,
            start_date=start_date,
            end_date=end_date,
        )
        query = query.order_by(CameraSession.started_at.desc()).offset(skip).limit(limit)

        result = await db.execute(query)
        rows = result.all()

        items = []
        for session_obj, cam_name, cam_thumb in rows:
            items.append({
                "id": session_obj.id,
                "camera_id": session_obj.camera_id,
                "camera_name": cam_name,
                "camera_thumbnail_url": cam_thumb,
                "pipeline_id": session_obj.pipeline_id,
                "source_index": session_obj.source_index,
                "status": session_obj.status,
                "started_at": session_obj.started_at,
                "stopped_at": session_obj.stopped_at,
                "ended_at": session_obj.stopped_at,
                "frames_processed": session_obj.frames_processed or 0,
                "avg_fps": round(float(session_obj.avg_fps or 0.0), 2),
                "error_message": session_obj.error_message,
            })
        return items

    @classmethod
    async def count_sessions(
        cls,
        db: AsyncSession,
        search: Optional[str] = None,
        camera_id: Optional[UUID] = None,
        status: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> int:
        query = (
            select(func.count(CameraSession.id))
            .outerjoin(Camera, CameraSession.camera_id == Camera.id)
        )
        query = cls._apply_filters(
            query,
            search=search,
            camera_id=camera_id,
            status=status,
            pipeline_id=pipeline_id,
            start_date=start_date,
            end_date=end_date,
        )
        result = await db.execute(query)
        return result.scalar() or 0

    @classmethod
    async def get_summary(
        cls,
        db: AsyncSession,
        search: Optional[str] = None,
        camera_id: Optional[UUID] = None,
        status: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        base_query = (
            select(CameraSession)
            .outerjoin(Camera, CameraSession.camera_id == Camera.id)
        )
        filtered_query = cls._apply_filters(
            base_query,
            search=search,
            camera_id=camera_id,
            status=status,
            pipeline_id=pipeline_id,
            start_date=start_date,
            end_date=end_date,
        ).subquery()

        # Aggregate metrics
        agg_stmt = select(
            func.count(filtered_query.c.id).label("total"),
            func.count(filtered_query.c.id).filter(func.upper(filtered_query.c.status) == "ERROR").label("error_cnt"),
            func.count(filtered_query.c.id).filter(func.upper(filtered_query.c.status) == "STOPPED").label("stopped_cnt"),
            func.count(filtered_query.c.id).filter(
                func.upper(filtered_query.c.status).in_(["ONLINE", "CONNECTING", "RECONNECTING"])
            ).label("running_cnt"),
            func.avg(filtered_query.c.avg_fps).filter(
                func.upper(filtered_query.c.status) == "STOPPED",
                filtered_query.c.avg_fps > 0,
            ).label("avg_fps"),
        )
        result = await db.execute(agg_stmt)
        row = result.one_or_none()

        if not row:
            return {
                "total_sessions": 0,
                "error_sessions": 0,
                "stopped_sessions": 0,
                "running_sessions": 0,
                "avg_fps": 0.0,
            }

        return {
            "total_sessions": row.total or 0,
            "error_sessions": row.error_cnt or 0,
            "stopped_sessions": row.stopped_cnt or 0,
            "running_sessions": row.running_cnt or 0,
            "avg_fps": round(float(row.avg_fps or 0.0), 1),
        }

    @classmethod
    async def get_analytics(
        cls,
        db: AsyncSession,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        camera_id: Optional[UUID] = None,
        pipeline_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        # Base filter condition
        conds = []
        if camera_id is not None:
            conds.append(CameraSession.camera_id == camera_id)
        if pipeline_id and pipeline_id.strip() and pipeline_id.strip().lower() != "all":
            conds.append(func.upper(CameraSession.pipeline_id) == pipeline_id.strip().upper())
        if start_date is not None:
            conds.append(CameraSession.started_at >= start_date)
        if end_date is not None:
            conds.append(CameraSession.started_at <= end_date)

        # 1. Status Distribution
        dist_query = (
            select(
                func.upper(CameraSession.status).label("st"),
                func.count(CameraSession.id).label("cnt"),
            )
            .where(*conds)
            .group_by(func.upper(CameraSession.status))
        )
        dist_res = await db.execute(dist_query)
        dist_rows = dist_res.all()

        total_sessions = sum(r.cnt for r in dist_rows)
        status_map = {r.st: r.cnt for r in dist_rows}

        error_cnt = status_map.get("ERROR", 0)
        stopped_cnt = status_map.get("STOPPED", 0)
        running_cnt = (
            status_map.get("ONLINE", 0)
            + status_map.get("CONNECTING", 0)
            + status_map.get("RECONNECTING", 0)
        )

        calc_pct = lambda c: round((c / total_sessions * 100), 1) if total_sessions > 0 else 0.0

        status_distribution = {
            "total": total_sessions,
            "error": {"count": error_cnt, "percentage": calc_pct(error_cnt)},
            "stopped": {"count": stopped_cnt, "percentage": calc_pct(stopped_cnt)},
            "running": {"count": running_cnt, "percentage": calc_pct(running_cnt)},
        }

        # 2. Daily Status Trend
        day_col = func.date(CameraSession.started_at).label("day")
        trend_query = (
            select(
                day_col,
                func.count(CameraSession.id).filter(func.upper(CameraSession.status) == "ERROR").label("error_cnt"),
                func.count(CameraSession.id).filter(func.upper(CameraSession.status) == "STOPPED").label("stopped_cnt"),
                func.count(CameraSession.id).filter(
                    func.upper(CameraSession.status).in_(["ONLINE", "CONNECTING", "RECONNECTING"])
                ).label("running_cnt"),
            )
            .where(*conds)
            .group_by(day_col)
            .order_by(day_col.asc())
        )
        trend_res = await db.execute(trend_query)
        trend_rows = trend_res.all()

        status_trend = []
        for r in trend_rows:
            date_str = r.day.strftime("%b %d") if hasattr(r.day, "strftime") else str(r.day)
            status_trend.append({
                "date": date_str,
                "error": r.error_cnt or 0,
                "stopped": r.stopped_cnt or 0,
                "running": r.running_cnt or 0,
            })

        # 3. Top Error Reasons
        err_conds = list(conds)
        err_conds.append(CameraSession.error_message.isnot(None))
        err_conds.append(func.trim(CameraSession.error_message) != "")

        err_query = (
            select(
                CameraSession.error_message.label("reason"),
                func.count(CameraSession.id).label("cnt"),
            )
            .where(*err_conds)
            .group_by(CameraSession.error_message)
            .order_by(func.count(CameraSession.id).desc())
            .limit(10)
        )
        err_res = await db.execute(err_query)
        err_rows = err_res.all()

        total_errors = sum(r.cnt for r in err_rows) or error_cnt or 1
        top_error_reasons = []
        for r in err_rows:
            pct = round((r.cnt / total_errors) * 100, 1)
            # Normalize long gstreamer error prefix for clean presentation if needed
            reason_text = r.reason.strip()
            top_error_reasons.append({
                "reason": reason_text,
                "count": r.cnt,
                "percentage": pct,
            })

        return {
            "status_trend": status_trend,
            "status_distribution": status_distribution,
            "top_error_reasons": top_error_reasons,
        }

    @classmethod
    async def get_by_id(cls, db: AsyncSession, session_id: UUID) -> Optional[Dict[str, Any]]:
        query = (
            select(
                CameraSession,
                Camera.name.label("camera_name"),
                Camera.thumbnail_url.label("camera_thumbnail_url"),
            )
            .outerjoin(Camera, CameraSession.camera_id == Camera.id)
            .where(CameraSession.id == session_id)
        )
        res = await db.execute(query)
        row = res.one_or_none()
        if not row:
            return None

        session_obj, cam_name, cam_thumb = row
        duration_seconds = None
        if session_obj.stopped_at and session_obj.started_at:
            duration_seconds = round((session_obj.stopped_at - session_obj.started_at).total_seconds(), 1)

        return {
            "id": session_obj.id,
            "camera_id": session_obj.camera_id,
            "camera_name": cam_name,
            "camera_thumbnail_url": cam_thumb,
            "worker_id": session_obj.worker_id,
            "pipeline_id": session_obj.pipeline_id,
            "source_index": session_obj.source_index,
            "status": session_obj.status,
            "started_at": session_obj.started_at,
            "stopped_at": session_obj.stopped_at,
            "ended_at": session_obj.stopped_at,
            "duration_seconds": duration_seconds,
            "frames_processed": session_obj.frames_processed or 0,
            "avg_fps": round(float(session_obj.avg_fps or 0.0), 2),
            "error_message": session_obj.error_message,
            "created_at": session_obj.created_at,
        }
