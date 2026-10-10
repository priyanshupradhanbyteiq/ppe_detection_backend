"""Idempotent PPE lifecycle writes; the caller owns the transaction."""

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from sqlalchemy import func, or_, select, update
from sqlalchemy.orm import selectinload

from app.models import Camera, CameraSession, PPEEvent


class PPEEventRepository:
    @staticmethod
    async def create(db, event):
        existing = await db.get(PPEEvent, UUID(str(event["id"])))
        if existing is not None:
            return existing
        camera_id = UUID(str(event["camera_id"]))
        # Camera deletion deliberately cascades its evidence history.
        if await db.get(Camera, camera_id) is None:
            return None
        session_id = event.get("camera_session_id")
        if session_id:
            session_id = UUID(str(session_id))
            if await db.get(CameraSession, session_id) is None:
                raise LookupError("PPE camera session has not been committed")
        record = PPEEvent(
            id=UUID(str(event["id"])), camera_id=camera_id,
            session_id=session_id,
            tracking_session_id=UUID(str(event["session_id"])),
            person_track_id=event["person_track_id"], ppe_type=event["ppe_type"],
            violation_type=event["violation_type"], event_type=event["event_type"],
            severity=event["severity"], first_observed_at=event["first_observed_at"],
            confirmed_at=event["confirmed_at"],
        )
        PPEEventRepository._apply(record, event)
        db.add(record)
        await db.flush()
        return record

    @staticmethod
    def _apply(record, event):
        for field in ("status", "last_observed_at", "confidence", "violation_ratio",
                      "missing_count", "present_count", "unknown_count",
                      "resolved_at", "resolution_reason"):
            setattr(record, field, event[field])

    @staticmethod
    async def update(db, event):
        record = await db.get(PPEEvent, UUID(str(event["id"])))
        if record is None:
            # Full action payloads can reconstruct an event after a lost create.
            return await PPEEventRepository.create(db, event)
        if record.status == "RESOLVED":
            return record  # Replayed older updates must never reopen an event.
        PPEEventRepository._apply(record, event)
        await db.flush()
        return record

    @staticmethod
    async def resolve(db, event):
        # Includes final evidence counts and last-observed time, not just status.
        return await PPEEventRepository.update(db, event)

    @staticmethod
    async def save_snapshot(db, event_id, snapshot_url, snapshot_public_id, captured_at=None):
        record = await db.get(PPEEvent, UUID(str(event_id)))
        if record is None:
            return False
        record.snapshot_url = snapshot_url
        record.snapshot_public_id = snapshot_public_id
        record.snapshot_captured_at = captured_at
        await db.flush()
        return True

    @staticmethod
    async def recover_worker(db, worker_id):
        """Close only events linked to this worker's inactive camera sessions."""
        session_ids = select(CameraSession.id).where(
            CameraSession.worker_id == UUID(str(worker_id)),
            CameraSession.stopped_at.is_not(None),
        )
        await db.execute(update(PPEEvent).where(
            PPEEvent.session_id.in_(session_ids), PPEEvent.status == "OPEN",
        ).values(status="RESOLVED", resolved_at=PPEEvent.last_observed_at,
                 resolution_reason="SESSION_INTERRUPTED"))

    @staticmethod
    async def recover_interrupted(db, tracking_session_id):
        await db.execute(update(PPEEvent).where(
            PPEEvent.tracking_session_id == UUID(str(tracking_session_id)),
            PPEEvent.status == "OPEN",
        ).values(status="RESOLVED", resolved_at=PPEEvent.last_observed_at,
                 resolution_reason="SESSION_INTERRUPTED"))

    @staticmethod
    def _apply_filters(
        query,
        camera_id: Optional[UUID] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        event_type: Optional[str] = None,
        severity: Optional[str] = None,
    ):
        if camera_id is not None:
            query = query.where(PPEEvent.camera_id == camera_id)
        if start_date is not None:
            query = query.where(PPEEvent.first_observed_at >= start_date)
        if end_date is not None:
            query = query.where(PPEEvent.last_observed_at <= end_date)
        if severity and severity.strip() and severity.lower() not in ("all", "all severities"):
            query = query.where(func.upper(PPEEvent.severity) == severity.strip().upper())
        if event_type and event_type.strip() and event_type.lower() not in ("all", "all event types"):
            pattern = f"%{event_type.strip()}%"
            query = query.where(
                or_(
                    PPEEvent.event_type.ilike(pattern),
                    PPEEvent.violation_type.ilike(pattern),
                    PPEEvent.ppe_type.ilike(pattern),
                )
            )
        return query

    @staticmethod
    async def get_events(
        db,
        camera_id: Optional[UUID] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        event_type: Optional[str] = None,
        severity: Optional[str] = None,
        skip: int = 0,
        limit: int = 50,
    ) -> List[PPEEvent]:
        query = (
            select(PPEEvent)
            .outerjoin(Camera, PPEEvent.camera_id == Camera.id)
            .options(selectinload(PPEEvent.camera))
        )
        query = PPEEventRepository._apply_filters(
            query,
            camera_id=camera_id,
            start_date=start_date,
            end_date=end_date,
            event_type=event_type,
            severity=severity,
        )
        query = query.order_by(PPEEvent.confirmed_at.desc()).offset(skip).limit(limit)
        result = await db.execute(query)
        return list(result.scalars().all())

    @staticmethod
    async def count_events(
        db,
        camera_id: Optional[UUID] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        event_type: Optional[str] = None,
        severity: Optional[str] = None,
    ) -> int:
        query = select(func.count(PPEEvent.id)).outerjoin(Camera, PPEEvent.camera_id == Camera.id)
        query = PPEEventRepository._apply_filters(
            query,
            camera_id=camera_id,
            start_date=start_date,
            end_date=end_date,
            event_type=event_type,
            severity=severity,
        )
        result = await db.execute(query)
        return result.scalar() or 0

    @staticmethod
    async def get_by_id(db, event_id: UUID) -> Optional[PPEEvent]:
        query = (
            select(PPEEvent)
            .where(PPEEvent.id == event_id)
            .options(selectinload(PPEEvent.camera))
        )
        result = await db.execute(query)
        return result.scalar_one_or_none()

    @staticmethod
    async def get_events_by_camera(db, camera_id: UUID) -> List[PPEEvent]:
        query = (
            select(PPEEvent)
            .where(PPEEvent.camera_id == camera_id)
            .options(selectinload(PPEEvent.camera))
            .order_by(PPEEvent.confirmed_at.desc())
        )
        result = await db.execute(query)
        return list(result.scalars().all())
