"""Idempotent PPE lifecycle writes; the caller owns the transaction."""

from uuid import UUID

from sqlalchemy import select, update

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
