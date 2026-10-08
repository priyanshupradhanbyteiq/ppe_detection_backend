"""Atomic runtime writes; every operation owns a short-lived DB session."""

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import func, select, update

from app.models import Camera, CameraRuntimeLog, CameraSession, CameraStatusLog, DeepStreamWorker


@dataclass(frozen=True)
class CameraConfig:
    id: UUID
    rtsp_url: str
    desired_state: str
    source_type: str


class CameraRuntimeRepository:
    def __init__(self, session_factory):
        self.session_factory = session_factory

    async def get_camera(self, camera_id):
        async with self.session_factory() as db:
            row = (await db.execute(select(
                Camera.id, Camera.rtsp_url, Camera.desired_state, Camera.source_type
            ).where(Camera.id == UUID(str(camera_id))))).one_or_none()
            return CameraConfig(*row) if row else None

    async def list_cameras(self):
        async with self.session_factory() as db:
            rows = (await db.execute(select(
                Camera.id, Camera.rtsp_url, Camera.desired_state, Camera.source_type
            ).order_by(Camera.created_at, Camera.id))).all()
            return [CameraConfig(*row) for row in rows]

    async def _transition(self, db, camera, worker_id, session, status, event_type, message=None):
        now = datetime.now(timezone.utc)
        old_status = camera.status
        if old_status != status:
            await db.execute(update(CameraStatusLog).where(
                CameraStatusLog.camera_id == camera.id,
                CameraStatusLog.ended_at.is_(None),
            ).values(ended_at=now))
            db.add(CameraStatusLog(camera_id=camera.id, status=status, started_at=now))
        camera.status = status
        db.add(CameraRuntimeLog(
            camera_id=camera.id, worker_id=worker_id,
            session_id=session.id if session else None,
            pipeline_id=session.pipeline_id if session else None,
            event_type=event_type, old_status=old_status, new_status=status,
            message=message,
            metadata_json={"source_index": session.source_index} if session else None,
        ))

    async def initialize_worker(self, name, hostname, max_cameras):
        """Called only while holding the deployment's exclusive worker lock."""
        async with self.session_factory() as db, db.begin():
            now = datetime.now(timezone.utc)
            sessions = (await db.execute(select(CameraSession).where(
                CameraSession.stopped_at.is_(None)
            ))).scalars().all()
            for session in sessions:
                session.status = "ERROR"
                session.stopped_at = now
                session.error_message = "Worker restarted before this session was closed"
                camera = await db.get(Camera, session.camera_id)
                if camera:
                    await self._transition(db, camera, session.worker_id, session,
                                           "OFFLINE", "WORKER_RECOVERED", session.error_message)
            await db.execute(update(DeepStreamWorker).values(status="OFFLINE", current_cameras=0))
            worker = (await db.execute(select(DeepStreamWorker).where(
                DeepStreamWorker.name == name
            ))).scalar_one_or_none()
            if worker is None:
                worker = DeepStreamWorker(name=name, hostname=hostname)
                db.add(worker)
            worker.hostname = hostname
            worker.max_cameras = max_cameras
            worker.current_cameras = 0
            worker.status = "ONLINE"
            worker.heartbeat_at = now
            await db.flush()
            return worker.id

    async def create_session(self, camera_id, worker_id, session_id, pipeline_id, source_index):
        async with self.session_factory() as db, db.begin():
            camera = await db.get(Camera, UUID(str(camera_id)))
            if camera is None:
                raise ValueError("Camera was deleted while starting")
            if camera.desired_state != "RUNNING":
                raise ValueError("Camera is no longer requested to run")
            session = CameraSession(id=session_id, camera_id=camera.id, worker_id=worker_id,
                                    pipeline_id=pipeline_id, source_index=source_index,
                                    status="CONNECTING")
            db.add(session)
            await db.flush()
            await self._transition(db, camera, worker_id, session, "CONNECTING", "CAMERA_SOURCE_ASSIGNED")
            worker = await db.get(DeepStreamWorker, worker_id)
            worker.current_cameras += 1

    async def record_frames(self, camera_id, worker_id, session_id, frames, last_frame_at, avg_fps):
        async with self.session_factory() as db, db.begin():
            camera = await db.get(Camera, UUID(str(camera_id)))
            session = await db.get(CameraSession, UUID(str(session_id)))
            if camera is None or session is None or session.stopped_at is not None:
                return False
            session.frames_processed = frames
            session.avg_fps = avg_fps
            camera.last_frame_at = last_frame_at
            camera.last_seen_at = last_frame_at
            if session.status != "ONLINE":
                session.status = "ONLINE"
                await self._transition(db, camera, worker_id, session, "ONLINE", "CAMERA_ONLINE")
            return True

    async def finish_session(self, camera_id, worker_id, session_id=None, error=None):
        async with self.session_factory() as db, db.begin():
            camera = await db.get(Camera, UUID(str(camera_id)))
            session = await db.get(CameraSession, UUID(str(session_id))) if session_id else None
            newly_closed = session is not None and session.stopped_at is None
            if newly_closed:
                session.status = "ERROR" if error else "STOPPED"
                session.error_message = error
                session.stopped_at = datetime.now(timezone.utc)
            # Deleting a camera cascades its session before the worker sees the
            # command, so derive the count from surviving open sessions.
            await db.flush()
            worker = await db.get(DeepStreamWorker, worker_id)
            worker.current_cameras = (await db.execute(select(func.count(CameraSession.id)).where(
                CameraSession.worker_id == worker_id, CameraSession.stopped_at.is_(None),
            ))).scalar_one()
            if camera and (newly_closed or camera.status != ("ERROR" if error else "OFFLINE")):
                await self._transition(db, camera, worker_id, session,
                                       "ERROR" if error else "OFFLINE",
                                       "CAMERA_ERROR" if error else "CAMERA_STOPPED", error)

    async def heartbeat(self, worker_id, current_cameras, offline=False):
        async with self.session_factory() as db, db.begin():
            worker = await db.get(DeepStreamWorker, worker_id)
            worker.heartbeat_at = datetime.now(timezone.utc)
            worker.current_cameras = current_cameras
            worker.status = "OFFLINE" if offline else "ONLINE"

    async def get_or_create_worker(
        self,
        name,
        max_cameras=5,
    ):

        async with self.session_factory() as db:

            result = await db.execute(
                select(
                    DeepStreamWorker
                ).where(
                    DeepStreamWorker.name == name
                )
            )

            worker = result.scalar_one_or_none()

            if not worker:

                worker = DeepStreamWorker(
                    name=name,
                    hostname=name,
                    status="ONLINE",
                    max_cameras=max_cameras,
                    current_cameras=0,
                    heartbeat_at=datetime.now(
                        timezone.utc
                    ),
                )

                db.add(worker)

            else:

                worker.status = "ONLINE"

                worker.heartbeat_at = (
                    datetime.now(
                        timezone.utc
                    )
                )

            await db.commit()
            await db.refresh(worker)

            return worker.id

    # =====================================================
    # CAMERA
    # =====================================================

    async def get_camera(
        self,
        camera_id,
    ):

        async with self.session_factory() as db:

            return await db.get(
                Camera,
                UUID(str(camera_id)),
            )

    # =====================================================
    # CREATE CAMERA SESSION
    # =====================================================

    async def create_session(
        self,
        camera_id,
        worker_id,
        pipeline_id,
        source_index,
        session_id=None,
    ):

        async with self.session_factory() as db:

            session = CameraSession(
                id=UUID(str(session_id)) if session_id is not None else None,
                camera_id=UUID(
                    str(camera_id)
                ),
                worker_id=worker_id,
                pipeline_id=pipeline_id,
                source_index=source_index,
                status="CONNECTING",
                started_at=datetime.now(
                    timezone.utc
                ),
            )

            db.add(session)

            await db.flush()

            camera = await db.get(Camera, UUID(str(camera_id)))
            await self._transition(
                db, camera, worker_id, session, "CONNECTING", "CAMERA_SOURCE_ASSIGNED",
                f"Camera started on source {source_index}",
            )

            # Worker count
            worker = await db.get(
                DeepStreamWorker,
                worker_id,
            )

            worker.current_cameras += 1

            await db.commit()

            await db.refresh(session)

            return session

    # =====================================================
    # STOP SESSION
    # =====================================================

    async def stop_session(
        self,
        camera_id,
        session_id,
        worker_id,
        pipeline_id,
        source_index,
    ):

        async with self.session_factory() as db:

            session = await db.get(
                CameraSession,
                UUID(str(session_id)),
            )

            if session:

                session.status = "STOPPED"

                session.stopped_at = (
                    datetime.now(
                        timezone.utc
                    )
                )

            log = CameraRuntimeLog(
                camera_id=UUID(
                    str(camera_id)
                ),
                worker_id=worker_id,
                session_id=UUID(
                    str(session_id)
                ),
                pipeline_id=pipeline_id,
                event_type="CAMERA_STOPPED",
                old_status="RUNNING",
                new_status="STOPPED",
                message=(
                    f"Camera stopped from "
                    f"source {source_index}"
                ),
                metadata_json={
                    "source_index":
                        source_index
                },
            )

            db.add(log)

            worker = await db.get(
                DeepStreamWorker,
                worker_id,
            )

            if worker.current_cameras > 0:

                worker.current_cameras -= 1

            await db.commit()
