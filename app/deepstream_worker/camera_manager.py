"""Serialize source changes and persist their session/status history."""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

logger = logging.getLogger(__name__)


@dataclass
class CameraRuntime:
    camera_id: str
    status: str = "OFFLINE"
    pipeline_id: Optional[str] = None
    source_index: Optional[int] = None
    session_id: Optional[str] = None
    rtsp_url: Optional[str] = None
    running: bool = False
    frames_processed: int = 0
    last_frame_at: Optional[datetime] = None
    started_at: float = 0
    retry_after: float = 0


class CameraManager:
    def __init__(self, repository, pipeline, worker_id, frame_timeout=30, retry_delay=30):
        self.repository = repository
        self.pipeline = pipeline
        self.worker_id = worker_id
        self.frame_timeout = frame_timeout
        self.retry_delay = retry_delay
        self.cameras = {}
        self._lock = asyncio.Lock()
        self._loop = asyncio.get_running_loop()
        self._error_tasks = set()
        self._closing = False
        self.pipeline.on_error = self.notify_error

    async def register_camera(self, camera_id):
        return await self.reconcile_camera(camera_id)

    async def start_camera(self, camera_id):
        return await self.reconcile_camera(camera_id)

    async def stop_camera(self, camera_id):
        # Commands are wake-ups. Read the latest desired state to reject stale events.
        return await self.reconcile_camera(camera_id)

    async def update_camera(self, camera_id):
        return await self.reconcile_camera(camera_id)

    async def remove_camera(self, camera_id):
        return await self.reconcile_camera(camera_id)

    async def restart_camera(self, camera_id):
        camera_id = str(UUID(str(camera_id)))
        async with self._lock:
            if self._closing:
                return None
            runtime = self.cameras.get(str(camera_id))
            if runtime:
                await self._stop(runtime)
            return await self._reconcile(str(camera_id))

    async def reconcile_camera(self, camera_id):
        async with self._lock:
            if self._closing:
                return None
            return await self._reconcile(str(UUID(str(camera_id))))

    async def _reconcile(self, camera_id):
        camera = await self.repository.get_camera(camera_id)
        runtime = self.cameras.get(camera_id)
        if camera is None:
            if runtime:
                await self._stop(runtime)
                self.cameras.pop(camera_id, None)
            return None
        if runtime is None:
            runtime = CameraRuntime(camera_id=camera_id)
            self.cameras[camera_id] = runtime
        if camera.desired_state != "RUNNING":
            await self._stop(runtime)
        else:
            if runtime.running and runtime.rtsp_url != camera.rtsp_url:
                await self._stop(runtime)
            if not runtime.running and time.monotonic() >= runtime.retry_after:
                if runtime.session_id:
                    await self._stop(runtime)
                await self._start(runtime, camera)
        return runtime

    async def _start(self, runtime, camera):
        token = str(uuid4())
        runtime.status = "CONNECTING"
        # Retain the token until compensation commits. A lost DB acknowledgement
        # must still let the next reconciliation close this exact session.
        runtime.session_id = token
        runtime.pipeline_id = self.pipeline.pipeline_id
        runtime.frames_processed = 0
        runtime.last_frame_at = None
        runtime.started_at = time.monotonic()
        try:
            if camera.source_type != "RTSP_STREAM":
                raise ValueError("DeepStream currently supports RTSP_STREAM cameras only")
            source_index = self.pipeline.add_camera(
                camera_id=runtime.camera_id, rtsp_url=camera.rtsp_url,
                session_id=token, on_frame=self.notify_frame,
            )
            runtime.source_index = source_index
            await self.repository.create_session(
                camera_id=runtime.camera_id, worker_id=self.worker_id,
                session_id=UUID(token), pipeline_id=self.pipeline.pipeline_id,
                source_index=source_index,
            )
        except Exception as exc:
            # A DB failure must not leave a live source without a session.
            self.pipeline.remove_camera(runtime.camera_id)
            runtime.running = False
            runtime.status = "ERROR"
            runtime.retry_after = time.monotonic() + self.retry_delay
            await self.repository.finish_session(runtime.camera_id, self.worker_id, token, error=str(exc))
            runtime.session_id = None
            runtime.pipeline_id = None
            runtime.source_index = None
            raise
        runtime.rtsp_url = camera.rtsp_url
        runtime.running = True
        runtime.retry_after = 0
        logger.info("Camera started: camera=%s session=%s", runtime.camera_id, token)

    async def _stop(self, runtime, error=None):
        # Remove before clearing the mapping. If DB persistence fails, retain the
        # session ID so reconciliation can retry closing the same session.
        self.pipeline.remove_camera(runtime.camera_id)
        runtime.running = False
        if runtime.session_id and runtime.frames_processed:
            await self._persist_frames(runtime)
        await self.repository.finish_session(runtime.camera_id, self.worker_id,
                                             runtime.session_id, error=error)
        runtime.status = "ERROR" if error else "OFFLINE"
        runtime.pipeline_id = None
        runtime.source_index = None
        runtime.session_id = None
        runtime.retry_after = time.monotonic() + self.retry_delay if error else 0

    def notify_frame(self, camera_id, session_id):
        """GStreamer threads only enqueue; runtime/DB writes stay on asyncio."""
        if not self._closing:
            self._loop.call_soon_threadsafe(self._record_frame, camera_id, session_id)

    def _record_frame(self, camera_id, session_id):
        runtime = self.cameras.get(str(camera_id))
        if not runtime or not runtime.running or runtime.session_id != session_id:
            return
        runtime.frames_processed += 1
        runtime.last_frame_at = datetime.now(timezone.utc)

    async def mark_frame_received(self, camera_id, session_id):
        async with self._lock:
            self._record_frame(str(camera_id), str(session_id))
            runtime = self.cameras.get(str(camera_id))
            if runtime and runtime.running and runtime.session_id == str(session_id):
                await self._persist_frames(runtime)

    async def _persist_frames(self, runtime):
        if not runtime.last_frame_at:
            return
        persisted = await self.repository.record_frames(
            runtime.camera_id, self.worker_id, runtime.session_id,
            runtime.frames_processed, runtime.last_frame_at,
            runtime.frames_processed / max(time.monotonic() - runtime.started_at, 0.001),
        )
        if persisted and runtime.running:
            runtime.status = "ONLINE"

    def notify_error(self, camera_id, session_id, message):
        if not self._closing:
            self._loop.call_soon_threadsafe(self._schedule_error, camera_id, session_id, message)

    def _schedule_error(self, camera_id, session_id, message):
        task = self._loop.create_task(self.mark_error(camera_id, session_id, message))
        self._error_tasks.add(task)
        task.add_done_callback(self._error_done)

    def _error_done(self, task):
        self._error_tasks.discard(task)
        if not task.cancelled() and task.exception():
            logger.error("Could not persist source error", exc_info=task.exception())

    async def mark_error(self, camera_id, session_id, message):
        async with self._lock:
            runtime = self.cameras.get(str(camera_id))
            if runtime and runtime.session_id == session_id:
                await self._stop(runtime, error=message)

    async def flush_frames(self):
        async with self._lock:
            for runtime in list(self.cameras.values()):
                if not runtime.running:
                    continue
                last_frame_age = ((datetime.now(timezone.utc) - runtime.last_frame_at).total_seconds()
                                  if runtime.last_frame_at else time.monotonic() - runtime.started_at)
                if last_frame_age > self.frame_timeout:
                    await self._stop(runtime, error="No decoded frames received before timeout")
                else:
                    await self._persist_frames(runtime)

    async def reconcile_all(self):
        async with self._lock:
            if self._closing:
                return
            cameras = await self.repository.list_cameras()
            known_ids = {str(camera.id) for camera in cameras}
            # Free stopped/deleted sources before filling the five available slots.
            stop_ids = set(self.cameras) - known_ids
            stop_ids.update(str(camera.id) for camera in cameras if camera.desired_state != "RUNNING")
            for camera_id in sorted(stop_ids):
                await self._reconcile(camera_id)
            for camera in cameras:
                if camera.desired_state == "RUNNING":
                    try:
                        await self._reconcile(str(camera.id))
                    except Exception:
                        logger.exception("Camera reconciliation failed: camera=%s", camera.id)
            await self.repository.heartbeat(self.worker_id, sum(r.running for r in self.cameras.values()))

    async def shutdown(self):
        self._closing = True
        async with self._lock:
            for runtime in list(self.cameras.values()):
                try:
                    await self._stop(runtime)
                except Exception:
                    logger.exception("Failed closing camera session: camera=%s", runtime.camera_id)
            self.pipeline.shutdown()
            await self.repository.heartbeat(self.worker_id, 0, offline=True)
        if self._error_tasks:
            await asyncio.gather(*self._error_tasks, return_exceptions=True)

    def get_runtime(self, camera_id):
        return self.cameras.get(str(camera_id))
