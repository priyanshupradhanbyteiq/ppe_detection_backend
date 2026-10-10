import asyncio
import logging
import time
from datetime import datetime, timezone
from uuid import uuid4


logger = logging.getLogger(__name__)


class CameraManager:

    def __init__(
        self,
        repository,
        pipeline,
        worker_id,
    ):

        self.repository = repository

        self.pipeline = pipeline

        self.worker_id = worker_id

        # camera_id -> runtime info
        self.cameras = {}
        self._loop = asyncio.get_running_loop()
        self._lock = asyncio.Lock()
        self._callback_tasks = set()
        self._closing = False
        self.pipeline.on_error = self.notify_error

    async def restore_cameras(self):
        """Restore saved running cameras before consuming new commands."""
        for camera in await self.repository.list_cameras():
            if camera.desired_state == "RUNNING":
                try:
                    await self.start_camera(camera.id)
                except Exception:
                    logger.exception("Failed restoring camera %s", camera.id)

    # =====================================================
    # START CAMERA
    # =====================================================

    async def start_camera(
        self,
        camera_id,
    ):
        async with self._lock:
            if not self._closing:
                return await self._start_camera(camera_id)

    async def _start_camera(self, camera_id):

        camera_id = str(camera_id)

        # -----------------------------------
        # Already running
        # -----------------------------------

        if camera_id in self.cameras:

            logger.info(
                "Camera already running: %s",
                camera_id,
            )

            return

        # -----------------------------------
        # Maximum 5
        # -----------------------------------

        if len(self.cameras) >= 5:

            raise RuntimeError(
                "Maximum 5 cameras allowed"
            )

        # -----------------------------------
        # Get camera from DB
        # -----------------------------------

        camera = (
            await self.repository.get_camera(
                camera_id
            )
        )

        if not camera:

            raise ValueError(
                f"Camera not found: {camera_id}"
            )

        if camera.desired_state != "RUNNING":
            logger.info("Ignoring stale start command for stopped camera: %s", camera_id)
            return

        logger.info(
            "Starting camera: %s",
            camera_id,
        )

        # -----------------------------------
        # Add to DeepStream
        # -----------------------------------

        session_id = uuid4()
        source_index = (
            self.pipeline.add_camera(
                camera_id=camera_id,
                rtsp_url=camera.rtsp_url,
                session_id=str(session_id),
                on_frame=self.notify_frame,
            )
        )

        try:

            # --------------------------------
            # Create DB session
            # --------------------------------

            session = (
                await self.repository.create_session(
                    camera_id=camera_id,
                    worker_id=self.worker_id,
                    pipeline_id=self.pipeline.pipeline_id,
                    source_index=source_index,
                    session_id=session_id,
                )
            )

        except Exception:

            # DB failed.
            # Remove camera from DeepStream.
            self.pipeline.remove_camera(
                camera_id
            )

            raise

        # -----------------------------------
        # Store in memory
        # -----------------------------------

        self.cameras[camera_id] = {

            "source_index":
                source_index,

            "session_id":
                str(session.id),

            "pipeline_id":
                self.pipeline.pipeline_id,
            "frames_processed": 0,
            "last_frame_at": None,
            "started_at": time.monotonic(),
            "last_frame_write": float("-inf"),
            "frame_write_pending": False,
        }

        # Evidence persistence must not race the CameraSession foreign key.
        if hasattr(self.pipeline, "activate_camera"):
            self.pipeline.activate_camera(camera_id, str(session.id))

        logger.info(
            "Camera started: "
            "camera=%s source=%s",
            camera_id,
            source_index,
        )

        return self.cameras[
            camera_id
        ]

    # =====================================================
    # STOP CAMERA
    # =====================================================

    async def stop_camera(
        self,
        camera_id,
    ):
        async with self._lock:
            return await self._stop_camera(camera_id)

    async def _stop_camera(self, camera_id, error=None):

        camera_id = str(camera_id)

        runtime = self.cameras.get(
            camera_id
        )

        if not runtime:

            logger.info(
                "Camera not running: %s",
                camera_id,
            )

            return

        logger.info(
            "Stopping camera: %s",
            camera_id,
        )

        # -----------------------------------
        # Remove from DeepStream
        # -----------------------------------

        self.pipeline.remove_camera(
            camera_id
        )

        # -----------------------------------
        # Update DB
        # -----------------------------------

        await self._persist_frames(camera_id, runtime)
        await self.repository.finish_session(
            camera_id=camera_id,
            session_id=runtime[
                "session_id"
            ],
            worker_id=self.worker_id,
            error=error,
        )

        # -----------------------------------
        # Remove from memory
        # -----------------------------------

        del self.cameras[
            camera_id
        ]

        logger.info(
            "Camera stopped: %s",
            camera_id,
        )

    # =====================================================
    # RESTART CAMERA
    # =====================================================

    async def restart_camera(
        self,
        camera_id,
    ):

        async with self._lock:
            if not self._closing:
                await self._stop_camera(camera_id)
                return await self._start_camera(camera_id)

    def notify_frame(self, camera_id, session_id):
        """Move streaming callbacks onto the asyncio loop before touching runtime."""
        if not self._closing:
            self._loop.call_soon_threadsafe(self._record_frame, camera_id, session_id)

    def _record_frame(self, camera_id, session_id):
        runtime = self.cameras.get(str(camera_id))
        if self._closing or not runtime or runtime["session_id"] != str(session_id):
            return
        runtime["frames_processed"] += 1
        runtime["last_frame_at"] = datetime.now(timezone.utc)
        # Persist the first frame immediately, then at most once per second.
        if not runtime["frame_write_pending"] and time.monotonic() - runtime["last_frame_write"] >= 1:
            runtime["frame_write_pending"] = True
            runtime["last_frame_write"] = time.monotonic()
            self._track_callback(self._flush_frames(str(camera_id), runtime))

    async def _flush_frames(self, camera_id, runtime):
        try:
            async with self._lock:
                if self.cameras.get(camera_id) is runtime:
                    await self._persist_frames(camera_id, runtime)
        finally:
            runtime["frame_write_pending"] = False

    async def _persist_frames(self, camera_id, runtime):
        if runtime["last_frame_at"] is not None:
            await self.repository.record_frames(
                camera_id=camera_id,
                worker_id=self.worker_id,
                session_id=runtime["session_id"],
                frames=runtime["frames_processed"],
                last_frame_at=runtime["last_frame_at"],
                avg_fps=runtime["frames_processed"] / max(time.monotonic() - runtime["started_at"], 0.001),
            )

    def notify_error(self, camera_id, session_id, message):
        if not self._closing:
            self._loop.call_soon_threadsafe(self._schedule_error, camera_id, session_id, message)

    def _schedule_error(self, camera_id, session_id, message):
        if not self._closing:
            self._track_callback(self._handle_error(str(camera_id), str(session_id), message))

    async def _handle_error(self, camera_id, session_id, message):
        async with self._lock:
            runtime = self.cameras.get(camera_id)
            if runtime and runtime["session_id"] == session_id:
                logger.error("Camera source failed: camera=%s error=%s", camera_id, message)
                await self._stop_camera(camera_id, error=message)

    def _track_callback(self, coroutine):
        task = self._loop.create_task(coroutine)
        self._callback_tasks.add(task)
        task.add_done_callback(self._callback_done)

    def _callback_done(self, task):
        self._callback_tasks.discard(task)
        if not task.cancelled() and task.exception():
            error = task.exception()
            logger.error("Camera callback failed", exc_info=(type(error), error, error.__traceback__))

    # =====================================================
    # CAMERA UPDATED
    # =====================================================

    async def update_camera(
        self,
        camera_id,
    ):

        # Simple approach:
        # restart camera so new RTSP URL is loaded.

        if str(camera_id) in self.cameras:

            await self.restart_camera(
                camera_id
            )

    # =====================================================
    # CAMERA DELETED
    # =====================================================

    async def remove_camera(
        self,
        camera_id,
    ):

        await self.stop_camera(
            camera_id
        )

    # =====================================================
    # SHUTDOWN
    # =====================================================

    async def shutdown(self):
        self._closing = True
        # Stop the shared pipeline before dismantling individual live sources.
        # Otherwise a source can wait for downstream streaming threads that
        # are still blocked in the mux/tracker/display chain.
        self.pipeline.shutdown()
        if self._callback_tasks:
            await asyncio.gather(*self._callback_tasks, return_exceptions=True)

        camera_ids = list(
            self.cameras.keys()
        )

        for camera_id in camera_ids:

            try:

                await self.stop_camera(
                    camera_id
                )

            except Exception:

                logger.exception(
                    "Failed stopping camera %s",
                    camera_id,
                )
