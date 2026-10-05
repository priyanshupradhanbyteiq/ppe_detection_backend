# deepstream_worker/camera_manager.py

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Optional


logger = logging.getLogger(__name__)


@dataclass
class CameraRuntime:

    camera_id: str

    status: str = "OFFLINE"

    pipeline_id: Optional[str] = None

    source_index: Optional[int] = None

    running: bool = False


class CameraManager:

    def __init__(self):

        self.cameras: Dict[
            str,
            CameraRuntime
        ] = {}

        self._lock = asyncio.Lock()

    # =========================================================
    # REGISTER
    # =========================================================

    async def register_camera(
        self,
        camera_id: str,
    ):

        async with self._lock:

            if camera_id in self.cameras:

                logger.info(
                    "Camera already registered. "
                    "camera_id=%s",
                    camera_id,
                )

                return

            runtime = CameraRuntime(
                camera_id=camera_id,
            )

            self.cameras[
                camera_id
            ] = runtime

            logger.info(
                "Camera registered. camera_id=%s",
                camera_id,
            )

    # =========================================================
    # START
    # =========================================================

    async def start_camera(
        self,
        camera_id: str,
    ):

        async with self._lock:

            runtime = self.cameras.get(
                camera_id
            )

            # Worker might receive camera.start without
            # having received camera.created.
            #
            # Therefore start must work independently.
            if runtime is None:

                runtime = CameraRuntime(
                    camera_id=camera_id
                )

                self.cameras[
                    camera_id
                ] = runtime

            # Idempotency
            if runtime.running:

                logger.info(
                    "Camera already running. "
                    "camera_id=%s",
                    camera_id,
                )

                return

            runtime.status = (
                "CONNECTING"
            )

            logger.info(
                "Starting camera. camera_id=%s",
                camera_id,
            )

            try:

                # =============================================
                # NEXT STEP:
                #
                # Fetch camera configuration.
                #
                # camera_config =
                #     await camera_client.get(camera_id)
                #
                # Then:
                #
                # await self._add_deepstream_source(
                #     camera_config
                # )
                # =============================================

                runtime.running = True

                #
                # DO NOT eventually set ONLINE here.
                #
                # ONLINE should happen when DeepStream
                # actually receives a frame.
                #

                logger.info(
                    "Camera start accepted. "
                    "camera_id=%s",
                    camera_id,
                )

            except Exception:

                runtime.status = "ERROR"
                runtime.running = False

                logger.exception(
                    "Failed to start camera. "
                    "camera_id=%s",
                    camera_id,
                )

                raise

    # =========================================================
    # STOP
    # =========================================================

    async def stop_camera(
        self,
        camera_id: str,
    ):

        async with self._lock:

            runtime = self.cameras.get(
                camera_id
            )

            if runtime is None:

                logger.info(
                    "Camera not registered. "
                    "Nothing to stop. "
                    "camera_id=%s",
                    camera_id,
                )

                return

            if not runtime.running:

                logger.info(
                    "Camera already stopped. "
                    "camera_id=%s",
                    camera_id,
                )

                return

            logger.info(
                "Stopping camera. camera_id=%s",
                camera_id,
            )

            try:

                # =============================================
                # Later:
                #
                # await self._remove_deepstream_source(
                #     camera_id
                # )
                # =============================================

                runtime.running = False
                runtime.status = "OFFLINE"

                runtime.pipeline_id = None
                runtime.source_index = None

                logger.info(
                    "Camera stopped. camera_id=%s",
                    camera_id,
                )

            except Exception:

                runtime.status = "ERROR"

                logger.exception(
                    "Failed to stop camera. "
                    "camera_id=%s",
                    camera_id,
                )

                raise

    # =========================================================
    # UPDATE
    # =========================================================

    async def update_camera(
        self,
        camera_id: str,
    ):

        runtime = self.cameras.get(
            camera_id
        )

        logger.info(
            "Camera update received. "
            "camera_id=%s",
            camera_id,
        )

        if runtime is None:

            await self.register_camera(
                camera_id
            )

            return

        #
        # Later:
        #
        # Fetch latest DB configuration.
        #
        # Determine whether the changed fields require:
        #
        # - nothing
        # - model config update
        # - source restart
        #

    # =========================================================
    # RESTART
    # =========================================================

    async def restart_camera(
        self,
        camera_id: str,
    ):

        logger.info(
            "Restarting camera. camera_id=%s",
            camera_id,
        )

        await self.stop_camera(
            camera_id
        )

        await self.start_camera(
            camera_id
        )

    # =========================================================
    # REMOVE
    # =========================================================

    async def remove_camera(
        self,
        camera_id: str,
    ):

        runtime = self.cameras.get(
            camera_id
        )

        if runtime is None:

            logger.info(
                "Camera already removed. "
                "camera_id=%s",
                camera_id,
            )

            return

        if runtime.running:

            await self.stop_camera(
                camera_id
            )

        async with self._lock:

            self.cameras.pop(
                camera_id,
                None,
            )

        logger.info(
            "Camera removed. camera_id=%s",
            camera_id,
        )

    # =========================================================
    # FRAME CALLBACK
    # =========================================================

    async def mark_frame_received(
        self,
        camera_id: str,
    ):

        runtime = self.cameras.get(
            camera_id
        )

        if runtime is None:
            return

        if runtime.status != "ONLINE":

            runtime.status = "ONLINE"

            logger.info(
                "Camera ONLINE. "
                "camera_id=%s",
                camera_id,
            )

            # Later publish:
            #
            # camera.online

    # =========================================================
    # STATUS
    # =========================================================

    def get_runtime(
        self,
        camera_id: str,
    ) -> Optional[CameraRuntime]:

        return self.cameras.get(
            camera_id
        )