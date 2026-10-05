# app/services/camera_service.py

import logging
import uuid

from typing import List, Optional
from uuid import UUID

from fastapi import HTTPException, status

from app.messaging.rabbitmq import rabbitmq_publisher

from app.repositories.camera_repository import (
    CameraRepository,
)

from app.schemas.camera import (
    CameraCreateRequest,
    CameraResponse,
    CameraUpdateRequest,
    RTSPValidateRequest,
    RTSPValidateResponse,
)

from app.services.cloudinary_service import (
    cloudinary_service,
)

from app.services.rtsp_validator_service import (
    rtsp_validator_service,
)


logger = logging.getLogger(__name__)


class CameraService:

    def __init__(
        self,
        repository: CameraRepository,
    ):
        self.repository = repository

        self.rtsp_validator = (
            rtsp_validator_service
        )

        self.cloudinary = (
            cloudinary_service
        )

        self.rabbitmq = (
            rabbitmq_publisher
        )

    # =========================================================
    # PRIVATE: PUBLISH EVENT
    # =========================================================

    async def _publish_camera_event(
        self,
        event_type: str,
        camera_id: UUID,
    ) -> None:

        event_id = uuid.uuid4()

        payload = {
            "event_id": str(event_id),
            "event_type": event_type,
            "camera_id": str(camera_id),
        }

        try:

            await self.rabbitmq.publish(
                routing_key=event_type,
                payload=payload,
                message_id=str(event_id),
            )

        except Exception:

            #
            # IMPORTANT:
            #
            # Database operation has already succeeded.
            #
            # Because we're intentionally NOT using
            # transactional outbox, we should not pretend
            # that rolling back here would undo the camera.
            #

            logger.exception(
                "Failed to publish camera event. "
                "event_type=%s camera_id=%s",
                event_type,
                camera_id,
            )

            #
            # For your current implementation we're allowing
            # the API operation to remain successful.
            #
            # Later you can add retry/outbox if guaranteed
            # delivery is required.
            #

    # =========================================================
    # VALIDATE RTSP
    # =========================================================

    async def validate_rtsp_link(
        self,
        payload: RTSPValidateRequest,
    ) -> RTSPValidateResponse:

        result = (
            await self.rtsp_validator.validate_rtsp_stream(
                rtsp_url=payload.rtsp_url,
                timeout_seconds=payload.timeout_seconds,
            )
        )

        if result["is_valid"]:

            return RTSPValidateResponse(
                is_valid=True,
                rtsp_url=payload.rtsp_url,
                resolution=result.get(
                    "resolution"
                ),
                width=result.get(
                    "width"
                ),
                height=result.get(
                    "height"
                ),
                fps=result.get(
                    "fps"
                ),
                error=None,
                message=(
                    "RTSP stream is valid, reachable, "
                    "and actively sending video frames."
                ),
            )

        return RTSPValidateResponse(
            is_valid=False,
            rtsp_url=payload.rtsp_url,
            resolution=None,
            width=None,
            height=None,
            fps=None,
            error=result.get(
                "error"
            ),
            message=(
                "RTSP stream verification failed."
            ),
        )

    # =========================================================
    # CREATE CAMERA
    # =========================================================

    async def create_camera(
        self,
        camera_data: CameraCreateRequest,
        verify_stream_first: bool = True,
        created_by: Optional[UUID] = None,
    ) -> CameraResponse:

        """
        Flow:

        1. Validate RTSP
        2. Generate thumbnail
        3. Save camera
        4. Publish camera.created
        """

        # -----------------------------------------------------
        # RTSP VALIDATION
        # -----------------------------------------------------

        if (
            verify_stream_first
            and camera_data.source_type
            == "RTSP_STREAM"
        ):

            probe = (
                await self.rtsp_validator
                .validate_rtsp_stream(
                    rtsp_url=camera_data.rtsp_url,
                    timeout_seconds=5,
                )
            )

            if not probe["is_valid"]:

                raise HTTPException(
                    status_code=(
                        status.HTTP_400_BAD_REQUEST
                    ),
                    detail=(
                        "Cannot register camera. "
                        "RTSP stream is invalid: "
                        f"{probe.get('error')}"
                    ),
                )

            # -------------------------------------------------
            # THUMBNAIL
            # -------------------------------------------------

            snapshot_frame = probe.get(
                "frame"
            )

            if snapshot_frame is not None:

                cloudinary_thumb_url = (
                    await self.cloudinary
                    .upload_thumbnail(
                        frame=snapshot_frame,
                        camera_name=camera_data.name,
                    )
                )

                if cloudinary_thumb_url:

                    camera_data.thumbnail_url = (
                        cloudinary_thumb_url
                    )

        # -----------------------------------------------------
        # DATABASE
        # -----------------------------------------------------

        camera = await self.repository.create(
            camera_data=camera_data,
            status="OFFLINE",
            created_by=created_by,
        )

        # -----------------------------------------------------
        # RABBITMQ
        # -----------------------------------------------------

        await self._publish_camera_event(
            event_type="camera.created",
            camera_id=camera.id,
        )

        return CameraResponse.model_validate(
            camera
        )

    # =========================================================
    # GET CAMERA
    # =========================================================

    async def get_camera(
        self,
        camera_id: UUID,
    ) -> CameraResponse:

        camera = await self.repository.get_by_id(
            camera_id
        )

        if not camera:

            raise HTTPException(
                status_code=(
                    status.HTTP_404_NOT_FOUND
                ),
                detail=(
                    f"Camera with ID "
                    f"'{camera_id}' not found."
                ),
            )

        return CameraResponse.model_validate(
            camera
        )

    # =========================================================
    # GET CAMERAS
    # =========================================================

    async def get_cameras(
        self,
        skip: int = 0,
        limit: int = 50,
    ) -> List[CameraResponse]:

        cameras = await self.repository.get_all(
            skip=skip,
            limit=limit,
        )

        return [
            CameraResponse.model_validate(
                camera
            )
            for camera in cameras
        ]

    # =========================================================
    # UPDATE CAMERA
    # =========================================================

    async def update_camera(
        self,
        camera_id: UUID,
        payload: CameraUpdateRequest,
    ) -> CameraResponse:

        camera = await self.repository.update(
            camera_id,
            payload,
        )

        if not camera:

            raise HTTPException(
                status_code=(
                    status.HTTP_404_NOT_FOUND
                ),
                detail=(
                    f"Camera with ID "
                    f"'{camera_id}' not found."
                ),
            )

        # -----------------------------------------------------
        # RabbitMQ
        # -----------------------------------------------------

        await self._publish_camera_event(
            event_type="camera.updated",
            camera_id=camera.id,
        )

        return CameraResponse.model_validate(
            camera
        )

    # =========================================================
    # DELETE CAMERA
    # =========================================================

    async def delete_camera(
        self,
        camera_id: UUID,
    ) -> None:

        # -----------------------------------------------------
        # Check first
        # -----------------------------------------------------

        camera = await self.repository.get_by_id(
            camera_id
        )

        if not camera:

            raise HTTPException(
                status_code=(
                    status.HTTP_404_NOT_FOUND
                ),
                detail=(
                    f"Camera with ID "
                    f"'{camera_id}' not found."
                ),
            )

        # -----------------------------------------------------
        # Delete from DB
        # -----------------------------------------------------

        deleted = await self.repository.delete(
            camera_id
        )

        if not deleted:

            raise HTTPException(
                status_code=(
                    status.HTTP_404_NOT_FOUND
                ),
                detail=(
                    f"Camera with ID "
                    f"'{camera_id}' not found."
                ),
            )

        # -----------------------------------------------------
        # RabbitMQ
        # -----------------------------------------------------

        await self._publish_camera_event(
            event_type="camera.deleted",
            camera_id=camera_id,
        )

    async def start_camera(
    self,
    camera_id: UUID,
) -> CameraResponse:

        camera = await self.repository.get_by_id(
            camera_id
        )

        if not camera:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Camera with ID '{camera_id}' not found.",
            )

        # Idempotent start.
        if camera.desired_state == "RUNNING":

            logger.info(
                "Camera already has desired_state=RUNNING. camera_id=%s",
                camera_id,
            )

            return CameraResponse.model_validate(camera)

        camera = await self.repository.update_desired_state(
            camera_id=camera_id,
            desired_state="RUNNING",
        )

        await self._publish_camera_event(
            event_type="camera.start",
            camera_id=camera_id,
        )

        logger.info(
            "Camera start requested. camera_id=%s",
            camera_id,
        )

        return CameraResponse.model_validate(camera)


    async def stop_camera(
        self,
        camera_id: UUID,
    ) -> CameraResponse:

        camera = await self.repository.get_by_id(
            camera_id
        )

        if not camera:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Camera with ID '{camera_id}' not found.",
            )

        # Idempotent stop.
        if camera.desired_state == "STOPPED":

            logger.info(
                "Camera already has desired_state=STOPPED. camera_id=%s",
                camera_id,
            )

            return CameraResponse.model_validate(camera)

        camera = await self.repository.update_desired_state(
            camera_id=camera_id,
            desired_state="STOPPED",
        )

        await self._publish_camera_event(
            event_type="camera.stop",
            camera_id=camera_id,
        )

        logger.info(
            "Camera stop requested. camera_id=%s",
            camera_id,
        )

        return CameraResponse.model_validate(camera)