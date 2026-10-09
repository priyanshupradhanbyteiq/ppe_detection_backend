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
    CameraStatusItem,
    CameraSummaryBreakdown,
    CameraSummaryResponse,
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
            # Desired state is durable. The worker periodically reconciles the
            # DB, so broker outages delay processing without losing the request.
            logger.exception(
                "Camera state saved, but immediate command delivery failed; "
                "worker reconciliation will retry. event_type=%s camera_id=%s",
                event_type, camera_id,
            )

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

    async def _set_desired_state(self, camera_id: UUID, desired_state: str, event_type: str) -> CameraResponse:
        camera = await self.repository.get_by_id(camera_id)
        if not camera:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Camera with ID '{camera_id}' not found.")
        if camera.desired_state != desired_state:
            camera = await self.repository.update_desired_state(camera_id, desired_state)
            if camera is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Camera was deleted")
        # Repeated requests also wake the worker after a previous delivery failure.
        await self._publish_camera_event(event_type, camera_id)
        return CameraResponse.model_validate(camera)

    async def start_camera(self, camera_id: UUID) -> CameraResponse:
        return await self._set_desired_state(camera_id, "RUNNING", "camera.start")

    async def stop_camera(self, camera_id: UUID) -> CameraResponse:
        return await self._set_desired_state(camera_id, "STOPPED", "camera.stop")

    async def get_camera_summary(self) -> CameraSummaryResponse:
        cameras = await self.repository.get_all_cameras()

        by_status: dict[str, int] = {}
        by_desired_state: dict[str, int] = {}
        camera_items: List[CameraStatusItem] = []

        active_count = 0

        for cam in cameras:
            # Update status counts
            status_val = cam.status or "OFFLINE"
            desired_val = cam.desired_state or "STOPPED"
            by_status[status_val] = by_status.get(status_val, 0) + 1
            by_desired_state[desired_val] = by_desired_state.get(desired_val, 0) + 1

            # Camera is considered active if online or actively running
            is_active = (
                status_val.upper() in ["ONLINE", "STREAMING", "RUNNING", "ACTIVE"]
                or desired_val.upper() == "RUNNING"
            )

            if is_active:
                active_count += 1

            camera_items.append(
                CameraStatusItem(
                    id=cam.id,
                    name=cam.name,
                    status=status_val,
                    desired_state=desired_val,
                    is_active=is_active,
                    location_name=cam.location_name,
                    thumbnail_url=cam.thumbnail_url,
                )
            )

        total = len(camera_items)
        inactive_count = total - active_count

        return CameraSummaryResponse(
            total=total,
            active_count=active_count,
            inactive_count=inactive_count,
            breakdown=CameraSummaryBreakdown(
                by_status=by_status,
                by_desired_state=by_desired_state,
            ),
            cameras=camera_items,
        )
