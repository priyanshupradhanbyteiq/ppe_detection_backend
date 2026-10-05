from typing import List, Optional
from uuid import UUID

from fastapi import HTTPException, status

from app.repositories.camera_repository import CameraRepository
from app.schemas.camera import (
    CameraCreateRequest,
    CameraResponse,
    CameraUpdateRequest,
    RTSPValidateRequest,
    RTSPValidateResponse,
)
from app.services.cloudinary_service import cloudinary_service
from app.services.rtsp_validator_service import rtsp_validator_service


class CameraService:
    def __init__(self, repository: CameraRepository):
        self.repository = repository
        self.rtsp_validator = rtsp_validator_service
        self.cloudinary = cloudinary_service

    async def validate_rtsp_link(
        self,
        payload: RTSPValidateRequest,
    ) -> RTSPValidateResponse:
        """Probes and verifies if an RTSP stream is accessible and decoding frames."""
        result = await self.rtsp_validator.validate_rtsp_stream(
            rtsp_url=payload.rtsp_url,
            timeout_seconds=payload.timeout_seconds,
        )

        if result["is_valid"]:
            return RTSPValidateResponse(
                is_valid=True,
                rtsp_url=payload.rtsp_url,
                resolution=result.get("resolution"),
                width=result.get("width"),
                height=result.get("height"),
                fps=result.get("fps"),
                error=None,
                message="RTSP stream is valid, reachable, and actively sending video frames.",
            )
        else:
            return RTSPValidateResponse(
                is_valid=False,
                rtsp_url=payload.rtsp_url,
                resolution=None,
                width=None,
                height=None,
                fps=None,
                error=result.get("error"),
                message="RTSP stream verification failed.",
            )

    async def create_camera(
        self,
        camera_data: CameraCreateRequest,
        verify_stream_first: bool = True,
        created_by: Optional[UUID] = None,
    ) -> CameraResponse:
        """
        Registers a new camera:
        1. Tests RTSP stream connectivity & streams for 5 seconds.
        2. Captures 5th-second stabilized snapshot & uploads to Cloudinary.
        3. Saves camera with thumbnail_url to PostgreSQL.
        """
        initial_status = "ONLINE"

        if verify_stream_first and camera_data.source_type == "RTSP_STREAM":
            probe = await self.rtsp_validator.validate_rtsp_stream(
                rtsp_url=camera_data.rtsp_url,
                timeout_seconds=5,
            )
            if not probe["is_valid"]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Cannot register camera. RTSP stream is invalid: {probe.get('error')}",
                )

            # Upload 5th-second stabilized video snapshot to Cloudinary
            snapshot_frame = probe.get("frame")
            if snapshot_frame is not None:
                cloudinary_thumb_url = await self.cloudinary.upload_thumbnail(
                    frame=snapshot_frame,
                    camera_name=camera_data.name,
                )
                if cloudinary_thumb_url:
                    camera_data.thumbnail_url = cloudinary_thumb_url

        camera = await self.repository.create(
            camera_data=camera_data,
            status=initial_status,
            created_by=created_by,
        )
        return CameraResponse.model_validate(camera)

    async def get_camera(self, camera_id: UUID) -> CameraResponse:
        camera = await self.repository.get_by_id(camera_id)
        if not camera:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Camera with ID '{camera_id}' not found.",
            )
        return CameraResponse.model_validate(camera)

    async def get_cameras(
        self,
        skip: int = 0,
        limit: int = 50,
    ) -> List[CameraResponse]:
        cameras = await self.repository.get_all(skip=skip, limit=limit)
        return [CameraResponse.model_validate(cam) for cam in cameras]

    async def update_camera(
        self,
        camera_id: UUID,
        payload: CameraUpdateRequest,
    ) -> CameraResponse:
        camera = await self.repository.update(camera_id, payload)
        if not camera:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Camera with ID '{camera_id}' not found.",
            )
        return CameraResponse.model_validate(camera)

    async def delete_camera(self, camera_id: UUID) -> None:
        deleted = await self.repository.delete(camera_id)
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Camera with ID '{camera_id}' not found.",
            )
