from typing import List
from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Response,
    status,
)
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.repositories.camera_repository import CameraRepository
from app.schemas.camera import (
    CameraCreateRequest,
    CameraResponse,
    CameraSummaryResponse,
    CameraUpdateRequest,
    RTSPValidateRequest,
    RTSPValidateResponse,
)
from app.services.camera_service import CameraService
from app.dependecies.auth import get_current_user
router = APIRouter(
    tags=["Cameras"],
)


def get_camera_service(
    db: AsyncSession = Depends(get_db),
) -> CameraService:
    repository = CameraRepository(db)
    return CameraService(repository)


# ============================================================
# RTSP STREAM VALIDATION ENDPOINT
# ============================================================
@router.post(
    "/validate-rtsp",
    response_model=RTSPValidateResponse,
    status_code=status.HTTP_200_OK,
    summary="Probe and validate an RTSP/Video Stream URL",

)
async def validate_rtsp_stream(
    payload: RTSPValidateRequest,
    service: CameraService = Depends(get_camera_service),
    current_user = Depends(get_current_user),
) -> RTSPValidateResponse:
    """
    Connects to the provided RTSP URL using OpenCV in the background,
    checks if it connects within timeout, and verifies video frames can be decoded.
    """
    result = await service.validate_rtsp_link(payload)
    if not result.is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result.error or "RTSP stream is unreachable or invalid.",
        )
    return result


# ============================================================
# CAMERA CRUD ENDPOINTS
# ============================================================
@router.post(
    "",
    response_model=CameraResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new Camera with RTSP stream validation",
)
async def create_camera(
    payload: CameraCreateRequest,
    verify_stream: bool = Query(default=True, description="Verify RTSP stream connectivity before saving"),
    service: CameraService = Depends(get_camera_service),
    current_user = Depends(get_current_user),
) -> CameraResponse:
    """
    Creates a new Camera entity in PostgreSQL.
    Optionally tests RTSP stream connectivity before allowing registration.
    """
    return await service.create_camera(
        camera_data=payload,
        verify_stream_first=verify_stream,
    )


@router.get(
    "",
    response_model=List[CameraResponse],
    summary="List all registered cameras",
)
async def get_cameras(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=100),
    service: CameraService = Depends(get_camera_service),
    current_user = Depends(get_current_user),
) -> List[CameraResponse]:
    return await service.get_cameras(skip=skip, limit=limit)


@router.get(
    "/summary",
    response_model=CameraSummaryResponse,
    summary="Get camera counts and total persisted PPE events",
)
async def get_cameras_summary(
    service: CameraService = Depends(get_camera_service),
    current_user = Depends(get_current_user),
) -> CameraSummaryResponse:
    return await service.get_camera_summary()


@router.get(
    "/{camera_id}",
    response_model=CameraResponse,
    summary="Get camera details by ID",
)
async def get_camera(
    camera_id: UUID,
    service: CameraService = Depends(get_camera_service),
    current_user = Depends(get_current_user),
) -> CameraResponse:
    return await service.get_camera(camera_id)

@router.post(
    "/{camera_id}/start",
    response_model=CameraResponse,
)
async def start_camera(
    camera_id: UUID,
    service: CameraService = Depends(
        get_camera_service
    ),
    current_user = Depends(get_current_user),
):

    return await service.start_camera(
        camera_id
    )


@router.post(
    "/{camera_id}/stop",
    response_model=CameraResponse,
)
async def stop_camera(
    camera_id: UUID,
    service: CameraService = Depends(
        get_camera_service
    ),
    current_user = Depends(get_current_user)
):

    return await service.stop_camera(
        camera_id
    )
@router.patch(
    "/{camera_id}",
    response_model=CameraResponse,
    summary="Update camera settings or desired state",
)
async def update_camera(
    camera_id: UUID,
    payload: CameraUpdateRequest,
    service: CameraService = Depends(get_camera_service),
    current_user = Depends(get_current_user)
) -> CameraResponse:
    return await service.update_camera(camera_id, payload)


@router.delete(
    "/{camera_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a camera",
)
async def delete_camera(
    camera_id: UUID,
    service: CameraService = Depends(get_camera_service),
    current_user = Depends(get_current_user)
):
    await service.delete_camera(camera_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
