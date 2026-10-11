"""Authenticated signaling for camera previews; media travels over WebRTC."""

import asyncio
from urllib.parse import urlsplit
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_db
from app.dependecies.auth import get_current_user
from app.repositories.camera_repository import CameraRepository
from app.schemas.camera_webrtc import WebRTCOffer, WebRTCAnswer, WebRTCConfiguration
from app.services.camera_webrtc_service import camera_webrtc_service, PreviewUnavailable

router = APIRouter()


def get_preview_service():
    return camera_webrtc_service


@router.get("/webrtc/config", response_model=WebRTCConfiguration)
async def webrtc_configuration(current_user=Depends(get_current_user)):
    return {"ice_servers": settings.WEBRTC_ICE_SERVERS}


@router.post("/{camera_id}/webrtc/offer", response_model=WebRTCAnswer)
async def create_preview(camera_id: UUID, payload: WebRTCOffer, request: Request,
                         db: AsyncSession = Depends(get_db),
                         current_user=Depends(get_current_user),
                         service=Depends(get_preview_service)):
    camera = await CameraRepository(db).get_by_id(camera_id)
    if camera is None:
        raise HTTPException(404, "Camera not found")
    if urlsplit(camera.rtsp_url).scheme.lower() not in {"rtsp", "rtsps", "http", "https"}:
        raise HTTPException(400, "This camera source does not support a network video preview.")
    try:
        answer = await service.negotiate(camera_id, camera.rtsp_url, current_user.id, payload)
        if await request.is_disconnected():
            await service.close_session(payload.session_id, current_user.id, camera_id)
            raise HTTPException(499, "Preview request was cancelled")
        return answer
    except PreviewUnavailable as error:
        raise HTTPException(503, str(error)) from error
    except asyncio.TimeoutError as error:
        raise HTTPException(504, "Camera preview negotiation timed out. Please reconnect.") from error
    except ValueError as error:
        raise HTTPException(400, "Invalid WebRTC offer or duplicate preview session.") from error


@router.delete("/{camera_id}/webrtc/{session_id}", status_code=204)
async def close_preview(camera_id: UUID, session_id: UUID,
                        current_user=Depends(get_current_user),
                        service=Depends(get_preview_service)):
    try:
        await service.close_session(session_id, current_user.id, camera_id)
    except PermissionError as error:
        raise HTTPException(403, "Preview session belongs to another user") from error
    return Response(status_code=204)
