from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


# ==========================================
# Base Schema
# ==========================================
class CameraBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Camera name/label")
    location_name: Optional[str] = Field(None, max_length=255, description="Physical location name")
    latitude: Optional[float] = Field(None, ge=-90.0, le=90.0, description="GPS Latitude")
    longitude: Optional[float] = Field(None, ge=-180.0, le=180.0, description="GPS Longitude")
    ppe_enabled: bool = Field(True, description="Enable PPE hazard detection")
    fire_enabled: bool = Field(False, description="Enable Fire hazard detection")
    smoke_enabled: bool = Field(False, description="Enable Smoke hazard detection")


# ==========================================
# REQUEST SCHEMAS
# ==========================================
class CameraCreateRequest(CameraBase):
    rtsp_url: str = Field(..., min_length=1, max_length=500, description="RTSP Stream URL")
    desired_state: str = Field("ACTIVE", description="Desired state: ACTIVE / STOPPED")
    source_type: str = Field("RTSP_STREAM", description="Source: RTSP_STREAM or VIDEO_UPLOAD")


class CameraUpdateRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    rtsp_url: Optional[str] = Field(None, min_length=1, max_length=500)
    location_name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    ppe_enabled: Optional[bool] = None
    fire_enabled: Optional[bool] = None
    smoke_enabled: Optional[bool] = None
    desired_state: Optional[str] = None
    status: Optional[str] = None


class CameraUploadRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Camera name/label")
    location_name: Optional[str] = Field(None, max_length=255, description="Physical location name")
    latitude: Optional[float] = Field(None, ge=-90.0, le=90.0, description="GPS Latitude")
    longitude: Optional[float] = Field(None, ge=-180.0, le=180.0, description="GPS Longitude")
    ppe_enabled: bool = Field(True, description="Enable PPE hazard detection")
    fire_enabled: bool = Field(False, description="Enable Fire hazard detection")
    smoke_enabled: bool = Field(False, description="Enable Smoke hazard detection")


# Aliases for backward compatibility
CameraCreate = CameraCreateRequest
CameraUpdate = CameraUpdateRequest
CameraMetadataPayload = CameraUploadRequest


# ==========================================
# RESPONSE SCHEMAS
# ==========================================
class CameraResponse(CameraBase):
    id: UUID
    rtsp_url: str
    desired_state: str
    status: str
    source_type: str
    created_by: Optional[UUID] = None
    last_frame_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class CameraUploadResponse(BaseModel):
    temp_file_id: str = Field(..., description="UUID identifier for temporary staging file")
    temp_file_path: str = Field(..., description="Staged file path")
    filename: str = Field(..., description="Original uploaded filename")
    size_bytes: int = Field(..., description="File size in bytes")
    camera_meta: CameraUploadRequest = Field(..., description="Camera request metadata submitted with upload")
    message: str = Field("Video uploaded and staged for pre-flight validation", description="Status message")


# ==========================================
# RTSP STREAM VALIDATION SCHEMAS
# ==========================================
class RTSPValidateRequest(BaseModel):
    rtsp_url: str = Field(..., min_length=5, max_length=500, description="RTSP/Video Stream URL (e.g. rtsp://ip:port/stream)")
    timeout_seconds: Optional[int] = Field(5, ge=1, le=30, description="Stream connection timeout in seconds")


class RTSPValidateResponse(BaseModel):
    is_valid: bool = Field(..., description="Whether the RTSP stream is accessible and valid")
    rtsp_url: str = Field(..., description="The tested RTSP URL")
    resolution: Optional[str] = Field(None, description="Stream video resolution (e.g. 1920x1080)")
    width: Optional[int] = Field(None, description="Stream frame width in pixels")
    height: Optional[int] = Field(None, description="Stream frame height in pixels")
    fps: Optional[float] = Field(None, description="Stream frames per second")
    error: Optional[str] = Field(None, description="Error reason if stream failed validation")
    message: str = Field(..., description="Human-readable result summary")

