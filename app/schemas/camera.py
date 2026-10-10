from datetime import datetime
from typing import Dict, List, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


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
    thumbnail_url: Optional[str] = Field(None, max_length=500, description="Cloudinary live preview thumbnail URL")


# ==========================================
# REQUEST SCHEMAS
# ==========================================
class CameraCreateRequest(CameraBase):
    rtsp_url: str = Field(..., min_length=1, max_length=500, description="RTSP Stream URL")
    desired_state: Literal["RUNNING", "STOPPED"] = "STOPPED"
    source_type: Literal["RTSP_STREAM", "VIDEO_UPLOAD"] = "RTSP_STREAM"


class CameraUpdateRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    rtsp_url: Optional[str] = Field(None, min_length=1, max_length=500)
    location_name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    ppe_enabled: Optional[bool] = None
    fire_enabled: Optional[bool] = None
    smoke_enabled: Optional[bool] = None
    desired_state: Optional[Literal["RUNNING", "STOPPED"]] = None
    thumbnail_url: Optional[str] = None

    model_config = ConfigDict(extra="forbid")

    @field_validator("name", "rtsp_url", "desired_state", "ppe_enabled", "fire_enabled", "smoke_enabled", mode="before")
    @classmethod
    def reject_null(cls, value):
        if value is None:
            raise ValueError("This field cannot be null")
        return value


class CameraUploadRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Camera name/label")
    location_name: Optional[str] = Field(None, max_length=255, description="Physical location name")
    latitude: Optional[float] = Field(None, ge=-90.0, le=90.0, description="GPS Latitude")
    longitude: Optional[float] = Field(None, ge=-180.0, le=180.0, description="GPS Longitude")
    ppe_enabled: bool = Field(True, description="Enable PPE hazard detection")
    fire_enabled: bool = Field(False, description="Enable Fire hazard detection")
    smoke_enabled: bool = Field(False, description="Enable Smoke hazard detection")
    thumbnail_url: Optional[str] = None


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
    thumbnail_url: Optional[str] = None
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


# ==========================================
# CAMERA SUMMARY & STATUS SCHEMAS
# ==========================================
class CameraStatusItem(BaseModel):
    id: UUID
    name: str
    status: str
    desired_state: str
    is_active: bool
    location_name: Optional[str] = None
    thumbnail_url: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class CameraSummaryBreakdown(BaseModel):
    by_status: Dict[str, int] = Field(default_factory=dict, description="Count of cameras per status")
    by_desired_state: Dict[str, int] = Field(default_factory=dict, description="Count of cameras per desired state")


class CameraSummaryResponse(BaseModel):
    total: int = Field(..., description="Total number of registered cameras")
    active_count: int = Field(..., description="Count of active cameras")
    inactive_count: int = Field(..., description="Count of inactive cameras")
    ppe_event_count: int = Field(
        ..., ge=0,
        description="Total persisted PPE events across all cameras, including open and resolved events",
    )
    breakdown: CameraSummaryBreakdown = Field(..., description="Breakdown counts by status and desired state")
    cameras: List[CameraStatusItem] = Field(default_factory=list, description="List of cameras with active status indicator")

