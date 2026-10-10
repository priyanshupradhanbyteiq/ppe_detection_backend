from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class CameraSessionResponse(BaseModel):
    id: UUID
    camera_id: UUID
    camera_name: Optional[str] = None
    camera_thumbnail_url: Optional[str] = None
    pipeline_id: str
    source_index: int
    status: str
    started_at: datetime
    stopped_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    frames_processed: int = 0
    avg_fps: float = 0.0
    error_message: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class CameraSessionDetailResponse(CameraSessionResponse):
    worker_id: UUID
    duration_seconds: Optional[float] = None
    created_at: datetime


class SessionSummaryResponse(BaseModel):
    total_sessions: int = Field(0, description="Total matching sessions")
    error_sessions: int = Field(0, description="Total sessions with ERROR status")
    stopped_sessions: int = Field(0, description="Total sessions with STOPPED status")
    running_sessions: int = Field(0, description="Total sessions currently active/online")
    avg_fps: float = Field(0.0, description="Average FPS among successful sessions")


class SessionLogsListResponse(BaseModel):
    summary: SessionSummaryResponse
    total: int = Field(..., description="Total matching records count")
    skip: int = Field(..., description="Pagination offset")
    limit: int = Field(..., description="Items per page")
    items: List[CameraSessionResponse] = Field(..., description="List of camera sessions")


class StatusTrendPoint(BaseModel):
    date: str = Field(..., description="Formatted date label, e.g. Oct 06")
    error: int = 0
    stopped: int = 0
    running: int = 0


class StatusDistributionItem(BaseModel):
    count: int = 0
    percentage: float = 0.0


class StatusDistributionResponse(BaseModel):
    total: int = 0
    error: StatusDistributionItem = Field(default_factory=StatusDistributionItem)
    stopped: StatusDistributionItem = Field(default_factory=StatusDistributionItem)
    running: StatusDistributionItem = Field(default_factory=StatusDistributionItem)


class TopErrorReasonItem(BaseModel):
    reason: str
    count: int
    percentage: float


class SessionAnalyticsResponse(BaseModel):
    status_trend: List[StatusTrendPoint]
    status_distribution: StatusDistributionResponse
    top_error_reasons: List[TopErrorReasonItem]
