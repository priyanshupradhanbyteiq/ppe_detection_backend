from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class PPEEventResponse(BaseModel):
    id: UUID
    camera_id: UUID
    camera_name: Optional[str] = None
    location_name: Optional[str] = None
    session_id: Optional[UUID] = None
    tracking_session_id: UUID
    person_track_id: int
    ppe_type: str
    violation_type: str
    event_type: str
    status: str
    severity: str
    confidence: Optional[float] = None
    violation_ratio: float = 0.0
    missing_count: int = 0
    present_count: int = 0
    unknown_count: int = 0
    first_observed_at: datetime
    confirmed_at: datetime
    last_observed_at: datetime
    resolved_at: Optional[datetime] = None
    resolution_reason: Optional[str] = None
    snapshot_url: Optional[str] = None
    snapshot_public_id: Optional[str] = None
    snapshot_captured_at: Optional[datetime] = None
    clip_url: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class PPEEventListResponse(BaseModel):
    total: int = Field(..., description="Total matching events count")
    skip: int = Field(..., description="Current pagination offset")
    limit: int = Field(..., description="Current pagination limit")
    items: List[PPEEventResponse] = Field(..., description="List of PPE events")
