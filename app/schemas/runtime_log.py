from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class RuntimeLogSummaryResponse(BaseModel):
    total_logs: int = Field(0, description="Total count of logs")
    camera_online: int = Field(0, description="Camera Online event count")
    camera_offline: int = Field(0, description="Camera Offline event count")
    errors: int = Field(0, description="Error event count")
    source_assigned: int = Field(0, description="Source Assigned event count")


class RuntimeLogResponse(BaseModel):
    id: UUID
    camera_id: UUID
    camera_name: Optional[str] = None
    session_id: Optional[UUID] = None
    worker_id: UUID
    pipeline_id: Optional[str] = None
    event_type: str
    old_status: Optional[str] = None
    new_status: Optional[str] = None
    source_index: Optional[int] = None
    message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class RuntimeLogListResponse(BaseModel):
    summary: RuntimeLogSummaryResponse
    total: int
    skip: int
    limit: int
    items: List[RuntimeLogResponse]
