import uuid

from sqlalchemy import (
    Column,
    String,
    Text,
    DateTime,
    ForeignKey,
    Index,
)
from sqlalchemy.dialects.postgresql import (
    UUID,
    JSONB,
)
from sqlalchemy.sql import func

from app.models import Base


class CameraRuntimeLog(Base):
    __tablename__ = "camera_runtime_logs"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    camera_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            "cameras.id",
            ondelete="CASCADE",
        ),
        nullable=False,
    )

    session_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            "camera_sessions.id",
            ondelete="SET NULL",
        ),
        nullable=True,
    )

    worker_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            "deepstream_workers.id",
            ondelete="CASCADE",
        ),
        nullable=False,
    )

    pipeline_id = Column(
        String(100),
        nullable=True,
    )

    event_type = Column(
        String(50),
        nullable=False,
    )

    old_status = Column(
        String(30),
        nullable=True,
    )

    new_status = Column(
        String(30),
        nullable=True,
    )

    message = Column(
        Text,
        nullable=True,
    )

    metadata_json = Column(
        "metadata",
        JSONB,
        nullable=True,
    )

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        Index(
            "ix_camera_runtime_logs_camera",
            "camera_id",
        ),
        Index(
            "ix_camera_runtime_logs_session",
            "session_id",
        ),
        Index(
            "ix_camera_runtime_logs_event",
            "event_type",
        ),
        Index(
            "ix_camera_runtime_logs_created",
            "created_at",
        ),
    )