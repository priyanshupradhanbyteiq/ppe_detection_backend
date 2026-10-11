import uuid

from sqlalchemy import (
    Column,
    String,
    Integer,
    BigInteger,
    Float,
    Text,
    DateTime,
    ForeignKey,
    Index,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.models import Base


class CameraSession(Base):
    __tablename__ = "camera_sessions"

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

    worker_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            "deepstream_workers.id",
            ondelete="CASCADE",
        ),
        nullable=False,
    )

    # For now one default pipeline.
    # Later this can become FK -> deepstream_pipelines.id
    pipeline_id = Column(
        String(100),
        nullable=False,
    )

    # 0..4
    source_index = Column(
        Integer,
        nullable=False,
    )

    # CONNECTING / ONLINE / RECONNECTING /
    # STOPPING / STOPPED / ERROR
    status = Column(
        String(30),
        nullable=False,
        default="CONNECTING",
    )

    started_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    stopped_at = Column(
        DateTime(timezone=True),
        nullable=True,
    )

    frames_processed = Column(
        BigInteger,
        nullable=False,
        default=0,
    )

    avg_fps = Column(
        Float,
        nullable=False,
        default=0.0,
    )

    error_message = Column(
        Text,
        nullable=True,
    )

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        Index("uq_camera_sessions_active_camera", "camera_id", unique=True,
              postgresql_where=text("stopped_at IS NULL"), sqlite_where=text("stopped_at IS NULL")),
        Index("uq_camera_sessions_active_source", "worker_id", "pipeline_id", "source_index", unique=True,
              postgresql_where=text("stopped_at IS NULL"), sqlite_where=text("stopped_at IS NULL")),
        Index(
            "ix_camera_sessions_camera_id",
            "camera_id",
        ),
        Index(
            "ix_camera_sessions_worker_id",
            "worker_id",
        ),
        Index(
            "ix_camera_sessions_status",
            "status",
        ),
        Index(
            "ix_camera_sessions_pipeline_source",
            "pipeline_id",
            "source_index",
        ),
    )