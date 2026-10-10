import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    String, Integer, BigInteger, Float,
    DateTime, ForeignKey, Index, Text,
    func, text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models import Base


class PPEEvent(Base):
    __tablename__ = "ppe_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    camera_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("cameras.id", ondelete="CASCADE"),
        nullable=False,
    )

    session_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("camera_sessions.id", ondelete="SET NULL"),
        nullable=True,
    )

    # Tracking identity
    tracking_session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
    )

    person_track_id: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
    )

    # PPE classification
    ppe_type: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
    )

    violation_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
    )

    event_type: Mapped[str] = mapped_column(
        String(50),
        default="PPE_VIOLATION",
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        String(20),
        default="OPEN",
        nullable=False,
    )

    severity: Mapped[str] = mapped_column(
        String(20),
        default="HIGH",
        nullable=False,
    )

    confidence: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True,
    )

    violation_ratio: Mapped[float] = mapped_column(
        Float,
        default=0.0,
        nullable=False,
    )

    missing_count: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False,
    )

    present_count: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False,
    )

    unknown_count: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False,
    )

    # Real-world UTC timestamps, not monotonic/PTS seconds
    first_observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    confirmed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    last_observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    resolved_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    resolution_reason: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
    )

    # Cloudinary
    snapshot_url: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    snapshot_public_id: Mapped[Optional[str]] = mapped_column(
        String(500),
        nullable=True,
    )

    snapshot_captured_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )

    clip_url: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    camera = relationship("Camera", lazy="selectin")
    camera_session = relationship(
        "CameraSession", lazy="selectin"
    )

    __table_args__ = (
        Index(
            "uq_ppe_events_open_tracking_identity",
            "tracking_session_id",
            "camera_id",
            "person_track_id",
            "ppe_type",
            unique=True,
            postgresql_where=text("status = 'OPEN'"),
            sqlite_where=text("status = 'OPEN'"),
        ),
        Index("ix_ppe_events_camera_status", "camera_id", "status"),
        Index("ix_ppe_events_confirmed", "confirmed_at"),
        Index("ix_ppe_events_violation", "violation_type"),
    )
