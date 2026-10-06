import uuid

from sqlalchemy import (
    Column,
    String,
    Integer,
    DateTime,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.models import Base


class DeepStreamWorker(Base):
    __tablename__ = "deepstream_workers"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    name = Column(
        String(100),
        nullable=False,
        unique=True,
    )

    hostname = Column(
        String(255),
        nullable=False,
    )

    ip_address = Column(
        String(45),
        nullable=True,
    )

    # STARTING / ONLINE / OFFLINE / ERROR
    status = Column(
        String(30),
        nullable=False,
        default="OFFLINE",
    )

    gpu_name = Column(
        String(255),
        nullable=True,
    )

    max_cameras = Column(
        Integer,
        nullable=False,
        default=5,
    )

    current_cameras = Column(
        Integer,
        nullable=False,
        default=0,
    )

    heartbeat_at = Column(
        DateTime(timezone=True),
        nullable=True,
    )

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )