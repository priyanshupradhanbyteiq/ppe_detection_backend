"""create detection_events table

Revision ID: g7h8i9
Revises: d4e5f6
Create Date: 2026-10-03 01:17:20.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'g7h8i9'
down_revision: Union[str, None] = 'd4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'detection_events',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column('camera_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('cameras.id', ondelete='CASCADE'), nullable=False),
        sa.Column('detection_type', sa.String(length=50), nullable=False),
        sa.Column('confidence', sa.Float(), nullable=True),
        sa.Column('detected_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('image_url', sa.String(length=500), nullable=True),
        sa.Column('video_timestamp', sa.Float(), nullable=True),
    )
    op.create_index(op.f('ix_detection_events_camera_id'), 'detection_events', ['camera_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_detection_events_camera_id'), table_name='detection_events')
    op.drop_table('detection_events')
