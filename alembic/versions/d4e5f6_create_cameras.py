"""create cameras table

Revision ID: d4e5f6
Revises: a1b2c3
Create Date: 2026-10-03 01:17:10.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'd4e5f6'
down_revision: Union[str, None] = 'a1b2c3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'cameras',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('rtsp_url', sa.String(length=500), nullable=False),
        sa.Column('location_name', sa.String(length=255), nullable=True),
        sa.Column('latitude', sa.Float(), nullable=True),
        sa.Column('longitude', sa.Float(), nullable=True),
        sa.Column('ppe_enabled', sa.Boolean(), default=True, nullable=False),
        sa.Column('fire_enabled', sa.Boolean(), default=False, nullable=False),
        sa.Column('smoke_enabled', sa.Boolean(), default=False, nullable=False),
        sa.Column('desired_state', sa.String(length=50), default='STOPPED', nullable=False),
        sa.Column('status', sa.String(length=50), default='OFFLINE', nullable=False),
        sa.Column('source_type', sa.String(length=30), nullable=False),
        sa.Column('thumbnail_url', sa.String(length=500), nullable=True),
        sa.Column('created_by', postgresql.UUID(as_uuid=True), sa.ForeignKey('users.id', ondelete='SET NULL'), nullable=True),
        sa.Column('last_frame_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_seen_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    )


def downgrade() -> None:
    op.drop_table('cameras')
