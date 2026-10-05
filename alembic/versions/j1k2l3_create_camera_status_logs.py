"""create camera_status_logs table

Revision ID: j1k2l3
Revises: g7h8i9
Create Date: 2026-10-03 01:17:30.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'j1k2l3'
down_revision: Union[str, None] = 'g7h8i9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'camera_status_logs',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column('camera_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('cameras.id', ondelete='CASCADE'), nullable=False),
        sa.Column('status', sa.String(length=50), nullable=False),
        sa.Column('started_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('ended_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(op.f('ix_camera_status_logs_camera_id'), 'camera_status_logs', ['camera_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_camera_status_logs_camera_id'), table_name='camera_status_logs')
    op.drop_table('camera_status_logs')
