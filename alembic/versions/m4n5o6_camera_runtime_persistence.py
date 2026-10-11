"""Persist DeepStream workers, sessions, and runtime history.

Revision ID: m4n5o6
Revises: j1k2l3
"""

from alembic import context, op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

revision = "m4n5o6"
down_revision = "j1k2l3"
branch_labels = None
depends_on = None


def upgrade():
    # Older API startup may already have created these model tables. Adopt them
    # without dropping data; create missing indexes in either case.
    offline = context.is_offline_mode()
    inspector = None if offline else sa.inspect(op.get_bind())
    existing = set() if offline else set(inspector.get_table_names())
    definitions = {
        "deepstream_workers": [
            sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
            sa.Column("name", sa.String(100), nullable=False, unique=True),
            sa.Column("hostname", sa.String(255), nullable=False),
            sa.Column("ip_address", sa.String(45)),
            sa.Column("status", sa.String(30), nullable=False),
            sa.Column("gpu_name", sa.String(255)),
            sa.Column("max_cameras", sa.Integer(), nullable=False),
            sa.Column("current_cameras", sa.Integer(), nullable=False),
            sa.Column("heartbeat_at", sa.DateTime(timezone=True)),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        ],
        "camera_sessions": [
            sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
            sa.Column("camera_id", pg.UUID(as_uuid=True), sa.ForeignKey("cameras.id", ondelete="CASCADE"), nullable=False),
            sa.Column("worker_id", pg.UUID(as_uuid=True), sa.ForeignKey("deepstream_workers.id", ondelete="CASCADE"), nullable=False),
            sa.Column("pipeline_id", sa.String(100), nullable=False),
            sa.Column("source_index", sa.Integer(), nullable=False),
            sa.Column("status", sa.String(30), nullable=False),
            sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("stopped_at", sa.DateTime(timezone=True)),
            sa.Column("frames_processed", sa.BigInteger(), nullable=False),
            sa.Column("avg_fps", sa.Float(), nullable=False),
            sa.Column("error_message", sa.Text()),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        ],
        "camera_runtime_logs": [
            sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
            sa.Column("camera_id", pg.UUID(as_uuid=True), sa.ForeignKey("cameras.id", ondelete="CASCADE"), nullable=False),
            sa.Column("session_id", pg.UUID(as_uuid=True), sa.ForeignKey("camera_sessions.id", ondelete="SET NULL")),
            sa.Column("worker_id", pg.UUID(as_uuid=True), sa.ForeignKey("deepstream_workers.id", ondelete="CASCADE"), nullable=False),
            sa.Column("pipeline_id", sa.String(100)),
            sa.Column("event_type", sa.String(50), nullable=False),
            sa.Column("old_status", sa.String(30)),
            sa.Column("new_status", sa.String(30)),
            sa.Column("message", sa.Text()),
            sa.Column("metadata", pg.JSONB()),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        ],
    }
    for table, columns in definitions.items():
        if table not in existing:
            op.create_table(table, *columns)
    indexes = {
        "camera_sessions": [
            ("ix_camera_sessions_camera_id", ["camera_id"], False),
            ("ix_camera_sessions_worker_id", ["worker_id"], False),
            ("ix_camera_sessions_status", ["status"], False),
            ("ix_camera_sessions_pipeline_source", ["pipeline_id", "source_index"], False),
            ("uq_camera_sessions_active_camera", ["camera_id"], True),
            ("uq_camera_sessions_active_source", ["worker_id", "pipeline_id", "source_index"], True),
        ],
        "camera_runtime_logs": [
            ("ix_camera_runtime_logs_camera", ["camera_id"], False),
            ("ix_camera_runtime_logs_session", ["session_id"], False),
            ("ix_camera_runtime_logs_event", ["event_type"], False),
            ("ix_camera_runtime_logs_created", ["created_at"], False),
        ],
    }
    for table, entries in indexes.items():
        names = set() if offline or table not in existing else {i["name"] for i in inspector.get_indexes(table)}
        for name, columns, unique in entries:
            if name not in names:
                options = {"postgresql_where": sa.text("stopped_at IS NULL")} if unique else {}
                op.create_index(name, table, columns, unique=unique, **options)
    op.execute("UPDATE cameras SET desired_state = 'RUNNING' WHERE desired_state = 'ACTIVE'")


def downgrade():
    op.drop_table("camera_runtime_logs")
    op.drop_table("camera_sessions")
    op.drop_table("deepstream_workers")
