"""Persist PPE lifecycle events and snapshot references.

Revision ID: n7p8q9
Revises: m4n5o6
"""

from alembic import context, op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

revision = "n7p8q9"
down_revision = "m4n5o6"
branch_labels = None
depends_on = None


def upgrade():
    offline = context.is_offline_mode()
    inspector = None if offline else sa.inspect(op.get_bind())
    existing = not offline and "ppe_events" in inspector.get_table_names()
    if not existing:
        op.create_table(
            "ppe_events",
            sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
            sa.Column("camera_id", pg.UUID(as_uuid=True), sa.ForeignKey("cameras.id", ondelete="CASCADE"), nullable=False),
            sa.Column("session_id", pg.UUID(as_uuid=True), sa.ForeignKey("camera_sessions.id", ondelete="SET NULL")),
            sa.Column("tracking_session_id", pg.UUID(as_uuid=True), nullable=False),
            sa.Column("person_track_id", sa.BigInteger(), nullable=False),
            sa.Column("ppe_type", sa.String(30), nullable=False),
            sa.Column("violation_type", sa.String(50), nullable=False),
            sa.Column("event_type", sa.String(50), nullable=False),
            sa.Column("status", sa.String(20), nullable=False),
            sa.Column("severity", sa.String(20), nullable=False),
            sa.Column("confidence", sa.Float()),
            sa.Column("violation_ratio", sa.Float(), nullable=False),
            sa.Column("missing_count", sa.Integer(), nullable=False),
            sa.Column("present_count", sa.Integer(), nullable=False),
            sa.Column("unknown_count", sa.Integer(), nullable=False),
            sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("resolved_at", sa.DateTime(timezone=True)),
            sa.Column("resolution_reason", sa.String(100)),
            sa.Column("snapshot_url", sa.Text()),
            sa.Column("snapshot_public_id", sa.String(500)),
            sa.Column("snapshot_captured_at", sa.DateTime(timezone=True)),
            sa.Column("clip_url", sa.Text()),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        )
    else:
        # Adopt a table created by API create_all() or the earlier model draft.
        columns = {column["name"] for column in inspector.get_columns("ppe_events")}
        if "snapshot_captured_at" not in columns:
            op.add_column("ppe_events", sa.Column("snapshot_captured_at", sa.DateTime(timezone=True)))
        for constraint in inspector.get_unique_constraints("ppe_events"):
            if constraint["name"] == "uq_ppe_event_tracking_identity":
                op.drop_constraint(constraint["name"], "ppe_events", type_="unique")

    names = {index["name"] for index in inspector.get_indexes("ppe_events")} if existing else set()
    indexes = {
        "ix_ppe_events_camera_status": ["camera_id", "status"],
        "ix_ppe_events_confirmed": ["confirmed_at"],
        "ix_ppe_events_violation": ["violation_type"],
        "uq_ppe_events_open_tracking_identity": ["tracking_session_id", "camera_id", "person_track_id", "ppe_type"],
    }
    for name, columns in indexes.items():
        if name not in names:
            options = {"unique": True, "postgresql_where": sa.text("status = 'OPEN'"),
                       "sqlite_where": sa.text("status = 'OPEN'")} if name.startswith("uq_") else {}
            op.create_index(name, "ppe_events", columns, **options)


def downgrade():
    op.drop_table("ppe_events")
