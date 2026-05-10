"""add v2 research metric snapshots"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260510_0005"
down_revision = "20250509_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_names = set(inspector.get_table_names())

    if "research_metric_snapshots" not in table_names:
        op.create_table(
            "research_metric_snapshots",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column("snapshot_key", sa.Text(), nullable=False),
            sa.Column("snapshot_scope", sa.Text(), nullable=False, server_default="global"),
            sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("pipeline_runs.id", ondelete="SET NULL")),
            sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        )
        inspector = sa.inspect(bind)

    existing_indexes = {
        index["name"]
        for index in inspector.get_indexes("research_metric_snapshots")
    }

    if "idx_research_metric_snapshots_key_captured" not in existing_indexes:
        op.create_index(
            "idx_research_metric_snapshots_key_captured",
            "research_metric_snapshots",
            ["snapshot_key", "captured_at"],
            unique=False,
        )
    if "idx_research_metric_snapshots_run_id" not in existing_indexes:
        op.create_index(
            "idx_research_metric_snapshots_run_id",
            "research_metric_snapshots",
            ["run_id"],
            unique=False,
        )


def downgrade() -> None:
    op.drop_index("idx_research_metric_snapshots_run_id", table_name="research_metric_snapshots")
    op.drop_index("idx_research_metric_snapshots_key_captured", table_name="research_metric_snapshots")
    op.drop_table("research_metric_snapshots")
