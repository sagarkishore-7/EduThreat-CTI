"""add v2 reenrich and manual review fields"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20250509_0002"
down_revision = "20250509_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "source_enrichments",
        sa.Column("re_enrich_attempts", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "source_enrichments",
        sa.Column("re_enrich_reason", sa.Text(), nullable=True),
    )
    op.add_column(
        "source_enrichments",
        sa.Column("manual_review_required", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.add_column(
        "source_enrichments",
        sa.Column("manual_review_reason", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_source_enrichments_manual_review_required",
        "source_enrichments",
        ["manual_review_required"],
        unique=False,
    )
    op.alter_column("source_enrichments", "re_enrich_attempts", server_default=None)
    op.alter_column("source_enrichments", "manual_review_required", server_default=None)


def downgrade() -> None:
    op.drop_index("idx_source_enrichments_manual_review_required", table_name="source_enrichments")
    op.drop_column("source_enrichments", "manual_review_reason")
    op.drop_column("source_enrichments", "manual_review_required")
    op.drop_column("source_enrichments", "re_enrich_reason")
    op.drop_column("source_enrichments", "re_enrich_attempts")
