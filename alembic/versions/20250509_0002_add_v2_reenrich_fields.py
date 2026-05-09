"""add v2 reenrich and manual review fields"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20250509_0002"
down_revision = "20250509_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE source_enrichments ADD COLUMN IF NOT EXISTS re_enrich_attempts INTEGER DEFAULT 0 NOT NULL"
    )
    op.execute(
        "ALTER TABLE source_enrichments ADD COLUMN IF NOT EXISTS re_enrich_reason TEXT"
    )
    op.execute(
        "ALTER TABLE source_enrichments ADD COLUMN IF NOT EXISTS manual_review_required BOOLEAN DEFAULT false NOT NULL"
    )
    op.execute(
        "ALTER TABLE source_enrichments ADD COLUMN IF NOT EXISTS manual_review_reason TEXT"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_source_enrichments_manual_review_required ON source_enrichments (manual_review_required)"
    )
    op.execute(
        "ALTER TABLE source_enrichments ALTER COLUMN re_enrich_attempts DROP DEFAULT"
    )
    op.execute(
        "ALTER TABLE source_enrichments ALTER COLUMN manual_review_required DROP DEFAULT"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_source_enrichments_manual_review_required")
    op.execute("ALTER TABLE IF EXISTS source_enrichments DROP COLUMN IF EXISTS manual_review_reason")
    op.execute("ALTER TABLE IF EXISTS source_enrichments DROP COLUMN IF EXISTS manual_review_required")
    op.execute("ALTER TABLE IF EXISTS source_enrichments DROP COLUMN IF EXISTS re_enrich_reason")
    op.execute("ALTER TABLE IF EXISTS source_enrichments DROP COLUMN IF EXISTS re_enrich_attempts")
