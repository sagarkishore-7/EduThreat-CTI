"""expand v2 canonical membership match types and score precision"""

from __future__ import annotations

from alembic import op

revision = "20250509_0004"
down_revision = "20250509_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE IF EXISTS canonical_memberships ALTER COLUMN match_score TYPE NUMERIC(7,2)"
    )
    op.execute(
        "ALTER TABLE IF EXISTS canonical_memberships ALTER COLUMN survivor_score TYPE NUMERIC(7,2)"
    )
    op.execute("ALTER TABLE IF EXISTS canonical_memberships DROP CONSTRAINT IF EXISTS canonical_memberships_match_type")
    op.execute("ALTER TABLE IF EXISTS canonical_memberships DROP CONSTRAINT IF EXISTS ck_membership_match_type")
    op.execute(
        """
        ALTER TABLE IF EXISTS canonical_memberships
        ADD CONSTRAINT ck_membership_match_type
        CHECK (
            match_type IN (
                'url_exact',
                'url_resolved',
                'name_date',
                'vendor_platform',
                'vendor_date',
                'vendor_followup',
                'manual',
                'seed'
            )
        )
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE IF EXISTS canonical_memberships DROP CONSTRAINT IF EXISTS ck_membership_match_type")
    op.execute(
        """
        ALTER TABLE IF EXISTS canonical_memberships
        ADD CONSTRAINT ck_membership_match_type
        CHECK (
            match_type IN (
                'url_exact',
                'url_resolved',
                'name_date',
                'vendor_platform',
                'manual',
                'seed'
            )
        )
        """
    )
    op.execute(
        "ALTER TABLE IF EXISTS canonical_memberships ALTER COLUMN survivor_score TYPE NUMERIC(5,2)"
    )
    op.execute(
        "ALTER TABLE IF EXISTS canonical_memberships ALTER COLUMN match_score TYPE NUMERIC(5,2)"
    )
