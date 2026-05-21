"""allow exact identity same-event canonical match type"""

from __future__ import annotations

from alembic import op

revision = "20260521_0006"
down_revision = "20260510_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
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
                'exact_identity_same_event',
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
                'vendor_date',
                'vendor_followup',
                'manual',
                'seed'
            )
        )
        """
    )
