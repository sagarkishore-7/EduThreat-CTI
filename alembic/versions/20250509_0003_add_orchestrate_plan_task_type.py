"""allow v2 orchestrate_plan pipeline tasks"""

from __future__ import annotations

from alembic import op

revision = "20250509_0003"
down_revision = "20250509_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE IF EXISTS pipeline_tasks DROP CONSTRAINT IF EXISTS ck_pipeline_tasks_pipeline_tasks_task_type")
    op.execute(
        """
        ALTER TABLE IF EXISTS pipeline_tasks
        ADD CONSTRAINT ck_pipeline_tasks_pipeline_tasks_task_type
        CHECK (
            task_type IN (
                'collect',
                'resolve_url',
                'fetch_article',
                'enrich_source',
                'canonicalize',
                'refresh_analytics',
                'reenrich',
                'orchestrate_plan'
            )
        )
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE IF EXISTS pipeline_tasks DROP CONSTRAINT IF EXISTS ck_pipeline_tasks_pipeline_tasks_task_type")
    op.execute(
        """
        ALTER TABLE IF EXISTS pipeline_tasks
        ADD CONSTRAINT ck_pipeline_tasks_pipeline_tasks_task_type
        CHECK (
            task_type IN (
                'collect',
                'resolve_url',
                'fetch_article',
                'enrich_source',
                'canonicalize',
                'refresh_analytics',
                'reenrich'
            )
        )
        """
    )
