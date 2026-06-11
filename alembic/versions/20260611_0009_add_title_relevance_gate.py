"""add LLM title-relevance gate to source_incidents + classify_titles task type

Additive and safe to apply on a live database:
  * adds ``relevance_status`` (+ score/reason/classified_at) to ``source_incidents``
  * backfills every pre-existing row to ``relevant`` — those rows were already
    routed past the legacy keyword pre-filter, so the new classifier must only
    ever judge titles collected *after* this migration (new rows default to
    ``pending``)
  * adds the ``(source_group, relevance_status)`` index the batch sweep selects on
  * widens the ``pipeline_tasks`` task_type CHECK to allow ``classify_titles``

Nothing existing is dropped, so the running pipeline keeps working unchanged
until ``TITLE_CLASSIFY_ENABLED`` is turned on.
"""

from __future__ import annotations

from alembic import op

revision = "20260611_0009"
down_revision = "20260607_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- source_incidents: title-relevance columns --------------------------
    op.execute(
        "ALTER TABLE source_incidents "
        "ADD COLUMN IF NOT EXISTS relevance_status TEXT NOT NULL DEFAULT 'pending'"
    )
    op.execute(
        "ALTER TABLE source_incidents "
        "ADD COLUMN IF NOT EXISTS title_relevance_score DOUBLE PRECISION"
    )
    op.execute(
        "ALTER TABLE source_incidents "
        "ADD COLUMN IF NOT EXISTS title_relevance_reason TEXT"
    )
    op.execute(
        "ALTER TABLE source_incidents "
        "ADD COLUMN IF NOT EXISTS title_classified_at TIMESTAMPTZ"
    )

    # Pre-existing rows were collected under the old keyword gate and already
    # have downstream tasks; mark them relevant so they are never re-classified.
    op.execute(
        "UPDATE source_incidents SET relevance_status = 'relevant' "
        "WHERE relevance_status = 'pending'"
    )

    op.execute(
        "ALTER TABLE source_incidents "
        "DROP CONSTRAINT IF EXISTS source_incidents_relevance_status"
    )
    op.execute(
        "ALTER TABLE source_incidents "
        "ADD CONSTRAINT source_incidents_relevance_status "
        "CHECK (relevance_status IN ('pending', 'relevant', 'irrelevant'))"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_source_incidents_relevance "
        "ON source_incidents (source_group, relevance_status)"
    )

    # --- pipeline_tasks: allow the classify_titles task type ----------------
    op.execute("ALTER TABLE IF EXISTS pipeline_tasks DROP CONSTRAINT IF EXISTS ck_pipeline_task_type")
    op.execute("ALTER TABLE IF EXISTS pipeline_tasks DROP CONSTRAINT IF EXISTS pipeline_tasks_task_type")
    op.execute(
        """
        ALTER TABLE IF EXISTS pipeline_tasks
        ADD CONSTRAINT ck_pipeline_task_type
        CHECK (
            task_type IN (
                'collect',
                'resolve_url',
                'fetch_article',
                'enrich_source',
                'canonicalize',
                'refresh_analytics',
                'campaign_correlate',
                'reenrich',
                'orchestrate_plan',
                'classify_titles'
            )
        )
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE IF EXISTS pipeline_tasks DROP CONSTRAINT IF EXISTS ck_pipeline_task_type")
    op.execute(
        """
        ALTER TABLE IF EXISTS pipeline_tasks
        ADD CONSTRAINT ck_pipeline_task_type
        CHECK (
            task_type IN (
                'collect',
                'resolve_url',
                'fetch_article',
                'enrich_source',
                'canonicalize',
                'refresh_analytics',
                'campaign_correlate',
                'reenrich',
                'orchestrate_plan'
            )
        )
        """
    )
    op.execute("DROP INDEX IF EXISTS ix_source_incidents_relevance")
    op.execute(
        "ALTER TABLE source_incidents "
        "DROP CONSTRAINT IF EXISTS source_incidents_relevance_status"
    )
    op.execute("ALTER TABLE source_incidents DROP COLUMN IF EXISTS title_classified_at")
    op.execute("ALTER TABLE source_incidents DROP COLUMN IF EXISTS title_relevance_reason")
    op.execute("ALTER TABLE source_incidents DROP COLUMN IF EXISTS title_relevance_score")
    op.execute("ALTER TABLE source_incidents DROP COLUMN IF EXISTS relevance_status")
