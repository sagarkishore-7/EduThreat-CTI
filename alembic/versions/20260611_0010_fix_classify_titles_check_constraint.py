"""fix pipeline_tasks task_type CHECK so classify_titles is allowed

Migration 0009 widened the task_type CHECK but only dropped the
``ck_pipeline_task_type`` / ``pipeline_tasks_task_type`` variant names. The
constraint actually enforced on the live table is the SQLAlchemy
naming-convention name ``ck_pipeline_tasks_pipeline_tasks_task_type`` (derived
from the model's ``CheckConstraint(name="pipeline_tasks_task_type")``), which
0009 never touched — so it kept rejecting ``classify_titles`` inserts.

This migration drops EVERY known variant name idempotently and re-adds the
canonical constraint (matching the ORM naming convention) with the full task
type list including ``classify_titles``. Safe to run repeatedly.
"""

from __future__ import annotations

from alembic import op

revision = "20260611_0010"
down_revision = "20260611_0009"
branch_labels = None
depends_on = None

_ALL_TYPES = (
    "'collect'",
    "'resolve_url'",
    "'fetch_article'",
    "'enrich_source'",
    "'canonicalize'",
    "'refresh_analytics'",
    "'campaign_correlate'",
    "'reenrich'",
    "'orchestrate_plan'",
    "'classify_titles'",
)

_VARIANT_NAMES = (
    "ck_pipeline_tasks_pipeline_tasks_task_type",  # ORM naming-convention (enforced live)
    "ck_pipeline_task_type",                        # raw-SQL name from 0007/0009
    "pipeline_tasks_task_type",                      # bare model name
    "ck_pipeline_tasks_task_type",                   # other possible variant
)


def _drop_all_variants() -> None:
    for name in _VARIANT_NAMES:
        op.execute(f"ALTER TABLE IF EXISTS pipeline_tasks DROP CONSTRAINT IF EXISTS {name}")


def upgrade() -> None:
    # ``pipeline_tasks`` is a hot table (~80k rows, actively written by the worker).
    # A plain ADD CONSTRAINT ... CHECK validation-scans every row while holding
    # ACCESS EXCLUSIVE, which stalls the boot migration past the API healthcheck.
    # ``NOT VALID`` skips the scan (instant, microsecond lock) and is safe here:
    # every existing row already has a valid task_type, so there is nothing to
    # validate — and NEW classify_titles inserts are enforced immediately.
    # A short lock_timeout makes the brief ACCESS EXCLUSIVE grab fail fast rather
    # than queue behind the worker and freeze the pipeline; the migration simply
    # retries on the next boot if it loses the race.
    op.execute("SET lock_timeout = '8s'")
    _drop_all_variants()
    op.execute(
        "ALTER TABLE IF EXISTS pipeline_tasks "
        "ADD CONSTRAINT ck_pipeline_tasks_pipeline_tasks_task_type "
        f"CHECK (task_type IN ({', '.join(_ALL_TYPES)})) NOT VALID"
    )
    op.execute("RESET lock_timeout")


def downgrade() -> None:
    # Restore the canonical constraint without classify_titles.
    _drop_all_variants()
    without_classify = ", ".join(t for t in _ALL_TYPES if t != "'classify_titles'")
    op.execute(
        "ALTER TABLE IF EXISTS pipeline_tasks "
        "ADD CONSTRAINT ck_pipeline_tasks_pipeline_tasks_task_type "
        f"CHECK (task_type IN ({without_classify}))"
    )
