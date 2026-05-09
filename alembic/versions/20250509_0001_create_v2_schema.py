"""create postgres v2 schema"""

from __future__ import annotations

from pathlib import Path

from alembic import op

revision = "20250509_0001"
down_revision = None
branch_labels = None
depends_on = None


def _load_schema_statements() -> list[str]:
    schema_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "edu_cti_v2"
        / "db"
        / "schema.sql"
    )
    text = schema_path.read_text(encoding="utf-8")
    statements = []
    chunks = text.split(";\n")
    for chunk in chunks:
        stmt = chunk.strip()
        if stmt:
            statements.append(stmt + ";")
    return statements


def upgrade() -> None:
    bind = op.get_bind()
    for statement in _load_schema_statements():
        bind.exec_driver_sql(statement)


def downgrade() -> None:
    bind = op.get_bind()
    drop_statements = [
        "DROP TABLE IF EXISTS analytics_refresh_state CASCADE;",
        "DROP TABLE IF EXISTS pipeline_tasks CASCADE;",
        "DROP TABLE IF EXISTS pipeline_runs CASCADE;",
        "DROP TABLE IF EXISTS canonical_timeline_events CASCADE;",
        "DROP TABLE IF EXISTS canonical_enrichments CASCADE;",
        "DROP TABLE IF EXISTS canonical_memberships CASCADE;",
        "DROP TABLE IF EXISTS canonical_incidents CASCADE;",
        "DROP TABLE IF EXISTS source_enrichments CASCADE;",
        "DROP TABLE IF EXISTS article_fetch_attempts CASCADE;",
        "DROP TABLE IF EXISTS article_documents CASCADE;",
        "DROP TABLE IF EXISTS source_state CASCADE;",
        "DROP TABLE IF EXISTS source_incident_urls CASCADE;",
        "DROP TABLE IF EXISTS source_incidents CASCADE;",
    ]
    for statement in drop_statements:
        bind.exec_driver_sql(statement)
