from pathlib import Path

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from src.edu_cti_v2.db.base import Base
from src.edu_cti_v2.models import CanonicalIncident, PipelineTask, SourceIncident


def test_v2_metadata_registers_core_tables():
    expected = {
        "source_incidents",
        "source_incident_urls",
        "source_state",
        "article_documents",
        "article_fetch_attempts",
        "source_enrichments",
        "canonical_incidents",
        "canonical_memberships",
        "canonical_enrichments",
        "canonical_timeline_events",
        "pipeline_runs",
        "pipeline_tasks",
        "analytics_refresh_state",
    }

    assert expected.issubset(set(Base.metadata.tables.keys()))


def test_pipeline_task_table_compiles_for_postgres():
    compiled = str(CreateTable(PipelineTask.__table__).compile(dialect=postgresql.dialect()))

    assert "pipeline_tasks" in compiled
    assert "lease_expires_at" in compiled
    assert "target_table" in compiled


def test_source_incident_table_compiles_for_postgres():
    compiled = str(CreateTable(SourceIncident.__table__).compile(dialect=postgresql.dialect()))

    assert "source_incidents" in compiled
    assert "raw_payload JSONB" in compiled


def test_canonical_incident_table_compiles_for_postgres():
    compiled = str(CreateTable(CanonicalIncident.__table__).compile(dialect=postgresql.dialect()))

    assert "canonical_incidents" in compiled
    assert "resolution_metadata JSONB" in compiled


def test_v2_schema_sql_exists():
    schema_path = Path("src/edu_cti_v2/db/schema.sql")
    assert schema_path.exists()
    assert "CREATE TABLE IF NOT EXISTS source_incidents" in schema_path.read_text(encoding="utf-8")
