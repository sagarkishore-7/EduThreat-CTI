from sqlalchemy.dialects import postgresql

from src.edu_cti_v2.repositories import (
    CanonicalIncidentRepository,
    PipelineTaskRepository,
    SourceIncidentRepository,
)


def test_pipeline_task_repository_lease_stmt_uses_skip_locked():
    stmt = PipelineTaskRepository.build_lease_batch_stmt(task_type="enrich_source", limit=5)
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "FOR UPDATE SKIP LOCKED" in compiled
    assert "enrich_source" in compiled


def test_source_incident_repository_lookup_stmt_filters_by_source_and_event_key():
    stmt = SourceIncidentRepository.build_get_by_source_event_key_stmt("googlenews_rss", "abc123")
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "source_incidents.source_name = 'googlenews_rss'" in compiled
    assert "source_incidents.source_event_key = 'abc123'" in compiled


def test_canonical_repository_membership_stmt_filters_by_source_incident():
    stmt = CanonicalIncidentRepository.build_get_by_source_incident_stmt("00000000-0000-0000-0000-000000000111")
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "canonical_memberships.source_incident_id" in compiled
