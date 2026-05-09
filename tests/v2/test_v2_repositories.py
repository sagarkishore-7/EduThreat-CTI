from datetime import date

from sqlalchemy.dialects import postgresql

from src.edu_cti_v2.repositories import (
    AnalyticsRefreshRepository,
    CanonicalIncidentRepository,
    PipelineTaskRepository,
    SourceEnrichmentRepository,
    SourceIncidentRepository,
    SourceStateRepository,
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


def test_canonical_repository_url_candidate_stmt_joins_source_urls():
    stmt = CanonicalIncidentRepository.build_find_by_url_candidates_stmt(
        ["https://example.com/article"]
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "source_incident_urls.normalized_url IN ('https://example.com/article')" in compiled
    assert "JOIN canonical_memberships" in compiled


def test_canonical_repository_name_date_candidate_stmt_filters_country_and_window():
    stmt = CanonicalIncidentRepository.build_find_name_date_candidates_stmt(
        incident_date=date(2026, 5, 9),
        country_code="US",
        window_days=14,
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "canonical_incidents.country_code = 'US'" in compiled
    assert "canonical_incidents.incident_date BETWEEN '2026-04-25'" in compiled


def test_canonical_repository_recent_stmt_joins_enrichment_and_orders_by_recency():
    stmt = CanonicalIncidentRepository.build_list_recent_stmt(limit=25)
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "LEFT OUTER JOIN canonical_enrichments" in compiled
    assert "ORDER BY canonical_incidents.last_seen_at DESC" in compiled
    assert "LIMIT 25" in compiled


def test_canonical_repository_dashboard_rollup_stmt_counts_enriched_rows():
    stmt = CanonicalIncidentRepository.build_dashboard_rollup_stmt()
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "count(canonical_incidents.id)" in compiled.lower()
    assert "count(canonical_enrichments.id)" in compiled.lower()


def test_analytics_refresh_repository_lookup_stmt_filters_by_refresh_key():
    stmt = AnalyticsRefreshRepository.build_get_by_key_stmt("dashboard:global")
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "analytics_refresh_state.refresh_key = 'dashboard:global'" in compiled


def test_pipeline_task_repository_active_target_stmt_filters_by_target():
    stmt = PipelineTaskRepository.build_active_target_task_stmt(
        task_type="fetch_article",
        target_table="source_incidents",
        target_id="00000000-0000-0000-0000-000000000111",
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "pipeline_tasks.task_type = 'fetch_article'" in compiled
    assert "pipeline_tasks.target_table = 'source_incidents'" in compiled
    assert "pipeline_tasks.target_id = '00000000-0000-0000-0000-000000000111'" in compiled


def test_source_state_repository_lookup_stmt_filters_by_scope_and_cursor():
    stmt = SourceStateRepository.build_get_state_stmt(
        "googlenews_rss",
        state_scope="historical",
        cursor_key="2026-01-01:2026-07-01",
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "source_state.source_name = 'googlenews_rss'" in compiled
    assert "source_state.state_scope = 'historical'" in compiled
    assert "source_state.cursor_key = '2026-01-01:2026-07-01'" in compiled


def test_source_enrichment_repository_lookup_stmt_filters_by_source_incident():
    stmt = SourceEnrichmentRepository.build_get_by_source_incident_stmt(
        "00000000-0000-0000-0000-000000000111"
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "source_enrichments.source_incident_id = '00000000-0000-0000-0000-000000000111'" in compiled
