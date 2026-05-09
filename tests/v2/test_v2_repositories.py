from datetime import date

from sqlalchemy.dialects import postgresql

from src.edu_cti_v2.repositories import (
    AnalyticsRefreshRepository,
    CanonicalIncidentRepository,
    PipelineRunRepository,
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
    assert "ORDER BY pipeline_tasks.priority DESC" in compiled
    assert "pipeline_tasks.status = 'queued'" in compiled
    assert "pipeline_tasks.status = 'leased'" in compiled
    assert "pipeline_tasks.lease_expires_at <" in compiled


def test_pipeline_task_repository_expired_lease_stmt_uses_skip_locked():
    stmt = PipelineTaskRepository.build_expired_leases_stmt(limit=10)
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "FOR UPDATE SKIP LOCKED" in compiled
    assert "pipeline_tasks.status = 'leased'" in compiled
    assert "pipeline_tasks.lease_expires_at <" in compiled


def test_pipeline_task_repository_lease_stmt_can_exclude_task_types():
    stmt = PipelineTaskRepository.build_lease_batch_stmt(
        exclude_task_types=("orchestrate_plan",),
        limit=5,
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "pipeline_tasks.task_type NOT IN ('orchestrate_plan')" in compiled


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


def test_canonical_repository_recent_stmt_supports_filters_and_pagination():
    stmt = CanonicalIncidentRepository.build_list_recent_stmt(
        statuses=("open", "excluded"),
        limit=10,
        offset=20,
        search="stanford",
        country_code="US",
        attack_category="ransomware_encryption",
        institution_type="university",
        severity="high",
        is_education_related=True,
        has_vendor=False,
        date_from=date(2026, 5, 1),
        date_to=date(2026, 5, 9),
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "canonical_incidents.status IN ('open', 'excluded')" in compiled
    assert "canonical_incidents.country_code = 'US'" in compiled
    assert "canonical_incidents.attack_category = 'ransomware_encryption'" in compiled
    assert "canonical_incidents.institution_type = 'university'" in compiled
    assert "canonical_incidents.severity = 'high'" in compiled
    assert "canonical_incidents.is_education_related IS true" in compiled
    assert "canonical_incidents.vendor_name IS NULL" in compiled
    assert "canonical_incidents.incident_date >= '2026-05-01'" in compiled
    assert "canonical_incidents.incident_date <= '2026-05-09'" in compiled
    assert "OFFSET 20" in compiled


def test_canonical_repository_recent_stmt_supports_sorting():
    stmt = CanonicalIncidentRepository.build_list_recent_stmt(
        limit=25,
        sort_by="incident_date",
        sort_order="asc",
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "ORDER BY canonical_incidents.incident_date ASC NULLS LAST" in compiled
    assert "canonical_incidents.last_seen_at ASC NULLS LAST" in compiled


def test_canonical_repository_country_facet_stmt_supports_filters():
    stmt = CanonicalIncidentRepository.build_country_facet_stmt(
        statuses=("open", "excluded"),
        search="stanford",
        country_code="US",
        attack_category="ransomware_encryption",
        institution_type="university",
        severity="high",
        is_education_related=True,
        has_vendor=False,
        date_from=date(2026, 5, 1),
        date_to=date(2026, 5, 9),
        limit=15,
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "canonical_incidents.country_code = 'US'" in compiled
    assert "canonical_incidents.attack_category = 'ransomware_encryption'" in compiled
    assert "canonical_incidents.institution_type = 'university'" in compiled
    assert "canonical_incidents.severity = 'high'" in compiled
    assert "canonical_incidents.is_education_related IS true" in compiled
    assert "canonical_incidents.vendor_name IS NULL" in compiled
    assert "GROUP BY canonical_incidents.country_code, canonical_incidents.country" in compiled
    assert "LIMIT 15" in compiled


def test_canonical_repository_incident_trend_stmt_supports_bucket_and_filters():
    stmt = CanonicalIncidentRepository.build_incident_trend_stmt(
        statuses=("open", "excluded"),
        search="stanford",
        country_code="US",
        attack_category="ransomware_encryption",
        institution_type="university",
        severity="high",
        is_education_related=True,
        has_vendor=False,
        date_from=date(2026, 5, 1),
        date_to=date(2026, 5, 9),
        bucket="week",
        limit=18,
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "date_trunc('week', canonical_incidents.incident_date)" in compiled
    assert "canonical_incidents.country_code = 'US'" in compiled
    assert "canonical_incidents.attack_category = 'ransomware_encryption'" in compiled
    assert "canonical_incidents.institution_type = 'university'" in compiled
    assert "canonical_incidents.severity = 'high'" in compiled
    assert "canonical_incidents.is_education_related IS true" in compiled
    assert "canonical_incidents.vendor_name IS NULL" in compiled
    assert "ORDER BY bucket_start DESC" in compiled
    assert "LIMIT 18" in compiled


def test_canonical_repository_dashboard_rollup_stmt_counts_enriched_rows():
    stmt = CanonicalIncidentRepository.build_dashboard_rollup_stmt()
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "count(canonical_incidents.id)" in compiled.lower()
    assert "count(canonical_enrichments.id)" in compiled.lower()


def test_analytics_refresh_repository_lookup_stmt_filters_by_refresh_key():
    stmt = AnalyticsRefreshRepository.build_get_by_key_stmt("dashboard:global")
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "analytics_refresh_state.refresh_key = 'dashboard:global'" in compiled


def test_pipeline_task_repository_status_summary_stmt_groups_by_type_and_status():
    stmt = PipelineTaskRepository.build_status_summary_stmt()
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "GROUP BY pipeline_tasks.task_type, pipeline_tasks.status" in compiled


def test_pipeline_run_repository_recent_stmt_orders_by_created_at():
    stmt = PipelineRunRepository.build_list_recent_stmt(limit=5)
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "ORDER BY pipeline_runs.created_at DESC" in compiled
    assert "LIMIT 5" in compiled


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


def test_pipeline_task_repository_count_active_stmt_can_exclude_orchestration_tasks():
    stmt = PipelineTaskRepository.build_count_active_stmt(
        exclude_task_types=("orchestrate_plan",),
        exclude_task_ids=("00000000-0000-0000-0000-000000000111",),
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "pipeline_tasks.status IN ('queued', 'leased')" in compiled
    assert "pipeline_tasks.task_type NOT IN ('orchestrate_plan')" in compiled
    assert "pipeline_tasks.id NOT IN ('00000000-0000-0000-0000-000000000111')" in compiled


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
