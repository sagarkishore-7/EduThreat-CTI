from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

from sqlalchemy.dialects import postgresql

from src.edu_cti_v2.repositories import (
    AnalyticsRefreshRepository,
    ArticleRepository,
    CanonicalIncidentRepository,
    PipelineRunRepository,
    PipelineTaskRepository,
    ResearchMetricSnapshotRepository,
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
    assert "pipeline_tasks.attempt_count < pipeline_tasks.max_attempts" in compiled


def test_pipeline_task_repository_expired_lease_stmt_uses_skip_locked():
    stmt = PipelineTaskRepository.build_expired_leases_stmt(limit=10)
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "FOR UPDATE SKIP LOCKED" in compiled
    assert "pipeline_tasks.status = 'leased'" in compiled
    assert "pipeline_tasks.lease_expires_at <" in compiled


def test_pipeline_task_repository_dead_letter_stmt_uses_skip_locked():
    stmt = PipelineTaskRepository.build_dead_letter_batch_stmt(task_type="canonicalize", limit=10)
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "FOR UPDATE SKIP LOCKED" in compiled
    assert "pipeline_tasks.status = 'dead_letter'" in compiled
    assert "canonicalize" in compiled


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


def test_canonical_repository_identity_candidate_stmt_matches_institution_or_vendor_name():
    stmt = CanonicalIncidentRepository.build_find_identity_candidates_stmt(
        ["PowerSchool", "Canvas"],
        statuses=("open", "excluded"),
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "canonical_incidents.status IN ('open', 'excluded')" in compiled
    assert "canonical_incidents.institution_name IN ('PowerSchool', 'Canvas')" in compiled
    assert "canonical_incidents.vendor_name IN ('PowerSchool', 'Canvas')" in compiled


def test_article_repository_fetch_attempt_stmt_orders_newest_first():
    stmt = ArticleRepository.build_list_fetch_attempts_stmt(
        "00000000-0000-0000-0000-000000000111",
        limit=5,
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "article_fetch_attempts.source_incident_id = '00000000-0000-0000-0000-000000000111'" in compiled
    assert "ORDER BY article_fetch_attempts.attempted_at DESC" in compiled
    assert "LIMIT 5" in compiled


def test_research_metric_snapshot_repository_latest_stmt_orders_by_capture_time():
    stmt = ResearchMetricSnapshotRepository.build_latest_stmt(
        snapshot_key="global",
        snapshot_scope="global",
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "research_metric_snapshots.snapshot_key = 'global'" in compiled
    assert "research_metric_snapshots.snapshot_scope = 'global'" in compiled
    assert "ORDER BY research_metric_snapshots.captured_at DESC" in compiled


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


def test_canonical_repository_selected_source_stmt_joins_source_and_article_tables():
    stmt = CanonicalIncidentRepository.build_selected_source_stmt(
        "00000000-0000-0000-0000-000000000111"
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "JOIN source_enrichments" in compiled
    assert "JOIN source_incidents" in compiled
    assert "LEFT OUTER JOIN article_documents" in compiled
    assert "LEFT OUTER JOIN source_incident_urls" in compiled
    assert "canonical_enrichments.canonical_incident_id = '00000000-0000-0000-0000-000000000111'" in compiled


def test_canonical_repository_membership_detail_stmt_joins_source_incidents():
    stmt = CanonicalIncidentRepository.build_list_membership_details_stmt(
        "00000000-0000-0000-0000-000000000111"
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "JOIN source_incidents" in compiled
    assert "canonical_memberships.canonical_incident_id = '00000000-0000-0000-0000-000000000111'" in compiled
    assert "ORDER BY canonical_memberships.is_primary_member DESC" in compiled


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


def test_canonical_repository_filter_years_stmt_preserves_incident_year_label():
    stmt = CanonicalIncidentRepository.build_filter_years_stmt(statuses=("open",))
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "AS incident_year" in compiled


def test_canonical_repository_filter_options_falls_back_to_tuple_year_rows():
    repo = CanonicalIncidentRepository()
    session = Mock()
    session.execute.side_effect = [
        Mock(all=Mock(return_value=[SimpleNamespace(country="United States")])),
        Mock(all=Mock(return_value=[SimpleNamespace(attack_category="ransomware_encryption")])),
        Mock(all=Mock(return_value=[SimpleNamespace(ransomware_family="Clop ransomware gang")])),
        Mock(all=Mock(return_value=[SimpleNamespace(threat_actor_name="INC ransomware gang")])),
        Mock(all=Mock(return_value=[SimpleNamespace(institution_type="university")])),
        Mock(all=Mock(return_value=[(2025,), (2024,)])),
    ]

    result = repo.get_filter_options(session, statuses=("open",))

    assert result["years"] == [2025, 2024]
    assert result["ransomware_families"] == ["Cl0p"]
    assert result["threat_actors"] == ["INC"]


def test_canonical_repository_dashboard_rollup_stmt_counts_enriched_rows():
    stmt = CanonicalIncidentRepository.build_dashboard_rollup_stmt()
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "count(canonical_incidents.id)" in compiled.lower()
    assert "count(canonical_enrichments.id)" in compiled.lower()
    assert "count(distinct(canonical_incidents.country_code))" in compiled.lower()
    assert "count(distinct(canonical_incidents.ransomware_family))" in compiled.lower()


def test_canonical_repository_ransomware_breakdown_stmt_orders_and_limits():
    stmt = CanonicalIncidentRepository.build_ransomware_breakdown_stmt(
        statuses=("open", "excluded"),
        limit=12,
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "canonical_incidents.status IN ('open', 'excluded')" in compiled
    assert "canonical_incidents.ransomware_family IS NOT NULL" in compiled
    assert "GROUP BY canonical_incidents.ransomware_family" in compiled
    assert "LIMIT 12" in compiled


def test_canonical_repository_threat_actor_breakdown_stmt_groups_and_limits():
    stmt = CanonicalIncidentRepository.build_threat_actor_breakdown_stmt(
        statuses=("open", "excluded"),
        limit=20,
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "canonical_incidents.status IN ('open', 'excluded')" in compiled
    assert "canonical_incidents.threat_actor_name IS NOT NULL" in compiled
    assert "GROUP BY canonical_incidents.threat_actor_name" in compiled
    assert "LIMIT 20" in compiled


def test_canonical_repository_filter_years_stmt_extracts_and_orders_desc():
    stmt = CanonicalIncidentRepository.build_filter_years_stmt(statuses=("open",))
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "EXTRACT(year FROM canonical_incidents.incident_date)" in compiled
    assert "canonical_incidents.incident_date IS NOT NULL" in compiled
    assert "ORDER BY incident_year DESC" in compiled


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
