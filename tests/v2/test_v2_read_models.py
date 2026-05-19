from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

from src.edu_cti_v2.services import V2CanonicalReadService


def test_read_service_lists_recent_incidents_from_canonical_rows():
    canonical = SimpleNamespace(
        id=uuid4(),
        institution_name="Penn State University",
        vendor_name=None,
        institution_type="university",
        country="United States",
        country_code="US",
        region="Pennsylvania",
        city="State College",
        incident_date=date(2026, 5, 8),
        date_precision="day",
        attack_category="ransomware_encryption",
        attack_vector="phishing_email",
        threat_actor_name="SomeGroup",
        ransomware_family="LockBit",
        is_education_related=True,
        severity="high",
        canonical_summary="Penn State suffered a ransomware incident.",
        status="open",
        first_seen_at=datetime(2026, 5, 8, 8, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 5, 9, 8, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 5, 9, 9, 0, tzinfo=timezone.utc),
    )
    enrichment = SimpleNamespace(
        selected_source_enrichment_id=uuid4(),
        analytics_projection={"attack_category": "ransomware_encryption"},
    )
    canonical_repo = Mock()
    canonical_repo.list_recent_with_enrichment.return_value = [(canonical, enrichment, 2)]

    service = V2CanonicalReadService(canonical_repository=canonical_repo)

    items = service.list_recent_incidents(Mock(), limit=10)

    assert len(items) == 1
    assert items[0]["display_name"] == "Penn State University"
    assert items[0]["membership_count"] == 2
    assert items[0]["country_code"] == "US"


def test_read_service_list_incidents_returns_items_and_total_with_filters():
    canonical = SimpleNamespace(
        id=uuid4(),
        institution_name="Stanford University",
        vendor_name=None,
        institution_type="university",
        country="United States",
        country_code="US",
        region="California",
        city="Stanford",
        incident_date=date(2026, 5, 8),
        date_precision="day",
        attack_category="ransomware_encryption",
        attack_vector="phishing_email",
        threat_actor_name="SomeGroup",
        ransomware_family="LockBit",
        is_education_related=True,
        severity="high",
        canonical_summary="Stanford suffered a ransomware incident.",
        status="open",
        first_seen_at=datetime(2026, 5, 8, 8, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 5, 9, 8, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 5, 9, 9, 0, tzinfo=timezone.utc),
    )
    enrichment = SimpleNamespace(
        selected_source_enrichment_id=uuid4(),
        analytics_projection={"attack_category": "ransomware_encryption"},
    )
    canonical_repo = Mock()
    canonical_repo.list_recent_with_enrichment.return_value = [(canonical, enrichment, 3)]
    canonical_repo.count_recent.return_value = 17

    service = V2CanonicalReadService(canonical_repository=canonical_repo)

    session = Mock()
    result = service.list_incidents(
        session,
        limit=10,
        offset=20,
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
        sort_by="incident_date",
        sort_order="asc",
    )

    assert result["total"] == 17
    assert result["items"][0]["display_name"] == "Stanford University"
    canonical_repo.list_recent_with_enrichment.assert_called_once_with(
        session,
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
        sort_by="incident_date",
        sort_order="asc",
    )
    canonical_repo.count_recent.assert_called_once()


def test_read_service_list_legacy_incidents_returns_old_pagination_shape():
    canonical = SimpleNamespace(
        id=uuid4(),
        institution_name="Stanford University",
        vendor_name=None,
        institution_type="university",
        country="United States",
        country_code="US",
        region="California",
        city="Stanford",
        incident_date=date(2026, 5, 8),
        date_precision="day",
        attack_category="ransomware_encryption",
        attack_vector="phishing_email",
        threat_actor_name="SomeGroup",
        ransomware_family="LockBit",
        is_education_related=True,
        severity="high",
        canonical_summary="Stanford suffered a ransomware incident.",
        status="open",
        first_seen_at=datetime(2026, 5, 8, 8, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 5, 9, 8, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 5, 9, 9, 0, tzinfo=timezone.utc),
    )
    enrichment = SimpleNamespace(
        selected_source_enrichment_id=uuid4(),
        analytics_projection={"attack_category": "ransomware_encryption"},
    )
    canonical_repo = Mock()
    canonical_repo.list_recent_with_enrichment.return_value = [(canonical, enrichment, 3)]
    canonical_repo.count_recent.return_value = 17

    service = V2CanonicalReadService(canonical_repository=canonical_repo)

    result = service.list_legacy_incidents(
        Mock(),
        limit=10,
        offset=20,
        statuses=("open", "excluded"),
    )

    assert result["incidents"][0]["incident_id"]
    assert result["pagination"]["page"] == 3
    assert result["pagination"]["total_pages"] == 2
    assert result["pagination"]["has_prev"] is True


def test_read_service_returns_detail_with_memberships_timeline_and_snapshot():
    canonical_id = str(uuid4())
    selected_source_enrichment_id = uuid4()
    source_incident_id = uuid4()
    canonical = SimpleNamespace(
        id=uuid4(),
        institution_name="Penn State University",
        vendor_name=None,
        institution_type="university",
        country="United States",
        country_code="US",
        region="Pennsylvania",
        city="State College",
        incident_date=date(2026, 5, 8),
        date_precision="day",
        attack_category="ransomware_encryption",
        attack_vector="phishing_email",
        threat_actor_name="SomeGroup",
        ransomware_family="LockBit",
        is_education_related=True,
        severity="high",
        canonical_summary="Penn State suffered a ransomware incident.",
        status="open",
        first_seen_at=datetime(2026, 5, 8, 8, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 5, 9, 8, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 5, 9, 9, 0, tzinfo=timezone.utc),
        resolution_metadata={"last_match_type": "url_exact"},
    )
    membership = SimpleNamespace(
        source_incident_id=source_incident_id,
        match_type="url_exact",
        match_score=100.0,
        survivor_score=55.0,
        is_primary_member=True,
        field_contribution={"institution_name": "abc"},
        matcher_version="v2",
        matched_at=datetime(2026, 5, 9, 9, 30, tzinfo=timezone.utc),
    )
    enrichment = SimpleNamespace(
        selected_source_enrichment_id=selected_source_enrichment_id,
        analytics_projection={"attack_category": "ransomware_encryption"},
        field_provenance={
            "field_sources": {"institution_name": str(selected_source_enrichment_id)},
            "source_disclosure": {
                "selection_basis": "highest_survivor_score",
                "selected_source_enrichment_id": str(selected_source_enrichment_id),
                "tracked_field_labels": {
                    "institution_name": "Institution",
                    "country": "Country",
                    "incident_date": "Incident Date",
                },
                "sources": [
                    {
                        "source_enrichment_id": str(selected_source_enrichment_id),
                        "source_incident_id": str(source_incident_id),
                        "source_name": "googlenews_rss",
                        "source_group": "rss",
                        "raw_title": "Penn State ransomware update",
                        "source_published_at": "2026-05-09T08:30:00+00:00",
                        "is_primary_member": True,
                        "survivor_score": 55.0,
                        "score_breakdown": {"source_rank": 20, "structured_field_coverage": 8, "identity_title_alignment_bonus": 8},
                        "field_count": 3,
                        "disclosed_fields": ["institution_name", "country", "incident_date"],
                        "field_values": {
                            "institution_name": "Penn State University",
                            "country": "United States",
                            "incident_date": "2026-05-08",
                        },
                    }
                ],
            },
        },
        canonical_projection={"institution_name": "Penn State University"},
    )
    timeline_event = SimpleNamespace(
        seq_order=1,
        event_date=date(2026, 5, 8),
        date_precision="day",
        event_type="impact",
        event_description="Systems were encrypted.",
        actor_attribution="SomeGroup",
        source_enrichment_id=uuid4(),
    )
    snapshot = SimpleNamespace(state_payload={"timeline_count": 1})

    canonical_repo = Mock()
    canonical_repo.get_by_id.return_value = canonical
    canonical_repo.get_enrichment.return_value = enrichment
    canonical_repo.list_membership_details.return_value = [
        {
            "membership": membership,
            "source_incident_id": str(membership.source_incident_id),
            "source_name": "googlenews_rss",
            "source_group": "rss",
            "collected_at": "2026-05-09T09:00:00+00:00",
            "source_published_at": "2026-05-09T08:30:00+00:00",
            "raw_title": "Penn State ransomware update",
            "raw_subtitle": None,
            "raw_victim_name": "Penn State University",
            "raw_institution_name": "Penn State University",
            "raw_institution_type": "university",
            "raw_country": "United States",
            "raw_region": "Pennsylvania",
            "raw_city": "State College",
            "source_urls": [
                {
                    "url": "https://news.google.com/rss/articles/abc",
                    "resolved_url": "https://example.com/article",
                    "url_kind": "rss_wrapper",
                    "is_wrapper": True,
                    "is_primary_from_source": True,
                    "is_resolved_primary": True,
                }
            ],
        }
    ]
    canonical_repo.list_timeline_events.return_value = [timeline_event]
    canonical_repo.get_selected_source_details.return_value = {
        "source_incident_id": "00000000-0000-0000-0000-000000000111",
        "source_name": "googlenews_rss",
        "article_title": "Penn State ransomware update",
        "article_publish_date": "2026-05-09",
        "article_url": "https://example.com/article",
        "article_resolved_url": "https://example.com/article",
    }

    analytics_repo = Mock()
    analytics_repo.get_by_key.return_value = snapshot
    article_repo = Mock()
    article_repo.list_fetch_attempts.return_value = [
        SimpleNamespace(
            fetch_tier="oxylabs",
            attempted_at=datetime(2026, 5, 9, 9, 15, tzinfo=timezone.utc),
            worker_id="v2-runtime:fetch:0",
            success=True,
            http_status=200,
            latency_ms=512,
            content_length=12000,
            error_code=None,
            error_message=None,
            response_metadata={"domain": "example.com"},
        )
    ]

    service = V2CanonicalReadService(
        canonical_repository=canonical_repo,
        analytics_refresh_repository=analytics_repo,
        article_repository=article_repo,
    )

    detail = service.get_incident_detail(Mock(), canonical_id)

    assert detail is not None
    assert detail["display_name"] == "Penn State University"
    assert detail["field_provenance"]["institution_name"] == str(selected_source_enrichment_id)
    assert detail["memberships"][0]["match_type"] == "url_exact"
    assert detail["memberships"][0]["source_name"] == "googlenews_rss"
    assert detail["memberships"][0]["raw_institution_name"] == "Penn State University"
    assert detail["memberships"][0]["source_urls"][0]["resolved_url"] == "https://example.com/article"
    assert detail["source_disclosure"]["selected_source_reason"]["source_name"] == "googlenews_rss"
    assert detail["source_disclosure"]["field_differences"][0]["field"] in {"institution_name", "country", "incident_date"}
    assert any(
        item.get("resolved_source_name") == "googlenews_rss"
        for item in detail["source_disclosure"]["field_differences"]
    )
    assert detail["timeline"][0]["event_description"] == "Systems were encrypted."
    assert detail["snapshot"]["timeline_count"] == 1
    assert detail["selected_source"]["source_name"] == "googlenews_rss"
    assert detail["selected_source"]["article_url"] == "https://example.com/article"
    assert detail["fetch_attempts"][0]["fetch_tier"] == "oxylabs"
    assert detail["fetch_attempts"][0]["http_status"] == 200
    assert detail["diamond_model"]["victim"]["name"] == "Penn State University"
    assert detail["diamond_model"]["adversary"]["name"] == "SomeGroup"
    assert detail["diamond_model"]["capability"]["attack_vector"] == "phishing_email"
    assert detail["diamond_model"]["event_meta"]["source_article_url"] == "https://example.com/article"


def test_read_service_can_build_legacy_incident_detail_for_report_and_compat():
    canonical_id = str(uuid4())
    selected_source_enrichment_id = uuid4()
    source_incident_id = uuid4()
    canonical = SimpleNamespace(
        id=uuid4(),
        institution_name="Penn State University",
        vendor_name=None,
        institution_type="university",
        country="United States",
        country_code="US",
        region="Pennsylvania",
        city="State College",
        incident_date=date(2026, 5, 8),
        date_precision="day",
        attack_category="ransomware_encryption",
        attack_vector="phishing_email",
        threat_actor_name="SomeGroup",
        ransomware_family="LockBit",
        is_education_related=True,
        severity="high",
        canonical_summary="Penn State suffered a ransomware incident.",
        status="open",
        first_seen_at=datetime(2026, 5, 8, 8, 0, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 5, 9, 8, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 5, 9, 9, 0, tzinfo=timezone.utc),
        resolution_metadata={},
    )
    membership = SimpleNamespace(
        source_incident_id=source_incident_id,
        match_type="url_exact",
        match_score=100.0,
        survivor_score=55.0,
        is_primary_member=True,
        field_contribution={},
        matcher_version="v2",
        matched_at=datetime(2026, 5, 9, 9, 30, tzinfo=timezone.utc),
    )
    enrichment = SimpleNamespace(
        selected_source_enrichment_id=selected_source_enrichment_id,
        analytics_projection={},
        field_provenance={
            "field_sources": {"records_affected_exact": str(selected_source_enrichment_id)},
            "source_disclosure": {
                "selection_basis": "highest_survivor_score",
                "selected_source_enrichment_id": str(selected_source_enrichment_id),
                "tracked_field_labels": {
                    "records_affected_exact": "Records Affected",
                    "attack_category": "Attack Category",
                },
                "sources": [
                    {
                        "source_enrichment_id": str(selected_source_enrichment_id),
                        "source_incident_id": str(source_incident_id),
                        "source_name": "googlenews_rss",
                        "source_group": "rss",
                        "raw_title": "Penn State ransomware update",
                        "source_published_at": "2026-05-09T08:30:00+00:00",
                        "is_primary_member": True,
                        "survivor_score": 55.0,
                        "score_breakdown": {"source_rank": 20, "structured_field_coverage": 8},
                        "field_count": 2,
                        "disclosed_fields": ["records_affected_exact", "attack_category"],
                        "field_values": {
                            "records_affected_exact": 5000,
                            "attack_category": "ransomware_encryption",
                        },
                    }
                ],
            },
        },
        canonical_projection={
            "attack_dynamics": {"attack_vector": "phishing_email"},
            "data_breached": True,
            "records_affected_exact": 5000,
            "mitre_attack_techniques": [{"technique_id": "T1486"}],
        },
    )
    canonical_repo = Mock()
    canonical_repo.get_by_id.return_value = canonical
    canonical_repo.get_enrichment.return_value = enrichment
    canonical_repo.list_membership_details.return_value = [
        {
            "membership": membership,
            "source_incident_id": str(membership.source_incident_id),
            "source_name": "googlenews_rss",
            "source_group": "rss",
            "collected_at": "2026-05-09T09:00:00+00:00",
            "source_published_at": "2026-05-09T08:30:00+00:00",
            "raw_title": "Penn State ransomware update",
            "raw_subtitle": None,
            "raw_victim_name": "Penn State University",
            "raw_institution_name": "Penn State University",
            "raw_institution_type": "university",
            "raw_country": "United States",
            "raw_region": "Pennsylvania",
            "raw_city": "State College",
            "source_urls": [
                {
                    "url": "https://news.google.com/rss/articles/abc",
                    "resolved_url": "https://example.com/article",
                    "url_kind": "rss_wrapper",
                    "is_wrapper": True,
                    "is_primary_from_source": True,
                    "is_resolved_primary": True,
                },
                {
                    "url": "https://example.com/supporting-report",
                    "resolved_url": None,
                    "url_kind": "article",
                    "is_wrapper": False,
                    "is_primary_from_source": False,
                    "is_resolved_primary": False,
                },
            ],
        }
    ]
    canonical_repo.list_timeline_events.return_value = [
        SimpleNamespace(
            seq_order=1,
            event_date=date(2026, 5, 8),
            date_precision="day",
            event_type="impact",
            event_description="Systems were encrypted.",
            actor_attribution="SomeGroup",
            source_enrichment_id=uuid4(),
        )
    ]
    canonical_repo.get_selected_source_details.return_value = {
        "source_incident_id": "00000000-0000-0000-0000-000000000111",
        "source_name": "googlenews_rss",
        "raw_subtitle": None,
        "article_title": "Penn State ransomware update",
        "article_publish_date": "2026-05-09",
        "article_url": "https://example.com/article",
        "article_resolved_url": "https://example.com/article",
    }
    analytics_repo = Mock()
    analytics_repo.get_by_key.return_value = None
    article_repo = Mock()
    article_repo.list_fetch_attempts.return_value = []

    service = V2CanonicalReadService(
        canonical_repository=canonical_repo,
        analytics_refresh_repository=analytics_repo,
        article_repository=article_repo,
    )

    detail = service.get_legacy_incident_detail(Mock(), canonical_id)

    assert detail is not None
    assert detail["incident_id"] == str(canonical.id)
    assert detail["title"] == "Penn State ransomware update"
    assert detail["primary_url"] == "https://example.com/article"
    assert detail["all_urls"] == [
        "https://example.com/article",
        "https://news.google.com/rss/articles/abc",
        "https://example.com/supporting-report",
    ]
    assert detail["timeline"][0]["date"] == "2026-05-08"
    assert detail["sources"][0]["source"] == "googlenews_rss"
    assert detail["sources"][0]["source_urls"][1]["url"] == "https://example.com/supporting-report"
    assert detail["source_disclosure"]["selected_source_reason"]["selection_basis"] == "highest_survivor_score"
    assert any(
        item.get("resolved_source_name") == "googlenews_rss"
        for item in detail["source_disclosure"]["field_differences"]
    )
    assert detail["data_breached"] is True
    assert detail["records_affected_exact"] == 5000


def test_read_service_legacy_detail_uses_nested_projection_sections():
    canonical_id = str(uuid4())
    canonical = SimpleNamespace(
        id=uuid4(),
        institution_name="Paris 1 Pantheon-Sorbonne University",
        vendor_name=None,
        institution_type="university",
        country="France",
        country_code="FR",
        region="Ile-de-France",
        city="Paris",
        incident_date=date(2024, 10, 10),
        date_precision="day",
        attack_category="data_breach_external",
        attack_vector="unknown",
        threat_actor_name=None,
        ransomware_family=None,
        is_education_related=True,
        severity=None,
        canonical_summary="Student and staff data was exposed.",
        status="open",
        first_seen_at=datetime(2026, 5, 12, 18, 5, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 5, 12, 18, 13, tzinfo=timezone.utc),
        updated_at=datetime(2026, 5, 12, 18, 13, tzinfo=timezone.utc),
        resolution_metadata={},
    )
    enrichment = SimpleNamespace(
        selected_source_enrichment_id=uuid4(),
        analytics_projection={},
        field_provenance={},
        canonical_projection={
            "attack_dynamics": {"attack_vector": "unknown", "attack_chain": ["initial_access", "exfiltration"]},
            "data_impact": {
                "student_data": True,
                "faculty_data": True,
                "data_exfiltrated": True,
                "data_types_affected": ["student_pii", "employee_pii"],
                "records_affected_exact": 73000,
            },
            "financial_impact": {
                "ransom_amount_exact": 2080000,
                "insurance_claim": True,
                "insurance_claim_amount": 900000,
                "legal_costs": 120000,
                "notification_costs": 45000,
                "total_cost_estimate": 2500000,
            },
            "system_impact": {
                "systems_affected": ["other"],
                "critical_systems_affected": True,
            },
            "recovery_metrics": {
                "incident_response_firm": "IR Partners",
                "mfa_implemented": False,
                "security_improvements": ["notification_regulator"],
            },
        },
    )
    membership = SimpleNamespace(
        source_incident_id=uuid4(),
        match_type="seed",
        match_score=100.0,
        survivor_score=117.0,
        is_primary_member=True,
        field_contribution={},
        matcher_version="v2",
        matched_at=datetime(2026, 5, 12, 18, 13, tzinfo=timezone.utc),
    )
    canonical_repo = Mock()
    canonical_repo.get_by_id.return_value = canonical
    canonical_repo.get_enrichment.return_value = enrichment
    canonical_repo.list_membership_details.return_value = [
        {
            "membership": membership,
            "source_incident_id": str(membership.source_incident_id),
            "source_name": "konbriefing",
            "source_group": "curated",
            "collected_at": "2026-05-12T18:05:56+00:00",
            "source_published_at": None,
            "raw_title": "Unauthorized access at a university in France",
            "raw_subtitle": None,
            "raw_victim_name": "Universite Paris 1 Pantheon",
            "raw_institution_name": "Universite Paris 1 Pantheon",
            "raw_institution_type": "university",
            "raw_country": "France",
            "raw_region": None,
            "raw_city": None,
        }
    ]
    canonical_repo.list_timeline_events.return_value = []
    canonical_repo.get_selected_source_details.return_value = {
        "source_incident_id": "c6b9f20f-c698-435c-b57d-a9495bc98c68",
        "source_name": "konbriefing",
        "raw_subtitle": None,
        "article_title": "Cyberattack exposes personal data",
        "article_publish_date": "2024-10-15",
        "article_url": "https://example.com/article",
        "article_resolved_url": "https://example.com/article",
    }
    analytics_repo = Mock()
    analytics_repo.get_by_key.return_value = None
    article_repo = Mock()
    article_repo.list_fetch_attempts.return_value = []

    service = V2CanonicalReadService(
        canonical_repository=canonical_repo,
        analytics_refresh_repository=analytics_repo,
        article_repository=article_repo,
    )

    detail = service.get_legacy_incident_detail(Mock(), canonical_id)

    assert detail is not None
    assert detail["data_impact"]["data_breached"] is True
    assert detail["data_impact"]["data_exfiltrated"] is True
    assert detail["data_impact"]["data_categories"] == ["student_pii", "employee_pii"]
    assert detail["data_impact"]["records_affected_exact"] == 73000
    assert detail["user_impact"]["total_individuals_affected"] == 73000
    assert detail["system_impact"]["critical_systems_affected"] is True
    assert detail["systems_affected"] == ["other"]
    assert detail["financial_impact"]["estimated_total_cost_usd"] == 2500000
    assert detail["financial_impact"]["ransom_cost_usd"] == 2080000
    assert detail["financial_impact"]["insurance_claim"] is True
    assert detail["financial_impact"]["insurance_payout_usd"] == 900000
    assert detail["financial_impact"]["legal_cost_usd"] == 120000
    assert detail["financial_impact"]["notification_cost_usd"] == 45000
    assert detail["recovery_metrics"]["ir_firm_engaged"] == "IR Partners"
    assert detail["recovery_metrics"]["mfa_implemented"] is False


def test_read_service_dashboard_summary_prefers_cached_snapshot():
    dashboard_snapshot = {
        "totals": {"canonical_incident_count": 5},
        "stats": {"total_incidents": 5},
        "intelligence_summary": {"overview": {"total_incidents": 5}},
        "incidents_by_country": [],
        "incidents_by_attack_type": [],
        "incidents_by_ransomware": [],
        "incidents_over_time": [],
        "recent_incidents": [],
    }
    canonical_repo = Mock()
    analytics_repo = Mock()
    analytics_repo.get_by_key.return_value = SimpleNamespace(state_payload=dashboard_snapshot)

    service = V2CanonicalReadService(
        canonical_repository=canonical_repo,
        analytics_refresh_repository=analytics_repo,
    )

    summary = service.get_dashboard_summary(Mock())

    assert summary == dashboard_snapshot
    canonical_repo.get_dashboard_rollup.assert_not_called()


def test_read_service_can_return_dashboard_stats_only():
    canonical_repo = Mock()
    analytics_repo = Mock()
    analytics_repo.get_by_key.return_value = SimpleNamespace(
        state_payload={
            "totals": {"canonical_incident_count": 5},
            "stats": {"total_incidents": 5, "education_incidents": 4},
            "intelligence_summary": {"overview": {"total_incidents": 5}},
            "incidents_by_country": [],
            "incidents_by_attack_type": [],
            "incidents_by_ransomware": [],
            "incidents_over_time": [],
            "recent_incidents": [],
        }
    )

    service = V2CanonicalReadService(
        canonical_repository=canonical_repo,
        analytics_refresh_repository=analytics_repo,
    )

    stats = service.get_dashboard_stats(Mock())

    assert stats["total_incidents"] == 5


def test_read_service_dashboard_summary_rebuilds_when_cached_snapshot_is_legacy_shape():
    canonical_repo = Mock()
    canonical_repo.get_dashboard_rollup.return_value = {
        "canonical_incident_count": 5,
        "enriched_canonical_count": 4,
        "education_related_count": 5,
        "incidents_with_ransomware": 3,
        "incidents_with_data_breach": 2,
        "countries_affected": 2,
        "unique_threat_actors": 2,
        "unique_ransomware_families": 2,
    }
    canonical_repo.get_country_breakdown.return_value = [
        {"country_code": "US", "country": "United States", "incident_count": 3}
    ]
    canonical_repo.get_attack_breakdown.return_value = [
        {"attack_category": "ransomware_encryption", "incident_count": 2}
    ]
    canonical_repo.get_ransomware_breakdown.return_value = [
        {"ransomware_family": "LockBit", "incident_count": 2}
    ]
    canonical_repo.get_incident_trend.return_value = [
        {"bucket_start": "2026-05-01", "incident_count": 4}
    ]
    canonical_repo.get_threat_actor_breakdown.return_value = {
        "threat_actors": [{"name": "SomeGroup", "incident_count": 2, "countries_targeted": ["United States"], "ransomware_families": ["LockBit"]}],
        "total": 1,
        "returned": 1,
        "total_incidents": 2,
        "countries_targeted_total": 1,
    }
    canonical_repo.list_recent_with_enrichment.return_value = [(
        SimpleNamespace(
            id=uuid4(),
            institution_name="Penn State University",
            vendor_name=None,
            institution_type="university",
            country="United States",
            country_code="US",
            region="Pennsylvania",
            city="State College",
            incident_date=date(2026, 5, 8),
            date_precision="day",
            attack_category="ransomware_encryption",
            attack_vector="phishing_email",
            threat_actor_name="SomeGroup",
            ransomware_family="LockBit",
            is_education_related=True,
            severity="high",
            canonical_summary="Penn State suffered a ransomware incident.",
            status="open",
            first_seen_at=datetime(2026, 5, 8, 8, 0, tzinfo=timezone.utc),
            last_seen_at=datetime(2026, 5, 9, 8, 0, tzinfo=timezone.utc),
            updated_at=datetime(2026, 5, 9, 9, 0, tzinfo=timezone.utc),
        ),
        SimpleNamespace(
            selected_source_enrichment_id=uuid4(),
            analytics_projection={"attack_category": "ransomware_encryption"},
        ),
        2,
    )]
    canonical_repo.count_recent.return_value = 1

    analytics_repo = Mock()
    analytics_repo.get_by_key.return_value = SimpleNamespace(state_payload={"totals": {"canonical_incident_count": 5}})

    service = V2CanonicalReadService(
        canonical_repository=canonical_repo,
        analytics_refresh_repository=analytics_repo,
    )

    summary = service.get_dashboard_summary(Mock())

    assert summary["stats"]["total_incidents"] == 5
    assert summary["incidents_by_ransomware"][0]["category"] == "LockBit"
    assert summary["recent_incidents"][0]["institution_name"] == "Penn State University"
    canonical_repo.get_dashboard_rollup.assert_called_once()


def test_read_service_incident_facets_use_repository_breakdowns():
    canonical_repo = Mock()
    canonical_repo.get_country_facets.return_value = [{"country_code": "US", "incident_count": 5}]
    canonical_repo.get_attack_category_facets.return_value = [{"attack_category": "ransomware_encryption", "incident_count": 4}]
    canonical_repo.get_institution_type_facets.return_value = [{"institution_type": "university", "incident_count": 3}]
    canonical_repo.get_severity_facets.return_value = [{"severity": "high", "incident_count": 2}]

    service = V2CanonicalReadService(canonical_repository=canonical_repo)

    facets = service.get_incident_facets(
        Mock(),
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
        facet_limit=15,
    )

    assert facets["countries"][0]["country_code"] == "US"
    canonical_repo.get_country_facets.assert_called_once()
    canonical_repo.get_attack_category_facets.assert_called_once()
    canonical_repo.get_institution_type_facets.assert_called_once()
    canonical_repo.get_severity_facets.assert_called_once()


def test_read_service_incident_trend_delegates_to_repository():
    canonical_repo = Mock()
    canonical_repo.get_incident_trend.return_value = [
        {"bucket_start": "2026-05-01", "incident_count": 4}
    ]

    service = V2CanonicalReadService(canonical_repository=canonical_repo)

    result = service.get_incident_trend(
        Mock(),
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

    assert result[0]["incident_count"] == 4
    canonical_repo.get_incident_trend.assert_called_once()


def test_read_service_build_dashboard_payload_shapes_full_dashboard_response():
    canonical_repo = Mock()
    canonical_repo.get_dashboard_rollup.return_value = {
        "canonical_incident_count": 8,
        "enriched_canonical_count": 6,
        "education_related_count": 7,
        "incidents_with_ransomware": 4,
        "incidents_with_data_breach": 3,
        "countries_affected": 5,
        "unique_threat_actors": 2,
        "unique_ransomware_families": 3,
    }
    canonical_repo.get_country_breakdown.return_value = [
        {"country_code": "US", "country": "United States", "incident_count": 4}
    ]
    canonical_repo.get_attack_breakdown.return_value = [
        {"attack_category": "ransomware_encryption", "incident_count": 3}
    ]
    canonical_repo.get_ransomware_breakdown.return_value = [
        {"ransomware_family": "LockBit", "incident_count": 2}
    ]
    canonical_repo.get_incident_trend.return_value = [
        {"bucket_start": "2026-05-01", "incident_count": 4}
    ]
    canonical_repo.list_recent_with_enrichment.return_value = [(
        SimpleNamespace(
            id=uuid4(),
            institution_name="Stanford University",
            vendor_name=None,
            institution_type="university",
            country="United States",
            country_code="US",
            region="California",
            city="Stanford",
            incident_date=date(2026, 5, 8),
            date_precision="day",
            attack_category="ransomware_encryption",
            attack_vector="phishing_email",
            threat_actor_name="SomeGroup",
            ransomware_family="LockBit",
            is_education_related=True,
            severity="high",
            canonical_summary="Stanford suffered a ransomware incident.",
            status="open",
            first_seen_at=datetime(2026, 5, 8, 8, 0, tzinfo=timezone.utc),
            last_seen_at=datetime(2026, 5, 9, 8, 0, tzinfo=timezone.utc),
            updated_at=datetime(2026, 5, 9, 9, 0, tzinfo=timezone.utc),
        ),
        SimpleNamespace(
            selected_source_enrichment_id=uuid4(),
            analytics_projection={"attack_category": "ransomware_encryption"},
        ),
        3,
    )]
    canonical_repo.count_recent.return_value = 1
    canonical_repo.get_threat_actor_breakdown.return_value = {
        "threat_actors": [{"name": "SomeGroup", "incident_count": 3, "countries_targeted": ["United States"], "ransomware_families": ["LockBit"]}],
        "total": 1,
        "returned": 1,
        "total_incidents": 3,
        "countries_targeted_total": 1,
    }

    service = V2CanonicalReadService(canonical_repository=canonical_repo)

    payload = service.build_dashboard_payload(Mock(), refreshed_at="2026-05-10T10:00:00+00:00")

    assert payload["totals"]["canonical_incident_count"] == 8
    assert payload["stats"]["enriched_incidents"] == 6
    assert payload["stats"]["unenriched_incidents"] == 2
    assert payload["incidents_by_country"][0]["category"] == "United States"
    assert payload["incidents_by_attack_type"][0]["category"] == "ransomware_encryption"
    assert payload["incidents_by_ransomware"][0]["category"] == "LockBit"
    assert payload["incidents_over_time"][0]["date"] == "2026-05-01"
    assert payload["recent_incidents"][0]["incident_id"]
    assert payload["top_ransomware_families"][0]["ransomware_family"] == "LockBit"
    assert payload["intelligence_summary"]["overview"]["total_incidents"] == 8
    assert payload["intelligence_summary"]["tradecraft"]["attack_clusters"][0]["cluster"] == "Ransomware & Extortion"
    assert payload["diamond_summary"]["coverage"]["victim_vertex_count"] == 1
    assert payload["diamond_summary"]["vertices"]["top_adversaries"][0]["name"] == "SomeGroup"


def test_read_service_intelligence_summary_uses_dashboard_snapshot_for_open_statuses():
    canonical_repo = Mock()
    analytics_repo = Mock()
    analytics_repo.get_by_key.return_value = SimpleNamespace(
        state_payload={
            "totals": {"canonical_incident_count": 5},
            "stats": {"education_incidents": 5},
            "intelligence_summary": {
                "overview": {"total_incidents": 5},
                "victimology": {"institution_segments": []},
            },
            "incidents_by_country": [],
            "incidents_by_attack_type": [],
            "incidents_by_ransomware": [],
            "incidents_over_time": [],
            "recent_incidents": [],
        }
    )

    service = V2CanonicalReadService(
        canonical_repository=canonical_repo,
        analytics_refresh_repository=analytics_repo,
    )

    summary = service.get_intelligence_summary(Mock(), statuses=("open",))

    assert summary["overview"]["total_incidents"] == 5
    canonical_repo.list_recent_with_enrichment.assert_not_called()


def test_read_service_compat_analytics_shapes_delegate_to_repository():
    canonical_repo = Mock()
    canonical_repo.get_country_breakdown.return_value = [
        {"country_code": "US", "country": "United States", "incident_count": 4}
    ]
    canonical_repo.get_attack_breakdown.return_value = [
        {"attack_category": "ransomware_encryption", "incident_count": 3}
    ]
    canonical_repo.get_ransomware_breakdown.return_value = [
        {"ransomware_family": "LockBit", "incident_count": 2}
    ]
    canonical_repo.get_incident_trend.return_value = [
        {"bucket_start": "2026-05-01", "incident_count": 5}
    ]

    service = V2CanonicalReadService(canonical_repository=canonical_repo)

    countries = service.get_country_analytics(Mock(), statuses=("open",), limit=25)
    attack_types = service.get_attack_type_analytics(Mock(), statuses=("open",), limit=12)
    ransomware = service.get_ransomware_analytics(Mock(), statuses=("open",), limit=8)
    timeline = service.get_timeline_analytics(Mock(), statuses=("open",), months=18)

    assert countries["data"][0]["category"] == "United States"
    assert countries["total"] == 4
    assert attack_types["data"][0]["category"] == "ransomware_encryption"
    assert ransomware["data"][0]["category"] == "LockBit"
    assert timeline["data"][0]["date"] == "2026-05-01"


def test_read_service_threat_actor_analytics_and_filter_options_delegate_to_repository():
    canonical_repo = Mock()
    canonical_repo.get_threat_actor_breakdown.return_value = {
        "threat_actors": [{"name": "SomeGroup"}],
        "total": 1,
        "returned": 1,
        "total_incidents": 3,
        "countries_targeted_total": 1,
    }
    canonical_repo.get_filter_options.return_value = {
        "countries": ["United States"],
        "attack_categories": ["ransomware_encryption"],
        "ransomware_families": ["LockBit"],
        "threat_actors": ["SomeGroup"],
        "institution_types": ["university"],
        "years": [2026],
    }

    service = V2CanonicalReadService(canonical_repository=canonical_repo)
    threat_actor_session = Mock()
    filters_session = Mock()
    threat_actors = service.get_threat_actor_analytics(
        threat_actor_session,
        statuses=("open", "excluded"),
        limit=30,
    )
    filters = service.get_filter_options(filters_session, statuses=("open",))

    assert threat_actors["threat_actors"][0]["name"] == "SomeGroup"
    canonical_repo.get_threat_actor_breakdown.assert_called_once_with(
        threat_actor_session,
        statuses=("open", "excluded"),
        limit=30,
    )
    canonical_repo.get_filter_options.assert_called_once_with(
        filters_session,
        statuses=("open",),
    )
    assert filters["ransomware_families"] == ["LockBit"]


def test_read_service_intelligence_summary_aggregates_analyst_metrics():
    canonical_repo = Mock()
    canonical_repo.count_recent.return_value = 2
    canonical_repo.list_recent_with_enrichment.return_value = [
        (
            SimpleNamespace(
                id=uuid4(),
                institution_name="Example University",
                vendor_name=None,
                institution_type="university",
                country="United States",
                country_code="US",
                incident_date=date(2026, 5, 8),
                last_seen_at=datetime(2026, 5, 9, 8, 0, tzinfo=timezone.utc),
                attack_category="ransomware_double_extortion",
                attack_vector=None,
                threat_actor_name="Example Crew",
                ransomware_family="LockBit",
            ),
            SimpleNamespace(
                canonical_projection={
                    "attack_dynamics": {"attack_vector": "phishing_email"},
                    "data_impact": {"records_affected_exact": 1200},
                },
            ),
            2,
        ),
        (
            SimpleNamespace(
                id=uuid4(),
                institution_name=None,
                vendor_name="PowerSchool",
                institution_type="education_technology_provider",
                country="Canada",
                country_code="CA",
                incident_date=date(2026, 2, 1),
                last_seen_at=datetime(2026, 2, 2, 8, 0, tzinfo=timezone.utc),
                attack_category="third_party_compromise",
                attack_vector=None,
                threat_actor_name=None,
                ransomware_family=None,
            ),
            SimpleNamespace(
                canonical_projection={
                    "attack_dynamics": {"attack_vector": "third_party_vendor"},
                    "data_impact": {},
                },
            ),
            1,
        ),
    ]
    canonical_repo.get_country_breakdown.return_value = [
        {"country_code": "US", "country": "United States", "incident_count": 1},
        {"country_code": "CA", "country": "Canada", "incident_count": 1},
    ]
    canonical_repo.get_ransomware_breakdown.return_value = [
        {"ransomware_family": "LockBit", "incident_count": 1}
    ]
    canonical_repo.get_threat_actor_breakdown.return_value = {
        "threat_actors": [
            {
                "name": "Example Crew",
                "incident_count": 1,
                "countries_targeted": ["United States"],
                "ransomware_families": ["LockBit"],
                "first_seen": "2026-05-08T00:00:00+00:00",
                "last_seen": "2026-05-09T00:00:00+00:00",
            }
        ],
        "total": 1,
        "returned": 1,
        "total_incidents": 1,
        "countries_targeted_total": 1,
    }

    service = V2CanonicalReadService(canonical_repository=canonical_repo)

    summary = service.get_intelligence_summary(Mock(), statuses=("open",))

    assert summary["overview"]["total_incidents"] == 2
    assert summary["overview"]["ransomware_count"] == 1
    assert summary["overview"]["vendor_linked_count"] == 1
    assert summary["exposure"]["known_record_volume"] == 1200
    assert summary["tradecraft"]["attack_vectors"][0]["vector"] == "Phishing Email"
    assert summary["victimology"]["institution_segments"][0]["segment"] in {
        "Higher Education",
        "Education Vendor / Provider",
    }


def test_read_service_diamond_summary_aggregates_vertex_coverage():
    canonical_repo = Mock()
    canonical_repo.count_recent.return_value = 2
    canonical_repo.list_recent_with_enrichment.return_value = [
        (
            SimpleNamespace(
                id=uuid4(),
                institution_name="Example University",
                vendor_name=None,
                institution_type="university",
                country="United States",
                country_code="US",
                region=None,
                city=None,
                incident_date=date(2026, 5, 8),
                date_precision="day",
                attack_category="ransomware_double_extortion",
                attack_vector=None,
                threat_actor_name="Example Crew",
                ransomware_family="LockBit",
                is_education_related=True,
                severity=None,
                canonical_summary=None,
                status="open",
                first_seen_at=datetime(2026, 5, 8, 8, 0, tzinfo=timezone.utc),
                last_seen_at=datetime(2026, 5, 8, 9, 0, tzinfo=timezone.utc),
                updated_at=datetime(2026, 5, 8, 9, 0, tzinfo=timezone.utc),
            ),
            SimpleNamespace(
                canonical_projection={
                    "attack_dynamics": {"attack_vector": "phishing_email", "attack_chain": ["initial_access", "impact"]},
                    "threat_actor_category": "ransomware_gang",
                    "threat_actor_claim_url": "https://exampleonion.site/post",
                    "leak_site_url": "https://exampleonion.site/",
                    "dark_web_posting_confirmed": True,
                    "vulnerabilities_exploited": [{"cve": "CVE-2026-0001"}],
                },
            ),
            1,
        ),
        (
            SimpleNamespace(
                id=uuid4(),
                institution_name=None,
                vendor_name="PowerSchool",
                institution_type="education_technology_provider",
                country="Canada",
                country_code="CA",
                region=None,
                city=None,
                incident_date=date(2026, 4, 2),
                date_precision="day",
                attack_category="third_party_compromise",
                attack_vector=None,
                threat_actor_name=None,
                ransomware_family=None,
                is_education_related=True,
                severity=None,
                canonical_summary=None,
                status="open",
                first_seen_at=datetime(2026, 4, 2, 8, 0, tzinfo=timezone.utc),
                last_seen_at=datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc),
                updated_at=datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc),
            ),
            SimpleNamespace(
                canonical_projection={
                    "attack_dynamics": {"attack_vector": "third_party_vendor"},
                    "third_party_vendor_impact": True,
                },
            ),
            1,
        ),
    ]

    service = V2CanonicalReadService(canonical_repository=canonical_repo)

    summary = service.get_diamond_analytics(Mock(), statuses=("open",))

    assert summary["overview"]["total_incidents"] == 2
    assert summary["coverage"]["victim_vertex_count"] == 2
    assert summary["coverage"]["adversary_vertex_count"] == 1
    assert summary["coverage"]["capability_vertex_count"] == 2
    assert summary["coverage"]["infrastructure_vertex_count"] == 2
    assert summary["vertices"]["top_adversaries"][0]["name"] == "Example Crew"
    assert summary["vertices"]["infrastructure_components"][0]["component"] in {
        "actor_claim_site",
        "leak_site",
        "third_party_platform",
        "exploited_public_vulnerability",
        "dark_web_posting",
    }
