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


def test_read_service_returns_detail_with_memberships_timeline_and_snapshot():
    canonical_id = str(uuid4())
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
    enrichment = SimpleNamespace(
        selected_source_enrichment_id=uuid4(),
        analytics_projection={"attack_category": "ransomware_encryption"},
        field_provenance={"institution_name": "abc"},
        canonical_projection={"institution_name": "Penn State University"},
    )
    membership = SimpleNamespace(
        source_incident_id=uuid4(),
        match_type="url_exact",
        match_score=100.0,
        survivor_score=55.0,
        is_primary_member=True,
        field_contribution={"institution_name": "abc"},
        matcher_version="v2",
        matched_at=datetime(2026, 5, 9, 9, 30, tzinfo=timezone.utc),
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
    canonical_repo.list_memberships.return_value = [membership]
    canonical_repo.list_timeline_events.return_value = [timeline_event]

    analytics_repo = Mock()
    analytics_repo.get_by_key.return_value = snapshot

    service = V2CanonicalReadService(
        canonical_repository=canonical_repo,
        analytics_refresh_repository=analytics_repo,
    )

    detail = service.get_incident_detail(Mock(), canonical_id)

    assert detail is not None
    assert detail["display_name"] == "Penn State University"
    assert detail["memberships"][0]["match_type"] == "url_exact"
    assert detail["timeline"][0]["event_description"] == "Systems were encrypted."
    assert detail["snapshot"]["timeline_count"] == 1


def test_read_service_dashboard_summary_prefers_cached_snapshot():
    dashboard_snapshot = {"totals": {"canonical_incident_count": 5}}
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

