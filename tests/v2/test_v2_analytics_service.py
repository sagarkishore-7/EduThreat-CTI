from unittest.mock import Mock
from uuid import uuid4

from src.edu_cti_v2.services import V2AnalyticsRefreshService


def test_analytics_refresh_service_updates_canonical_and_dashboard_snapshots():
    canonical_id = str(uuid4())
    detail = {
        "canonical_incident_id": canonical_id,
        "display_name": "Penn State University",
        "country_code": "US",
        "incident_date": "2026-05-08",
        "attack_category": "ransomware_encryption",
        "membership_count": 2,
        "selected_source_enrichment_id": str(uuid4()),
        "timeline": [{"event_type": "impact"}],
        "last_seen_at": "2026-05-09T09:00:00+00:00",
    }
    read_service = Mock()
    read_service.get_incident_detail.return_value = detail

    canonical_repo = Mock()
    canonical_repo.get_dashboard_rollup.return_value = {
        "canonical_incident_count": 3,
        "enriched_canonical_count": 2,
        "education_related_count": 2,
    }
    canonical_repo.get_country_breakdown.return_value = [{"country_code": "US", "incident_count": 2}]
    canonical_repo.get_attack_breakdown.return_value = [{"attack_category": "ransomware_encryption", "incident_count": 2}]

    analytics_repo = Mock()

    service = V2AnalyticsRefreshService(
        canonical_repository=canonical_repo,
        analytics_refresh_repository=analytics_repo,
        read_service=read_service,
    )

    result = service.refresh_for_canonical_incident(Mock(), canonical_id)

    assert result["refreshed"] is True
    assert result["canonical_incident_id"] == canonical_id
    assert result["snapshots_updated"] == 2
    assert analytics_repo.upsert_snapshot.call_count == 2

