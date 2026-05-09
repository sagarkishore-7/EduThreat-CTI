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
    read_service.build_dashboard_payload.return_value = {
        "totals": {"canonical_incident_count": 3},
        "stats": {"total_incidents": 3},
        "incidents_by_country": [],
        "incidents_by_attack_type": [],
        "incidents_by_ransomware": [],
        "incidents_over_time": [],
        "recent_incidents": [],
        "refreshed_at": "2026-05-10T10:00:00+00:00",
    }

    canonical_repo = Mock()
    canonical_repo.get_dashboard_rollup.return_value = {
        "canonical_incident_count": 3,
        "enriched_canonical_count": 2,
        "education_related_count": 2,
        "incidents_with_ransomware": 1,
        "incidents_with_data_breach": 1,
        "countries_affected": 1,
        "unique_threat_actors": 1,
        "unique_ransomware_families": 1,
    }
    canonical_repo.get_country_breakdown.return_value = [{"country_code": "US", "incident_count": 2}]
    canonical_repo.get_attack_breakdown.return_value = [{"attack_category": "ransomware_encryption", "incident_count": 2}]
    canonical_repo.get_ransomware_breakdown.return_value = [{"ransomware_family": "LockBit", "incident_count": 1}]
    canonical_repo.get_incident_trend.return_value = [{"bucket_start": "2026-05-01", "incident_count": 2}]
    canonical_repo.list_recent_with_enrichment.return_value = []
    canonical_repo.count_recent.return_value = 0

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


def test_analytics_refresh_service_can_refresh_dashboard_only():
    canonical_repo = Mock()
    canonical_repo.get_dashboard_rollup.return_value = {
        "canonical_incident_count": 5,
        "enriched_canonical_count": 4,
        "education_related_count": 5,
        "incidents_with_ransomware": 2,
        "incidents_with_data_breach": 1,
        "countries_affected": 3,
        "unique_threat_actors": 2,
        "unique_ransomware_families": 2,
    }
    canonical_repo.get_country_breakdown.return_value = [{"country_code": "US", "incident_count": 3}]
    canonical_repo.get_attack_breakdown.return_value = [{"attack_category": "ransomware_encryption", "incident_count": 2}]
    canonical_repo.get_ransomware_breakdown.return_value = [{"ransomware_family": "LockBit", "incident_count": 2}]
    canonical_repo.get_incident_trend.return_value = [{"bucket_start": "2026-05-01", "incident_count": 3}]
    canonical_repo.list_recent_with_enrichment.return_value = []
    canonical_repo.count_recent.return_value = 0
    analytics_repo = Mock()

    service = V2AnalyticsRefreshService(
        canonical_repository=canonical_repo,
        analytics_refresh_repository=analytics_repo,
    )

    result = service.refresh_dashboard_snapshot(Mock(), last_trigger_canonical_incident_id="abc")

    assert result["refreshed"] is True
    assert result["snapshot_scope"] == "global"
    assert result["snapshots_updated"] == 1
    analytics_repo.upsert_snapshot.assert_called_once()
    snapshot_payload = analytics_repo.upsert_snapshot.call_args.kwargs["state_payload"]
    assert snapshot_payload["stats"]["total_incidents"] == 5
    assert snapshot_payload["incidents_by_ransomware"][0]["category"] == "LockBit"
