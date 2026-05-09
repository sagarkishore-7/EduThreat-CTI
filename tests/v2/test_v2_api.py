from datetime import date

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.edu_cti.api.v2 import get_v2_read_service, get_v2_session, router


def _build_client(read_service):
    app = FastAPI()
    app.include_router(router)

    def _override_session():
        yield object()

    app.dependency_overrides[get_v2_session] = _override_session
    app.dependency_overrides[get_v2_read_service] = lambda: read_service
    return TestClient(app)


def test_v2_dashboard_endpoint_returns_read_service_payload():
    class _ReadService:
        def get_dashboard_summary(self, _session):
            return {
                "totals": {"canonical_incident_count": 3},
                "stats": {"total_incidents": 3},
                "incidents_by_country": [{"category": "United States", "count": 2}],
                "incidents_by_attack_type": [],
                "incidents_by_ransomware": [],
                "incidents_over_time": [],
                "recent_incidents": [],
            }

    client = _build_client(_ReadService())

    response = client.get("/api/v2/dashboard")

    assert response.status_code == 200
    assert response.json()["totals"]["canonical_incident_count"] == 3
    assert response.json()["stats"]["total_incidents"] == 3
    assert response.json()["incidents_by_country"][0]["category"] == "United States"


def test_v2_incidents_endpoint_returns_items_and_meta():
    class _ReadService:
        def __init__(self):
            self.called = None

        def list_incidents(self, _session, **kwargs):
            self.called = kwargs
            return {"items": [{"canonical_incident_id": "abc"}], "total": 42}

    service = _ReadService()
    client = _build_client(service)

    response = client.get(
        "/api/v2/incidents",
        params={
            "limit": 10,
            "offset": 20,
            "status": ["open", "excluded"],
            "search": "stanford",
            "country_code": "us",
            "attack_category": "ransomware_encryption",
            "institution_type": "university",
            "severity": "high",
            "is_education_related": "true",
            "has_vendor": "false",
            "date_from": "2026-05-01",
            "date_to": "2026-05-09",
            "sort_by": "incident_date",
            "sort_order": "asc",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["items"][0]["canonical_incident_id"] == "abc"
    assert payload["meta"]["returned"] == 1
    assert payload["meta"]["total"] == 42
    assert service.called == {
        "limit": 10,
        "offset": 20,
        "statuses": ("open", "excluded"),
        "search": "stanford",
        "country_code": "US",
        "attack_category": "ransomware_encryption",
        "institution_type": "university",
        "severity": "high",
        "is_education_related": True,
        "has_vendor": False,
        "date_from": date(2026, 5, 1),
        "date_to": date(2026, 5, 9),
        "sort_by": "incident_date",
        "sort_order": "asc",
    }
    assert payload["meta"]["sort_by"] == "incident_date"
    assert payload["meta"]["sort_order"] == "asc"


def test_v2_incident_detail_endpoint_returns_404_for_missing_canonical():
    class _ReadService:
        def get_incident_detail(self, _session, _canonical_incident_id):
            return None

    client = _build_client(_ReadService())

    response = client.get("/api/v2/incidents/missing")

    assert response.status_code == 404
    assert response.json()["detail"] == "Canonical incident not found"


def test_v2_incident_facets_endpoint_returns_filtered_facets():
    class _ReadService:
        def __init__(self):
            self.called = None

        def get_incident_facets(self, _session, **kwargs):
            self.called = kwargs
            return {
                "countries": [{"country_code": "US", "incident_count": 5}],
                "attack_categories": [{"attack_category": "ransomware_encryption", "incident_count": 4}],
                "institution_types": [{"institution_type": "university", "incident_count": 3}],
                "severities": [{"severity": "high", "incident_count": 2}],
            }

    service = _ReadService()
    client = _build_client(service)

    response = client.get(
        "/api/v2/incidents/facets",
        params={
            "status": ["open", "excluded"],
            "search": "stanford",
            "country_code": "us",
            "attack_category": "ransomware_encryption",
            "institution_type": "university",
            "severity": "high",
            "is_education_related": "true",
            "has_vendor": "false",
            "date_from": "2026-05-01",
            "date_to": "2026-05-09",
            "facet_limit": 15,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["countries"][0]["country_code"] == "US"
    assert service.called == {
        "statuses": ("open", "excluded"),
        "search": "stanford",
        "country_code": "US",
        "attack_category": "ransomware_encryption",
        "institution_type": "university",
        "severity": "high",
        "is_education_related": True,
        "has_vendor": False,
        "date_from": date(2026, 5, 1),
        "date_to": date(2026, 5, 9),
        "facet_limit": 15,
    }


def test_v2_analytics_breakdowns_endpoint_returns_filtered_breakdowns():
    class _ReadService:
        def __init__(self):
            self.called = None

        def get_analytics_breakdowns(self, _session, **kwargs):
            self.called = kwargs
            return {"countries": [{"country_code": "US", "incident_count": 5}]}

    service = _ReadService()
    client = _build_client(service)

    response = client.get(
        "/api/v2/analytics/breakdowns",
        params={
            "status": ["open"],
            "country_code": "us",
            "breakdown_limit": 12,
        },
    )

    assert response.status_code == 200
    assert response.json()["countries"][0]["country_code"] == "US"
    assert service.called["statuses"] == ("open",)
    assert service.called["country_code"] == "US"
    assert service.called["breakdown_limit"] == 12


def test_v2_analytics_trend_endpoint_returns_bucketed_items():
    class _ReadService:
        def __init__(self):
            self.called = None

        def get_incident_trend(self, _session, **kwargs):
            self.called = kwargs
            return [{"bucket_start": "2026-05-01", "incident_count": 4}]

    service = _ReadService()
    client = _build_client(service)

    response = client.get(
        "/api/v2/analytics/trend",
        params={
            "status": ["open", "excluded"],
            "attack_category": "ransomware_encryption",
            "bucket": "week",
            "limit": 18,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["bucket"] == "week"
    assert payload["items"][0]["incident_count"] == 4
    assert service.called["statuses"] == ("open", "excluded")
    assert service.called["attack_category"] == "ransomware_encryption"
    assert service.called["bucket"] == "week"
    assert service.called["limit"] == 18
