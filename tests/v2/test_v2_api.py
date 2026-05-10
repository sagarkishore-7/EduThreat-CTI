from datetime import date

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.edu_cti.api.v2 import (
    get_v2_read_service,
    get_v2_research_metrics_service,
    get_v2_session,
    router,
)


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


def test_v2_stats_endpoint_returns_dashboard_stats_payload():
    class _ReadService:
        def get_dashboard_stats(self, _session):
            return {"total_incidents": 7, "education_incidents": 6}

    client = _build_client(_ReadService())

    response = client.get("/api/v2/stats")

    assert response.status_code == 200
    assert response.json()["total_incidents"] == 7


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


def test_v2_incidents_endpoint_supports_legacy_format():
    class _ReadService:
        def __init__(self):
            self.called = None

        def list_legacy_incidents(self, _session, **kwargs):
            self.called = kwargs
            return {
                "incidents": [{"incident_id": "abc", "institution_name": "Stanford University"}],
                "pagination": {"page": 1, "per_page": 20, "total": 1, "total_pages": 1, "has_next": False, "has_prev": False},
            }

    service = _ReadService()
    client = _build_client(service)

    response = client.get("/api/v2/incidents", params={"format": "legacy", "limit": 20, "offset": 0})

    assert response.status_code == 200
    payload = response.json()
    assert payload["incidents"][0]["incident_id"] == "abc"
    assert payload["pagination"]["total"] == 1
    assert service.called["limit"] == 20


def test_v2_incident_detail_endpoint_returns_404_for_missing_canonical():
    class _ReadService:
        def get_incident_detail(self, _session, _canonical_incident_id):
            return None

    client = _build_client(_ReadService())

    response = client.get("/api/v2/incidents/missing")

    assert response.status_code == 404
    assert response.json()["detail"] == "Canonical incident not found"


def test_v2_incident_detail_endpoint_supports_legacy_format():
    class _ReadService:
        def get_legacy_incident_detail(self, _session, _canonical_incident_id):
            return {"incident_id": "abc", "institution_name": "Stanford University"}

    client = _build_client(_ReadService())

    response = client.get("/api/v2/incidents/abc", params={"format": "legacy"})

    assert response.status_code == 200
    assert response.json()["incident_id"] == "abc"


def test_v2_incident_report_endpoint_returns_markdown():
    class _ReadService:
        def get_legacy_incident_detail(self, _session, _canonical_incident_id):
            return {
                "incident_id": "abc",
                "institution_name": "Stanford University",
                "country": "United States",
                "country_code": "US",
                "incident_date": "2026-05-08",
                "enriched_summary": "Stanford suffered a ransomware incident.",
                "attack_category": "ransomware_encryption",
                "timeline": [],
            }

    client = _build_client(_ReadService())

    response = client.get("/api/v2/incidents/abc/report")

    assert response.status_code == 200
    assert response.headers["content-disposition"] == 'attachment; filename="cti-report-abc.md"'
    assert "# CYBER THREAT INTELLIGENCE REPORT" in response.text


def test_v2_pipeline_research_metrics_endpoints_return_payloads():
    class _ReadService:
        pass

    class _ResearchService:
        def get_latest_or_live(self, _session, **kwargs):
            assert kwargs["snapshot_key"] == "global"
            return {
                "dataset_construction": {"source_incidents_total": 12},
                "fetch_performance": {"tiers": []},
            }

        def render_prometheus_text(self, payload):
            assert payload["dataset_construction"]["source_incidents_total"] == 12
            return "eduthreat_v2_dataset_source_incidents_total 12\n"

    app = FastAPI()
    app.include_router(router)

    def _override_session():
        yield object()

    app.dependency_overrides[get_v2_session] = _override_session
    app.dependency_overrides[get_v2_read_service] = lambda: _ReadService()
    app.dependency_overrides[get_v2_research_metrics_service] = lambda: _ResearchService()
    client = TestClient(app)

    response = client.get("/api/v2/analytics/pipeline-research")
    assert response.status_code == 200
    assert response.json()["dataset_construction"]["source_incidents_total"] == 12

    response = client.get("/api/v2/analytics/pipeline-research/prometheus")
    assert response.status_code == 200
    assert "eduthreat_v2_dataset_source_incidents_total 12" in response.text


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


def test_v2_compat_analytics_endpoints_return_old_style_shapes():
    class _ReadService:
        def __init__(self):
            self.calls = {}

        def get_country_analytics(self, _session, **kwargs):
            self.calls["countries"] = kwargs
            return {"data": [{"category": "United States", "count": 4}], "total": 4}

        def get_attack_type_analytics(self, _session, **kwargs):
            self.calls["attack_types"] = kwargs
            return {"data": [{"category": "ransomware_encryption", "count": 3}], "total": 3}

        def get_ransomware_analytics(self, _session, **kwargs):
            self.calls["ransomware"] = kwargs
            return {"data": [{"category": "LockBit", "count": 2}], "total": 2}

        def get_timeline_analytics(self, _session, **kwargs):
            self.calls["timeline"] = kwargs
            return {"data": [{"date": "2026-05-01", "count": 5}], "total": 5}

    service = _ReadService()
    client = _build_client(service)

    countries = client.get("/api/v2/analytics/countries", params={"limit": 25, "status": ["open", "excluded"]})
    attack_types = client.get("/api/v2/analytics/attack-types", params={"limit": 12})
    ransomware = client.get("/api/v2/analytics/ransomware", params={"limit": 8})
    timeline = client.get("/api/v2/analytics/timeline", params={"months": 18})

    assert countries.status_code == 200
    assert countries.json()["data"][0]["category"] == "United States"
    assert countries.json()["total"] == 4
    assert service.calls["countries"] == {"statuses": ("open", "excluded"), "limit": 25}

    assert attack_types.status_code == 200
    assert attack_types.json()["data"][0]["category"] == "ransomware_encryption"
    assert service.calls["attack_types"] == {"statuses": ("open",), "limit": 12}

    assert ransomware.status_code == 200
    assert ransomware.json()["data"][0]["category"] == "LockBit"
    assert service.calls["ransomware"] == {"statuses": ("open",), "limit": 8}

    assert timeline.status_code == 200
    assert timeline.json()["data"][0]["date"] == "2026-05-01"
    assert service.calls["timeline"] == {"statuses": ("open",), "months": 18}


def test_v2_threat_actor_analytics_and_filters_endpoints_return_payloads():
    class _ReadService:
        def __init__(self):
            self.calls = {}

        def get_threat_actor_analytics(self, _session, **kwargs):
            self.calls["threat_actors"] = kwargs
            return {
                "threat_actors": [{"name": "SomeGroup", "incident_count": 3, "countries_targeted": ["United States"], "ransomware_families": ["LockBit"], "first_seen": "2026-05-01", "last_seen": "2026-05-09"}],
                "total": 1,
                "returned": 1,
                "total_incidents": 3,
                "countries_targeted_total": 1,
            }

        def get_filter_options(self, _session, **kwargs):
            self.calls["filters"] = kwargs
            return {
                "countries": ["United States"],
                "attack_categories": ["ransomware_encryption"],
                "ransomware_families": ["LockBit"],
                "threat_actors": ["SomeGroup"],
                "institution_types": ["university"],
                "years": [2026],
            }

    service = _ReadService()
    client = _build_client(service)

    threat_actors = client.get("/api/v2/analytics/threat-actors", params={"limit": 30, "status": ["open", "excluded"]})
    filters = client.get("/api/v2/filters", params={"status": ["open"]})

    assert threat_actors.status_code == 200
    assert threat_actors.json()["threat_actors"][0]["name"] == "SomeGroup"
    assert service.calls["threat_actors"] == {"statuses": ("open", "excluded"), "limit": 30}

    assert filters.status_code == 200
    assert filters.json()["countries"] == ["United States"]
    assert service.calls["filters"] == {"statuses": ("open",)}


def test_v2_intelligence_analytics_endpoint_returns_payload():
    class _ReadService:
        def get_intelligence_summary(self, _session, **kwargs):
            assert kwargs["statuses"] == ("open", "excluded")
            return {
                "overview": {"total_incidents": 12},
                "priority_findings": [{"title": "Primary intrusion pattern", "value": "Ransomware & Extortion"}],
            }

    client = _build_client(_ReadService())

    response = client.get("/api/v2/analytics/intelligence", params={"status": ["open", "excluded"]})

    assert response.status_code == 200
    payload = response.json()
    assert payload["overview"]["total_incidents"] == 12
    assert payload["priority_findings"][0]["value"] == "Ransomware & Extortion"


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
