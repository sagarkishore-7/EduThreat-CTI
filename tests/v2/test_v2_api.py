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
            return {"totals": {"canonical_incident_count": 3}}

    client = _build_client(_ReadService())

    response = client.get("/api/v2/dashboard")

    assert response.status_code == 200
    assert response.json()["totals"]["canonical_incident_count"] == 3


def test_v2_incidents_endpoint_returns_items_and_meta():
    class _ReadService:
        def __init__(self):
            self.called = None

        def list_recent_incidents(self, _session, *, limit, statuses):
            self.called = (limit, statuses)
            return [{"canonical_incident_id": "abc"}]

    service = _ReadService()
    client = _build_client(service)

    response = client.get("/api/v2/incidents", params={"limit": 10, "status": ["open", "excluded"]})

    assert response.status_code == 200
    payload = response.json()
    assert payload["items"][0]["canonical_incident_id"] == "abc"
    assert payload["meta"]["returned"] == 1
    assert service.called == (10, ("open", "excluded"))


def test_v2_incident_detail_endpoint_returns_404_for_missing_canonical():
    class _ReadService:
        def get_incident_detail(self, _session, _canonical_incident_id):
            return None

    client = _build_client(_ReadService())

    response = client.get("/api/v2/incidents/missing")

    assert response.status_code == 404
    assert response.json()["detail"] == "Canonical incident not found"

