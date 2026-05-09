from fastapi.testclient import TestClient

from src.edu_cti.api.admin import authenticate
from src.edu_cti.api.v2 import get_v2_read_service, get_v2_session
from src.edu_cti.api.v2_admin import (
    get_v2_collection_service,
    get_v2_operations_service,
    get_v2_orchestration_service,
    get_v2_scheduler_service,
)
from src.edu_cti_v2.api_app import create_app


def test_v2_api_app_health_endpoints_exist():
    client = TestClient(create_app())

    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["service"] == "v2-api"

    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json()["layer"] == "v2"


def test_v2_api_app_mounts_public_and_admin_routes():
    app = create_app()

    def _override_session():
        yield object()

    class _ReadService:
        def get_dashboard_summary(self, _session):
            return {"totals": {"canonical_incident_count": 2}}

    class _OperationsService:
        def get_runtime_status(self, _session):
            return {"counts": {"queued_tasks": 4}}

    app.dependency_overrides[get_v2_session] = _override_session
    app.dependency_overrides[get_v2_read_service] = lambda: _ReadService()
    app.dependency_overrides[get_v2_operations_service] = lambda: _OperationsService()
    app.dependency_overrides[get_v2_collection_service] = lambda: object()
    app.dependency_overrides[get_v2_orchestration_service] = lambda: object()
    app.dependency_overrides[get_v2_scheduler_service] = lambda: object()
    app.dependency_overrides[authenticate] = lambda: True

    client = TestClient(app)

    dashboard = client.get("/api/v2/dashboard")
    assert dashboard.status_code == 200
    assert dashboard.json()["totals"]["canonical_incident_count"] == 2

    admin_status = client.get("/api/admin/v2/status")
    assert admin_status.status_code == 200
    assert admin_status.json()["counts"]["queued_tasks"] == 4

