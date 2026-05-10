from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.edu_cti.api.v2 import get_v2_session
from src.edu_cti.api.v2_admin import (
    get_v2_collection_service,
    get_v2_data_quality_service,
    get_v2_operations_service,
    get_v2_orchestration_service,
    get_v2_preflight_service,
    get_v2_research_metrics_service,
    get_v2_scheduler_service,
    router,
)
from src.edu_cti_v2.auth import authenticate


def _build_client(operations_service):
    app = FastAPI()
    app.include_router(router, prefix="/api")

    def _override_session():
        yield object()

    class _NullCollectionService:
        def collect_into_v2(self, **_kwargs):
            raise AssertionError("unexpected collection call")

    class _NullOrchestrationService:
        def list_plans(self):
            raise AssertionError("unexpected plans call")

        def run_plan(self, **_kwargs):
            raise AssertionError("unexpected run-plan call")

    class _NullSchedulerService:
        def get_status(self):
            raise AssertionError("unexpected scheduler status call")

        def start(self):
            raise AssertionError("unexpected scheduler start call")

        def stop(self):
            raise AssertionError("unexpected scheduler stop call")

        def trigger_job(self, *_args, **_kwargs):
            raise AssertionError("unexpected scheduler trigger call")

    class _NullPreflightService:
        def get_status(self, _session):
            raise AssertionError("unexpected preflight call")

    class _NullDataQualityService:
        def run_sweep(self, **_kwargs):
            raise AssertionError("unexpected data quality sweep call")

        def list_manual_review_queue(self, *_args, **_kwargs):
            raise AssertionError("unexpected manual review queue call")

    class _NullResearchMetricsService:
        def get_latest_or_live(self, *_args, **_kwargs):
            raise AssertionError("unexpected research metrics read call")

        def list_recent_snapshots(self, *_args, **_kwargs):
            raise AssertionError("unexpected research metrics history call")

        def capture_snapshot(self, *_args, **_kwargs):
            raise AssertionError("unexpected research metrics refresh call")

        def render_prometheus_text(self, *_args, **_kwargs):
            raise AssertionError("unexpected prometheus render call")

    app.dependency_overrides[authenticate] = lambda: True
    app.dependency_overrides[get_v2_session] = _override_session
    app.dependency_overrides[get_v2_operations_service] = lambda: operations_service
    app.dependency_overrides[get_v2_collection_service] = lambda: _NullCollectionService()
    app.dependency_overrides[get_v2_orchestration_service] = lambda: _NullOrchestrationService()
    app.dependency_overrides[get_v2_scheduler_service] = lambda: _NullSchedulerService()
    app.dependency_overrides[get_v2_preflight_service] = lambda: _NullPreflightService()
    app.dependency_overrides[get_v2_data_quality_service] = lambda: _NullDataQualityService()
    app.dependency_overrides[get_v2_research_metrics_service] = lambda: _NullResearchMetricsService()
    return TestClient(app)


def test_v2_admin_status_endpoint_returns_operations_payload():
    class _OperationsService:
        def get_runtime_status(self, _session):
            return {"counts": {"source_incidents": 3}}

    client = _build_client(_OperationsService())

    response = client.get("/api/admin/v2/status")

    assert response.status_code == 200
    assert response.json()["counts"]["source_incidents"] == 3


def test_v2_admin_tasks_endpoint_passes_filters():
    class _OperationsService:
        def __init__(self):
            self.called = None

        def list_tasks(self, _session, *, limit, task_type, statuses):
            self.called = (limit, task_type, statuses)
            return [{"task_id": "abc"}]

    service = _OperationsService()
    client = _build_client(service)

    response = client.get("/api/admin/v2/tasks", params={"limit": 10, "task_type": "fetch_article", "status": ["queued"]})

    assert response.status_code == 200
    assert response.json()["items"][0]["task_id"] == "abc"
    assert service.called == (10, "fetch_article", ("queued",))


def test_v2_admin_worker_run_endpoint_returns_batch_result():
    class _OperationsService:
        def run_worker_batch(self, *, worker_id, task_type, max_tasks, stop_when_idle):
            return {
                "run_id": "run-1",
                "status": "completed",
                "result": {
                    "worker_id": worker_id,
                    "task_type": task_type,
                    "processed_tasks": max_tasks,
                    "stop_reason": "max_tasks" if not stop_when_idle else "idle",
                },
            }

    client = _build_client(_OperationsService())

    response = client.post("/api/admin/v2/worker/run", params={"max_tasks": 5, "task_type": "canonicalize"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "completed"
    assert payload["result"]["processed_tasks"] == 5
    assert payload["result"]["task_type"] == "canonicalize"


def test_v2_admin_research_metrics_endpoints_return_payloads():
    class _OperationsService:
        def get_runtime_status(self, _session):
            return {}

    class _ResearchMetricsService:
        def __init__(self):
            self.history_called = None
            self.refresh_called = False

        def get_latest_or_live(self, _session):
            return {"dataset_construction": {"source_incidents_total": 10}}

        def list_recent_snapshots(self, _session, *, snapshot_key, snapshot_scope, limit):
            self.history_called = (snapshot_key, snapshot_scope, limit)
            return [{"snapshot_id": "snap-1"}]

        def capture_snapshot(self, _session, *, snapshot_key, snapshot_scope, trigger):
            self.refresh_called = (snapshot_key, snapshot_scope, trigger)
            return {"snapshot_key": snapshot_key, "dataset_construction": {"source_incidents_total": 10}}

        def render_prometheus_text(self, payload):
            assert payload["dataset_construction"]["source_incidents_total"] == 10
            return "eduthreat_v2_dataset_source_incidents_total 10\n"

    research = _ResearchMetricsService()
    app = FastAPI()
    app.include_router(router, prefix="/api")

    def _override_session():
        yield object()

    app.dependency_overrides[authenticate] = lambda: True
    app.dependency_overrides[get_v2_session] = _override_session
    app.dependency_overrides[get_v2_operations_service] = lambda: _OperationsService()
    app.dependency_overrides[get_v2_collection_service] = lambda: type("_NullCollectionService", (), {"collect_into_v2": lambda *args, **kwargs: None})()
    app.dependency_overrides[get_v2_orchestration_service] = lambda: type("_NullOrchestrationService", (), {"list_plans": lambda *args, **kwargs: [], "run_plan": lambda *args, **kwargs: None})()
    app.dependency_overrides[get_v2_scheduler_service] = lambda: type("_NullSchedulerService", (), {"get_status": lambda *args, **kwargs: {}, "start": lambda *args, **kwargs: None, "stop": lambda *args, **kwargs: None, "trigger_job": lambda *args, **kwargs: None})()
    app.dependency_overrides[get_v2_preflight_service] = lambda: type("_NullPreflightService", (), {"get_status": lambda *args, **kwargs: {}})()
    app.dependency_overrides[get_v2_data_quality_service] = lambda: type("_NullDataQualityService", (), {"run_sweep": lambda *args, **kwargs: {}, "list_manual_review_queue": lambda *args, **kwargs: []})()
    app.dependency_overrides[get_v2_research_metrics_service] = lambda: research
    client = TestClient(app)

    response = client.get("/api/admin/v2/metrics/research")
    assert response.status_code == 200
    assert response.json()["dataset_construction"]["source_incidents_total"] == 10

    response = client.get("/api/admin/v2/metrics/research/history", params={"limit": 5})
    assert response.status_code == 200
    assert response.json()["meta"]["returned"] == 1
    assert research.history_called == ("global", "global", 5)

    response = client.post("/api/admin/v2/metrics/research/refresh")
    assert response.status_code == 200
    assert response.json()["snapshot_key"] == "global"
    assert research.refresh_called[2]["source"] == "admin_refresh"

    response = client.get("/api/admin/v2/metrics/research/prometheus")
    assert response.status_code == 200
    assert "eduthreat_v2_dataset_source_incidents_total 10" in response.text


def test_v2_admin_canonicalize_sweep_endpoint_queues_recanonicalization():
    class _OperationsService:
        def __init__(self):
            self.called = None

        def queue_recanonicalization_sweep(self, _session, *, limit):
            self.called = limit
            return {"limit": limit, "queued": 12, "skipped_existing": 3}

    service = _OperationsService()
    client = _build_client(service)

    response = client.post("/api/admin/v2/canonicalize/sweep-now", params={"limit": 250})

    assert response.status_code == 200
    assert response.json()["queued"] == 12
    assert service.called == 250


def test_v2_admin_canonicalize_by_canonical_endpoint_queues_targeted_recanonicalization():
    class _OperationsService:
        def __init__(self):
            self.called = None

        def queue_recanonicalization_for_canonical(self, _session, *, canonical_incident_id):
            self.called = canonical_incident_id
            return {
                "canonical_incident_id": canonical_incident_id,
                "found": True,
                "membership_count": 3,
                "queued": 2,
                "skipped_existing": 1,
            }

    service = _OperationsService()
    client = _build_client(service)

    response = client.post("/api/admin/v2/canonicalize/by-canonical/canonical-123")

    assert response.status_code == 200
    assert response.json()["queued"] == 2
    assert service.called == "canonical-123"


def test_v2_admin_canonicalize_consistency_candidates_endpoint_lists_items():
    class _OperationsService:
        def __init__(self):
            self.called = None

        def list_canonical_consistency_candidates(self, _session, *, limit, scan_limit):
            self.called = (limit, scan_limit)
            return [{"canonical_incident_id": "canonical-123", "mismatch_fields": ["institution_name"]}]

    service = _OperationsService()
    client = _build_client(service)

    response = client.get(
        "/api/admin/v2/canonicalize/consistency-candidates",
        params={"limit": 25, "scan_limit": 400},
    )

    assert response.status_code == 200
    assert response.json()["meta"]["returned"] == 1
    assert service.called == (25, 400)


def test_v2_admin_canonicalize_consistency_sweep_endpoint_queues_candidates():
    class _OperationsService:
        def __init__(self):
            self.called = None

        def queue_canonical_consistency_sweep(self, _session, *, limit, scan_limit):
            self.called = (limit, scan_limit)
            return {
                "scan_limit": scan_limit,
                "candidates_considered": 3,
                "canonicals_queued": 2,
                "queued_tasks": 5,
                "skipped_existing_tasks": 1,
            }

    service = _OperationsService()
    client = _build_client(service)

    response = client.post(
        "/api/admin/v2/canonicalize/consistency-sweep-now",
        params={"limit": 30, "scan_limit": 500},
    )

    assert response.status_code == 200
    assert response.json()["queued_tasks"] == 5
    assert service.called == (30, 500)


def test_v2_admin_requeue_dead_letter_endpoint_requeues_tasks():
    class _OperationsService:
        def __init__(self):
            self.called = None

        def requeue_dead_letter_tasks(self, _session, *, task_type, limit):
            self.called = (task_type, limit)
            return {"task_type": task_type, "limit": limit, "requeued": 7}

    service = _OperationsService()
    client = _build_client(service)

    response = client.post(
        "/api/admin/v2/tasks/requeue-dead-letter",
        params={"task_type": "canonicalize", "limit": 25},
    )

    assert response.status_code == 200
    assert response.json()["requeued"] == 7
    assert service.called == ("canonicalize", 25)


def test_v2_admin_collect_endpoint_returns_collection_result():
    class _OperationsService:
        def get_runtime_status(self, _session):
            return {}

    class _CollectionService:
        def __init__(self):
            self.called = None

        def collect_into_v2(self, **kwargs):
            self.called = kwargs
            return {"run_id": "collect-1", "counts": {"incidents_collected": 2}}

    collection = _CollectionService()
    app = FastAPI()
    app.include_router(router, prefix="/api")

    def _override_session():
        yield object()

    app.dependency_overrides[authenticate] = lambda: True
    app.dependency_overrides[get_v2_session] = _override_session
    app.dependency_overrides[get_v2_operations_service] = lambda: _OperationsService()
    app.dependency_overrides[get_v2_collection_service] = lambda: collection
    app.dependency_overrides[get_v2_orchestration_service] = lambda: object()
    app.dependency_overrides[get_v2_scheduler_service] = lambda: object()
    app.dependency_overrides[get_v2_preflight_service] = lambda: object()
    app.dependency_overrides[get_v2_data_quality_service] = lambda: object()
    client = TestClient(app)

    response = client.post(
        "/api/admin/v2/collect",
        params={"groups": ["news"], "sources": ["therecord"], "max_pages": 5, "incremental": "true"},
    )

    assert response.status_code == 200
    assert response.json()["run_id"] == "collect-1"
    assert collection.called["groups"] == ["news"]
    assert collection.called["sources"] == ["therecord"]
    assert collection.called["max_pages"] == 5


def test_v2_admin_plans_endpoint_returns_named_plan_list():
    class _OperationsService:
        def get_runtime_status(self, _session):
            return {}

    class _OrchestrationService:
        def list_plans(self):
            return [{"name": "historical_full"}]

    app = FastAPI()
    app.include_router(router, prefix="/api")

    def _override_session():
        yield object()

    app.dependency_overrides[authenticate] = lambda: True
    app.dependency_overrides[get_v2_session] = _override_session
    app.dependency_overrides[get_v2_operations_service] = lambda: _OperationsService()
    app.dependency_overrides[get_v2_collection_service] = lambda: object()
    app.dependency_overrides[get_v2_orchestration_service] = lambda: _OrchestrationService()
    app.dependency_overrides[get_v2_scheduler_service] = lambda: object()
    app.dependency_overrides[get_v2_preflight_service] = lambda: object()
    app.dependency_overrides[get_v2_data_quality_service] = lambda: object()
    client = TestClient(app)

    response = client.get("/api/admin/v2/plans")

    assert response.status_code == 200
    assert response.json()["items"][0]["name"] == "historical_full"


def test_v2_admin_run_plan_endpoint_queues_orchestrated_result_by_default():
    class _OperationsService:
        def get_runtime_status(self, _session):
            return {}

    class _OrchestrationService:
        def __init__(self):
            self.called = None

        def enqueue_plan(self, **kwargs):
            self.called = kwargs
            return {"run_id": "plan-1", "plan_name": kwargs["plan_name"], "status": "queued"}

    orchestrator = _OrchestrationService()
    app = FastAPI()
    app.include_router(router, prefix="/api")

    def _override_session():
        yield object()

    app.dependency_overrides[authenticate] = lambda: True
    app.dependency_overrides[get_v2_session] = _override_session
    app.dependency_overrides[get_v2_operations_service] = lambda: _OperationsService()
    app.dependency_overrides[get_v2_collection_service] = lambda: object()
    app.dependency_overrides[get_v2_orchestration_service] = lambda: orchestrator
    app.dependency_overrides[get_v2_scheduler_service] = lambda: object()
    app.dependency_overrides[get_v2_preflight_service] = lambda: object()
    app.dependency_overrides[get_v2_data_quality_service] = lambda: object()
    client = TestClient(app)

    response = client.post(
        "/api/admin/v2/run-plan",
        params={"plan_name": "incremental_refresh", "worker_max_tasks": 123, "include_paid_rss": "true"},
    )

    assert response.status_code == 200
    assert response.json()["run_id"] == "plan-1"
    assert response.json()["status"] == "queued"
    assert orchestrator.called["plan_name"] == "incremental_refresh"
    assert orchestrator.called["worker_max_tasks"] == 123
    assert orchestrator.called["collect_overrides"] == {"include_paid_rss": True}


def test_v2_admin_run_plan_endpoint_can_run_synchronously():
    class _OperationsService:
        def get_runtime_status(self, _session):
            return {}

    class _OrchestrationService:
        def __init__(self):
            self.called = None

        def run_plan(self, **kwargs):
            self.called = kwargs
            return {"run_id": "plan-sync", "plan_name": kwargs["plan_name"], "status": "completed"}

    orchestrator = _OrchestrationService()
    app = FastAPI()
    app.include_router(router, prefix="/api")

    def _override_session():
        yield object()

    app.dependency_overrides[authenticate] = lambda: True
    app.dependency_overrides[get_v2_session] = _override_session
    app.dependency_overrides[get_v2_operations_service] = lambda: _OperationsService()
    app.dependency_overrides[get_v2_collection_service] = lambda: object()
    app.dependency_overrides[get_v2_orchestration_service] = lambda: orchestrator
    app.dependency_overrides[get_v2_scheduler_service] = lambda: object()
    app.dependency_overrides[get_v2_preflight_service] = lambda: object()
    app.dependency_overrides[get_v2_data_quality_service] = lambda: object()
    client = TestClient(app)

    response = client.post(
        "/api/admin/v2/run-plan",
        params={"plan_name": "incremental_refresh", "background": "false"},
    )

    assert response.status_code == 200
    assert response.json()["run_id"] == "plan-sync"
    assert orchestrator.called["plan_name"] == "incremental_refresh"


def test_v2_admin_scheduler_endpoints_proxy_service():
    class _OperationsService:
        def get_runtime_status(self, _session):
            return {}

    class _SchedulerService:
        def get_status(self):
            return {"running": False}

        def start(self):
            return {"running": True}

        def stop(self):
            return {"running": False}

        def trigger_job(self, job_name, background=True):
            return {"job_name": job_name, "background": background, "status": "started"}

    scheduler = _SchedulerService()
    app = FastAPI()
    app.include_router(router, prefix="/api")

    def _override_session():
        yield object()

    app.dependency_overrides[authenticate] = lambda: True
    app.dependency_overrides[get_v2_session] = _override_session
    app.dependency_overrides[get_v2_operations_service] = lambda: _OperationsService()
    app.dependency_overrides[get_v2_collection_service] = lambda: object()
    app.dependency_overrides[get_v2_orchestration_service] = lambda: object()
    app.dependency_overrides[get_v2_scheduler_service] = lambda: scheduler
    app.dependency_overrides[get_v2_preflight_service] = lambda: object()
    app.dependency_overrides[get_v2_data_quality_service] = lambda: object()
    client = TestClient(app)

    assert client.get("/api/admin/v2/scheduler/status").json()["running"] is False
    assert client.post("/api/admin/v2/scheduler/start").json()["running"] is True
    assert client.post("/api/admin/v2/scheduler/stop").json()["running"] is False
    trigger = client.post("/api/admin/v2/scheduler/trigger/rss_fast_refresh", params={"background": "false"})
    assert trigger.status_code == 200
    assert trigger.json()["job_name"] == "rss_fast_refresh"
    assert trigger.json()["background"] is False


def test_v2_admin_preflight_endpoint_returns_service_payload():
    class _OperationsService:
        def get_runtime_status(self, _session):
            return {}

    class _PreflightService:
        def get_status(self, _session):
            return {"ready": True, "warnings": []}

    app = FastAPI()
    app.include_router(router, prefix="/api")

    def _override_session():
        yield object()

    app.dependency_overrides[authenticate] = lambda: True
    app.dependency_overrides[get_v2_session] = _override_session
    app.dependency_overrides[get_v2_operations_service] = lambda: _OperationsService()
    app.dependency_overrides[get_v2_collection_service] = lambda: object()
    app.dependency_overrides[get_v2_orchestration_service] = lambda: object()
    app.dependency_overrides[get_v2_scheduler_service] = lambda: object()
    app.dependency_overrides[get_v2_preflight_service] = lambda: _PreflightService()
    app.dependency_overrides[get_v2_data_quality_service] = lambda: object()
    client = TestClient(app)

    response = client.get("/api/admin/v2/preflight")

    assert response.status_code == 200
    assert response.json()["ready"] is True


def test_v2_admin_login_and_logout_endpoints(monkeypatch):
    app = FastAPI()
    app.include_router(router, prefix="/api")
    client = TestClient(app)

    monkeypatch.setenv("EDUTHREAT_ADMIN_PASSWORD", "secret123")

    response = client.post(
        "/api/admin/v2/login",
        json={"username": "admin", "password": "secret123"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["session_token"]

    logout = client.post(
        "/api/admin/v2/logout",
        headers={"X-Session-Token": payload["session_token"]},
    )
    assert logout.status_code == 200
    assert logout.json()["success"] is True


def test_v2_admin_data_quality_endpoints_proxy_service():
    class _OperationsService:
        def get_runtime_status(self, _session):
            return {}

    class _DataQualityService:
        def run_sweep(self, **kwargs):
            return {"requeued_for_reenrichment": 2, "limit": kwargs.get("limit")}

        def list_manual_review_queue(self, _session, *, limit):
            return [{"source_incident_id": "abc", "limit": limit}]

    quality = _DataQualityService()
    app = FastAPI()
    app.include_router(router, prefix="/api")

    def _override_session():
        yield object()

    app.dependency_overrides[authenticate] = lambda: True
    app.dependency_overrides[get_v2_session] = _override_session
    app.dependency_overrides[get_v2_operations_service] = lambda: _OperationsService()
    app.dependency_overrides[get_v2_collection_service] = lambda: object()
    app.dependency_overrides[get_v2_orchestration_service] = lambda: object()
    app.dependency_overrides[get_v2_scheduler_service] = lambda: object()
    app.dependency_overrides[get_v2_preflight_service] = lambda: object()
    app.dependency_overrides[get_v2_data_quality_service] = lambda: quality
    client = TestClient(app)

    sweep = client.post("/api/admin/v2/data-quality/sweep-now", params={"limit": 55})
    queue = client.get("/api/admin/v2/manual-review-queue", params={"limit": 7})

    assert sweep.status_code == 200
    assert sweep.json()["requeued_for_reenrichment"] == 2
    assert sweep.json()["limit"] == 55
    assert queue.status_code == 200
    assert queue.json()["items"][0]["source_incident_id"] == "abc"
