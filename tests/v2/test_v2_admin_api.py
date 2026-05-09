from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.edu_cti.api.admin import authenticate
from src.edu_cti.api.v2 import get_v2_session
from src.edu_cti.api.v2_admin import (
    get_v2_collection_service,
    get_v2_operations_service,
    get_v2_orchestration_service,
    get_v2_scheduler_service,
    router,
)


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

    app.dependency_overrides[authenticate] = lambda: True
    app.dependency_overrides[get_v2_session] = _override_session
    app.dependency_overrides[get_v2_operations_service] = lambda: operations_service
    app.dependency_overrides[get_v2_collection_service] = lambda: _NullCollectionService()
    app.dependency_overrides[get_v2_orchestration_service] = lambda: _NullOrchestrationService()
    app.dependency_overrides[get_v2_scheduler_service] = lambda: _NullSchedulerService()
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
    client = TestClient(app)

    response = client.get("/api/admin/v2/plans")

    assert response.status_code == 200
    assert response.json()["items"][0]["name"] == "historical_full"


def test_v2_admin_run_plan_endpoint_returns_orchestrated_result():
    class _OperationsService:
        def get_runtime_status(self, _session):
            return {}

    class _OrchestrationService:
        def __init__(self):
            self.called = None

        def run_plan(self, **kwargs):
            self.called = kwargs
            return {"run_id": "plan-1", "plan_name": kwargs["plan_name"]}

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
    client = TestClient(app)

    response = client.post(
        "/api/admin/v2/run-plan",
        params={"plan_name": "incremental_refresh", "worker_max_tasks": 123, "include_paid_rss": "true"},
    )

    assert response.status_code == 200
    assert response.json()["run_id"] == "plan-1"
    assert orchestrator.called["plan_name"] == "incremental_refresh"
    assert orchestrator.called["worker_max_tasks"] == 123
    assert orchestrator.called["collect_overrides"] == {"include_paid_rss": True}


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
    client = TestClient(app)

    assert client.get("/api/admin/v2/scheduler/status").json()["running"] is False
    assert client.post("/api/admin/v2/scheduler/start").json()["running"] is True
    assert client.post("/api/admin/v2/scheduler/stop").json()["running"] is False
    trigger = client.post("/api/admin/v2/scheduler/trigger/rss_fast_refresh", params={"background": "false"})
    assert trigger.status_code == 200
    assert trigger.json()["job_name"] == "rss_fast_refresh"
    assert trigger.json()["background"] is False
