from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

from src.edu_cti_v2.services.orchestration import V2OrchestrationService


class _FakeSession:
    def __init__(self):
        self.commits = 0
        self.flushes = 0

    def commit(self):
        self.commits += 1

    def flush(self):
        self.flushes += 1


class _FakeSessionContext:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc, tb):
        return False


def test_orchestration_service_lists_named_plans():
    service = V2OrchestrationService(
        session_factory=lambda: _FakeSessionContext(_FakeSession()),
        collection_service=Mock(),
        operations_service=Mock(),
        data_quality_service=Mock(),
        pipeline_run_repository=Mock(),
    )

    plans = service.list_plans()

    names = {plan["name"] for plan in plans}
    assert "historical_full" in names
    assert "incremental_refresh" in names
    assert "collect_only" in names
    assert "daily_quality_refresh" in names


def test_orchestration_service_runs_plan_and_combines_collect_and_worker_results():
    collection_service = Mock()
    collection_service.collect_into_v2.return_value = {"run_id": "collect-1", "counts": {"incidents_collected": 7}}

    operations_service = Mock()
    operations_service.run_worker_batch.return_value = {"run_id": "worker-1", "result": {"processed_tasks": 4}}
    data_quality_service = Mock()

    run_repo = Mock()
    run_repo.get_by_id.return_value = SimpleNamespace(id=uuid4())

    sessions = [_FakeSession(), _FakeSession()]

    def _session_factory():
        return _FakeSessionContext(sessions.pop(0))

    service = V2OrchestrationService(
        session_factory=_session_factory,
        collection_service=collection_service,
        operations_service=operations_service,
        data_quality_service=data_quality_service,
        pipeline_run_repository=run_repo,
    )

    result = service.run_plan(plan_name="incremental_refresh", worker_id="tester", worker_max_tasks=25)

    assert result["plan_name"] == "incremental_refresh"
    assert result["collect_result"]["run_id"] == "collect-1"
    assert result["worker_result"]["run_id"] == "worker-1"
    collection_service.collect_into_v2.assert_called_once()
    operations_service.run_worker_batch.assert_called_once()
    assert run_repo.add.called
    assert run_repo.mark_started.called
    assert run_repo.mark_finished.called


def test_orchestration_service_runs_data_quality_and_reenrich_for_quality_plan():
    collection_service = Mock()
    collection_service.collect_into_v2.return_value = {"run_id": "collect-1"}

    operations_service = Mock()
    operations_service.run_worker_batch.side_effect = [
        {"run_id": "worker-1", "result": {"processed_tasks": 4}},
        {"run_id": "reenrich-1", "result": {"processed_tasks": 2}},
    ]
    data_quality_service = Mock()
    data_quality_service.run_sweep.return_value = {"requeued_for_reenrichment": 2}

    run_repo = Mock()
    run_repo.get_by_id.return_value = SimpleNamespace(id=uuid4())
    sessions = [_FakeSession(), _FakeSession()]

    def _session_factory():
        return _FakeSessionContext(sessions.pop(0))

    service = V2OrchestrationService(
        session_factory=_session_factory,
        collection_service=collection_service,
        operations_service=operations_service,
        data_quality_service=data_quality_service,
        pipeline_run_repository=run_repo,
    )

    result = service.run_plan(plan_name="daily_quality_refresh", worker_id="tester")

    assert result["data_quality_result"]["requeued_for_reenrichment"] == 2
    assert result["reenrich_worker_result"]["run_id"] == "reenrich-1"
    assert operations_service.run_worker_batch.call_count == 2
    second_call = operations_service.run_worker_batch.call_args_list[1]
    assert second_call.kwargs["task_type"] == "reenrich"
