from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

from src.edu_cti_v2.models import PipelineTask
from src.edu_cti_v2.services.operations import V2OperationsService
from src.edu_cti_v2.worker import V2WorkerRunSummary


class _ExecuteResult:
    def __init__(self, value):
        self._value = value

    def scalar_one(self):
        return self._value


class _FakeSession:
    def __init__(self, execute_values=None):
        self.execute_values = list(execute_values or [])
        self.commits = 0
        self.flushes = 0

    def execute(self, _stmt):
        return _ExecuteResult(self.execute_values.pop(0))

    def flush(self):
        self.flushes += 1

    def commit(self):
        self.commits += 1


class _FakeSessionContext:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc, tb):
        return False


def test_operations_service_runtime_status_uses_repo_and_count_queries():
    task_repo = Mock()
    task_repo.get_status_summary.return_value = [{"task_type": "fetch_article", "status": "queued", "task_count": 3}]
    task_repo.list_recent.return_value = []
    task_repo.count_expired_leases.return_value = 2

    run_repo = Mock()
    run_repo.list_recent.return_value = []

    analytics_repo = Mock()
    analytics_repo.get_by_key.return_value = SimpleNamespace(last_refreshed_at=None, needs_refresh=False)

    session = _FakeSession(execute_values=[12, 7, 5, 4])
    service = V2OperationsService(
        pipeline_task_repository=task_repo,
        pipeline_run_repository=run_repo,
        analytics_refresh_repository=analytics_repo,
    )

    payload = service.get_runtime_status(session)

    assert payload["counts"]["source_incidents"] == 12
    assert payload["counts"]["canonical_incidents"] == 4
    assert payload["queue_health"]["expired_leases"] == 2
    assert payload["task_summary"][0]["task_count"] == 3


def test_operations_service_run_worker_batch_records_completed_run():
    run_repo = Mock()
    run_repo.get_by_id.return_value = SimpleNamespace(id=uuid4())
    session_one = _FakeSession()
    session_two = _FakeSession()
    sessions = [session_one, session_two]

    def _session_factory():
        return _FakeSessionContext(sessions.pop(0))

    service = V2OperationsService(
        pipeline_run_repository=run_repo,
        session_factory=_session_factory,
    )

    with patch(
        "src.edu_cti_v2.services.operations.run_worker_loop",
        return_value=V2WorkerRunSummary(
            processed_tasks=4,
            idle_polls=1,
            stop_reason="idle",
            worker_id="admin-v2",
            task_type=None,
        ),
    ):
        result = service.run_worker_batch(worker_id="admin-v2", max_tasks=4)

    assert result["status"] == "completed"
    assert result["result"]["processed_tasks"] == 4
    assert run_repo.add.called
    assert run_repo.mark_started.called
    assert run_repo.mark_finished.called


def test_operations_service_queues_recanonicalization_tasks_without_duplicates():
    task_repo = Mock()
    task_repo.get_active_for_target.side_effect = [None, object(), None]
    source_enrichment_repo = Mock()
    source_enrichment_repo.list_source_incident_ids_for_recanonicalize.return_value = [
        "sid-1",
        "sid-2",
        "sid-3",
    ]

    queued_tasks = []

    def _capture_enqueue(_session, task):
        queued_tasks.append(task)
        return task

    task_repo.enqueue.side_effect = _capture_enqueue

    service = V2OperationsService(
        pipeline_task_repository=task_repo,
        source_enrichment_repository=source_enrichment_repo,
    )

    session = _FakeSession()
    result = service.queue_recanonicalization_sweep(session, limit=3)

    assert result["candidates_considered"] == 3
    assert result["queued"] == 2
    assert result["skipped_existing"] == 1
    assert len(queued_tasks) == 2
    assert all(isinstance(task, PipelineTask) for task in queued_tasks)
    assert all(task.task_type == "canonicalize" for task in queued_tasks)
    assert all(task.payload["trigger"] == "recanonicalize_sweep" for task in queued_tasks)


def test_operations_service_requeues_dead_letter_tasks():
    task_repo = Mock()
    task_repo.requeue_dead_letters.return_value = 7

    service = V2OperationsService(
        pipeline_task_repository=task_repo,
    )

    session = _FakeSession()
    result = service.requeue_dead_letter_tasks(session, task_type="canonicalize", limit=25)

    assert result == {
        "task_type": "canonicalize",
        "limit": 25,
        "requeued": 7,
    }
    task_repo.requeue_dead_letters.assert_called_once_with(
        session,
        task_type="canonicalize",
        limit=25,
    )
