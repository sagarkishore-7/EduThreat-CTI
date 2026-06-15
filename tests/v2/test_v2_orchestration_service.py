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
    research_metrics_service = Mock()
    service = V2OrchestrationService(
        session_factory=lambda: _FakeSessionContext(_FakeSession()),
        collection_service=Mock(),
        operations_service=Mock(),
        data_quality_service=Mock(),
        research_metrics_service=research_metrics_service,
        pipeline_run_repository=Mock(),
    )

    plans = service.list_plans()

    names = {plan["name"] for plan in plans}
    assert "historical" in names
    assert "historical_full" not in names
    assert "historical_max_coverage" not in names
    assert "incremental_refresh" in names
    assert "collect_only" in names
    assert "daily_quality_refresh" in names
    assert "google_historical" in names
    # google_historical must be Google-News-only, full-historical
    gh = next(p for p in plans if p["name"] == "google_historical")
    assert gh["collect_kwargs"]["groups"] == ["rss"]
    assert gh["collect_kwargs"]["sources"] == ["googlenews_rss"]
    assert gh["collect_kwargs"]["incremental"] is False


def test_orchestration_service_runs_plan_and_combines_collect_and_worker_results():
    collection_service = Mock()
    collection_service.collect_into_v2.return_value = {"run_id": "collect-1", "counts": {"incidents_collected": 7}}

    operations_service = Mock()
    operations_service.run_worker_batch.return_value = {"run_id": "worker-1", "result": {"processed_tasks": 4}}
    operations_service.queue_canonical_consistency_sweep.return_value = {
        "candidates_considered": 0,
        "canonicals_queued": 0,
        "queued_tasks": 0,
        "skipped_existing_tasks": 0,
        "scan_limit": 1000,
    }
    data_quality_service = Mock()
    research_metrics_service = Mock()
    research_metrics_service.capture_snapshot.return_value = {"snapshot_key": "global"}

    run_repo = Mock()
    run_repo.get_by_id.return_value = SimpleNamespace(id=uuid4())

    sessions = [_FakeSession(), _FakeSession(), _FakeSession()]

    def _session_factory():
        return _FakeSessionContext(sessions.pop(0))

    service = V2OrchestrationService(
        session_factory=_session_factory,
        collection_service=collection_service,
        operations_service=operations_service,
        data_quality_service=data_quality_service,
        research_metrics_service=research_metrics_service,
        pipeline_run_repository=run_repo,
    )

    result = service.run_plan(plan_name="incremental_refresh", worker_id="tester", worker_max_tasks=25)

    assert result["plan_name"] == "incremental_refresh"
    assert result["collect_result"]["run_id"] == "collect-1"
    assert result["worker_result"]["run_id"] == "worker-1"
    collection_service.collect_into_v2.assert_called_once()
    operations_service.run_worker_batch.assert_called_once()
    research_metrics_service.capture_snapshot.assert_called_once()
    assert run_repo.add.called
    assert run_repo.mark_started.called
    assert run_repo.mark_finished.called


def test_orchestration_service_enqueue_plan_creates_pending_run_and_task():
    run_repo = Mock()
    task_repo = Mock()
    session = _FakeSession()

    service = V2OrchestrationService(
        session_factory=lambda: _FakeSessionContext(session),
        collection_service=Mock(),
        operations_service=Mock(),
        data_quality_service=Mock(),
        research_metrics_service=Mock(),
        pipeline_run_repository=run_repo,
        pipeline_task_repository=task_repo,
    )

    result = service.enqueue_plan(plan_name="historical", worker_id="tester")

    assert result["plan_name"] == "historical"
    assert result["status"] == "queued"
    assert run_repo.add.called
    assert task_repo.enqueue.called
    assert session.commits == 1


def test_orchestration_service_accepts_legacy_historical_aliases():
    run_repo = Mock()
    task_repo = Mock()
    session = _FakeSession()

    service = V2OrchestrationService(
        session_factory=lambda: _FakeSessionContext(session),
        collection_service=Mock(),
        operations_service=Mock(),
        data_quality_service=Mock(),
        research_metrics_service=Mock(),
        pipeline_run_repository=run_repo,
        pipeline_task_repository=task_repo,
    )

    result = service.enqueue_plan(plan_name="historical_max_coverage", worker_id="tester")

    assert result["plan_name"] == "historical_max_coverage"
    task = task_repo.enqueue.call_args.args[1]
    assert task.payload["plan_name"] == "historical_max_coverage"
    assert "include_paid_rss" not in task.payload["collect_kwargs"]


def test_orchestration_service_runs_data_quality_and_reenrich_for_quality_plan():
    collection_service = Mock()
    collection_service.collect_into_v2.return_value = {"run_id": "collect-1"}

    operations_service = Mock()
    operations_service.run_worker_batch.side_effect = [
        {"run_id": "worker-1", "result": {"processed_tasks": 4}},
        {"run_id": "reenrich-1", "result": {"processed_tasks": 2}},
        {"run_id": "consistency-1", "result": {"processed_tasks": 3}},
    ]
    operations_service.queue_canonical_consistency_sweep.return_value = {
        "candidates_considered": 2,
        "canonicals_queued": 2,
        "queued_tasks": 3,
        "skipped_existing_tasks": 0,
        "scan_limit": 1000,
    }
    data_quality_service = Mock()
    data_quality_service.run_sweep.return_value = {"requeued_for_reenrichment": 2}
    campaign_service = Mock()
    campaign_service.run_correlation.return_value = {"campaign_candidates": 2}
    research_metrics_service = Mock()
    research_metrics_service.capture_snapshot.return_value = {"snapshot_key": "global"}

    run_repo = Mock()
    run_repo.get_by_id.return_value = SimpleNamespace(id=uuid4())
    sessions = [_FakeSession(), _FakeSession(), _FakeSession()]

    def _session_factory():
        return _FakeSessionContext(sessions.pop(0))

    service = V2OrchestrationService(
        session_factory=_session_factory,
        collection_service=collection_service,
        operations_service=operations_service,
        data_quality_service=data_quality_service,
        campaign_service=campaign_service,
        research_metrics_service=research_metrics_service,
        pipeline_run_repository=run_repo,
    )

    result = service.run_plan(plan_name="daily_quality_refresh", worker_id="tester")

    assert result["data_quality_result"]["requeued_for_reenrichment"] == 2
    assert result["reenrich_worker_result"]["run_id"] == "reenrich-1"
    assert result["consistency_sweep_result"]["queued_tasks"] == 3
    assert result["consistency_worker_result"]["run_id"] == "consistency-1"
    assert result["campaign_correlation_result"]["campaign_candidates"] == 2
    assert operations_service.run_worker_batch.call_count == 3
    second_call = operations_service.run_worker_batch.call_args_list[1]
    assert second_call.kwargs["task_type"] == "reenrich"
    third_call = operations_service.run_worker_batch.call_args_list[2]
    assert third_call.kwargs["task_type"] == "canonicalize"
    campaign_service.run_correlation.assert_called_once()
    research_metrics_service.capture_snapshot.assert_called_once()


def test_orchestration_service_execute_enqueued_plan_waits_for_drain(monkeypatch):
    collection_service = Mock()
    collection_service.collect_into_v2.return_value = {"run_id": None, "counts": {"incidents_collected": 3}}
    data_quality_service = Mock()
    data_quality_service.run_sweep.return_value = {"requeued_for_reenrichment": 0}
    operations_service = Mock()
    operations_service.queue_canonical_consistency_sweep.return_value = {
        "candidates_considered": 0,
        "canonicals_queued": 0,
        "queued_tasks": 0,
        "skipped_existing_tasks": 0,
        "scan_limit": 1000,
    }
    run_repo = Mock()
    run_repo.get_by_id.return_value = SimpleNamespace(id=uuid4(), status="pending")
    task_repo = Mock()
    task_repo.count_active.side_effect = [2, 0]
    research_metrics_service = Mock()
    research_metrics_service.capture_snapshot.return_value = {"snapshot_key": "global"}
    monkeypatch.setattr("src.edu_cti_v2.services.orchestration.time.sleep", lambda _seconds: None)

    service = V2OrchestrationService(
        session_factory=lambda: _FakeSessionContext(_FakeSession()),
        collection_service=collection_service,
        operations_service=operations_service,
        data_quality_service=data_quality_service,
        research_metrics_service=research_metrics_service,
        pipeline_run_repository=run_repo,
        pipeline_task_repository=task_repo,
    )

    task = SimpleNamespace(
        id=uuid4(),
        run_id=uuid4(),
        payload={
            "plan_name": "incremental_refresh",
            "collect_kwargs": {
                "groups": ["curated", "news", "rss", "api"],
                "incremental": True,
                "max_pages": 20,
                "rss_max_age_days": 30,
            },
            "drain_tasks": True,
            "worker_max_tasks": 1000,
        },
    )

    result = service.execute_enqueued_plan(task, worker_id="worker-1")

    assert result["execution_mode"] == "queued"
    assert collection_service.collect_into_v2.call_args.kwargs["persist_run"] is False
    assert task_repo.count_active.call_count == 2
    research_metrics_service.capture_snapshot.assert_called_once()
    assert run_repo.mark_started.called
    assert run_repo.mark_finished.called
