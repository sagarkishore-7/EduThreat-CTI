from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

from src.edu_cti_v2.services import V2TaskRuntime


def test_task_runtime_processes_fetch_article_task_and_marks_complete():
    task_repo = Mock()
    source_repo = Mock()
    fetch_service = Mock()

    task = SimpleNamespace(id=uuid4(), task_type="fetch_article", target_id=uuid4())
    source_incident = SimpleNamespace(id=task.target_id)
    task_repo.lease_batch.return_value = [task]
    source_repo.get_by_id.return_value = source_incident
    fetch_service.fetch_articles_for_source_incident.return_value = {"articles_saved": 1}

    runtime = V2TaskRuntime(
        pipeline_task_repository=task_repo,
        source_incident_repository=source_repo,
        fetch_service=fetch_service,
    )
    session = Mock()
    session.get.return_value = task

    processed = runtime.process_next_task(session, worker_id="worker-1")

    assert processed is task
    task_repo.requeue_expired_leases.assert_called_once_with(session, limit=50)
    fetch_service.fetch_articles_for_source_incident.assert_called_once_with(
        session,
        source_incident,
        worker_id="worker-1",
    )
    task_repo.mark_completed.assert_called_once_with(session, task, {"articles_saved": 1})


def test_task_runtime_prefers_fetch_over_resolve_when_unspecified():
    task_repo = Mock()
    source_repo = Mock()
    fetch_service = Mock()
    resolve_service = Mock()

    fetch_task = SimpleNamespace(id=uuid4(), task_type="fetch_article", target_id=uuid4())
    fetch_source_incident = SimpleNamespace(id=fetch_task.target_id)
    source_repo.get_by_id.return_value = fetch_source_incident
    fetch_service.fetch_articles_for_source_incident.return_value = {"articles_saved": 1}

    def _lease_batch(_session, *, worker_id, task_type, exclude_task_types=None, limit, lease_seconds):
        if task_type == "fetch_article":
            return [fetch_task]
        if task_type == "resolve_url":
            return [SimpleNamespace(task_type="resolve_url", target_id=uuid4())]
        return []

    task_repo.lease_batch.side_effect = _lease_batch

    runtime = V2TaskRuntime(
        pipeline_task_repository=task_repo,
        source_incident_repository=source_repo,
        fetch_service=fetch_service,
        resolve_url_service=resolve_service,
    )
    session = Mock()
    session.get.return_value = fetch_task

    processed = runtime.process_next_task(session, worker_id="worker-1")

    assert processed is fetch_task
    task_repo.requeue_expired_leases.assert_called_once_with(session, limit=50)
    fetch_service.fetch_articles_for_source_incident.assert_called_once_with(
        session,
        fetch_source_incident,
        worker_id="worker-1",
    )
    resolve_service.resolve_source_incident_urls.assert_not_called()


def test_task_runtime_can_exclude_task_types():
    task_repo = Mock()
    source_repo = Mock()
    fetch_service = Mock()

    fetch_task = SimpleNamespace(id=uuid4(), task_type="fetch_article", target_id=uuid4())
    fetch_source_incident = SimpleNamespace(id=fetch_task.target_id)
    source_repo.get_by_id.return_value = fetch_source_incident
    fetch_service.fetch_articles_for_source_incident.return_value = {"articles_saved": 1}

    def _lease_batch(_session, *, worker_id, task_type, exclude_task_types=None, limit, lease_seconds):
        if task_type == "orchestrate_plan":
            return [SimpleNamespace(task_type="orchestrate_plan", target_id=uuid4())]
        if task_type == "fetch_article":
            return [fetch_task]
        return []

    task_repo.lease_batch.side_effect = _lease_batch

    runtime = V2TaskRuntime(
        pipeline_task_repository=task_repo,
        source_incident_repository=source_repo,
        fetch_service=fetch_service,
    )
    session = Mock()
    session.get.return_value = fetch_task

    processed = runtime.process_next_task(
        session,
        worker_id="worker-1",
        exclude_task_types=("orchestrate_plan",),
    )

    assert processed is fetch_task
    fetch_service.fetch_articles_for_source_incident.assert_called_once()


def test_task_runtime_prefers_canonicalize_over_fetch_when_unspecified():
    task_repo = Mock()
    source_repo = Mock()
    fetch_service = Mock()
    canonicalization_service = Mock()

    canonical_task = SimpleNamespace(id=uuid4(), task_type="canonicalize", target_id=uuid4())
    canonical_source_incident = SimpleNamespace(id=canonical_task.target_id)
    source_repo.get_by_id.return_value = canonical_source_incident
    canonicalization_service.canonicalize_source_incident.return_value = {"canonicalized": True}

    def _lease_batch(_session, *, worker_id, task_type, exclude_task_types=None, limit, lease_seconds):
        if task_type == "canonicalize":
            return [canonical_task]
        if task_type == "fetch_article":
            return [SimpleNamespace(id=uuid4(), task_type="fetch_article", target_id=uuid4())]
        return []

    task_repo.lease_batch.side_effect = _lease_batch

    runtime = V2TaskRuntime(
        pipeline_task_repository=task_repo,
        source_incident_repository=source_repo,
        fetch_service=fetch_service,
        canonicalization_service=canonicalization_service,
    )
    session = Mock()
    session.get.return_value = canonical_task

    processed = runtime.process_next_task(session, worker_id="worker-1")

    assert processed is canonical_task
    canonicalization_service.canonicalize_source_incident.assert_called_once_with(
        session,
        canonical_source_incident.id,
    )
    fetch_service.fetch_articles_for_source_incident.assert_not_called()


def test_task_runtime_dead_letters_unimplemented_task_types():
    task_repo = Mock()
    source_repo = Mock()
    fetch_service = Mock()
    task = SimpleNamespace(id=uuid4(), task_type="collect", target_id=uuid4(), payload={})
    task_repo.lease_batch.return_value = [task]

    runtime = V2TaskRuntime(
        pipeline_task_repository=task_repo,
        source_incident_repository=source_repo,
        fetch_service=fetch_service,
    )
    session = Mock()
    session.get.return_value = task

    processed = runtime.process_next_task(session, worker_id="worker-1")

    assert processed is task
    task_repo.mark_failed.assert_called_once()
    assert task_repo.mark_failed.call_args.kwargs["dead_letter"] is True


def test_task_runtime_processes_resolve_url_task_and_marks_complete():
    task_repo = Mock()
    source_repo = Mock()
    fetch_service = Mock()
    resolve_service = Mock()
    enrich_service = Mock()

    task = SimpleNamespace(id=uuid4(), task_type="resolve_url", target_id=uuid4())
    source_incident = SimpleNamespace(id=task.target_id)
    task_repo.lease_batch.return_value = [task]
    source_repo.get_by_id.return_value = source_incident
    resolve_service.resolve_source_incident_urls.return_value = {"urls_added": 1}

    runtime = V2TaskRuntime(
        pipeline_task_repository=task_repo,
        source_incident_repository=source_repo,
        fetch_service=fetch_service,
        resolve_url_service=resolve_service,
        enrichment_service=enrich_service,
    )
    session = Mock()
    session.get.return_value = task

    processed = runtime.process_next_task(session, worker_id="worker-1")

    assert processed is task
    resolve_service.resolve_source_incident_urls.assert_called_once_with(session, source_incident)
    task_repo.mark_completed.assert_called_once_with(session, task, {"urls_added": 1})


def test_task_runtime_pauses_resolve_when_fetch_backlog_is_too_high():
    task_repo = Mock()
    source_repo = Mock()
    resolve_service = Mock()

    task_repo.count_active.return_value = 700

    runtime = V2TaskRuntime(
        pipeline_task_repository=task_repo,
        source_incident_repository=source_repo,
        resolve_url_service=resolve_service,
        max_fetch_backlog=600,
    )
    session = Mock()

    processed = runtime.process_next_task(
        session,
        worker_id="resolve-worker-1",
        task_type="resolve_url",
    )

    assert processed is None
    task_repo.count_active.assert_called_once_with(
        session,
        task_types=("fetch_article",),
    )
    task_repo.lease_batch.assert_not_called()
    resolve_service.resolve_source_incident_urls.assert_not_called()


def test_task_runtime_still_leases_resolve_when_fetch_backlog_is_below_threshold():
    task_repo = Mock()
    source_repo = Mock()
    resolve_service = Mock()

    task = SimpleNamespace(id=uuid4(), task_type="resolve_url", target_id=uuid4())
    source_incident = SimpleNamespace(id=task.target_id)
    task_repo.count_active.return_value = 200
    task_repo.lease_batch.return_value = [task]
    source_repo.get_by_id.return_value = source_incident
    resolve_service.resolve_source_incident_urls.return_value = {"urls_added": 1}

    runtime = V2TaskRuntime(
        pipeline_task_repository=task_repo,
        source_incident_repository=source_repo,
        resolve_url_service=resolve_service,
        max_fetch_backlog=600,
    )
    session = Mock()
    session.get.return_value = task

    processed = runtime.process_next_task(
        session,
        worker_id="resolve-worker-1",
        task_type="resolve_url",
    )

    assert processed is task
    task_repo.count_active.assert_called_once_with(
        session,
        task_types=("fetch_article",),
    )
    resolve_service.resolve_source_incident_urls.assert_called_once_with(session, source_incident)


def test_task_runtime_processes_enrich_source_task_and_marks_complete():
    task_repo = Mock()
    source_repo = Mock()
    fetch_service = Mock()
    resolve_service = Mock()
    enrich_service = Mock()

    task = SimpleNamespace(id=uuid4(), task_type="enrich_source", target_id=uuid4())
    source_incident = SimpleNamespace(id=task.target_id)
    task_repo.lease_batch.return_value = [task]
    source_repo.get_by_id.return_value = source_incident
    enrich_service.enrich_source_incident.return_value = {"enriched": True}

    runtime = V2TaskRuntime(
        pipeline_task_repository=task_repo,
        source_incident_repository=source_repo,
        fetch_service=fetch_service,
        resolve_url_service=resolve_service,
        enrichment_service=enrich_service,
    )
    session = Mock()
    session.get.return_value = task

    processed = runtime.process_next_task(session, worker_id="worker-1")

    assert processed is task
    enrich_service.enrich_source_incident.assert_called_once_with(session, source_incident)
    task_repo.mark_completed.assert_called_once_with(session, task, {"enriched": True})


def test_task_runtime_processes_reenrich_task_and_marks_complete():
    task_repo = Mock()
    source_repo = Mock()
    fetch_service = Mock()
    resolve_service = Mock()
    enrich_service = Mock()

    task = SimpleNamespace(
        id=uuid4(),
        task_type="reenrich",
        target_id=uuid4(),
        payload={"re_enrich_attempts": 2, "re_enrich_reason": "incident_date='2099-01-01'"},
    )
    source_incident = SimpleNamespace(id=task.target_id)
    task_repo.lease_batch.return_value = [task]
    source_repo.get_by_id.return_value = source_incident
    enrich_service.enrich_source_incident.return_value = {"enriched": True, "canonicalize_tasks_enqueued": 1}

    runtime = V2TaskRuntime(
        pipeline_task_repository=task_repo,
        source_incident_repository=source_repo,
        fetch_service=fetch_service,
        resolve_url_service=resolve_service,
        enrichment_service=enrich_service,
    )
    session = Mock()
    session.get.return_value = task

    processed = runtime.process_next_task(session, worker_id="worker-1")

    assert processed is task
    enrich_service.enrich_source_incident.assert_called_once_with(
        session,
        source_incident,
        re_enrich_attempts=2,
        re_enrich_reason="incident_date='2099-01-01'",
        force_canonicalize=True,
    )
    task_repo.mark_completed.assert_called_once_with(
        session,
        task,
        {"enriched": True, "canonicalize_tasks_enqueued": 1},
    )


def test_task_runtime_processes_canonicalize_task_and_marks_complete():
    task_repo = Mock()
    source_repo = Mock()
    fetch_service = Mock()
    resolve_service = Mock()
    enrich_service = Mock()
    canonicalization_service = Mock()

    task = SimpleNamespace(id=uuid4(), task_type="canonicalize", target_id=uuid4())
    source_incident = SimpleNamespace(id=task.target_id)
    task_repo.lease_batch.return_value = [task]
    source_repo.get_by_id.return_value = source_incident
    canonicalization_service.canonicalize_source_incident.return_value = {"canonicalized": True}

    runtime = V2TaskRuntime(
        pipeline_task_repository=task_repo,
        source_incident_repository=source_repo,
        fetch_service=fetch_service,
        resolve_url_service=resolve_service,
        enrichment_service=enrich_service,
        canonicalization_service=canonicalization_service,
    )
    session = Mock()
    session.get.return_value = task

    processed = runtime.process_next_task(session, worker_id="worker-1")

    assert processed is task
    canonicalization_service.canonicalize_source_incident.assert_called_once_with(session, source_incident.id)
    task_repo.mark_completed.assert_called_once_with(session, task, {"canonicalized": True})


def test_task_runtime_processes_refresh_analytics_task_and_marks_complete():
    task_repo = Mock()
    source_repo = Mock()
    fetch_service = Mock()
    analytics_service = Mock()

    canonical_id = uuid4()
    task = SimpleNamespace(
        id=uuid4(),
        task_type="refresh_analytics",
        target_id=canonical_id,
        payload={"canonical_incident_id": str(canonical_id)},
    )
    task_repo.lease_batch.return_value = [task]
    analytics_service.refresh_canonical_incident_snapshot.return_value = {"refreshed": True}

    runtime = V2TaskRuntime(
        pipeline_task_repository=task_repo,
        source_incident_repository=source_repo,
        fetch_service=fetch_service,
        analytics_refresh_service=analytics_service,
    )
    session = Mock()
    session.get.return_value = task

    processed = runtime.process_next_task(session, worker_id="worker-1")

    assert processed is task
    analytics_service.refresh_canonical_incident_snapshot.assert_called_once_with(session, str(canonical_id))
    task_repo.mark_completed.assert_called_once_with(session, task, {"refreshed": True})


def test_task_runtime_processes_global_refresh_analytics_task_and_marks_complete():
    task_repo = Mock()
    source_repo = Mock()
    fetch_service = Mock()
    analytics_service = Mock()

    task = SimpleNamespace(
        id=uuid4(),
        task_type="refresh_analytics",
        target_id=None,
        target_table="analytics_refresh_state",
        payload={"refresh_key": "dashboard:global", "canonical_incident_id": "abc"},
    )
    task_repo.lease_batch.return_value = [task]
    analytics_service.refresh_dashboard_snapshot.return_value = {"refreshed": True, "snapshot_scope": "global"}

    runtime = V2TaskRuntime(
        pipeline_task_repository=task_repo,
        source_incident_repository=source_repo,
        fetch_service=fetch_service,
        analytics_refresh_service=analytics_service,
    )
    session = Mock()
    session.get.return_value = task

    processed = runtime.process_next_task(session, worker_id="worker-1")

    assert processed is task
    analytics_service.refresh_dashboard_snapshot.assert_called_once_with(
        session,
        last_trigger_canonical_incident_id="abc",
    )
    task_repo.mark_completed.assert_called_once_with(
        session,
        task,
        {"refreshed": True, "snapshot_scope": "global"},
    )


def test_task_runtime_processes_orchestrate_plan_task_and_marks_complete():
    task_repo = Mock()
    source_repo = Mock()
    fetch_service = Mock()
    orchestration_service = Mock()

    task = SimpleNamespace(
        task_type="orchestrate_plan",
        id=uuid4(),
        target_id=uuid4(),
        run_id=uuid4(),
        payload={"plan_name": "historical_full"},
    )
    task_repo.lease_batch.return_value = [task]
    orchestration_service.execute_enqueued_plan.return_value = {"run_id": str(task.run_id), "status": "completed"}

    runtime = V2TaskRuntime(
        pipeline_task_repository=task_repo,
        source_incident_repository=source_repo,
        fetch_service=fetch_service,
        orchestration_service=orchestration_service,
    )
    session = Mock()
    session.get.return_value = task

    processed = runtime.process_next_task(session, worker_id="worker-1")

    assert processed is task
    orchestration_service.execute_enqueued_plan.assert_called_once_with(task, worker_id="worker-1")
    task_repo.mark_completed.assert_called_once_with(
        session,
        task,
        {"run_id": str(task.run_id), "status": "completed"},
    )


def test_task_runtime_rolls_back_before_marking_failed():
    task_repo = Mock()
    source_repo = Mock()
    fetch_service = Mock()

    task = SimpleNamespace(id=uuid4(), task_type="fetch_article", target_id=uuid4(), attempt_count=1, max_attempts=5)
    source_incident = SimpleNamespace(id=task.target_id)
    task_repo.lease_batch.return_value = [task]
    source_repo.get_by_id.return_value = source_incident
    fetch_service.fetch_articles_for_source_incident.side_effect = RuntimeError("boom")

    runtime = V2TaskRuntime(
        pipeline_task_repository=task_repo,
        source_incident_repository=source_repo,
        fetch_service=fetch_service,
    )
    session = Mock()
    session.get.return_value = task

    processed = runtime.process_next_task(session, worker_id="worker-1")

    assert processed is task
    session.rollback.assert_called_once()
    task_repo.mark_failed.assert_called_once_with(
        session,
        task,
        error="boom",
        dead_letter=False,
    )
