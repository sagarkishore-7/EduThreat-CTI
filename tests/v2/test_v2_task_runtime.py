from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

from src.edu_cti_v2.services import V2TaskRuntime


def test_task_runtime_processes_fetch_article_task_and_marks_complete():
    task_repo = Mock()
    source_repo = Mock()
    fetch_service = Mock()

    task = SimpleNamespace(task_type="fetch_article", target_id=uuid4())
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

    processed = runtime.process_next_task(session, worker_id="worker-1")

    assert processed is task
    fetch_service.fetch_articles_for_source_incident.assert_called_once_with(
        session,
        source_incident,
        worker_id="worker-1",
    )
    task_repo.mark_completed.assert_called_once_with(session, task, {"articles_saved": 1})


def test_task_runtime_dead_letters_unimplemented_task_types():
    task_repo = Mock()
    source_repo = Mock()
    fetch_service = Mock()
    task = SimpleNamespace(task_type="canonicalize", target_id=uuid4())
    task_repo.lease_batch.return_value = [task]

    runtime = V2TaskRuntime(
        pipeline_task_repository=task_repo,
        source_incident_repository=source_repo,
        fetch_service=fetch_service,
    )
    session = Mock()

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

    task = SimpleNamespace(task_type="resolve_url", target_id=uuid4())
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

    processed = runtime.process_next_task(session, worker_id="worker-1")

    assert processed is task
    resolve_service.resolve_source_incident_urls.assert_called_once_with(session, source_incident)
    task_repo.mark_completed.assert_called_once_with(session, task, {"urls_added": 1})


def test_task_runtime_processes_enrich_source_task_and_marks_complete():
    task_repo = Mock()
    source_repo = Mock()
    fetch_service = Mock()
    resolve_service = Mock()
    enrich_service = Mock()

    task = SimpleNamespace(task_type="enrich_source", target_id=uuid4())
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

    processed = runtime.process_next_task(session, worker_id="worker-1")

    assert processed is task
    enrich_service.enrich_source_incident.assert_called_once_with(session, source_incident)
    task_repo.mark_completed.assert_called_once_with(session, task, {"enriched": True})
