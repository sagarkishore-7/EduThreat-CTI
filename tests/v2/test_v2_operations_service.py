from types import SimpleNamespace
from datetime import date, datetime, timezone
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


class _AllRowsResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _RowsSession:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, _stmt):
        return _AllRowsResult(self.rows)


def test_operations_service_runtime_status_uses_repo_and_count_queries():
    task_repo = Mock()
    task_repo.get_status_summary.return_value = [{"task_type": "fetch_article", "status": "queued", "task_count": 3}]
    task_repo.list_recent.return_value = []
    task_repo.count_expired_leases.return_value = 2

    run_repo = Mock()
    run_repo.list_recent.return_value = []

    analytics_repo = Mock()
    analytics_repo.get_by_key.return_value = SimpleNamespace(last_refreshed_at=None, needs_refresh=False)

    session = _FakeSession(execute_values=[12, 7, 6, 20, 15, 5, 4])
    service = V2OperationsService(
        pipeline_task_repository=task_repo,
        pipeline_run_repository=run_repo,
        analytics_refresh_repository=analytics_repo,
    )

    payload = service.get_runtime_status(session)

    assert payload["counts"]["source_incidents"] == 12
    assert payload["counts"]["article_documents"] == 7
    assert payload["counts"]["selected_article_sources"] == 6
    assert payload["counts"]["article_fetch_attempts"] == 20
    assert payload["counts"]["successful_article_fetch_attempts"] == 15
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


def test_operations_service_queues_recanonicalization_for_one_canonical():
    task_repo = Mock()
    task_repo.get_active_for_target.side_effect = [None, object()]

    canonical_repo = Mock()
    canonical_repo.get_by_id.return_value = SimpleNamespace(id="canonical-1")
    canonical_repo.list_memberships.return_value = [
        SimpleNamespace(source_incident_id="sid-1"),
        SimpleNamespace(source_incident_id="sid-2"),
    ]

    queued_tasks = []

    def _capture_enqueue(_session, task):
        queued_tasks.append(task)
        return task

    task_repo.enqueue.side_effect = _capture_enqueue

    service = V2OperationsService(
        pipeline_task_repository=task_repo,
        canonical_incident_repository=canonical_repo,
    )

    session = _FakeSession()
    result = service.queue_recanonicalization_for_canonical(
        session,
        canonical_incident_id="canonical-1",
    )

    assert result == {
        "canonical_incident_id": "canonical-1",
        "found": True,
        "membership_count": 2,
        "queued": 1,
        "skipped_existing": 1,
    }
    assert len(queued_tasks) == 1
    assert queued_tasks[0].payload["trigger"] == "recanonicalize_canonical"


def test_operations_service_returns_not_found_for_missing_canonical_recanonicalization_request():
    canonical_repo = Mock()
    canonical_repo.get_by_id.return_value = None

    service = V2OperationsService(
        canonical_incident_repository=canonical_repo,
    )

    session = _FakeSession()
    result = service.queue_recanonicalization_for_canonical(
        session,
        canonical_incident_id="missing",
    )

    assert result == {
        "canonical_incident_id": "missing",
        "found": False,
        "membership_count": 0,
        "queued": 0,
        "skipped_existing": 0,
    }


def test_operations_service_lists_canonical_consistency_candidates():
    canonical = SimpleNamespace(
        id=uuid4(),
        institution_name="Cincinnati Public Schools",
        vendor_name="PowerSchool",
        institution_type="education_technology_provider",
        country="United States",
        country_code="US",
        region="Arkansas",
        city="Forrest City",
        incident_date=date(2024, 12, 28),
        attack_category="third_party_compromise",
        attack_vector="stolen_credentials",
        threat_actor_name=None,
        ransomware_family=None,
        severity=None,
        is_education_related=True,
        status="open",
        updated_at=datetime(2026, 5, 9, 21, 40, tzinfo=timezone.utc),
        created_at=datetime(2026, 5, 9, 17, 21, tzinfo=timezone.utc),
    )
    canonical_enrichment = SimpleNamespace(
        analytics_projection={
            "institution_name": "PowerSchool",
            "institution_type": "education_technology_provider",
            "vendor_name": "PowerSchool",
            "country": "United States",
            "country_code": "US",
            "region": None,
            "city": None,
            "incident_date": "2024-12-28",
            "attack_category": "third_party_compromise",
            "attack_vector": "stolen_credentials",
            "threat_actor_name": None,
            "ransomware_family": None,
            "severity": None,
            "is_education_related": True,
        }
    )
    session = _RowsSession([(canonical, canonical_enrichment)])
    service = V2OperationsService()

    items = service.list_canonical_consistency_candidates(session, limit=10, scan_limit=50)

    assert len(items) == 1
    assert items[0]["display_name"] == "Cincinnati Public Schools"
    assert items[0]["mismatch_fields"] == ["institution_name"]


def test_operations_service_flags_generic_canonical_display_name_even_without_projection_drift():
    canonical = SimpleNamespace(
        id=uuid4(),
        institution_name="school district",
        vendor_name=None,
        institution_type="k12_school_district",
        country="United States",
        country_code="US",
        region=None,
        city=None,
        incident_date=None,
        attack_category="data_breach_external",
        attack_vector=None,
        threat_actor_name=None,
        ransomware_family=None,
        severity=None,
        is_education_related=True,
        status="open",
        updated_at=datetime(2026, 5, 10, 0, 0, tzinfo=timezone.utc),
        created_at=datetime(2026, 5, 9, 17, 21, tzinfo=timezone.utc),
    )
    canonical_enrichment = SimpleNamespace(
        analytics_projection={
            "institution_name": "school district",
            "institution_type": "k12_school_district",
            "vendor_name": None,
            "country": "United States",
            "country_code": "US",
            "region": None,
            "city": None,
            "incident_date": None,
            "attack_category": "data_breach_external",
            "attack_vector": None,
            "threat_actor_name": None,
            "ransomware_family": None,
            "severity": None,
            "is_education_related": True,
        }
    )
    session = _RowsSession([(canonical, canonical_enrichment)])
    service = V2OperationsService()

    items = service.list_canonical_consistency_candidates(session, limit=10, scan_limit=50)

    assert len(items) == 1
    assert items[0]["display_name"] == "school district"
    assert items[0]["mismatch_fields"] == ["generic_display_name"]


def test_operations_service_queues_canonical_consistency_sweep():
    service = V2OperationsService()
    service.list_canonical_consistency_candidates = Mock(
        return_value=[
            {"canonical_incident_id": "canonical-1"},
            {"canonical_incident_id": "canonical-2"},
        ]
    )
    service.queue_recanonicalization_for_canonical = Mock(
        side_effect=[
            {"found": True, "queued": 2, "skipped_existing": 0},
            {"found": True, "queued": 1, "skipped_existing": 1},
        ]
    )

    session = _FakeSession()
    result = service.queue_canonical_consistency_sweep(session, limit=2, scan_limit=20)

    assert result == {
        "scan_limit": 20,
        "candidates_considered": 2,
        "canonicals_queued": 2,
        "queued_tasks": 3,
        "skipped_existing_tasks": 1,
    }
