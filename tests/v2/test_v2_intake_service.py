from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

from src.edu_cti_v2.models import SourceIncident, SourceIncidentUrl
from src.edu_cti_v2.services import V2IntakeService, determine_initial_task_type


def _source_incident(*, with_article_url: bool = True) -> SourceIncident:
    incident = SourceIncident(
        id=uuid4(),
        source_name="googlenews_rss",
        source_group="rss",
        source_event_key="story-123",
        collected_at=datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc),
        source_published_at=datetime(2026, 5, 8, 0, 0, tzinfo=timezone.utc),
        raw_title="Canvas outage causes university disruption",
        ingest_hash="googlenews_rss_hash",
        raw_payload={},
        is_deleted=False,
    )
    if with_article_url:
        incident.urls = [
            SourceIncidentUrl(
                id=uuid4(),
                source_incident_id=incident.id,
                url="https://example.com/story",
                normalized_url="https://example.com/story",
                resolved_url="https://example.com/story",
                url_kind="article",
                is_wrapper=False,
                is_primary_from_source=True,
                is_resolved_primary=True,
                created_at=incident.collected_at,
            )
        ]
    else:
        incident.urls = [
            SourceIncidentUrl(
                id=uuid4(),
                source_incident_id=incident.id,
                url="https://news.google.com/rss/articles/CBMi...",
                normalized_url="https://news.google.com/rss/articles/CBMi...",
                resolved_url=None,
                url_kind="rss_wrapper",
                is_wrapper=True,
                is_primary_from_source=True,
                is_resolved_primary=False,
                created_at=incident.collected_at,
            )
        ]
    return incident


def test_determine_initial_task_type_prefers_fetch_when_article_url_exists():
    assert determine_initial_task_type(_source_incident(with_article_url=True)) == "fetch_article"


def test_determine_initial_task_type_uses_resolve_without_real_article_url():
    assert determine_initial_task_type(_source_incident(with_article_url=False)) == "resolve_url"


def test_record_incremental_state_upserts_latest_source_marker():
    state_repo = Mock()
    task_repo = Mock()
    service = V2IntakeService(
        source_state_repository=state_repo,
        pipeline_task_repository=task_repo,
    )
    session = Mock()
    incident = _source_incident()

    service.record_incremental_state(session, incident)

    _, kwargs = state_repo.upsert_state.call_args
    assert kwargs["source_name"] == "googlenews_rss"
    assert kwargs["last_seen_published_at"] == incident.source_published_at
    assert kwargs["state_payload"]["latest_source_event_key"] == "story-123"
    assert kwargs["state_payload"]["latest_ingest_hash"] == "googlenews_rss_hash"


def test_ensure_initial_processing_task_enqueues_fetch_task_once():
    state_repo = Mock()
    article_repo = Mock()
    article_repo.get_selected_document.return_value = None
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    service = V2IntakeService(
        article_repository=article_repo,
        source_state_repository=state_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=task_repo,
    )
    session = Mock()
    incident = _source_incident(with_article_url=True)

    task = service.ensure_initial_processing_task(session, incident)

    task_repo.enqueue.assert_called_once()
    assert task.task_type == "fetch_article"
    assert task.target_table == "source_incidents"
    assert task.target_id == incident.id
    assert task.priority == 60
    assert task.payload["source_incident_id"] == str(incident.id)


def test_ensure_initial_processing_task_enqueues_lower_priority_resolve_task():
    state_repo = Mock()
    article_repo = Mock()
    article_repo.get_selected_document.return_value = None
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    service = V2IntakeService(
        article_repository=article_repo,
        source_state_repository=state_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=task_repo,
    )
    session = Mock()
    incident = _source_incident(with_article_url=False)

    task = service.ensure_initial_processing_task(session, incident)

    assert task.task_type == "resolve_url"
    assert task.priority == 20


def test_ensure_initial_processing_task_reuses_existing_active_task():
    state_repo = Mock()
    article_repo = Mock()
    article_repo.get_selected_document.return_value = None
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    task_repo = Mock()
    existing = SimpleNamespace(task_type="resolve_url")
    task_repo.get_active_for_target.return_value = existing
    service = V2IntakeService(
        article_repository=article_repo,
        source_state_repository=state_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=task_repo,
    )
    session = Mock()
    incident = _source_incident(with_article_url=False)

    task = service.ensure_initial_processing_task(session, incident)

    assert task is existing
    task_repo.enqueue.assert_not_called()


def test_ensure_initial_processing_task_skips_already_enriched_source():
    state_repo = Mock()
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = object()
    task_repo = Mock()
    service = V2IntakeService(
        article_repository=article_repo,
        source_state_repository=state_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=task_repo,
    )
    session = Mock()
    incident = _source_incident(with_article_url=True)

    task = service.ensure_initial_processing_task(session, incident)

    assert task is None
    article_repo.get_selected_document.assert_not_called()
    task_repo.get_active_for_target.assert_not_called()
    task_repo.enqueue.assert_not_called()


def test_ensure_initial_processing_task_enqueues_enrichment_for_existing_selected_article():
    state_repo = Mock()
    article_repo = Mock()
    article_repo.get_selected_document.return_value = object()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    service = V2IntakeService(
        article_repository=article_repo,
        source_state_repository=state_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=task_repo,
    )
    session = Mock()
    incident = _source_incident(with_article_url=True)

    task = service.ensure_initial_processing_task(session, incident)

    task_repo.enqueue.assert_called_once()
    assert task.task_type == "enrich_source"
    assert task.priority == 80
