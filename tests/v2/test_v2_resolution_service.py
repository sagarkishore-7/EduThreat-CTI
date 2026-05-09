from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import uuid4

from src.edu_cti_v2.models import SourceIncident, SourceIncidentUrl
from src.edu_cti_v2.services import V2ResolveUrlService, determine_initial_task_type
from src.edu_cti_v2.services.resolution import source_incident_to_discovery_payload


def _source_incident(*, with_fetchable_url: bool = False) -> SourceIncident:
    incident = SourceIncident(
        id=uuid4(),
        source_name="googlenews_rss",
        source_group="rss",
        source_event_key="story-1",
        collected_at=datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc),
        source_published_at=datetime(2026, 5, 8, 8, 0, tzinfo=timezone.utc),
        raw_title="Canvas cyberattack affects universities",
        raw_institution_name="Canvas",
        raw_attack_hint="cyberattack",
        raw_incident_date="2026-05-08",
        ingest_hash="story-1",
        raw_payload={},
        is_deleted=False,
    )
    if with_fetchable_url:
        incident.urls = [
            SourceIncidentUrl(
                id=uuid4(),
                source_incident_id=incident.id,
                url="https://example.com/article",
                normalized_url="https://example.com/article",
                resolved_url="https://example.com/article",
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


def test_resolve_url_service_adds_discovered_article_and_enqueues_fetch():
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    service = V2ResolveUrlService(
        pipeline_task_repository=task_repo,
        article_discovery=lambda payload: ["https://example.com/article"],
    )
    session = Mock()
    incident = _source_incident(with_fetchable_url=False)

    result = service.resolve_source_incident_urls(session, incident)

    assert result == {
        "urls_discovered": 1,
        "urls_added": 1,
        "fetch_tasks_enqueued": 1,
    }
    assert len(incident.urls) == 2
    assert determine_initial_task_type(incident) == "fetch_article"
    task_repo.enqueue.assert_called_once()


def test_resolve_url_service_reuses_existing_fetch_task_and_dedupes_urls():
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = object()
    service = V2ResolveUrlService(
        pipeline_task_repository=task_repo,
        article_discovery=lambda payload: ["https://example.com/article"],
    )
    session = Mock()
    incident = _source_incident(with_fetchable_url=True)

    result = service.resolve_source_incident_urls(session, incident)

    assert result["urls_discovered"] == 1
    assert result["urls_added"] == 0
    assert result["fetch_tasks_enqueued"] == 0
    task_repo.enqueue.assert_not_called()


def test_discovery_payload_drops_placeholder_institution_name_and_uses_victim_name():
    incident = _source_incident(with_fetchable_url=False)
    incident.raw_institution_name = "?"
    incident.raw_victim_name = "University of Example"

    payload = source_incident_to_discovery_payload(incident)

    assert payload["institution_name"] is None
    assert payload["victim_raw_name"] == "University of Example"
