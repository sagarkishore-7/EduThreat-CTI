from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import uuid4

from src.edu_cti.pipeline.phase2.storage import ArticleContent
from src.edu_cti_v2.models import SourceIncident, SourceIncidentUrl
from src.edu_cti_v2.services import V2FetchService


def _source_incident() -> SourceIncident:
    incident = SourceIncident(
        id=uuid4(),
        source_name="therecord",
        source_group="news",
        source_event_key="story-1",
        collected_at=datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc),
        source_published_at=datetime(2026, 5, 8, 8, 0, tzinfo=timezone.utc),
        raw_title="University hit by ransomware",
        ingest_hash="story-1",
        raw_payload={},
        is_deleted=False,
    )
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
    return incident


def test_fetch_service_persists_successful_article_and_enqueues_enrichment():
    article_repository = Mock()
    article_repository.get_document_by_source_url.return_value = None
    pipeline_task_repository = Mock()
    pipeline_task_repository.get_active_for_target.return_value = None
    article_fetcher = Mock()
    article_fetcher.fetch_article.return_value = ArticleContent(
        url="https://example.com/story",
        title="University hit by ransomware",
        content="Article body",
        author="Reporter",
        publish_date="2026-05-08",
        fetch_successful=True,
        error_message=None,
        content_length=12,
        fetch_metadata={
            "selected_tier": "oxylabs",
            "tier_attempts": [
                {
                    "tier": "newspaper3k",
                    "success": False,
                    "latency_ms": 31,
                    "content_length": 0,
                    "error_code": "unknown_failure",
                    "error_message": None,
                },
                {
                    "tier": "oxylabs",
                    "success": True,
                    "latency_ms": 402,
                    "content_length": 12,
                    "error_code": None,
                    "error_message": None,
                },
            ],
        },
    )
    service = V2FetchService(
        article_fetcher=article_fetcher,
        article_repository=article_repository,
        pipeline_task_repository=pipeline_task_repository,
    )
    session = Mock()
    incident = _source_incident()

    result = service.fetch_articles_for_source_incident(session, incident, worker_id="worker-1")

    assert article_repository.add_fetch_attempt.call_count == 2
    article_repository.add_document.assert_called_once()
    pipeline_task_repository.enqueue.assert_called_once()
    success_attempt = article_repository.add_fetch_attempt.call_args_list[1].args[1]
    assert success_attempt.fetch_tier == "oxylabs"
    assert success_attempt.response_metadata["selected_for_enrichment"] is True
    assert result == {
        "urls_total": 1,
        "articles_saved": 1,
        "articles_failed": 0,
        "enrich_tasks_enqueued": 1,
    }


def test_fetch_service_records_failures_without_enqueuing_enrichment():
    article_repository = Mock()
    article_repository.get_document_by_source_url.return_value = None
    pipeline_task_repository = Mock()
    article_fetcher = Mock()
    article_fetcher.fetch_article.return_value = ArticleContent(
        url="https://example.com/story",
        title="",
        content="",
        author=None,
        publish_date=None,
        fetch_successful=False,
        error_message="timeout",
        content_length=0,
        fetch_metadata={
            "selected_tier": None,
            "tier_attempts": [
                {
                    "tier": "newspaper3k",
                    "success": False,
                    "latency_ms": 25,
                    "content_length": 0,
                    "error_code": "timeout",
                    "error_message": "timeout",
                },
                {
                    "tier": "httpclient",
                    "success": False,
                    "latency_ms": 110,
                    "content_length": 0,
                    "error_code": "timeout",
                    "error_message": "timeout",
                },
            ],
        },
    )
    service = V2FetchService(
        article_fetcher=article_fetcher,
        article_repository=article_repository,
        pipeline_task_repository=pipeline_task_repository,
    )
    session = Mock()
    incident = _source_incident()

    result = service.fetch_articles_for_source_incident(session, incident, worker_id="worker-1")

    assert article_repository.add_fetch_attempt.call_count == 2
    article_repository.add_document.assert_not_called()
    pipeline_task_repository.enqueue.assert_not_called()
    assert result["articles_saved"] == 0
    assert result["articles_failed"] == 1
    assert result["enrich_tasks_enqueued"] == 0
