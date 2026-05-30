from datetime import datetime, timedelta, timezone
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


def _source_incident_with_urls(*urls: tuple[str, bool]) -> SourceIncident:
    incident = _source_incident()
    incident.urls = []
    for index, (url, is_primary) in enumerate(urls, start=1):
        incident.urls.append(
            SourceIncidentUrl(
                id=uuid4(),
                source_incident_id=incident.id,
                url=url,
                normalized_url=url,
                resolved_url=url,
                url_kind="article",
                is_wrapper=False,
                is_primary_from_source=is_primary,
                is_resolved_primary=is_primary,
                created_at=incident.collected_at + timedelta(seconds=index),
            )
        )
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
        "resolve_tasks_enqueued": 0,
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


def test_fetch_service_retries_discovery_when_curated_urls_all_fail():
    article_repository = Mock()
    article_repository.get_document_by_source_url.return_value = None
    pipeline_task_repository = Mock()
    pipeline_task_repository.get_active_for_target.return_value = None
    article_fetcher = Mock()
    article_fetcher.fetch_article.return_value = ArticleContent(
        url="https://example.com/stale-story",
        title="",
        content="",
        author=None,
        publish_date=None,
        fetch_successful=False,
        error_message="All fetch methods failed",
        content_length=0,
        fetch_metadata={
            "selected_tier": None,
            "tier_attempts": [
                {
                    "tier": "scrapling",
                    "success": False,
                    "latency_ms": 25,
                    "content_length": 0,
                    "error_code": "timeout",
                    "error_message": "timeout",
                },
                {
                    "tier": "oxylabs",
                    "success": False,
                    "latency_ms": 410,
                    "content_length": 42,
                    "error_code": "empty_content",
                    "error_message": "Oxylabs extracted content too short or empty",
                    "raw_content_length": 51000,
                    "extracted_content_length": 42,
                    "low_content_reason": "insufficient_extracted_content",
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
    incident = _source_incident_with_urls(("https://example.com/stale-story", True))
    incident.source_name = "comparitech"
    incident.source_group = "curated"
    incident.raw_title = "Ransomware attack on Example University (2021)"
    incident.raw_institution_name = "Example University"
    incident.raw_victim_name = "Example University"
    incident.raw_attack_hint = "ransomware"

    result = service.fetch_articles_for_source_incident(session, incident, worker_id="worker-1")

    assert result["articles_saved"] == 0
    assert result["articles_failed"] == 1
    assert result["enrich_tasks_enqueued"] == 0
    assert result["resolve_tasks_enqueued"] == 1
    task = pipeline_task_repository.enqueue.call_args.args[1]
    assert task.task_type == "resolve_url"
    assert task.payload["force_discovery"] is True
    assert task.payload["reason"] == "all_fetch_tiers_failed"
    oxylabs_attempt = article_repository.add_fetch_attempt.call_args_list[1].args[1]
    assert oxylabs_attempt.response_metadata["raw_content_length"] == 51000
    assert oxylabs_attempt.response_metadata["extracted_content_length"] == 42
    assert oxylabs_attempt.response_metadata["low_content_reason"] == "insufficient_extracted_content"


def test_fetch_service_selects_best_matching_article_instead_of_first_success():
    article_repository = Mock()
    article_repository.get_document_by_source_url.return_value = None
    pipeline_task_repository = Mock()
    pipeline_task_repository.get_active_for_target.return_value = None
    article_fetcher = Mock()
    def fetch_article(url):
        if "nytimes.com" in url:
            return ArticleContent(
                url="https://www.nytimes.com/2022/05/09/us/lincoln-college-illinois-closure.html",
                title="Lincoln College to Close, Hurt by Pandemic and Ransomware Attack",
                content="Lincoln College said a ransomware attack contributed to its closure.",
                author="Reporter",
                publish_date="2022-05-09",
                fetch_successful=True,
                error_message=None,
                content_length=68,
                fetch_metadata={"selected_tier": "httpclient", "tier_attempts": [{"tier": "httpclient", "success": True}]},
            )
        return ArticleContent(
            url="https://www.cnn.com/2026/05/07/us/canvas-hack-strands-college-students-finals-week",
            title="Canvas hack strands college students during finals week",
            content="Thousands of schools use Canvas. ShinyHunters claimed responsibility.",
            author="Reporter",
            publish_date="2026-05-07",
            fetch_successful=True,
            error_message=None,
            content_length=70,
            fetch_metadata={"selected_tier": "httpclient", "tier_attempts": [{"tier": "httpclient", "success": True}]},
        )

    article_fetcher.fetch_article.side_effect = fetch_article
    service = V2FetchService(
        article_fetcher=article_fetcher,
        article_repository=article_repository,
        pipeline_task_repository=pipeline_task_repository,
    )
    session = Mock()
    incident = _source_incident_with_urls(
        ("https://www.cnn.com/2026/05/07/us/canvas-hack-strands-college-students-finals-week", True),
        ("https://www.nytimes.com/2022/05/09/us/lincoln-college-illinois-closure.html", False),
    )
    incident.raw_title = "Lincoln College to Close, Hurt by Pandemic and Ransomware Attack - The New York Times"
    incident.raw_institution_name = "Lincoln College"
    incident.source_published_at = datetime(2022, 5, 9, 8, 0, tzinfo=timezone.utc)

    result = service.fetch_articles_for_source_incident(session, incident, worker_id="worker-1")

    assert result["articles_saved"] == 2
    assert result["enrich_tasks_enqueued"] == 1
    documents = [call.args[1] for call in article_repository.add_document.call_args_list]
    selected = [document for document in documents if document.is_selected_for_enrichment]
    assert len(selected) == 1
    assert selected[0].title == "Lincoln College to Close, Hurt by Pandemic and Ransomware Attack"
    attempts = [call.args[1] for call in article_repository.add_fetch_attempt.call_args_list]
    selected_attempts = [attempt for attempt in attempts if attempt.response_metadata["selected_for_enrichment"]]
    assert len(selected_attempts) == 1
    assert "nytimes.com" in selected_attempts[0].response_metadata["fetched_url"]


def test_fetch_service_rejects_article_far_after_news_source_date():
    article_repository = Mock()
    article_repository.get_document_by_source_url.return_value = None
    pipeline_task_repository = Mock()
    article_fetcher = Mock()
    article_fetcher.fetch_article.return_value = ArticleContent(
        url="https://www.desmoinesregister.com/story/news/education/2026/05/08/des-moines-public-schools-canvas-instructure-access-disrupted-cyberattack/89987446007/",
        title="Des Moines schools' Canvas access disrupted by cyberattack",
        content=(
            "Des Moines Public Schools is among schools affected by a Canvas "
            "cybersecurity attack disclosed by Instructure."
        ),
        author="Reporter",
        publish_date="2026-05-08",
        fetch_successful=True,
        error_message=None,
        content_length=112,
        fetch_metadata={"selected_tier": "scrapling", "tier_attempts": [{"tier": "scrapling", "success": True}]},
    )
    service = V2FetchService(
        article_fetcher=article_fetcher,
        article_repository=article_repository,
        pipeline_task_repository=pipeline_task_repository,
    )
    session = Mock()
    incident = _source_incident_with_urls(
        (
            "https://www.desmoinesregister.com/story/news/education/2026/05/08/des-moines-public-schools-canvas-instructure-access-disrupted-cyberattack/89987446007/",
            True,
        ),
    )
    incident.source_name = "googlenews_rss"
    incident.source_group = "rss"
    incident.raw_title = "Data exposed in Des Moines schools ransomware attack that disrupted district - The Des Moines Register"
    incident.raw_institution_name = None
    incident.raw_incident_date = "2023-02-17"
    incident.source_published_at = datetime(2023, 2, 17, 8, 0, tzinfo=timezone.utc)

    result = service.fetch_articles_for_source_incident(session, incident, worker_id="worker-1")

    assert result["articles_saved"] == 1
    assert result["enrich_tasks_enqueued"] == 0
    document = article_repository.add_document.call_args.args[1]
    assert document.is_selected_for_enrichment is False
    pipeline_task_repository.enqueue.assert_not_called()


def test_fetch_service_keeps_exact_title_match_with_stale_article_metadata_date():
    article_repository = Mock()
    article_repository.get_document_by_source_url.return_value = None
    pipeline_task_repository = Mock()
    pipeline_task_repository.get_active_for_target.return_value = None
    article_fetcher = Mock()
    article_fetcher.fetch_article.return_value = ArticleContent(
        url="https://example.edu/news/feu-student-portal-leak",
        title="Alleged hacker leaks FEU student portal account details",
        content="An alleged hacker leaked Far Eastern University student portal account details.",
        author="Reporter",
        publish_date="2026-05-18",
        fetch_successful=True,
        error_message=None,
        content_length=76,
        fetch_metadata={"selected_tier": "scrapling", "tier_attempts": [{"tier": "scrapling", "success": True}]},
    )
    service = V2FetchService(
        article_fetcher=article_fetcher,
        article_repository=article_repository,
        pipeline_task_repository=pipeline_task_repository,
    )
    session = Mock()
    incident = _source_incident_with_urls(("https://example.edu/news/feu-student-portal-leak", True))
    incident.source_name = "googlenews_rss"
    incident.source_group = "rss"
    incident.raw_title = "Alleged hacker leaks FEU student portal account details - FEU Advocate"
    incident.raw_institution_name = "Far Eastern University"
    incident.raw_incident_date = "2020-06-17"
    incident.source_published_at = datetime(2020, 6, 17, 8, 0, tzinfo=timezone.utc)

    result = service.fetch_articles_for_source_incident(session, incident, worker_id="worker-1")

    assert result["articles_saved"] == 1
    assert result["enrich_tasks_enqueued"] == 1
    document = article_repository.add_document.call_args.args[1]
    assert document.is_selected_for_enrichment is True
    pipeline_task_repository.enqueue.assert_called_once()


def test_fetch_service_skips_enrichment_when_no_article_is_relevant():
    article_repository = Mock()
    article_repository.get_document_by_source_url.return_value = None
    pipeline_task_repository = Mock()
    article_fetcher = Mock()
    article_fetcher.fetch_article.side_effect = [
        ArticleContent(
            url="https://www.cnn.com/2026/05/07/us/canvas-hack-strands-college-students-finals-week",
            title="Canvas hack strands college students during finals week",
            content="Thousands of schools use Canvas. ShinyHunters claimed responsibility.",
            author="Reporter",
            publish_date="2026-05-07",
            fetch_successful=True,
            error_message=None,
            content_length=70,
            fetch_metadata={"selected_tier": "httpclient", "tier_attempts": [{"tier": "httpclient", "success": True}]},
        )
    ]
    service = V2FetchService(
        article_fetcher=article_fetcher,
        article_repository=article_repository,
        pipeline_task_repository=pipeline_task_repository,
    )
    session = Mock()
    incident = _source_incident_with_urls(
        ("https://www.cnn.com/2026/05/07/us/canvas-hack-strands-college-students-finals-week", True),
    )
    incident.raw_title = "Parents warned over identity theft after school cyber attack - Kent Online"
    incident.raw_institution_name = "Kent Online school cyber attack"
    incident.source_published_at = datetime(2020, 11, 24, 8, 0, tzinfo=timezone.utc)

    result = service.fetch_articles_for_source_incident(session, incident, worker_id="worker-1")

    assert result["articles_saved"] == 1
    assert result["enrich_tasks_enqueued"] == 0
    documents = [call.args[1] for call in article_repository.add_document.call_args_list]
    assert documents[0].is_selected_for_enrichment is False
    pipeline_task_repository.enqueue.assert_not_called()


def test_fetch_service_retries_discovery_for_curated_stale_homepage():
    article_repository = Mock()
    article_repository.get_document_by_source_url.return_value = None
    pipeline_task_repository = Mock()
    pipeline_task_repository.get_active_for_target.return_value = None
    article_fetcher = Mock()
    article_fetcher.fetch_article.return_value = ArticleContent(
        url="https://www.uni-due.de/index.php",
        title="Willkommen an der Universität Duisburg-Essen",
        content=(
            "Uni Duisburg welcomes prospective students to campus. "
            "Study programmes, admissions, events and research news are available here."
        ),
        author=None,
        publish_date=None,
        fetch_successful=True,
        error_message=None,
        content_length=126,
        fetch_metadata={"selected_tier": "scrapling", "tier_attempts": [{"tier": "scrapling", "success": True}]},
    )
    service = V2FetchService(
        article_fetcher=article_fetcher,
        article_repository=article_repository,
        pipeline_task_repository=pipeline_task_repository,
    )
    session = Mock()
    incident = _source_incident_with_urls(("https://www.uni-due.de/index.php", True))
    incident.source_name = "konbriefing"
    incident.source_group = "curated"
    incident.raw_title = "Cyber attack on a university in Germany"
    incident.raw_institution_name = "Uni Duisburg"
    incident.raw_victim_name = "Uni Duisburg"
    incident.raw_incident_date = "2022-11-27"
    incident.source_published_at = datetime(2022, 11, 27, 8, 0, tzinfo=timezone.utc)

    result = service.fetch_articles_for_source_incident(session, incident, worker_id="worker-1")

    assert result["articles_saved"] == 1
    assert result["enrich_tasks_enqueued"] == 0
    assert result["resolve_tasks_enqueued"] == 1
    document = article_repository.add_document.call_args.args[1]
    assert document.is_selected_for_enrichment is False
    task = pipeline_task_repository.enqueue.call_args.args[1]
    assert task.task_type == "resolve_url"
    assert task.payload["force_discovery"] is True


def test_fetch_service_rejects_year_only_match_when_article_names_different_victim():
    article_repository = Mock()
    article_repository.get_document_by_source_url.return_value = None
    pipeline_task_repository = Mock()
    article_fetcher = Mock()
    article_fetcher.fetch_article.return_value = ArticleContent(
        url="https://www.galesburg.com/story/news/local/2022/12/09/knox-college-ransomware/",
        title="Knox College president addresses ransomware incident",
        content="Hive claimed credit for disruptions to Knox College computers.",
        author="Reporter",
        publish_date="2022-12-09",
        fetch_successful=True,
        error_message=None,
        content_length=61,
        fetch_metadata={"selected_tier": "scrapling", "tier_attempts": [{"tier": "scrapling", "success": True}]},
    )
    service = V2FetchService(
        article_fetcher=article_fetcher,
        article_repository=article_repository,
        pipeline_task_repository=pipeline_task_repository,
    )
    session = Mock()
    incident = _source_incident_with_urls(
        ("https://www.galesburg.com/story/news/local/2022/12/09/knox-college-ransomware/", True),
    )
    incident.source_name = "comparitech"
    incident.raw_title = "Ransomware attack on Guilford College (2022)"
    incident.raw_institution_name = "Guilford College"
    incident.raw_victim_name = "Guilford College"
    incident.raw_incident_date = "2022-10"
    incident.source_published_at = datetime(2022, 10, 1, 8, 0, tzinfo=timezone.utc)

    result = service.fetch_articles_for_source_incident(session, incident, worker_id="worker-1")

    assert result["articles_saved"] == 1
    assert result["enrich_tasks_enqueued"] == 0
    document = article_repository.add_document.call_args.args[1]
    assert document.is_selected_for_enrichment is False
    pipeline_task_repository.enqueue.assert_not_called()


def test_fetch_service_strips_nul_bytes_before_persisting():
    article_repository = Mock()
    article_repository.get_document_by_source_url.return_value = None
    pipeline_task_repository = Mock()
    pipeline_task_repository.get_active_for_target.return_value = None
    article_fetcher = Mock()
    article_fetcher.fetch_article.return_value = ArticleContent(
        url="https://example.com/story",
        title="University\x00 hit by ransomware",
        content="Body\x00 text",
        author="Re\x00porter",
        publish_date="2026-05-08",
        fetch_successful=True,
        error_message=None,
        content_length=10,
        fetch_metadata={"selected_tier": "httpclient", "tier_attempts": [{"tier": "httpclient", "success": True}]},
    )
    service = V2FetchService(
        article_fetcher=article_fetcher,
        article_repository=article_repository,
        pipeline_task_repository=pipeline_task_repository,
    )
    session = Mock()
    incident = _source_incident()

    result = service.fetch_articles_for_source_incident(session, incident, worker_id="worker-1")

    document = article_repository.add_document.call_args.args[1]
    assert document.title == "University hit by ransomware"
    assert document.author == "Reporter"
    assert document.content_text == "Body text"
    assert result["articles_saved"] == 1


def test_fetch_service_rejects_binary_pdf_payloads():
    article_repository = Mock()
    article_repository.get_document_by_source_url.return_value = None
    pipeline_task_repository = Mock()
    article_fetcher = Mock()
    article_fetcher.fetch_article.return_value = ArticleContent(
        url="https://example.com/document.html",
        title="",
        content="%PDF-1.7\x00binary payload",
        author=None,
        publish_date=None,
        fetch_successful=True,
        error_message=None,
        content_length=24,
        fetch_metadata={"selected_tier": "oxylabs", "tier_attempts": [{"tier": "oxylabs", "success": True}]},
    )
    service = V2FetchService(
        article_fetcher=article_fetcher,
        article_repository=article_repository,
        pipeline_task_repository=pipeline_task_repository,
    )
    session = Mock()
    incident = _source_incident()

    result = service.fetch_articles_for_source_incident(session, incident, worker_id="worker-1")

    assert article_repository.add_document.call_count == 0
    attempt = article_repository.add_fetch_attempt.call_args.args[1]
    assert attempt.success is False
    assert attempt.error_code == "binary_content"
    assert result["articles_failed"] == 1
    assert result["enrich_tasks_enqueued"] == 0
