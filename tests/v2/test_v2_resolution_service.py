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
        article_discovery=lambda payload: ["https://www.cnn.com/2026/05/08/us/canvas-cyberattack-affects-universities"],
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


def test_resolve_url_service_prefers_direct_google_wrapper_resolution(monkeypatch):
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    discovery_calls = []

    service = V2ResolveUrlService(
        pipeline_task_repository=task_repo,
        article_discovery=lambda payload: discovery_calls.append(payload) or ["https://www.wikipedia.org/wiki/Canvas"],
    )
    session = Mock()
    incident = _source_incident(with_fetchable_url=False)
    incident.source_event_key = incident.urls[0].url

    monkeypatch.setattr(
        "src.edu_cti_v2.services.resolution._resolve_google_news_article_url",
        lambda _link: "https://www.reuters.com/world/us/canvas-breach-2026-05-08/",
    )

    result = service.resolve_source_incident_urls(session, incident)

    assert result == {
        "urls_discovered": 1,
        "urls_added": 1,
        "fetch_tasks_enqueued": 1,
    }
    article_rows = [row for row in incident.urls if row.url_kind == "article"]
    assert len(article_rows) == 1
    assert article_rows[0].url == "https://www.reuters.com/world/us/canvas-breach-2026-05-08/"
    assert discovery_calls == []
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

    assert result["urls_discovered"] == 0
    assert result["urls_added"] == 0
    assert result["fetch_tasks_enqueued"] == 0
    task_repo.enqueue.assert_not_called()


def test_resolve_url_service_force_discovery_with_existing_fetchable_url():
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    service = V2ResolveUrlService(
        pipeline_task_repository=task_repo,
        article_discovery=lambda payload: [
            "https://www.example-news.com/2022/11/uni-duisburg-cyber-attack"
        ],
    )
    session = Mock()
    incident = _source_incident(with_fetchable_url=True)
    incident.source_name = "konbriefing"
    incident.raw_title = "Cyber attack on a university in Germany"
    incident.raw_institution_name = "Uni Duisburg"
    incident.raw_victim_name = "Uni Duisburg"
    incident.raw_incident_date = "2022-11-27"
    incident.source_published_at = datetime(2022, 11, 27, 8, 0, tzinfo=timezone.utc)

    result = service.resolve_source_incident_urls(session, incident, force_discovery=True)

    assert result == {
        "urls_discovered": 1,
        "urls_added": 1,
        "fetch_tasks_enqueued": 1,
    }
    assert len([row for row in incident.urls if row.url_kind == "article"]) == 2
    task_repo.enqueue.assert_called_once()


def test_resolve_url_service_force_discovery_does_not_refetch_when_nothing_new():
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    service = V2ResolveUrlService(
        pipeline_task_repository=task_repo,
        article_discovery=lambda payload: ["https://example.com/article"],
    )
    session = Mock()
    incident = _source_incident(with_fetchable_url=True)

    result = service.resolve_source_incident_urls(session, incident, force_discovery=True)

    assert result == {
        "urls_discovered": 1,
        "urls_added": 0,
        "fetch_tasks_enqueued": 0,
    }
    task_repo.enqueue.assert_not_called()


def test_resolve_url_service_filters_irrelevant_discovered_urls():
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    service = V2ResolveUrlService(
        pipeline_task_repository=task_repo,
        article_discovery=lambda payload: [
            "https://www.cnn.com/2026/05/07/us/canvas-hack-strands-college-students-finals-week",
            "https://www.kentonline.co.uk/dartford/news/parents-warned-over-identity-theft-after-school-cyber-attack-237886/",
        ],
    )
    session = Mock()
    incident = _source_incident(with_fetchable_url=False)
    incident.raw_title = "Parents warned over identity theft after school cyber attack - Kent Online"
    incident.raw_institution_name = "Kent Online school cyber attack"
    incident.raw_incident_date = "2020-11-24"
    incident.source_published_at = datetime(2020, 11, 24, 8, 0, tzinfo=timezone.utc)

    result = service.resolve_source_incident_urls(session, incident)

    assert result == {
        "urls_discovered": 2,
        "urls_added": 1,
        "fetch_tasks_enqueued": 1,
    }
    added_article_urls = [row.url for row in incident.urls if row.url_kind == "article"]
    assert added_article_urls == [
        "https://www.kentonline.co.uk/dartford/news/parents-warned-over-identity-theft-after-school-cyber-attack-237886/"
    ]
    task_repo.enqueue.assert_called_once()


def test_resolve_url_service_rejects_discovered_articles_far_after_news_source_date():
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    service = V2ResolveUrlService(
        pipeline_task_repository=task_repo,
        article_discovery=lambda payload: [
            "https://www.desmoinesregister.com/story/news/education/2026/05/08/des-moines-public-schools-canvas-instructure-access-disrupted-cyberattack/89987446007/",
            "https://www.desmoinesregister.com/story/news/education/2023/02/17/data-exposed-des-moines-schools-ransomware-attack/",
        ],
    )
    session = Mock()
    incident = _source_incident(with_fetchable_url=False)
    incident.source_name = "googlenews_rss"
    incident.source_group = "rss"
    incident.raw_title = "Data exposed in Des Moines schools ransomware attack that disrupted district - The Des Moines Register"
    incident.raw_institution_name = None
    incident.raw_incident_date = "2023-02-17"
    incident.source_published_at = datetime(2023, 2, 17, 8, 0, tzinfo=timezone.utc)

    result = service.resolve_source_incident_urls(session, incident)

    assert result == {
        "urls_discovered": 2,
        "urls_added": 1,
        "fetch_tasks_enqueued": 1,
    }
    added_article_urls = [row.url for row in incident.urls if row.url_kind == "article"]
    assert added_article_urls == [
        "https://www.desmoinesregister.com/story/news/education/2023/02/17/data-exposed-des-moines-schools-ransomware-attack/"
    ]


def test_resolve_url_service_skips_fetch_when_only_irrelevant_urls_are_discovered():
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    service = V2ResolveUrlService(
        pipeline_task_repository=task_repo,
        article_discovery=lambda payload: [
            "https://www.cnn.com/2026/05/07/us/canvas-hack-strands-college-students-finals-week",
        ],
    )
    session = Mock()
    incident = _source_incident(with_fetchable_url=False)
    incident.raw_title = "Parents warned over identity theft after school cyber attack - Kent Online"
    incident.raw_institution_name = "Kent Online school cyber attack"
    incident.raw_incident_date = "2020-11-24"
    incident.source_published_at = datetime(2020, 11, 24, 8, 0, tzinfo=timezone.utc)

    result = service.resolve_source_incident_urls(session, incident)

    assert result == {
        "urls_discovered": 1,
        "urls_added": 0,
        "fetch_tasks_enqueued": 0,
    }
    assert len(incident.urls) == 1
    task_repo.enqueue.assert_not_called()


def test_resolve_url_service_skips_blocked_discovery_hosts():
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    service = V2ResolveUrlService(
        pipeline_task_repository=task_repo,
        article_discovery=lambda payload: [
            "https://en.wikipedia.org/wiki/2026_Canvas_security_incident",
            "https://www.threads.com/@news/post/abc123",
            "https://www.reuters.com/world/us/canvas-breach-2026-05-08/",
        ],
    )
    session = Mock()
    incident = _source_incident(with_fetchable_url=False)
    incident.raw_title = "Schools reach out to Canvas hackers as breach hits US classrooms, source says - Reuters"
    incident.raw_institution_name = incident.raw_title

    result = service.resolve_source_incident_urls(session, incident)

    assert result == {
        "urls_discovered": 3,
        "urls_added": 1,
        "fetch_tasks_enqueued": 1,
    }
    added_article_urls = [row.url for row in incident.urls if row.url_kind == "article"]
    assert added_article_urls == [
        "https://www.reuters.com/world/us/canvas-breach-2026-05-08/"
    ]
    task_repo.enqueue.assert_called_once()


def test_resolve_url_service_creates_structured_document_for_leak_site_api_source():
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    article_repo = Mock()
    article_repo.get_selected_document.return_value = None
    service = V2ResolveUrlService(
        pipeline_task_repository=task_repo,
        article_repository=article_repo,
        article_discovery=lambda payload: [],
    )
    session = Mock()
    incident = _source_incident(with_fetchable_url=False)
    incident.source_name = "ransomwarelive"
    incident.source_group = "api"
    incident.raw_title = "Minneapolis Public Schools"
    incident.raw_institution_name = "Minneapolis Public Schools"
    incident.raw_institution_type = "School"
    incident.raw_country = "US"
    incident.raw_attack_hint = "ransomware"
    incident.raw_threat_actor = "Medusa"
    incident.raw_payload = {
        "raw_source_payload": {
            "activity": "Education",
            "description": "Threat actor leak-site claim for Minneapolis Public Schools.",
            "website": "mpls.k12.mn.us",
        }
    }
    incident.urls = [
        SourceIncidentUrl(
            id=uuid4(),
            source_incident_id=incident.id,
            url="http://exampleonionabcd.onion/detail?id=abc",
            normalized_url="http://exampleonionabcd.onion/detail?id=abc",
            resolved_url="http://exampleonionabcd.onion/detail?id=abc",
            url_kind="leak_site",
            is_wrapper=False,
            is_primary_from_source=False,
            is_resolved_primary=False,
            created_at=incident.collected_at,
        )
    ]

    result = service.resolve_source_incident_urls(session, incident)

    assert result == {
        "urls_discovered": 0,
        "urls_added": 0,
        "fetch_tasks_enqueued": 0,
        "structured_documents_created": 1,
        "enrich_tasks_enqueued": 1,
    }
    article_repo.add_document.assert_called_once()
    document = article_repo.add_document.call_args.args[1]
    assert document.is_selected_for_enrichment is True
    assert document.document_metadata["fetch_tier"] == "structured_source"
    assert "Minneapolis Public Schools" in document.content_text
    task_repo.enqueue.assert_called_once()
    task = task_repo.enqueue.call_args.args[1]
    assert task.task_type == "enrich_source"
    assert task.payload["trigger"] == "structured_source_evidence"


def test_discovery_payload_drops_placeholder_institution_name_and_uses_victim_name():
    incident = _source_incident(with_fetchable_url=False)
    incident.raw_institution_name = "?"
    incident.raw_victim_name = "University of Example"

    payload = source_incident_to_discovery_payload(incident)

    assert payload["institution_name"] is None
    assert payload["victim_raw_name"] == "University of Example"


def test_discovery_payload_drops_google_headline_as_institution_name():
    incident = _source_incident(with_fetchable_url=False)
    incident.raw_institution_name = incident.raw_title

    payload = source_incident_to_discovery_payload(incident)

    assert payload["institution_name"] is None


def test_discovery_payload_drops_collective_commentary_institution_name():
    incident = _source_incident(with_fetchable_url=False)
    incident.raw_title = "Chinese Hackers Target 27 Universities to Acquire Military Technology"
    incident.raw_institution_name = "27 Universities to Acquire Military Technology Campus Safety Magazine"

    payload = source_incident_to_discovery_payload(incident)

    assert payload["institution_name"] is None
