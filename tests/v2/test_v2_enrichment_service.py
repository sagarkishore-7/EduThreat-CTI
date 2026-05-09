from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

from src.edu_cti_v2.models import ArticleDocument, SourceIncident, SourceIncidentUrl
from src.edu_cti_v2.services import V2EnrichmentService, source_incident_to_base_incident


def _source_incident() -> SourceIncident:
    incident = SourceIncident(
        id=uuid4(),
        source_name="therecord",
        source_group="news",
        source_event_key="story-1",
        collected_at=datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc),
        source_published_at=datetime(2026, 5, 8, 8, 0, tzinfo=timezone.utc),
        raw_title="University hit by ransomware",
        raw_subtitle="Detailed story",
        raw_institution_name="Penn State University",
        raw_victim_name="Penn State University",
        raw_institution_type="university",
        raw_country="United States",
        raw_region="Pennsylvania",
        raw_city="State College",
        raw_incident_date="2026-05-08",
        raw_date_precision="day",
        raw_status="confirmed",
        raw_attack_hint="ransomware",
        raw_threat_actor="SomeGroup",
        raw_notes="Records affected: 5000",
        source_confidence="high",
        ingest_hash="story-1",
        raw_payload={},
        is_deleted=False,
    )
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
    return incident


def _article_document(source_incident: SourceIncident) -> ArticleDocument:
    return ArticleDocument(
        id=uuid4(),
        source_incident_id=source_incident.id,
        source_incident_url_id=source_incident.urls[0].id,
        title="University hit by ransomware",
        author="Reporter",
        publish_date=date(2026, 5, 8),
        content_text="Long article body",
        content_hash="abc123",
        content_language="en",
        document_metadata={"source_url": "https://example.com/article"},
        is_selected_for_enrichment=True,
        fetched_at=source_incident.collected_at,
    )


def test_source_incident_to_base_incident_preserves_key_context():
    incident = _source_incident()

    base = source_incident_to_base_incident(incident, "https://example.com/article")

    assert base.incident_id == str(incident.id)
    assert base.source == "therecord"
    assert base.institution_name == "Penn State University"
    assert base.all_urls == ["https://example.com/article"]


def test_enrichment_service_persists_typed_source_enrichment():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    pipeline_task_repo.get_active_for_target.return_value = None
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {
        "institution_name": "Penn State University",
        "attack_category": "ransomware_encryption",
    }
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "confidence_score": 0.91,
            "_storage_debug": {
                "llm_metadata": {
                    "provider": "ollama",
                    "model": "deepseek-v3.1:671b-cloud",
                    "prompt_version": "phase2_prompt_v1",
                    "schema_version": "phase2_schema_v1",
                    "mapper_version": "phase2_mapper_v1",
                    "post_processing_version": "phase2_post_processing_v1",
                },
                "raw_llm_responses": {"extraction": "{\"ok\":true}"},
            },
            "institution_name": "Penn State University",
        },
    )

    incident = _source_incident()
    document = _article_document(incident)
    article_repo.get_selected_document.return_value = document
    service = V2EnrichmentService(
        article_repository=article_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=pipeline_task_repo,
        enricher=enricher,
    )
    session = Mock()

    outcome = service.enrich_source_incident(session, incident)

    assert outcome["enriched"] is True
    assert outcome["is_education_related"] is True
    assert outcome["canonicalize_tasks_enqueued"] == 1
    saved = source_enrichment_repo.add.call_args.args[1]
    assert saved.article_document_id == document.id
    assert saved.llm_model == "deepseek-v3.1:671b-cloud"
    assert saved.typed_enrichment["attack_category"] == "ransomware_encryption"
    assert saved.raw_response == {"extraction": "{\"ok\":true}"}
    pipeline_task_repo.enqueue.assert_called_once()


def test_enrichment_service_records_missing_article_failure():
    article_repo = Mock()
    article_repo.get_selected_document.return_value = None
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    service = V2EnrichmentService(
        article_repository=article_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=pipeline_task_repo,
        enricher=Mock(),
    )
    session = Mock()
    incident = _source_incident()

    outcome = service.enrich_source_incident(session, incident)

    assert outcome == {"enriched": False, "reason": "missing_article", "canonicalize_tasks_enqueued": 0}
    saved = source_enrichment_repo.add.call_args.args[1]
    assert saved.failed_reason == "No selected article available for enrichment"


def test_enrichment_service_passes_reenrich_hint_and_forces_canonicalize():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    pipeline_task_repo.get_active_for_target.return_value = None
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {"institution_name": "Penn State University"}
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": False,
            "_reason": "roundup article",
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    document = _article_document(incident)
    article_repo.get_selected_document.return_value = document
    service = V2EnrichmentService(
        article_repository=article_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=pipeline_task_repo,
        enricher=enricher,
    )
    session = Mock()

    outcome = service.enrich_source_incident(
        session,
        incident,
        re_enrich_attempts=2,
        re_enrich_reason="incident_date='2099-01-01'",
        force_canonicalize=True,
    )

    base_incident = enricher._enrich_article.call_args.args[0]
    assert base_incident.re_enrich_attempts == 2
    assert base_incident.re_enrich_reason == "incident_date='2099-01-01'"
    assert outcome["canonicalize_tasks_enqueued"] == 1
    queued = pipeline_task_repo.enqueue.call_args.args[1]
    assert queued.payload["trigger"] == "reenrich"
