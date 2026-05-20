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


def test_source_incident_to_base_incident_recovers_identity_from_subtitle():
    incident = _source_incident()
    incident.raw_institution_name = None
    incident.raw_victim_name = None
    incident.raw_title = "DDoS attack on the website of a university in Jerusalem, Israel"
    incident.raw_subtitle = (
        "Hebrew University of Jerusalem (HUJI) / "
        "הַאוּנִיבֶרְסִיטָה הַעִבְרִית בִּירוּשָׁלַיִם - Jerusalem / ירושלים, Israel"
    )

    base = source_incident_to_base_incident(incident, "https://example.com/article")

    assert base.institution_name == "Hebrew University of Jerusalem (HUJI)"


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

    assert outcome == {
        "enriched": False,
        "reason": "missing_article",
        "canonicalize_tasks_enqueued": 0,
        "secondary_source_incidents_created": 0,
    }
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


def test_enrichment_service_repairs_headline_identity_with_source_anchor():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    pipeline_task_repo.get_active_for_target.return_value = None
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {
        "institution_name": "University hit by ransomware",
        "attack_category": "ransomware_encryption",
    }
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "institution_name": "University hit by ransomware",
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

    outcome = service.enrich_source_incident(session, incident)

    assert outcome["enriched"] is True
    saved = source_enrichment_repo.add.call_args.args[1]
    assert saved.typed_enrichment["institution_name"] == "Penn State University"
    assert saved.raw_extraction["institution_name"] == "Penn State University"
    assert saved.raw_extraction["institution_name_basis"] == "source_anchor_fallback"


def test_enrichment_service_repairs_compound_generic_identity_with_source_anchor():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    pipeline_task_repo.get_active_for_target.return_value = None
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {
        "institution_name": "a university institute in Germany",
        "attack_category": "unauthorized_access",
    }
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "institution_name": "a university institute in Germany",
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    incident.raw_title = "Cyber attack on a university institute in Germany"
    incident.raw_subtitle = "Universität Bremen, Institut für Didaktik der Naturwissenschaften - Bremen, Germany"
    incident.raw_institution_name = "Universität Bremen, Institut für Didaktik der Naturwissenschaften"
    incident.raw_victim_name = "Universität Bremen, Institut für Didaktik der Naturwissenschaften"
    document = _article_document(incident)
    document.title = "Hackerangriff auf unseren Server und Zugang zu Unterrichtsmaterial"
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
    saved = source_enrichment_repo.add.call_args.args[1]
    assert saved.typed_enrichment["institution_name"] == "Universität Bremen, Institut für Didaktik der Naturwissenschaften"
    assert saved.raw_extraction["institution_name"] == "Universität Bremen, Institut für Didaktik der Naturwissenschaften"
    assert saved.raw_extraction["institution_name_basis"] == "source_anchor_fallback"


def test_enrichment_service_rejects_collective_commentary_without_specific_victim():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    pipeline_task_repo.get_active_for_target.return_value = None
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {
        "institution_name": "27 Universities to Acquire Military Technology Campus Safety Magazine",
        "attack_category": "espionage",
    }
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "institution_name": "27 Universities to Acquire Military Technology Campus Safety Magazine",
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    incident.raw_title = "Chinese Hackers Target 27 Universities to Acquire Military Technology"
    incident.raw_institution_name = incident.raw_title
    incident.raw_victim_name = None
    document = _article_document(incident)
    document.title = incident.raw_title
    article_repo.get_selected_document.return_value = document
    service = V2EnrichmentService(
        article_repository=article_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=pipeline_task_repo,
        enricher=enricher,
    )
    session = Mock()

    outcome = service.enrich_source_incident(session, incident)

    assert outcome["enriched"] is False
    assert outcome["is_education_related"] is False
    assert outcome["canonicalize_tasks_enqueued"] == 0
    saved = source_enrichment_repo.add.call_args.args[1]
    assert saved.typed_enrichment is None
    assert saved.raw_extraction["_not_education_related"] is True
    assert "specific victim" in saved.raw_extraction["_reason"].lower()


def test_enrichment_service_rejects_geography_only_identity_without_specific_victim():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    pipeline_task_repo.get_active_for_target.return_value = None
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {
        "institution_name": "Ukraine",
        "attack_category": "website_compromise",
    }
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "institution_name": "Ukraine",
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    incident.raw_title = "Massive attacks on Wordpress sites of Ukrainian universities"
    incident.raw_subtitle = "Ukraine"
    incident.raw_institution_name = None
    incident.raw_victim_name = None
    incident.raw_country = "Ukraine"
    document = _article_document(incident)
    document.title = incident.raw_title
    article_repo.get_selected_document.return_value = document
    service = V2EnrichmentService(
        article_repository=article_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=pipeline_task_repo,
        enricher=enricher,
    )
    session = Mock()

    outcome = service.enrich_source_incident(session, incident)

    assert outcome["enriched"] is False
    assert outcome["is_education_related"] is False
    assert outcome["canonicalize_tasks_enqueued"] == 0
    saved = source_enrichment_repo.add.call_args.args[1]
    assert saved.typed_enrichment is None
    assert saved.raw_extraction["_not_education_related"] is True
    assert "specific victim" in saved.raw_extraction["_reason"].lower()


def test_enrichment_service_rejects_victim_drift_from_source_anchor():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    pipeline_task_repo.get_active_for_target.return_value = None
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {
        "institution_name": "Check Point",
        "attack_category": "ddos_application",
    }
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "institution_name": "Check Point",
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    incident.raw_title = "DDoS attack on the website of a university in Jerusalem, Israel"
    incident.raw_subtitle = "Hebrew University of Jerusalem (HUJI) / האוניברסיטה העברית בירושלים"
    incident.raw_institution_name = None
    incident.raw_victim_name = None
    document = _article_document(incident)
    document.title = "Israeli cyber security website briefly taken down in cyberattack"
    article_repo.get_selected_document.return_value = document
    service = V2EnrichmentService(
        article_repository=article_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=pipeline_task_repo,
        enricher=enricher,
    )
    session = Mock()

    outcome = service.enrich_source_incident(session, incident)

    assert outcome["enriched"] is False
    assert outcome["is_education_related"] is None
    saved = source_enrichment_repo.add.call_args.args[1]
    assert saved.typed_enrichment is None
    assert saved.manual_review_required is True
    assert "drifted from source anchor" in saved.manual_review_reason
    assert "drifted from source anchor" in saved.raw_extraction["_reason"]


def test_enrichment_service_accepts_translated_victim_with_native_alias():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    pipeline_task_repo.get_active_for_target.return_value = None
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {
        "institution_name": "Sorbonne University",
        "institution_aliases": ["Sorbonne Université", "SU"],
        "attack_category": "unauthorized_access",
    }
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "institution_name": "Sorbonne University",
            "institution_aliases": ["Sorbonne Université", "SU"],
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    incident.raw_title = "Cyber attack on a university in France"
    incident.raw_subtitle = "Sorbonne Université - Paris, Île-de-France, France"
    incident.raw_institution_name = "Sorbonne Université"
    incident.raw_victim_name = "Sorbonne Université"
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
    saved = source_enrichment_repo.add.call_args.args[1]
    assert saved.manual_review_required is False
    assert saved.typed_enrichment["institution_name"] == "Sorbonne University"


def test_enrichment_service_accepts_acronym_variant_with_canonical_name():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    pipeline_task_repo.get_active_for_target.return_value = None
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {
        "institution_name": "Kansas State University",
        "institution_aliases": ["K-State"],
        "attack_category": "ransomware_encryption",
    }
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "institution_name": "Kansas State University",
            "institution_aliases": ["K-State"],
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    incident.raw_title = "Cyber attack on a university in Kansas, USA"
    incident.raw_subtitle = "Kansas State University (K-State) - Manhattan, Kansas, USA"
    incident.raw_institution_name = "Kansas State University (K-State)"
    incident.raw_victim_name = "Kansas State University (K-State)"
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
    saved = source_enrichment_repo.add.call_args.args[1]
    assert saved.manual_review_required is False
    assert saved.typed_enrichment["institution_name"] == "Kansas State University"


def test_enrichment_service_creates_roundup_secondary_stubs():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    source_incident_repo = Mock()
    source_incident_repo.get_by_source_event_key.return_value = None
    pipeline_task_repo = Mock()
    pipeline_task_repo.get_active_for_target.return_value = None
    intake_service = Mock()
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
            "institution_name": "Penn State University",
            "other_edu_incidents": [
                {
                    "victim_name": "Secondary College",
                    "incident_date": "2026-05-07",
                    "attack_type": ["ransomware_encryption"],
                    "country": "Canada",
                    "brief_description": "A second victim was briefly noted.",
                }
            ],
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    document = _article_document(incident)
    article_repo.get_selected_document.return_value = document
    service = V2EnrichmentService(
        article_repository=article_repo,
        source_enrichment_repository=source_enrichment_repo,
        source_incident_repository=source_incident_repo,
        pipeline_task_repository=pipeline_task_repo,
        intake_service=intake_service,
        enricher=enricher,
    )
    session = Mock()

    outcome = service.enrich_source_incident(session, incident)

    assert outcome["secondary_source_incidents_created"] == 1
    added_stub = source_incident_repo.add.call_args.args[1]
    assert added_stub.raw_institution_name == "Secondary College"
    assert added_stub.raw_attack_hint == "ransomware_encryption"
    assert added_stub.raw_notes.startswith("Extracted from roundup:")
    assert added_stub.urls == []
    intake_service.ensure_initial_processing_task.assert_called_once_with(session, added_stub)


def test_enrichment_service_skips_duplicate_roundup_secondary_stub():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    source_incident_repo = Mock()
    existing_stub = SourceIncident(
        id=uuid4(),
        source_name="therecord",
        source_group="news",
        source_event_key="existing",
        collected_at=datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc),
        ingest_hash="existing",
        raw_payload={},
        is_deleted=False,
    )
    source_incident_repo.get_by_source_event_key.return_value = existing_stub
    pipeline_task_repo = Mock()
    pipeline_task_repo.get_active_for_target.return_value = None
    intake_service = Mock()
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {
        "institution_name": "Penn State University",
    }
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "institution_name": "Penn State University",
            "other_edu_incidents": [{"victim_name": "Secondary College", "incident_date": "2026-05-07"}],
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    document = _article_document(incident)
    article_repo.get_selected_document.return_value = document
    service = V2EnrichmentService(
        article_repository=article_repo,
        source_enrichment_repository=source_enrichment_repo,
        source_incident_repository=source_incident_repo,
        pipeline_task_repository=pipeline_task_repo,
        intake_service=intake_service,
        enricher=enricher,
    )
    session = Mock()

    outcome = service.enrich_source_incident(session, incident)

    assert outcome["secondary_source_incidents_created"] == 0
    source_incident_repo.add.assert_not_called()
    intake_service.ensure_initial_processing_task.assert_called_once_with(session, existing_stub)


def test_enrichment_service_skips_primary_victim_in_roundup_secondaries():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    source_incident_repo = Mock()
    source_incident_repo.get_by_source_event_key.return_value = None
    pipeline_task_repo = Mock()
    pipeline_task_repo.get_active_for_target.return_value = None
    intake_service = Mock()
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {"institution_name": "Penn State University"}
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "institution_name": "Penn State University",
            "other_edu_incidents": [{"victim_name": "Penn State University", "incident_date": "2026-05-08"}],
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    document = _article_document(incident)
    article_repo.get_selected_document.return_value = document
    service = V2EnrichmentService(
        article_repository=article_repo,
        source_enrichment_repository=source_enrichment_repo,
        source_incident_repository=source_incident_repo,
        pipeline_task_repository=pipeline_task_repo,
        intake_service=intake_service,
        enricher=enricher,
    )
    session = Mock()

    outcome = service.enrich_source_incident(session, incident)

    assert outcome["secondary_source_incidents_created"] == 0
    source_incident_repo.add.assert_not_called()
    intake_service.ensure_initial_processing_task.assert_not_called()
