from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase2.enrichment import _build_target_institution_line
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


def test_target_institution_line_is_added_for_known_target_without_subtitle():
    incident = BaseIncident(
        incident_id="incident-1",
        source="comparitech",
        source_event_id="comparitech-1",
        institution_name="Southeastern Louisiana University",
        victim_raw_name="Southeastern Louisiana University",
        institution_type="university",
        country="United States",
        region=None,
        city=None,
        incident_date="2023-02-23",
        date_precision="day",
        source_published_date=None,
        ingested_at=None,
        title="Ransomware attack on Southeastern Louisiana University (2023)",
        subtitle=None,
        primary_url=None,
        all_urls=[],
    )

    line = _build_target_institution_line(
        incident,
        "5 Louisiana colleges shut down internet after security threat",
    )

    assert "TARGET INSTITUTION: Southeastern Louisiana University" in line
    assert "multiple institutions" in line


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
                "raw_llm_responses": {"extraction": '{"ok":true}'},
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
    assert saved.raw_response == {"extraction": '{"ok":true}'}
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
    incident.raw_subtitle = (
        "Universität Bremen, Institut für Didaktik der Naturwissenschaften - Bremen, Germany"
    )
    incident.raw_institution_name = (
        "Universität Bremen, Institut für Didaktik der Naturwissenschaften"
    )
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
    assert (
        saved.typed_enrichment["institution_name"]
        == "Universität Bremen, Institut für Didaktik der Naturwissenschaften"
    )
    assert (
        saved.raw_extraction["institution_name"]
        == "Universität Bremen, Institut für Didaktik der Naturwissenschaften"
    )
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


def test_enrichment_service_accepts_victim_named_in_title_despite_noisy_anchor():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    pipeline_task_repo.get_active_for_target.return_value = None
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {
        "institution_name": "University of North Florida",
        "attack_category": "data_breach_external",
    }
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "institution_name": "University of North Florida",
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    incident.raw_title = "University of North Florida Data Breach - 106,884 Individuals Potentially Exposed to Hackers"
    incident.raw_subtitle = "Related: University of Calgary pays ransom after cyberattack"
    incident.raw_institution_name = None
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

    assert outcome["enriched"] is True
    assert outcome["is_education_related"] is True
    saved = source_enrichment_repo.add.call_args.args[1]
    assert saved.manual_review_required is False
    assert saved.typed_enrichment["institution_name"] == "University of North Florida"


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


def test_enrichment_service_accepts_victim_evidenced_in_main_article_body():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    pipeline_task_repo.get_active_for_target.return_value = None
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {
        "institution_name": "Highline School District",
        "attack_category": "unauthorized_access",
    }
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "institution_name": "Highline School District",
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    incident.source_name = "googlenews_rss"
    incident.raw_title = (
        "Cyber Attack Shutters Seattle-Area School District for 2nd Day - NEWStalk 870"
    )
    incident.raw_subtitle = "Seattle-Area School District"
    incident.raw_institution_name = None
    incident.raw_victim_name = None
    document = _article_document(incident)
    document.title = "Cyber Attack Shutters Seattle-Area School District for 2nd Day"
    document.content_text = (
        "Highline School District's 17,500 students will be out of class again "
        "after a cyber attack disrupted district technology systems."
    )
    article_repo.get_selected_document.return_value = document
    service = V2EnrichmentService(
        article_repository=article_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=pipeline_task_repo,
        enricher=enricher,
    )

    outcome = service.enrich_source_incident(Mock(), incident)

    assert outcome["enriched"] is True
    saved = source_enrichment_repo.add.call_args.args[1]
    assert saved.manual_review_required is False
    assert saved.typed_enrichment["institution_name"] == "Highline School District"


def test_enrichment_service_reviews_discovery_victim_not_evidenced_in_article():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {
        "institution_name": "Ohio State University",
        "attack_category": "supply_chain_software",
    }
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "institution_name": "Ohio State University",
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    incident.source_name = "googlenews_rss"
    incident.source_group = "rss"
    incident.raw_title = "Canvas cyberattack impacts schools nationwide - Example News"
    incident.raw_subtitle = None
    incident.raw_institution_name = None
    incident.raw_victim_name = None
    document = _article_document(incident)
    document.title = "Canvas cyberattack impacts schools nationwide"
    document.content_text = (
        "Instructure said it was investigating a cyberattack against Canvas. "
        "Several school districts reported outages, but this story does not name the affected university."
    )
    article_repo.get_selected_document.return_value = document
    service = V2EnrichmentService(
        article_repository=article_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=pipeline_task_repo,
        enricher=enricher,
    )

    outcome = service.enrich_source_incident(Mock(), incident)

    assert outcome["enriched"] is False
    assert outcome["is_education_related"] is None
    saved = source_enrichment_repo.add.call_args.args[1]
    assert saved.typed_enrichment is None
    assert saved.manual_review_required is True
    assert "not supported" in saved.manual_review_reason


def test_enrichment_service_rejects_related_link_contamination():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {
        "institution_name": "University of Phoenix",
        "attack_category": "data_breach_external",
    }
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "institution_name": "University of Phoenix",
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    incident.source_name = "securityweek"
    incident.raw_title = (
        "Hacker Claims Theft of 40 Million Conde Nast Records After Wired Data Leak"
    )
    incident.raw_subtitle = "Related: 3.5 Million Affected by University of Phoenix Data Breach"
    incident.raw_institution_name = ""
    incident.raw_victim_name = ""
    document = _article_document(incident)
    document.title = incident.raw_title
    document.content_text = (
        "A hacker claimed to have stolen records from Conde Nast after a Wired data leak. "
        "The company investigated the breach and said the attacker tried to profit from the hack. "
        + ("Additional main article context. " * 60)
        + "Related: Nissan Confirms Impact From Red Hat Data Breach. "
        "Related: 3.5 Million Affected by University of Phoenix Data Breach. "
        "Written ByEduard Kovacs"
    )
    article_repo.get_selected_document.return_value = document
    service = V2EnrichmentService(
        article_repository=article_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=pipeline_task_repo,
        enricher=enricher,
    )

    outcome = service.enrich_source_incident(Mock(), incident)

    assert outcome["enriched"] is False
    assert outcome["is_education_related"] is False
    saved = source_enrichment_repo.add.call_args.args[1]
    assert saved.typed_enrichment is None
    assert saved.manual_review_required is False
    assert "related-story" in saved.failed_reason


def test_enrichment_service_reviews_multi_victim_list_instead_of_rejecting():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    source_incident_repo = Mock()
    source_incident_repo.get_by_source_event_key.return_value = None
    pipeline_task_repo = Mock()
    intake_service = Mock()
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {
        "institution_name": "Virginia Beach City Public Schools",
        "attack_category": "supply_chain_software",
    }
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "institution_name": "Virginia Beach City Public Schools",
            "other_edu_incidents": [
                {
                    "victim_name": "Chesapeake Public Schools",
                    "attack_type": "supply_chain_software",
                    "country": "United States",
                }
            ],
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    incident.source_name = "googlenews_rss"
    incident.source_group = "rss"
    incident.raw_title = "Hackers claim Virginia schools are impacted by Canvas cyberattack"
    incident.raw_subtitle = None
    incident.raw_institution_name = None
    incident.raw_victim_name = None
    document = _article_document(incident)
    document.title = incident.raw_title
    document.content_text = (
        "A Canvas cyberattack affected several Virginia education institutions. "
        + ("The investigation is ongoing. " * 60)
        + "Related: Virginia Beach City Public Schools, Chesapeake Public Schools, "
        "and Old Dominion University were named in an attached list."
    )
    article_repo.get_selected_document.return_value = document
    service = V2EnrichmentService(
        article_repository=article_repo,
        source_enrichment_repository=source_enrichment_repo,
        source_incident_repository=source_incident_repo,
        pipeline_task_repository=pipeline_task_repo,
        intake_service=intake_service,
        enricher=enricher,
    )

    outcome = service.enrich_source_incident(Mock(), incident)

    assert outcome["enriched"] is False
    assert outcome["is_education_related"] is None
    saved = source_enrichment_repo.add.call_args.args[1]
    assert saved.typed_enrichment is None
    assert saved.manual_review_required is True
    assert "multiple education victims" in saved.manual_review_reason


def test_enrichment_service_reviews_curated_target_when_selected_article_is_unrelated():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {
        "enriched_summary": "This article is not an education-sector cyber incident.",
    }
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": False,
            "education_relevance_reasoning": "The selected article is about a school construction budget.",
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    incident.source_name = "comparitech"
    incident.source_group = "curated"
    incident.raw_title = "Ransomware attack on Neenah School District (2022)"
    incident.raw_institution_name = "Neenah School District"
    incident.raw_victim_name = "Neenah School District"
    incident.raw_attack_hint = "ransomware"
    document = _article_document(incident)
    document.title = "Higher labor, material costs push Neenah High School project over budget"
    document.content_text = "The article discusses school construction inflation and budgets."
    article_repo.get_selected_document.return_value = document
    service = V2EnrichmentService(
        article_repository=article_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=pipeline_task_repo,
        enricher=enricher,
    )

    outcome = service.enrich_source_incident(Mock(), incident)

    assert outcome["enriched"] is False
    assert outcome["is_education_related"] is None
    assert outcome["canonicalize_tasks_enqueued"] == 0
    saved = source_enrichment_repo.add.call_args.args[1]
    assert saved.typed_enrichment is None
    assert saved.manual_review_required is True
    assert "supporting article" in saved.manual_review_reason


def test_enrichment_service_keeps_structured_source_multi_victim_drift_in_review():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {
        "institution_name": "Butler County Community College",
        "attack_category": "ransomware_encryption",
    }
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "institution_name": "Butler County Community College",
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    incident.source_name = "comparitech"
    incident.raw_title = "Ransomware attack on Lewis and Clark Community College (2021)"
    incident.raw_institution_name = "Lewis and Clark Community College"
    incident.raw_victim_name = "Lewis and Clark Community College"
    document = _article_document(incident)
    document.title = "2 More Community Colleges Targeted by Ransomware"
    document.content_text = (
        "Two community colleges were victims of ransomware attacks. "
        "Butler County Community College in Pennsylvania and Lewis and Clark "
        "Community College in Illinois remain closed as officials respond."
    )
    article_repo.get_selected_document.return_value = document
    service = V2EnrichmentService(
        article_repository=article_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=pipeline_task_repo,
        enricher=enricher,
    )

    outcome = service.enrich_source_incident(Mock(), incident)

    assert outcome["enriched"] is False
    assert outcome["is_education_related"] is None
    saved = source_enrichment_repo.add.call_args.args[1]
    assert saved.typed_enrichment is None
    assert saved.manual_review_required is True
    assert "structured source target" in saved.manual_review_reason


def test_enrichment_service_trims_related_tail_before_llm():
    article_repo = Mock()
    source_enrichment_repo = Mock()
    source_enrichment_repo.get_by_source_incident.return_value = None
    pipeline_task_repo = Mock()
    pipeline_task_repo.get_active_for_target.return_value = None
    enricher = Mock()
    result_model = Mock()
    result_model.model_dump.return_value = {"institution_name": "Saint Xavier University"}
    enricher._enrich_article.return_value = (
        result_model,
        {
            "is_edu_cyber_incident": True,
            "institution_name": "Saint Xavier University",
            "_storage_debug": {"llm_metadata": {}, "raw_llm_responses": {}},
        },
    )

    incident = _source_incident()
    document = _article_document(incident)
    document.title = "210,000 Impacted by Saint Xavier University Data Breach"
    document.content_text = (
        "Saint Xavier University notified individuals that their personal information "
        "was compromised in a data breach. "
        + ("The investigation is ongoing. " * 60)
        + "Related: University of Phoenix Data Breach. Written ByReporter"
    )
    article_repo.get_selected_document.return_value = document
    service = V2EnrichmentService(
        article_repository=article_repo,
        source_enrichment_repository=source_enrichment_repo,
        pipeline_task_repository=pipeline_task_repo,
        enricher=enricher,
    )

    service.enrich_source_incident(Mock(), incident)

    article_map = enricher._enrich_article.call_args.args[1]
    article = next(iter(article_map.values()))
    assert "Saint Xavier University" in article.content
    assert "University of Phoenix" not in article.content


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
            "other_edu_incidents": [
                {"victim_name": "Secondary College", "incident_date": "2026-05-07"}
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
            "other_edu_incidents": [
                {"victim_name": "Penn State University", "incident_date": "2026-05-08"}
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

    assert outcome["secondary_source_incidents_created"] == 0
    source_incident_repo.add.assert_not_called()
    intake_service.ensure_initial_processing_task.assert_not_called()
