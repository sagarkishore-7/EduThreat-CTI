from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import uuid4

from src.edu_cti_v2.models import SourceEnrichment, SourceIncident
from src.edu_cti_v2.services.data_quality import MAX_REENRICH_ATTEMPTS, V2DataQualityService


def _source_incident() -> SourceIncident:
    return SourceIncident(
        id=uuid4(),
        source_name="googlenews_rss",
        source_group="rss",
        source_event_key="story-1",
        collected_at=datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc),
        source_published_at=datetime(2026, 5, 8, 8, 0, tzinfo=timezone.utc),
        raw_title="Hackers hit Penn State University in cyberattack",
        raw_institution_name="Hackers hit Penn State University in cyberattack",
        raw_victim_name="Hackers hit Penn State University in cyberattack",
        raw_institution_type="university",
        raw_country="United States",
        raw_region=None,
        raw_city=None,
        raw_incident_date="2026-13-40",
        raw_date_precision="day",
        raw_status="confirmed",
        raw_attack_hint="ransomware",
        raw_threat_actor=None,
        raw_notes=None,
        source_confidence="medium",
        ingest_hash="story-1",
        raw_payload={},
        is_deleted=False,
    )


def _source_enrichment(source_incident: SourceIncident) -> SourceEnrichment:
    return SourceEnrichment(
        id=uuid4(),
        source_incident_id=source_incident.id,
        article_document_id=uuid4(),
        llm_provider="ollama",
        llm_model="deepseek-v3.1:671b-cloud",
        raw_extraction={
            "institution_name": "Hackers hit Penn State University in cyberattack",
            "incident_date": "2026-13-40",
            "timeline": [{"date": "2099-01-01"}],
        },
        typed_enrichment={
            "institution_name": "Hackers hit Penn State University in cyberattack",
            "incident_date": "2026-13-40",
            "timeline": [{"date": "2099-01-01"}],
        },
        is_education_related=True,
        re_enrich_attempts=0,
        manual_review_required=False,
    )


def test_data_quality_service_requeues_bad_enrichment_for_reenrich():
    source_incident = _source_incident()
    enrichment = _source_enrichment(source_incident)
    enrichment_repo = Mock()
    enrichment_repo.list_for_quality_sweep.return_value = [enrichment]
    source_repo = Mock()
    source_repo.get_by_id.return_value = source_incident
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    session = Mock()

    service = V2DataQualityService(
        source_enrichment_repository=enrichment_repo,
        source_incident_repository=source_repo,
        pipeline_task_repository=task_repo,
    )

    result = service.sweep_invalid_source_enrichments(session)

    assert result["requeued_for_reenrichment"] == 1
    assert enrichment.re_enrich_attempts == 1
    assert "incident_date=" in enrichment.re_enrich_reason
    assert "timeline_dates=" in enrichment.re_enrich_reason
    task_repo.enqueue.assert_called_once()
    queued_task = task_repo.enqueue.call_args.args[1]
    assert queued_task.task_type == "reenrich"
    assert queued_task.payload["re_enrich_attempts"] == 1


def test_data_quality_service_flags_manual_review_after_max_attempts():
    source_incident = _source_incident()
    enrichment = _source_enrichment(source_incident)
    enrichment.re_enrich_attempts = MAX_REENRICH_ATTEMPTS - 1
    enrichment_repo = Mock()
    enrichment_repo.list_for_quality_sweep.return_value = [enrichment]
    source_repo = Mock()
    source_repo.get_by_id.return_value = source_incident
    task_repo = Mock()
    session = Mock()

    service = V2DataQualityService(
        source_enrichment_repository=enrichment_repo,
        source_incident_repository=source_repo,
        pipeline_task_repository=task_repo,
    )

    result = service.sweep_invalid_source_enrichments(session)

    assert result["flagged_for_manual_review"] == 1
    assert enrichment.manual_review_required is True
    assert enrichment.manual_review_reason
    task_repo.enqueue.assert_not_called()


def test_data_quality_service_flags_generic_placeholder_identity_for_reenrich():
    source_incident = _source_incident()
    source_incident.raw_title = "Officials disclose cyber incident affecting unnamed district"
    source_incident.raw_institution_name = "school district"
    source_incident.raw_victim_name = "school district"
    source_incident.raw_incident_date = "2026-05-08"

    enrichment = _source_enrichment(source_incident)
    enrichment.typed_enrichment = {
        "institution_name": "research university in Southern District of Texas",
        "incident_date": "2026-05-08",
        "timeline": [{"date": "2026-05-08"}],
    }
    enrichment.raw_extraction = {
        "institution_name": "school district",
        "incident_date": "2026-05-08",
        "timeline": [{"date": "2026-05-08"}],
    }
    enrichment_repo = Mock()
    enrichment_repo.list_for_quality_sweep.return_value = [enrichment]
    source_repo = Mock()
    source_repo.get_by_id.return_value = source_incident
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    session = Mock()

    service = V2DataQualityService(
        source_enrichment_repository=enrichment_repo,
        source_incident_repository=source_repo,
        pipeline_task_repository=task_repo,
    )

    result = service.sweep_invalid_source_enrichments(session)

    assert result["requeued_for_reenrichment"] == 1
    assert "institution_name_" in enrichment.re_enrich_reason
    task_repo.enqueue.assert_called_once()


def test_data_quality_service_flags_compound_generic_identity_for_reenrich():
    source_incident = _source_incident()
    source_incident.raw_title = "Cyber attack on a university institute in Germany"
    source_incident.raw_subtitle = "Universität Bremen, Institut für Didaktik der Naturwissenschaften - Bremen, Germany"
    source_incident.raw_institution_name = "Universität Bremen, Institut für Didaktik der Naturwissenschaften"
    source_incident.raw_victim_name = "Universität Bremen, Institut für Didaktik der Naturwissenschaften"
    source_incident.raw_incident_date = "2023-10-23"

    enrichment = _source_enrichment(source_incident)
    enrichment.typed_enrichment = {
        "institution_name": "a university institute in Germany",
        "incident_date": "2023-10-23",
        "timeline": [{"date": "2023-10-23"}],
    }
    enrichment.raw_extraction = {
        "institution_name": "a university institute in Germany",
        "incident_date": "2023-10-23",
        "timeline": [{"date": "2023-10-23"}],
    }
    enrichment_repo = Mock()
    enrichment_repo.list_for_quality_sweep.return_value = [enrichment]
    source_repo = Mock()
    source_repo.get_by_id.return_value = source_incident
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    session = Mock()

    service = V2DataQualityService(
        source_enrichment_repository=enrichment_repo,
        source_incident_repository=source_repo,
        pipeline_task_repository=task_repo,
    )

    result = service.sweep_invalid_source_enrichments(session)

    assert result["requeued_for_reenrichment"] == 1
    assert "institution_name_too_generic" in enrichment.re_enrich_reason
    task_repo.enqueue.assert_called_once()


def test_data_quality_service_flags_sentence_like_identity_for_reenrich():
    source_incident = _source_incident()
    source_incident.raw_title = "Ransomware: Refusing to Negotiate with Attackers"
    source_incident.raw_subtitle = (
        "Last week, the information security community was saddened to learn of Joseph Edwards, "
        "a 17-year-old secondary school student who committed suicide after..."
    )
    source_incident.raw_institution_name = source_incident.raw_subtitle
    source_incident.raw_victim_name = source_incident.raw_subtitle
    source_incident.raw_incident_date = None

    enrichment = _source_enrichment(source_incident)
    enrichment.typed_enrichment = {
        "institution_name": source_incident.raw_subtitle,
        "timeline": [],
    }
    enrichment.raw_extraction = {
        "institution_name": source_incident.raw_subtitle,
        "timeline": [],
    }
    enrichment_repo = Mock()
    enrichment_repo.list_for_quality_sweep.return_value = [enrichment]
    source_repo = Mock()
    source_repo.get_by_id.return_value = source_incident
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    session = Mock()

    service = V2DataQualityService(
        source_enrichment_repository=enrichment_repo,
        source_incident_repository=source_repo,
        pipeline_task_repository=task_repo,
    )

    result = service.sweep_invalid_source_enrichments(session)

    assert result["requeued_for_reenrichment"] == 1
    assert "institution_name_" in enrichment.re_enrich_reason
    task_repo.enqueue.assert_called_once()


def test_data_quality_service_clears_stale_reenrich_state_once_row_is_clean():
    source_incident = _source_incident()
    source_incident.raw_title = "Penn State University confirms cyber incident"
    source_incident.raw_institution_name = "Penn State University"
    source_incident.raw_victim_name = "Penn State University"
    source_incident.raw_incident_date = "2026-05-08"

    enrichment = _source_enrichment(source_incident)
    enrichment.typed_enrichment = {
        "institution_name": "Penn State University",
        "incident_date": "2026-05-08",
        "timeline": [{"date": "2026-05-08"}],
    }
    enrichment.raw_extraction = {
        "institution_name": "Penn State University",
        "incident_date": "2026-05-08",
        "timeline": [{"date": "2026-05-08"}],
    }
    enrichment.re_enrich_attempts = 2
    enrichment.re_enrich_reason = "old"
    enrichment_repo = Mock()
    enrichment_repo.list_for_quality_sweep.return_value = [enrichment]
    source_repo = Mock()
    source_repo.get_by_id.return_value = source_incident
    task_repo = Mock()
    session = Mock()

    service = V2DataQualityService(
        source_enrichment_repository=enrichment_repo,
        source_incident_repository=source_repo,
        pipeline_task_repository=task_repo,
    )

    result = service.sweep_invalid_source_enrichments(session)

    assert result["cleared_clean_state"] == 1
    assert enrichment.re_enrich_attempts == 0
    assert enrichment.re_enrich_reason is None
    task_repo.enqueue.assert_not_called()
