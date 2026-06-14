from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import uuid4

from src.edu_cti_v2.models import ArticleDocument, SourceEnrichment, SourceIncident
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


def test_data_quality_service_requeues_dates_after_source_publication_window():
    source_incident = _source_incident()
    source_incident.source_published_at = datetime(2026, 1, 12, 8, 0, tzinfo=timezone.utc)
    source_incident.raw_incident_date = None
    enrichment = _source_enrichment(source_incident)
    enrichment.raw_extraction = {
        "institution_name": "University of Hawaii",
        "incident_date": "2026-08-01",
        "source_published_date": "2026-01-12",
        "timeline": [{"date": "2026-08-01"}],
    }
    enrichment.typed_enrichment = {
        "institution_name": "University of Hawaii",
        "incident_date": "2026-08-01",
        "source_published_date": "2026-01-12",
        "timeline": [{"date": "2026-08-01"}],
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
    assert "incident_date_after_source_published_date=" in enrichment.re_enrich_reason
    assert "timeline_dates=" in enrichment.re_enrich_reason
    task_repo.enqueue.assert_called_once()


def test_data_quality_service_allows_curated_dates_after_listing_date():
    source_incident = _source_incident()
    source_incident.source_name = "comparitech"
    source_incident.source_group = "curated"
    source_incident.source_published_at = datetime(2020, 9, 9, 8, 0, tzinfo=timezone.utc)
    source_incident.raw_incident_date = "2023-10-05"

    enrichment = _source_enrichment(source_incident)
    enrichment.raw_extraction = {
        "institution_name": "Clark County School District",
        "incident_date": "2023-10-05",
        "source_published_date": "2020-09-09",
        "timeline": [{"date": "2023-10-05"}],
    }
    enrichment.typed_enrichment = {
        "institution_name": "Clark County School District",
        "incident_date": "2023-10-05",
        "source_published_date": "2020-09-09",
        "timeline": [{"date": "2023-10-05"}],
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

    assert result["requeued_for_reenrichment"] == 0
    assert enrichment.re_enrich_reason is None
    task_repo.enqueue.assert_not_called()


def test_data_quality_service_flags_manual_review_after_max_attempts():
    source_incident = _source_incident()
    enrichment = _source_enrichment(source_incident)
    enrichment.re_enrich_attempts = MAX_REENRICH_ATTEMPTS - 1
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

    assert result["flagged_for_manual_review"] == 1
    assert result["requeued_for_canonical_cleanup"] == 1
    assert enrichment.manual_review_required is True
    assert enrichment.manual_review_reason
    task_repo.enqueue.assert_called_once()
    queued_task = task_repo.enqueue.call_args.args[1]
    assert queued_task.task_type == "canonicalize"
    assert queued_task.payload["trigger"] == "manual_review_quality_sweep"
    assert queued_task.payload["manual_review_reason"] == enrichment.manual_review_reason


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


def test_data_quality_service_flags_generic_university_health_center_identity():
    source_incident = _source_incident()
    source_incident.raw_title = "Old-School Mac OS Malware Spotted Targeting Biomedical Industry"
    source_incident.raw_institution_name = None
    source_incident.raw_victim_name = None
    source_incident.raw_incident_date = "2015-01-01"

    enrichment = _source_enrichment(source_incident)
    enrichment.typed_enrichment = {
        "institution_name": "University Health Center",
        "incident_date": "2015-01-01",
    }
    enrichment.raw_extraction = {
        "institution_name": "University Health Center",
        "incident_date": "2015-01-01",
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


def test_data_quality_service_flags_broad_collective_identity_for_reenrich():
    source_incident = _source_incident()
    source_incident.raw_title = "122 Aussie Schools & Unis Impacted In Data Breach Affecting Millions"
    source_incident.raw_institution_name = "Aussie Schools"
    source_incident.raw_victim_name = "Aussie Schools"
    source_incident.raw_incident_date = "2026-05-13"

    enrichment = _source_enrichment(source_incident)
    enrichment.typed_enrichment = {
        "institution_name": "Aussie Schools",
        "incident_date": "2026-05-13",
        "timeline": [{"date": "2026-05-13"}],
    }
    enrichment.raw_extraction = {
        "institution_name": "Aussie Schools",
        "incident_date": "2026-05-13",
        "timeline": [{"date": "2026-05-13"}],
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


def test_data_quality_service_promotes_existing_unselected_drift_documents():
    source_incident = _source_incident()
    document = ArticleDocument(
        id=uuid4(),
        source_incident_id=source_incident.id,
        source_incident_url_id=uuid4(),
        title="Canvas breach affects schools",
        content_text="Canvas cyberattack affected schools and student portal data.",
        content_hash="hash",
        document_metadata={"source_url": "https://example.com/canvas"},
        is_selected_for_enrichment=False,
        fetched_at=datetime(2026, 5, 8, 8, 0, tzinfo=timezone.utc),
    )
    session = Mock()
    session.execute.side_effect = [
        Mock(all=Mock(return_value=[(document, source_incident)])),
        Mock(first=Mock(return_value=None)),
    ]
    enrichment_repo = Mock()
    enrichment_repo.get_by_source_incident.return_value = None
    fetch_service = Mock()
    fetch_service.promote_existing_unselected_document_as_drift_candidate.return_value = True

    service = V2DataQualityService(
        source_enrichment_repository=enrichment_repo,
        source_incident_repository=Mock(),
        pipeline_task_repository=Mock(),
        fetch_service=fetch_service,
    )

    result = service.promote_drifted_unselected_articles(session, limit=25)

    assert result["scanned"] == 1
    assert result["promoted"] == 1
    fetch_service.promote_existing_unselected_document_as_drift_candidate.assert_called_once()


# --------------------------------------------------------------------------- #
# Curated recovery: requeue parked manual-review + rejected curated/api rows.
# --------------------------------------------------------------------------- #
def _enr_for(si):
    e = _source_enrichment(si)
    e.source_incident_id = si.id
    return e


def test_requeue_curated_recovers_review_and_rejected_but_skips_news():
    cur = _source_incident(); cur.source_group = "curated"; cur.source_name = "comparitech"
    api = _source_incident(); api.source_group = "api"; api.source_name = "ransomwarelive"
    news = _source_incident(); news.source_group = "news"; news.source_name = "therecord"
    review_enr = _enr_for(cur); review_enr.manual_review_required = True
    rejected_enr = _enr_for(api); rejected_enr.is_education_related = False
    news_enr = _enr_for(news); news_enr.manual_review_required = True

    enrichment_repo = Mock()
    enrichment_repo.list_manual_review_queue.return_value = [review_enr, news_enr]
    enrichment_repo.list_rejected_enrichments.return_value = [rejected_enr]
    by = {cur.id: cur, api.id: api, news.id: news}
    source_repo = Mock()
    source_repo.get_by_id.side_effect = lambda _s, sid: by.get(sid)
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None

    service = V2DataQualityService(
        source_enrichment_repository=enrichment_repo,
        source_incident_repository=source_repo,
        pipeline_task_repository=task_repo,
    )
    result = service.requeue_curated_for_reenrichment(Mock())

    assert result["requeued_for_reenrichment"] == 2   # curated + api only
    assert result["skipped_non_curated"] == 1          # news skipped
    assert review_enr.manual_review_required is False   # unblocked
    assert review_enr.re_enrich_attempts == 0
    assert task_repo.enqueue.call_count == 2
    assert {c.args[1].task_type for c in task_repo.enqueue.call_args_list} == {"reenrich"}


def test_requeue_google_wrappers_enqueues_resolve_with_force_discovery():
    gn1 = _source_incident(); gn1.source_name = "googlenews_rss"; gn1.relevance_status = "relevant"
    gn2 = _source_incident(); gn2.source_name = "googlenews_rss"; gn2.relevance_status = "relevant"
    session = Mock()
    session.execute.return_value.scalars.return_value = [gn1, gn2]
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None

    service = V2DataQualityService(pipeline_task_repository=task_repo)
    result = service.requeue_google_wrappers_for_resolution(session)

    assert result["candidates"] == 2
    assert result["requeued_for_resolution"] == 2
    assert task_repo.enqueue.call_count == 2
    tasks = [c.args[1] for c in task_repo.enqueue.call_args_list]
    assert {t.task_type for t in tasks} == {"resolve_url"}
    assert all(t.payload["force_discovery"] is True for t in tasks)
    assert all(t.payload["resolved_via"] == "google_wrapper_recovery" for t in tasks)


def test_requeue_google_wrappers_dry_run_counts_without_enqueue():
    gn = _source_incident(); gn.source_name = "googlenews_rss"; gn.relevance_status = "relevant"
    session = Mock()
    session.execute.return_value.scalars.return_value = [gn]
    task_repo = Mock()

    service = V2DataQualityService(pipeline_task_repository=task_repo)
    result = service.requeue_google_wrappers_for_resolution(session, dry_run=True)

    assert result["dry_run"] is True
    assert result["candidates"] == 1
    assert result["requeued_for_resolution"] == 0
    assert result["estimated_max_serp_queries"] == 1
    task_repo.enqueue.assert_not_called()


def test_requeue_google_wrappers_skips_rows_with_active_resolve_task():
    gn = _source_incident(); gn.source_name = "googlenews_rss"; gn.relevance_status = "relevant"
    session = Mock()
    session.execute.return_value.scalars.return_value = [gn]
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = object()  # already queued

    service = V2DataQualityService(pipeline_task_repository=task_repo)
    result = service.requeue_google_wrappers_for_resolution(session)

    assert result["requeued_for_resolution"] == 0
    assert result["already_queued"] == 1
    task_repo.enqueue.assert_not_called()


def test_requeue_curated_skips_rows_with_active_reenrich_task():
    cur = _source_incident(); cur.source_group = "curated"
    enr = _enr_for(cur); enr.manual_review_required = True
    enrichment_repo = Mock()
    enrichment_repo.list_manual_review_queue.return_value = [enr]
    enrichment_repo.list_rejected_enrichments.return_value = []
    source_repo = Mock()
    source_repo.get_by_id.return_value = cur
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = object()  # already has an active reenrich

    service = V2DataQualityService(
        source_enrichment_repository=enrichment_repo,
        source_incident_repository=source_repo,
        pipeline_task_repository=task_repo,
    )
    result = service.requeue_curated_for_reenrichment(Mock())

    assert result["requeued_for_reenrichment"] == 0
    assert result["already_queued"] == 1
    task_repo.enqueue.assert_not_called()
