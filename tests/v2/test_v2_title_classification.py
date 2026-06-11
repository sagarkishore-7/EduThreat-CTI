"""Unit tests for the bulk LLM title-relevance classifier and its intake gate."""

import json
from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import uuid4

import pytest

from src.edu_cti_v2.models import SourceIncident
from src.edu_cti_v2.services.intake import V2IntakeService
from src.edu_cti_v2.services.title_classification import (
    V2TitleClassificationService,
    _normalize_title,
)


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #
class FakeLLM:
    """Stand-in for OllamaLLMClient that returns canned verdicts."""

    def __init__(self, results=None, raise_exc=None):
        self.results = results if results is not None else []
        self.raise_exc = raise_exc
        self.calls = 0
        self.last_user_prompt = None

    def extract_json(self, *, system_prompt, user_prompt, schema=None, max_retries=2):
        self.calls += 1
        self.last_user_prompt = user_prompt
        if self.raise_exc is not None:
            raise self.raise_exc
        return json.dumps({"results": self.results})


def _row(title, *, group="news", subtitle="", status="pending"):
    return SourceIncident(
        id=uuid4(),
        source_name="securityweek",
        source_group=group,
        source_event_key=f"key-{uuid4()}",
        collected_at=datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc),
        raw_title=title,
        raw_subtitle=subtitle,
        ingest_hash="hash",
        raw_payload={},
        is_deleted=False,
        relevance_status=status,
    )


def _service(rows, *, llm, pending_remaining=0, intake=None):
    svc = V2TitleClassificationService(
        intake_service=intake or Mock(),
        llm_client=llm,
        batch_size=75,
        drop_confidence=0.7,
    )
    svc._lease_pending_rows = lambda session: rows
    svc._count_pending = lambda session: pending_remaining
    return svc


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #
def test_normalize_title_folds_case_and_whitespace():
    assert _normalize_title("  Ransomware  Hits   State University ") == "ransomware hits state university"
    assert _normalize_title(None) == ""


def test_is_relevant_is_recall_biased():
    svc = V2TitleClassificationService(intake_service=Mock(), llm_client=FakeLLM(), drop_confidence=0.7)
    # positive — always kept regardless of confidence
    assert svc._is_relevant({"edu_cyber": True, "confidence": 0.1}) is True
    # confident negative — dropped
    assert svc._is_relevant({"edu_cyber": False, "confidence": 0.9}) is False
    # uncertain negative — kept (coverage bias)
    assert svc._is_relevant({"edu_cyber": False, "confidence": 0.3}) is True


# --------------------------------------------------------------------------- #
# run_batch
# --------------------------------------------------------------------------- #
def test_run_batch_applies_verdicts_and_enqueues_only_relevant():
    intake = Mock()
    r_keep = _row("University X hit by ransomware")
    r_drop = _row("School guard hacked to death")
    llm = FakeLLM(results=[
        {"idx": 1, "edu_cyber": True, "confidence": 0.95, "reason": "named university breach"},
        {"idx": 2, "edu_cyber": False, "confidence": 0.92, "reason": "physical crime, not cyber"},
    ])
    svc = _service([r_keep, r_drop], llm=llm, intake=intake)

    result = svc.run_batch(Mock(), current_task_id=uuid4())

    assert r_keep.relevance_status == "relevant"
    assert r_keep.title_relevance_score == 0.95
    assert r_keep.title_classified_at is not None
    assert r_drop.relevance_status == "irrelevant"
    assert result["relevant"] == 1 and result["irrelevant"] == 1
    # only the relevant row is routed onward to fetch
    enqueued_rows = [c.args[1] for c in intake.ensure_initial_processing_task.call_args_list]
    assert enqueued_rows == [r_keep]


def test_run_batch_dedups_titles_one_llm_call_fans_out_verdict():
    intake = Mock()
    a1 = _row("State College data breach", subtitle="from source A")
    a2 = _row("state college  DATA breach", group="rss", subtitle="from source B")  # dup title
    llm = FakeLLM(results=[{"idx": 1, "edu_cyber": True, "confidence": 0.9, "reason": "ok"}])
    svc = _service([a1, a2], llm=llm, intake=intake)

    result = svc.run_batch(Mock())

    assert llm.calls == 1
    assert result["unique_titles"] == 1
    assert a1.relevance_status == "relevant" and a2.relevance_status == "relevant"
    assert intake.ensure_initial_processing_task.call_count == 2


def test_run_batch_missing_verdict_fails_open_to_relevant():
    intake = Mock()
    r = _row("Some ambiguous headline")
    llm = FakeLLM(results=[])  # valid response, no verdicts
    svc = _service([r], llm=llm, intake=intake)

    result = svc.run_batch(Mock())

    assert r.relevance_status == "relevant"
    assert result["failed_open"] == 1
    assert r.title_relevance_reason == "classifier_no_verdict_fail_open"
    intake.ensure_initial_processing_task.assert_called_once()


def test_run_batch_blank_title_fails_open_without_llm():
    intake = Mock()
    r = _row("   ")
    llm = FakeLLM(results=[])
    svc = _service([r], llm=llm, intake=intake)

    result = svc.run_batch(Mock())

    assert r.relevance_status == "relevant"
    assert result["failed_open"] == 1
    assert llm.calls == 0  # blank titles never reach the LLM


def test_run_batch_llm_failure_propagates_and_leaves_rows_pending():
    intake = Mock()
    r = _row("University X breach")
    llm = FakeLLM(raise_exc=RuntimeError("ollama down"))
    svc = _service([r], llm=llm, intake=intake)

    with pytest.raises(RuntimeError):
        svc.run_batch(Mock())

    assert r.relevance_status == "pending"  # untouched → task retries
    intake.ensure_initial_processing_task.assert_not_called()


def test_run_batch_empty_returns_zero_without_continuation():
    intake = Mock()
    svc = _service([], llm=FakeLLM(), intake=intake)

    result = svc.run_batch(Mock())

    assert result["classified"] == 0
    intake.ensure_classify_sweep_task.assert_not_called()


def test_run_batch_reenqueues_continuation_when_pending_remain():
    intake = Mock()
    r = _row("University X breach")
    task_id = uuid4()
    llm = FakeLLM(results=[{"idx": 1, "edu_cyber": True, "confidence": 0.9, "reason": "ok"}])
    svc = _service([r], llm=llm, pending_remaining=1, intake=intake)

    result = svc.run_batch(Mock(), current_task_id=task_id)

    assert result["pending_remaining"] == 1
    intake.ensure_classify_sweep_task.assert_called_once()
    _, kwargs = intake.ensure_classify_sweep_task.call_args
    assert kwargs["exclude_task_id"] == task_id


# --------------------------------------------------------------------------- #
# Intake gate routing
# --------------------------------------------------------------------------- #
def _intake_service():
    state_repo = Mock()
    article_repo = Mock()
    article_repo.get_selected_document.return_value = None
    enrich_repo = Mock()
    enrich_repo.get_by_source_incident.return_value = None
    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None
    task_repo.count_active.return_value = 0
    return V2IntakeService(
        article_repository=article_repo,
        source_state_repository=state_repo,
        source_enrichment_repository=enrich_repo,
        pipeline_task_repository=task_repo,
    ), task_repo


def test_gate_news_pending_defers_fetch_and_seeds_sweep(monkeypatch):
    monkeypatch.setenv("TITLE_CLASSIFY_ENABLED", "1")
    service, task_repo = _intake_service()
    incident = _row("Some headline", group="news", status="pending")

    task = service.ensure_initial_processing_task(Mock(), incident)

    assert task is None  # no resolve/fetch enqueued
    # a single classify_titles sweep task was seeded
    enqueued = [c.args[1] for c in task_repo.enqueue.call_args_list]
    assert len(enqueued) == 1 and enqueued[0].task_type == "classify_titles"


def test_gate_news_irrelevant_never_fetched(monkeypatch):
    monkeypatch.setenv("TITLE_CLASSIFY_ENABLED", "1")
    service, task_repo = _intake_service()
    incident = _row("School guard hacked to death", group="news", status="irrelevant")

    task = service.ensure_initial_processing_task(Mock(), incident)

    assert task is None
    task_repo.enqueue.assert_not_called()


def test_gate_news_relevant_falls_through_to_fetch(monkeypatch):
    monkeypatch.setenv("TITLE_CLASSIFY_ENABLED", "1")
    service, task_repo = _intake_service()
    incident = _row("University X ransomware", group="news", status="relevant")
    # give it an article url so it routes to fetch_article
    from src.edu_cti_v2.models import SourceIncidentUrl
    incident.urls = [
        SourceIncidentUrl(
            id=uuid4(), source_incident_id=incident.id, url="https://x/y",
            normalized_url="https://x/y", resolved_url="https://x/y", url_kind="article",
            is_wrapper=False, is_primary_from_source=True, is_resolved_primary=True,
            created_at=incident.collected_at,
        )
    ]

    task = service.ensure_initial_processing_task(Mock(), incident)

    assert task is not None and task.task_type == "fetch_article"


def test_gate_curated_bypasses_classification(monkeypatch):
    monkeypatch.setenv("TITLE_CLASSIFY_ENABLED", "1")
    service, task_repo = _intake_service()
    incident = _row("Leak-site listing", group="api", status="pending")
    incident.urls = []

    task = service.ensure_initial_processing_task(Mock(), incident)

    assert incident.relevance_status == "relevant"  # marked relevant, not classified
    assert task is not None and task.task_type == "resolve_url"


def test_gate_flag_off_is_unchanged(monkeypatch):
    monkeypatch.delenv("TITLE_CLASSIFY_ENABLED", raising=False)
    service, task_repo = _intake_service()
    incident = _row("anything", group="news", status="pending")
    incident.urls = []

    task = service.ensure_initial_processing_task(Mock(), incident)

    # legacy behavior: enqueues resolve/fetch regardless of relevance_status
    assert task is not None and task.task_type == "resolve_url"
    task_repo.enqueue.assert_called_once()
