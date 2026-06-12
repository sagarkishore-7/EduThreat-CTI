from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

from src.edu_cti.core.discovery_policy import (
    QUERY_SCOPED_HIGH_RECALL,
    record_source_discovery_metrics,
)
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti_v2.services.collection import V2CollectionService


def _incident(source: str, event_key: str) -> BaseIncident:
    return BaseIncident(
        incident_id=make_incident_id(source, event_key),
        source=source,
        source_event_id=event_key,
        title=f"{source} story",
        subtitle="summary",
        institution_name="Penn State University",
        victim_raw_name="Penn State University",
        institution_type="University",
        country="United States",
        region="Pennsylvania",
        city="State College",
        incident_date="2026-05-09",
        date_precision="day",
        source_published_date="2026-05-09",
        ingested_at="2026-05-09T12:00:00Z",
        primary_url=None,
        all_urls=[f"https://example.com/{source}/{event_key}"],
    )


class _FakeSession:
    def __init__(self):
        self.commits = 0
        self.flushes = 0

    def commit(self):
        self.commits += 1

    def flush(self):
        self.flushes += 1


class _FakeSessionContext:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc, tb):
        return False


def test_collection_service_collects_group_and_records_run():
    dual_writer = Mock()
    dual_writer.write_observation.side_effect = [uuid4(), uuid4()]

    run_repo = Mock()
    task_repo = Mock()
    task_repo.count_active.return_value = 0
    persisted_run = SimpleNamespace(id=uuid4())
    run_repo.get_by_id.return_value = persisted_run

    def _session_factory():
        return _FakeSessionContext(_FakeSession())

    incidents = [_incident("therecord", "story-1"), _incident("therecord", "story-2")]

    def _collector(*, max_pages, sources, save_callback, incremental):
        save_callback(incidents)
        return {"therecord": incidents}

    service = V2CollectionService(
        session_factory=_session_factory,
        dual_writer=dual_writer,
        pipeline_run_repository=run_repo,
        pipeline_task_repository=task_repo,
    )

    with patch.dict("src.edu_cti_v2.services.collection._COLLECTORS", {"news": _collector}, clear=False):
        result = service.collect_into_v2(groups=["news"], sources=["therecord"], max_pages=5)

    assert result["groups"] == ["news"]
    assert result["counts"]["incidents_collected"] == 2
    assert result["counts"]["observations_processed"] == 2
    assert result["per_source_counts"]["therecord"] == 2
    assert dual_writer.write_observation.call_count == 2
    assert run_repo.add.called
    assert run_repo.mark_started.called
    assert run_repo.mark_finished.called


def test_collection_service_exposes_source_discovery_policy_and_metrics():
    dual_writer = Mock()
    dual_writer.write_observation.side_effect = [uuid4()]
    run_repo = Mock()
    task_repo = Mock()
    task_repo.count_active.return_value = 0
    incidents = [_incident("googlenews_rss", "story-1")]

    def _collector(*, sources, max_age_days, save_callback, incremental, include_paid):
        record_source_discovery_metrics(
            "googlenews_rss",
            {
                "rss_results_seen": 2,
                "source_rows_created": 1,
                "duplicates_skipped": 1,
                "invalid_url_skipped": 0,
                "out_of_window_skipped": 0,
                "semantic_skipped": 0,
            },
        )
        save_callback(incidents)
        return {"googlenews_rss": incidents}

    service = V2CollectionService(
        session_factory=lambda: _FakeSessionContext(_FakeSession()),
        dual_writer=dual_writer,
        pipeline_run_repository=run_repo,
        pipeline_task_repository=task_repo,
    )

    with patch.dict("src.edu_cti_v2.services.collection._COLLECTORS", {"rss": _collector}, clear=False):
        result = service.collect_into_v2(
            groups=["rss"],
            sources=["googlenews_rss"],
            persist_run=False,
        )

    assert result["source_discovery_policies"] == {"googlenews_rss": QUERY_SCOPED_HIGH_RECALL}
    assert result["source_discovery_metrics"]["googlenews_rss"]["rss_results_seen"] == 2
    assert result["source_discovery_metrics"]["googlenews_rss"]["semantic_skipped"] == 0


def test_collection_service_uses_env_for_paid_rss_default(monkeypatch):
    # The legacy EDU_CTI_INCLUDE_* aliases were removed in the env-prefix cleanup;
    # the supported flag is OXYLABS_ENABLED (legacy EDU_CTI_OXYLABS_ENABLED).
    monkeypatch.setenv("OXYLABS_ENABLED", "1")
    dual_writer = Mock()
    run_repo = Mock()
    task_repo = Mock()
    task_repo.count_active.return_value = 0

    captured = {}

    def _collector(*, sources, max_age_days, save_callback, incremental, include_paid):
        captured["include_paid"] = include_paid
        return {}

    service = V2CollectionService(
        session_factory=lambda: _FakeSessionContext(_FakeSession()),
        dual_writer=dual_writer,
        pipeline_run_repository=run_repo,
        pipeline_task_repository=task_repo,
    )

    with patch.dict("src.edu_cti_v2.services.collection._COLLECTORS", {"rss": _collector}, clear=False):
        result = service.collect_into_v2(groups=["rss"], persist_run=False)

    assert captured["include_paid"] is True
    assert result["include_paid_rss"] is True
    assert result["include_paid_rss_source"] == "env"


def test_collection_service_pauses_when_fetch_backlog_is_above_limit():
    dual_writer = Mock()
    dual_writer.write_observation.side_effect = [uuid4()]

    run_repo = Mock()
    task_repo = Mock()
    task_repo.count_active.side_effect = [7, 4]
    sleeps: list[float] = []

    incidents = [_incident("therecord", "story-1")]

    def _collector(*, max_pages, sources, save_callback, incremental):
        save_callback(incidents)
        return {"therecord": incidents}

    service = V2CollectionService(
        session_factory=lambda: _FakeSessionContext(_FakeSession()),
        dual_writer=dual_writer,
        pipeline_run_repository=run_repo,
        pipeline_task_repository=task_repo,
        sleep_fn=sleeps.append,
    )

    with patch.dict("src.edu_cti_v2.services.collection._COLLECTORS", {"news": _collector}, clear=False):
        result = service.collect_into_v2(
            groups=["news"],
            sources=["therecord"],
            persist_run=False,
            fetch_backlog_limit=5,
            resolve_backlog_limit=0,
            enrich_backlog_limit=0,
            fetch_backlog_resume_ratio=1.0,
            backlog_poll_seconds=2.5,
        )

    assert result["counts"]["fetch_backpressure_wait_cycles"] == 1
    assert result["counts"]["max_fetch_backlog_observed"] == 7
    assert sleeps == [2.5]
    assert task_repo.count_active.call_count == 2


def test_collection_service_pauses_when_resolve_backlog_is_above_limit():
    dual_writer = Mock()
    dual_writer.write_observation.side_effect = [uuid4()]

    run_repo = Mock()
    task_repo = Mock()
    task_repo.count_active.side_effect = [9, 4, 3]
    sleeps: list[float] = []

    incidents = [_incident("therecord", "story-1")]

    def _collector(*, max_pages, sources, save_callback, incremental):
        save_callback(incidents)
        return {"therecord": incidents}

    service = V2CollectionService(
        session_factory=lambda: _FakeSessionContext(_FakeSession()),
        dual_writer=dual_writer,
        pipeline_run_repository=run_repo,
        pipeline_task_repository=task_repo,
        sleep_fn=sleeps.append,
    )

    with patch.dict("src.edu_cti_v2.services.collection._COLLECTORS", {"news": _collector}, clear=False):
        result = service.collect_into_v2(
            groups=["news"],
            sources=["therecord"],
            persist_run=False,
            resolve_backlog_limit=5,
            resolve_backlog_resume_ratio=1.0,
            fetch_backlog_limit=5,
            fetch_backlog_resume_ratio=1.0,
            enrich_backlog_limit=0,
            backlog_poll_seconds=1.5,
        )

    assert result["counts"]["resolve_backpressure_wait_cycles"] == 1
    assert result["counts"]["max_resolve_backlog_observed"] == 9
    assert result["counts"]["fetch_backpressure_wait_cycles"] == 0
    assert sleeps == [1.5]
    assert task_repo.count_active.call_count == 3

def test_collection_service_pauses_when_enrich_backlog_is_above_limit():
    dual_writer = Mock()
    dual_writer.write_observation.side_effect = [uuid4()]

    run_repo = Mock()
    task_repo = Mock()
    # resolve (1), fetch (1), then enrich: 8 > limit 5 -> wait, then 3 <= 5 -> exit
    task_repo.count_active.side_effect = [0, 0, 8, 3]
    sleeps: list[float] = []

    incidents = [_incident("therecord", "story-1")]

    def _collector(*, max_pages, sources, save_callback, incremental):
        save_callback(incidents)
        return {"therecord": incidents}

    service = V2CollectionService(
        session_factory=lambda: _FakeSessionContext(_FakeSession()),
        dual_writer=dual_writer,
        pipeline_run_repository=run_repo,
        pipeline_task_repository=task_repo,
        sleep_fn=sleeps.append,
    )

    with patch.dict("src.edu_cti_v2.services.collection._COLLECTORS", {"news": _collector}, clear=False):
        result = service.collect_into_v2(
            groups=["news"],
            sources=["therecord"],
            persist_run=False,
            fetch_backlog_limit=5,
            resolve_backlog_limit=5,
            enrich_backlog_limit=5,
            enrich_backlog_resume_ratio=1.0,
            fetch_backlog_resume_ratio=1.0,
            resolve_backlog_resume_ratio=1.0,
            backlog_poll_seconds=2.0,
        )

    assert result["counts"]["enrich_backpressure_wait_cycles"] == 1
    assert result["counts"]["max_enrich_backlog_observed"] == 8
    assert sleeps == [2.0]
    assert task_repo.count_active.call_count == 4
