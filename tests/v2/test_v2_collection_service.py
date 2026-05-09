from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

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
    persisted_run = SimpleNamespace(id=uuid4())
    run_repo.get_by_id.return_value = persisted_run

    sessions = [_FakeSession(), _FakeSession()]

    def _session_factory():
        return _FakeSessionContext(sessions.pop(0))

    incidents = [_incident("therecord", "story-1"), _incident("therecord", "story-2")]

    def _collector(*, max_pages, sources, save_callback, incremental):
        save_callback(incidents)
        return {"therecord": incidents}

    service = V2CollectionService(
        session_factory=_session_factory,
        dual_writer=dual_writer,
        pipeline_run_repository=run_repo,
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
