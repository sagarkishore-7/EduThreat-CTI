"""Regression tests for Phase 1 ingestion orchestration."""

from unittest.mock import Mock, patch

from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.pipeline.phase1 import __main__ as phase1_main


def _incident(source: str, event_key: str) -> BaseIncident:
    return BaseIncident(
        incident_id=make_incident_id(source, event_key),
        source=source,
        source_event_id=event_key,
        university_name="",
        victim_raw_name="",
        institution_type=None,
        country=None,
        region=None,
        city=None,
        incident_date="2026-04-16",
        date_precision="day",
        source_published_date="2026-04-16",
        ingested_at="2026-04-16T00:00:00Z",
        title="Test incident",
        subtitle=None,
        primary_url=None,
        all_urls=[f"https://example.com/{event_key}"],
        leak_site_url=None,
        source_detail_url=None,
        screenshot_url=None,
        attack_type_hint=None,
        status="suspected",
        source_confidence="medium",
        notes=None,
    )


def test_ingest_group_saves_incremental_batches_without_reingesting():
    first = _incident("therecord", "story-1")
    second = _incident("therecord", "story-2")

    def _collector(*, save_callback=None, incremental=True):
        assert save_callback is not None
        assert incremental is False
        assert save_callback([first]) == 1
        assert save_callback([second]) == 1
        return {"therecord": [first, second]}

    with patch.object(phase1_main, "_ingest_batch", side_effect=[1, 1]) as mock_ingest_batch:
        total_new = phase1_main._ingest_group(
            conn=Mock(),
            label="News sources",
            collector=_collector,
            incremental=False,
        )

    assert total_new == 2
    assert mock_ingest_batch.call_count == 2
