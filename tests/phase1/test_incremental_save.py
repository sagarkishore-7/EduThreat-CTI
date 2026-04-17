"""Tests for incremental save functionality."""

from typing import List

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase1.incremental_save import IncrementalSaver


def _make_incident(index: int) -> BaseIncident:
    return BaseIncident(
        incident_id=f"test_{index}",
        source="test",
        source_event_id=f"event_{index}",
        institution_name="Test University",
        victim_raw_name="Test University",
        institution_type="University",
        country="US",
        region=None,
        city=None,
        incident_date="2024-01-01",
        date_precision="day",
        source_published_date="2024-01-01",
        ingested_at="2024-01-01T00:00:00Z",
        title=f"Test Incident {index}",
        subtitle=None,
        primary_url=None,
        all_urls=[f"https://example.com/{index}"],
        leak_site_url=None,
        source_detail_url=None,
        screenshot_url=None,
        attack_type_hint=None,
        status="suspected",
        source_confidence="medium",
        notes=None,
    )


class TestIncrementalSaver:
    """Test IncrementalSaver class."""

    def test_saver_initialization(self):
        """Test IncrementalSaver initializes correctly."""

        def mock_save(incidents: List[BaseIncident]) -> int:
            return len(incidents)

        saver = IncrementalSaver(save_callback=mock_save, batch_size=10, source_name="test")
        assert saver.batch_size == 10
        assert saver.buffer == []
        assert saver.total_saved == 0
        assert saver.total_processed == 0

    def test_saver_adds_incidents_to_buffer(self):
        """Test that incidents are buffered until the batch is full."""
        saved = []

        def mock_save(incidents: List[BaseIncident]) -> int:
            saved.extend(incidents)
            return len(incidents)

        saver = IncrementalSaver(save_callback=mock_save, batch_size=5)

        for i in range(3):
            saver.add(_make_incident(i))

        assert saved == []
        assert len(saver.buffer) == 3
        assert saver.total_processed == 3

    def test_saver_flushes_on_batch_full(self):
        """Test that a full batch is saved automatically."""
        saved = []

        def mock_save(incidents: List[BaseIncident]) -> int:
            saved.extend(incidents)
            return len(incidents)

        saver = IncrementalSaver(save_callback=mock_save, batch_size=2)

        for i in range(3):
            saver.add(_make_incident(i))

        assert [incident.incident_id for incident in saved] == ["test_0", "test_1"]
        assert len(saver.buffer) == 1
        assert saver.total_saved == 2
        assert saver.total_processed == 3

    def test_saver_flush_saves_remaining_buffer(self):
        """Test that flush saves buffered incidents immediately."""
        saved = []

        def mock_save(incidents: List[BaseIncident]) -> int:
            saved.extend(incidents)
            return len(incidents)

        saver = IncrementalSaver(save_callback=mock_save, batch_size=10)

        for i in range(3):
            saver.add(_make_incident(i))

        assert saver.flush() == 3
        assert [incident.incident_id for incident in saved] == [
            "test_0",
            "test_1",
            "test_2",
        ]
        assert saver.buffer == []
        assert saver.total_saved == 3

    def test_finish_returns_total_saved_across_auto_and_final_flushes(self):
        """Test that finish flushes the remainder and returns the total saved."""
        saved = []

        def mock_save(incidents: List[BaseIncident]) -> int:
            saved.extend(incidents)
            return len(incidents)

        saver = IncrementalSaver(save_callback=mock_save, batch_size=2)

        for i in range(3):
            saver.add(_make_incident(i))

        assert saver.finish() == 3
        assert len(saved) == 3
        assert saver.buffer == []
