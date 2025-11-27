"""Tests for incremental save functionality."""

import pytest
import tempfile
import os
from typing import List

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase1.incremental_save import IncrementalSaver


class TestIncrementalSaver:
    """Test IncrementalSaver class."""
    
    def test_saver_initialization(self):
        """Test IncrementalSaver initializes correctly."""
        def mock_save(incidents: List[BaseIncident]) -> None:
            pass
        
        saver = IncrementalSaver(save_func=mock_save, batch_size=10)
        assert saver.batch_size == 10
        assert len(saver.batch) == 0
    
    def test_saver_adds_incidents_to_batch(self):
        """Test that incidents are added to batch."""
        saved = []
        
        def mock_save(incidents: List[BaseIncident]) -> None:
            saved.extend(incidents)
        
        saver = IncrementalSaver(save_func=mock_save, batch_size=5)
        
        # Add less than batch_size incidents
        for i in range(3):
            incident = BaseIncident(
                incident_id=f"test_{i}",
                source="test",
                source_event_id=f"event_{i}",
                university_name="Test University",
                victim_raw_name="Test University",
                institution_type="University",
                country="US",
                region=None,
                city=None,
                incident_date="2024-01-01",
                date_precision="day",
                source_published_date="2024-01-01",
                ingested_at="2024-01-01T00:00:00Z",
                title=f"Test Incident {i}",
                subtitle=None,
                primary_url=None,
                all_urls=[f"https://example.com/{i}"],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            )
            saver.add(incident)
        
        # Should not have saved yet (batch not full)
        assert len(saved) == 0
        assert len(saver.batch) == 3
    
    def test_saver_flushes_on_batch_full(self):
        """Test that batch is saved when full."""
        saved = []
        
        def mock_save(incidents: List[BaseIncident]) -> None:
            saved.extend(incidents)
        
        saver = IncrementalSaver(save_func=mock_save, batch_size=2)
        
        for i in range(3):
            incident = BaseIncident(
                incident_id=f"test_{i}",
                source="test",
                source_event_id=f"event_{i}",
                university_name="Test University",
                victim_raw_name="Test University",
                institution_type="University",
                country="US",
                region=None,
                city=None,
                incident_date="2024-01-01",
                date_precision="day",
                source_published_date="2024-01-01",
                ingested_at="2024-01-01T00:00:00Z",
                title=f"Test Incident {i}",
                subtitle=None,
                primary_url=None,
                all_urls=[f"https://example.com/{i}"],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            )
            saver.add(incident)
        
        # Should have saved first 2 incidents
        assert len(saved) == 2
        assert len(saver.batch) == 1
    
    def test_saver_flush_saves_remaining(self):
        """Test that flush saves remaining incidents."""
        saved = []
        
        def mock_save(incidents: List[BaseIncident]) -> None:
            saved.extend(incidents)
        
        saver = IncrementalSaver(save_func=mock_save, batch_size=10)
        
        for i in range(3):
            incident = BaseIncident(
                incident_id=f"test_{i}",
                source="test",
                source_event_id=f"event_{i}",
                university_name="Test University",
                victim_raw_name="Test University",
                institution_type="University",
                country="US",
                region=None,
                city=None,
                incident_date="2024-01-01",
                date_precision="day",
                source_published_date="2024-01-01",
                ingested_at="2024-01-01T00:00:00Z",
                title=f"Test Incident {i}",
                subtitle=None,
                primary_url=None,
                all_urls=[f"https://example.com/{i}"],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            )
            saver.add(incident)
        
        # Not saved yet
        assert len(saved) == 0
        
        # Flush remaining
        saver.flush()
        
        # Now should be saved
        assert len(saved) == 3
        assert len(saver.batch) == 0
