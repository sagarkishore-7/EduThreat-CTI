"""Tests for the curated pipeline functionality."""

import pytest
from unittest.mock import patch, MagicMock
from typing import List

from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.pipeline.phase1.curated import collect_curated_incidents, run_curated_pipeline
from src.edu_cti.pipeline.phase1.base_io import write_base_csv


class TestCuratedPipeline:
    """Test curated source pipeline functionality."""
    
    @patch("src.edu_cti.pipeline.phase1.curated.get_curated_builder")
    def test_collect_curated_incidents_with_sources(self, mock_get_builder):
        """Test collecting incidents from specific curated sources."""
        # Mock curated builder
        mock_builder = MagicMock(return_value=[
            BaseIncident(
                incident_id="curated_test_1",
                source="konbriefing",
                source_event_id="event_123",
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
                title="Test Curated Incident",
                subtitle=None,
                primary_url=None,
                all_urls=["https://example.com/curated"],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint="ransomware",
                status="confirmed",
                source_confidence="high",
                notes=None,
            )
        ])
        
        mock_get_builder.return_value = mock_builder
        
        with patch("src.edu_cti.pipeline.phase1.curated.CURATED_SOURCE_REGISTRY") as mock_registry:
            mock_registry.keys.return_value = ["konbriefing"]
            mock_registry.__contains__ = lambda self, key: key == "konbriefing"
            
            results = collect_curated_incidents(sources=["konbriefing"])
            
            assert "konbriefing" in results
            assert len(results["konbriefing"]) == 1
            assert results["konbriefing"][0].incident_id == "curated_test_1"
            mock_builder.assert_called_once()


class TestIncidentId:
    """Test incident ID generation."""
    
    def test_make_incident_id_deterministic(self):
        """Test that incident IDs are deterministic."""
        id1 = make_incident_id("source", "unique_string")
        id2 = make_incident_id("source", "unique_string")
        assert id1 == id2
    
    def test_make_incident_id_different_sources(self):
        """Test that different sources produce different IDs."""
        id1 = make_incident_id("source1", "unique_string")
        id2 = make_incident_id("source2", "unique_string")
        assert id1 != id2
    
    def test_make_incident_id_format(self):
        """Test incident ID format is source_hash."""
        incident_id = make_incident_id("testsource", "unique_string")
        assert incident_id.startswith("testsource_")
        assert len(incident_id) > len("testsource_")
