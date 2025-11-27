"""Tests for RSS feed pipeline functionality."""

import pytest
from unittest.mock import patch, MagicMock
from typing import List

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase1.rss import collect_rss_incidents, run_rss_pipeline
from src.edu_cti.core.sources import RSS_SOURCE_REGISTRY


class TestRSSPipeline:
    """Test RSS feed pipeline functionality."""
    
    @patch("src.edu_cti.pipeline.phase1.rss.get_rss_builder")
    def test_collect_rss_incidents_with_sources(self, mock_get_builder):
        """Test collecting incidents from specific RSS sources."""
        # Mock RSS builder
        mock_builder = MagicMock(return_value=[
            BaseIncident(
                incident_id="rss_test_1",
                source="databreaches_rss",
                source_event_id="guid_123",
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
                title="Test RSS Incident",
                subtitle=None,
                primary_url=None,
                all_urls=["https://example.com/rss"],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            )
        ])
        
        mock_get_builder.return_value = mock_builder
        
        with patch("src.edu_cti.pipeline.phase1.rss.RSS_SOURCE_REGISTRY") as mock_registry:
            mock_registry.keys.return_value = ["databreaches_rss"]
            mock_registry.__contains__ = lambda self, key: key == "databreaches_rss"
            
            results = collect_rss_incidents(sources=["databreaches_rss"], max_age_days=1)
            
            assert "databreaches_rss" in results
            assert len(results["databreaches_rss"]) == 1
            assert results["databreaches_rss"][0].incident_id == "rss_test_1"
            mock_builder.assert_called_once_with(max_age_days=1)
    
    @patch("src.edu_cti.pipeline.phase1.rss.get_rss_builder")
    def test_collect_rss_incidents_with_max_age_days(self, mock_get_builder):
        """Test that max_age_days parameter is passed to RSS builders."""
        mock_builder = MagicMock(return_value=[])
        mock_get_builder.return_value = mock_builder
        
        with patch("src.edu_cti.pipeline.phase1.rss.RSS_SOURCE_REGISTRY") as mock_registry:
            mock_registry.keys.return_value = ["databreaches_rss"]
            mock_registry.__contains__ = lambda self, key: key == "databreaches_rss"
            
            collect_rss_incidents(sources=["databreaches_rss"], max_age_days=7)
            
            # Verify max_age_days was passed
            mock_builder.assert_called_once_with(max_age_days=7)
