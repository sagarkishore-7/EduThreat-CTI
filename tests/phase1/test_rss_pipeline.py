"""Tests for RSS feed pipeline functionality."""

import pytest
from unittest.mock import patch, MagicMock
from typing import List

from src.edu_cti.models import BaseIncident
from src.edu_cti.pipelines.rss import collect_rss_incidents, run_rss_pipeline
from src.edu_cti.sources import RSS_SOURCE_REGISTRY


class TestRSSPipeline:
    """Test RSS feed pipeline functionality."""
    
    @patch("src.edu_cti.pipelines.rss.get_rss_builder")
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
        
        with patch("src.edu_cti.pipelines.rss.RSS_SOURCE_REGISTRY") as mock_registry:
            mock_registry.keys.return_value = ["databreaches_rss"]
            mock_registry.__contains__ = lambda self, key: key == "databreaches_rss"
            
            results = collect_rss_incidents(sources=["databreaches_rss"], max_age_days=1)
            
            assert "databreaches_rss" in results
            assert len(results["databreaches_rss"]) == 1
            assert results["databreaches_rss"][0].incident_id == "rss_test_1"
            mock_builder.assert_called_once_with(max_age_days=1)
    
    @patch("src.edu_cti.pipelines.rss.get_rss_builder")
    def test_collect_rss_incidents_with_max_age_days(self, mock_get_builder):
        """Test that max_age_days parameter is passed to RSS builders."""
        mock_builder = MagicMock(return_value=[])
        mock_get_builder.return_value = mock_builder
        
        with patch("src.edu_cti.pipelines.rss.RSS_SOURCE_REGISTRY") as mock_registry:
            mock_registry.keys.return_value = ["databreaches_rss"]
            mock_registry.__contains__ = lambda self, key: key == "databreaches_rss"
            
            collect_rss_incidents(sources=["databreaches_rss"], max_age_days=7)
            
            # Verify max_age_days was passed
            mock_builder.assert_called_once_with(max_age_days=7)
    
    @patch("src.edu_cti.pipelines.rss.get_rss_builder")
    def test_collect_rss_incidents_all_sources(self, mock_get_builder):
        """Test collecting from all RSS sources when sources=None."""
        mock_builder = MagicMock(return_value=[])
        mock_get_builder.return_value = mock_builder
        
        with patch("src.edu_cti.pipelines.rss.RSS_SOURCE_REGISTRY") as mock_registry:
            mock_registry.keys.return_value = ["databreaches_rss", "source2"]
            mock_registry.__contains__ = lambda self, key: key in ["databreaches_rss", "source2"]
            
            results = collect_rss_incidents(sources=None, max_age_days=1)
            
            # Should have called builder for each source
            assert mock_builder.call_count == 2
            assert len(results) == 2
    
    @patch("src.edu_cti.pipelines.rss.collect_rss_incidents")
    def test_run_rss_pipeline(self, mock_collect):
        """Test run_rss_pipeline function."""
        mock_incidents = [
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
        ]
        
        mock_collect.return_value = {"databreaches_rss": mock_incidents}
        
        incidents = run_rss_pipeline(sources=None, max_age_days=1, write_raw=False)
        
        assert len(incidents) == 1
        assert incidents[0].incident_id == "rss_test_1"
        mock_collect.assert_called_once_with(sources=None, max_age_days=1)
    
    @patch("src.edu_cti.pipelines.rss.collect_rss_incidents")
    @patch("src.edu_cti.pipelines.rss.write_base_csv")
    def test_run_rss_pipeline_with_write_raw(self, mock_write_csv, mock_collect):
        """Test run_rss_pipeline with write_raw=True."""
        mock_incidents = [
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
        ]
        
        mock_collect.return_value = {"databreaches_rss": mock_incidents}
        
        incidents = run_rss_pipeline(sources=None, max_age_days=1, write_raw=True)
        
        # Should have written CSV
        assert mock_write_csv.called
        assert len(incidents) == 1

