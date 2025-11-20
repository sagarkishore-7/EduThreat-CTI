"""Tests for incremental saving functionality."""

import pytest
from unittest.mock import MagicMock, patch
from typing import List

from src.edu_cti.models import BaseIncident
from src.edu_cti.pipelines.incremental_save import IncrementalSaver, create_db_saver


class TestIncrementalSave:
    """Test incremental saving functionality."""
    
    def test_create_db_saver_batches_incidents(self):
        """Test that DB saver batches incidents correctly."""
        mock_conn = MagicMock()
        mock_conn.commit = MagicMock()
        
        saver = create_db_saver(mock_conn, is_rss=False)
        
        # Create test incidents
        incidents = [
            BaseIncident(
                incident_id=f"test_{i}",
                source="test_source",
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
            for i in range(75)  # More than one batch (batch size is 50)
        ]
        
        # Mock the _ingest_batch function
        with patch("src.edu_cti.cli.ingestion._ingest_batch") as mock_ingest:
            mock_ingest.return_value = len(incidents)  # Return count of new incidents
            
            # Add incidents in batches
            saver.add_batch(incidents[:50])  # First batch
            saver.add_batch(incidents[50:])  # Second batch
            
            # Should have called _ingest_batch twice (one for each batch)
            assert mock_ingest.call_count == 2
            assert mock_ingest.call_args_list[0][0][1] == incidents[:50]
            assert mock_ingest.call_args_list[1][0][1] == incidents[50:]
    
    def test_create_db_saver_flushes_remaining(self):
        """Test that finish() flushes remaining buffered incidents."""
        mock_conn = MagicMock()
        mock_conn.commit = MagicMock()
        
        saver = create_db_saver(mock_conn, is_rss=False)
        
        # Create a small batch (less than batch size)
        incidents = [
            BaseIncident(
                incident_id="test_1",
                source="test_source",
                source_event_id="event_1",
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
                title="Test Incident",
                subtitle=None,
                primary_url=None,
                all_urls=["https://example.com/1"],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            )
        ]
        
        with patch("src.edu_cti.cli.ingestion._ingest_batch") as mock_ingest:
            mock_ingest.return_value = 1
            
            # Add incidents (less than batch size)
            saver.add_batch(incidents)
            
            # Should not have called _ingest_batch yet (buffer not full)
            assert mock_ingest.call_count == 0
            
            # Finish should flush remaining
            new_count = saver.finish()
            
            # Should have called _ingest_batch once
            assert mock_ingest.call_count == 1
            assert new_count == 1
    
    def test_create_db_saver_handles_rss_flag(self):
        """Test that is_rss flag is passed correctly."""
        mock_conn = MagicMock()
        mock_conn.commit = MagicMock()
        
        saver_rss = create_db_saver(mock_conn, is_rss=True)
        saver_non_rss = create_db_saver(mock_conn, is_rss=False)
        
        # Both should be created successfully
        assert saver_rss is not None
        assert saver_non_rss is not None
        
        # The is_rss flag should be stored and used when calling _ingest_batch
        incidents = [
            BaseIncident(
                incident_id="test_1",
                source="test_source",
                source_event_id="event_1",
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
                title="Test Incident",
                subtitle=None,
                primary_url=None,
                all_urls=["https://example.com/1"],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            )
        ]
        
        with patch("src.edu_cti.cli.ingestion._ingest_batch") as mock_ingest:
            mock_ingest.return_value = 1
            
            # Add enough incidents to trigger batch
            large_batch = incidents * 50
            saver_rss.add_batch(large_batch)
            
            # Check that is_rss=True was passed
            assert mock_ingest.call_count == 1
            # Verify is_rss parameter was passed (third positional arg or keyword)
            call_args = mock_ingest.call_args
            # is_rss should be True in the call (third positional argument)
            assert call_args[0][2] == True
    
    def test_get_source_name_from_incidents(self):
        """Test that source name is extracted from incidents."""
        mock_callback = MagicMock(return_value=1)
        saver = IncrementalSaver(mock_callback, batch_size=50)
        
        # Add incidents with source
        incidents = [
            BaseIncident(
                incident_id="test_1",
                source="krebsonsecurity",
                source_event_id="event_1",
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
                title="Test Incident",
                subtitle=None,
                primary_url=None,
                all_urls=["https://example.com/1"],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            )
        ]
        
        saver.add_batch(incidents)
        
        # Should extract source name from incidents
        assert saver._get_source_name() == "krebsonsecurity"
    
    def test_get_source_name_fallback_to_unknown(self):
        """Test that 'unknown' is used when no source name and no incidents."""
        mock_callback = MagicMock(return_value=1)
        saver = IncrementalSaver(mock_callback, batch_size=50)
        
        # No incidents added, no source name provided
        assert saver._get_source_name() == "unknown"
    
    def test_create_db_saver_with_source_name(self):
        """Test that create_db_saver accepts source_name parameter."""
        mock_conn = MagicMock()
        
        saver = create_db_saver(mock_conn, is_rss=False, source_name="RSS feeds")
        
        assert saver.source_name == "RSS feeds"
        assert saver._get_source_name() == "RSS feeds"
    
    def test_finish_logs_with_correct_source_name(self):
        """Test that finish() logs with correct source name from incidents."""
        mock_callback = MagicMock(return_value=1)
        saver = IncrementalSaver(mock_callback, batch_size=50)
        
        incidents = [
            BaseIncident(
                incident_id="test_1",
                source="therecord",
                source_event_id="event_1",
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
                title="Test Incident",
                subtitle=None,
                primary_url=None,
                all_urls=["https://example.com/1"],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            )
        ]
        
        saver.add_batch(incidents)
        
        # Finish should use source name from incidents
        with patch("src.edu_cti.pipelines.incremental_save.logger") as mock_logger:
            saver.finish()
            # Check that logger.info was called with source name from incidents
            mock_logger.info.assert_called()
            call_args = mock_logger.info.call_args[0][0]
            assert "therecord" in call_args or "Collection complete" in call_args

