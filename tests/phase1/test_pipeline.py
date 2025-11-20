"""Tests for pipeline functionality."""

import pytest
from unittest.mock import patch, MagicMock
from typing import List

from src.edu_cti.models import BaseIncident, make_incident_id
from src.edu_cti.pipelines.curated import collect_curated_incidents, run_curated_pipeline
from src.edu_cti.pipelines.base_io import write_base_csv
from src.edu_cti.cli.build_dataset import build_dataset, ensure_primary_url_is_none


class TestEnsurePrimaryUrlIsNone:
    """Test Phase 1 requirement: primary_url must be None."""
    
    def test_ensures_primary_url_is_none(self):
        """Test that primary_url is set to None and moved to all_urls."""
        incidents = [
            BaseIncident(
                incident_id="test_1",
                source="test",
                source_event_id=None,
                university_name="Test",
                victim_raw_name="Test",
                institution_type=None,
                country=None,
                region=None,
                city=None,
                incident_date=None,
                date_precision="unknown",
                source_published_date=None,
                ingested_at="2024-01-01T00:00:00Z",
                title=None,
                subtitle=None,
                primary_url="https://example.com/article",  # Has primary_url
                all_urls=["https://example.com/other"],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            ),
        ]
        
        fixed = ensure_primary_url_is_none(incidents)
        
        assert fixed[0].primary_url is None
        assert "https://example.com/article" in fixed[0].all_urls
        assert "https://example.com/other" in fixed[0].all_urls
        assert len(fixed[0].all_urls) == 2
    
    def test_handles_already_none_primary_url(self):
        """Test that incidents with primary_url=None are unchanged."""
        incidents = [
            BaseIncident(
                incident_id="test_1",
                source="test",
                source_event_id=None,
                university_name="Test",
                victim_raw_name="Test",
                institution_type=None,
                country=None,
                region=None,
                city=None,
                incident_date=None,
                date_precision="unknown",
                source_published_date=None,
                ingested_at="2024-01-01T00:00:00Z",
                title=None,
                subtitle=None,
                primary_url=None,  # Already None
                all_urls=["https://example.com/article"],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            ),
        ]
        
        fixed = ensure_primary_url_is_none(incidents)
        
        assert fixed[0].primary_url is None
        assert len(fixed[0].all_urls) == 1
    
    def test_deduplicates_urls_when_moving(self):
        """Test that duplicate URLs are removed when moving primary_url to all_urls."""
        incidents = [
            BaseIncident(
                incident_id="test_1",
                source="test",
                source_event_id=None,
                university_name="Test",
                victim_raw_name="Test",
                institution_type=None,
                country=None,
                region=None,
                city=None,
                incident_date=None,
                date_precision="unknown",
                source_published_date=None,
                ingested_at="2024-01-01T00:00:00Z",
                title=None,
                subtitle=None,
                primary_url="https://example.com/article",
                all_urls=["https://example.com/article"],  # Duplicate
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            ),
        ]
        
        fixed = ensure_primary_url_is_none(incidents)
        
        assert fixed[0].primary_url is None
        # Should have only one URL (deduplicated)
        assert len(fixed[0].all_urls) == 1
        assert fixed[0].all_urls[0] == "https://example.com/article"


class TestCollectCuratedIncidents:
    """Test curated incident collection."""
    
    @patch("src.edu_cti.pipelines.curated.get_curated_builder")
    def test_collect_curated_incidents_with_max_pages(self, mock_get_builder):
        """Test that max_pages is passed to databreach."""
        # Mock databreach builder
        mock_databreach_builder = MagicMock(return_value=[
            BaseIncident(
                incident_id="test_1",
                source="databreach",
                source_event_id=None,
                university_name="Test",
                victim_raw_name="Test",
                institution_type=None,
                country=None,
                region=None,
                city=None,
                incident_date=None,
                date_precision="unknown",
                source_published_date=None,
                ingested_at="2024-01-01T00:00:00Z",
                title=None,
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
        ])
        
        # Mock konbriefing builder (doesn't accept max_pages)
        mock_konbriefing_builder = MagicMock(return_value=[])
        
        mock_get_builder.side_effect = lambda name: {
            "databreach": mock_databreach_builder,
            "konbriefing": mock_konbriefing_builder,
        }[name]
        
        with patch("src.edu_cti.pipelines.curated.CURATED_SOURCE_REGISTRY") as mock_registry:
            mock_registry.keys.return_value = ["databreach", "konbriefing"]
            mock_registry.__contains__ = lambda self, key: key in ["databreach", "konbriefing"]
            
            results = collect_curated_incidents(sources=["databreach"], max_pages=1)
            
            # Verify databreach was called with max_pages
            mock_databreach_builder.assert_called_once_with(max_pages=1)
            # konbriefing should not be called since we only requested databreach
            mock_konbriefing_builder.assert_not_called()


class TestPipelineIntegration:
    """Integration tests for pipeline components."""
    
    def test_build_dataset_ensures_primary_url_none(self):
        """Test that build_dataset ensures primary_url is None."""
        # Create test incidents with primary_url set
        incidents = [
            BaseIncident(
                incident_id="test_1",
                source="test",
                source_event_id=None,
                university_name="Test",
                victim_raw_name="Test",
                institution_type=None,
                country=None,
                region=None,
                city=None,
                incident_date=None,
                date_precision="unknown",
                source_published_date=None,
                ingested_at="2024-01-01T00:00:00Z",
                title=None,
                subtitle=None,
                primary_url="https://example.com/article",
                all_urls=[],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            ),
        ]
        
        # Mock the pipeline runners to return our test incidents
        with patch("src.edu_cti.cli.build_dataset.GROUP_RUNNERS") as mock_runners:
            mock_runner = MagicMock(return_value=incidents)
            mock_runners.__getitem__ = MagicMock(return_value=mock_runner)
            mock_runners.keys = MagicMock(return_value=["test_group"])
            
            result = build_dataset(
                ["test_group"],
                news_max_pages=None,
                news_sources=None,
                curated_sources=None,
                deduplicate=False,
                from_database=False,
            )
            
            # All incidents should have primary_url=None
            for incident in result:
                assert incident.primary_url is None
                assert len(incident.all_urls) > 0


class TestDataIntegrity:
    """Test data integrity across pipeline stages."""
    
    def test_incident_fields_preserved_through_pipeline(self):
        """Test that all incident fields are preserved through pipeline."""
        original_incident = BaseIncident(
            incident_id="test_integrity",
            source="test_source",
            source_event_id="event_123",
            university_name="Harvard University",
            victim_raw_name="Harvard University",
            institution_type="University",
            country="US",
            region="MA",
            city="Cambridge",
            incident_date="2024-01-15",
            date_precision="day",
            source_published_date="2024-01-16",
            ingested_at="2024-01-17T10:30:00Z",
            title="Test Title",
            subtitle="Test Subtitle",
            primary_url="https://example.com/article",
            all_urls=["https://example.com/other"],
            leak_site_url="https://leak.example.com",
            source_detail_url="https://source.example.com",
            screenshot_url="https://screenshot.example.com",
            attack_type_hint="ransomware",
            status="confirmed",
            source_confidence="high",
            notes="Test notes",
        )
        
        # Pass through ensure_primary_url_is_none
        fixed = ensure_primary_url_is_none([original_incident])
        
        fixed_incident = fixed[0]
        
        # Check that all non-URL fields are preserved
        assert fixed_incident.incident_id == original_incident.incident_id
        assert fixed_incident.source == original_incident.source
        assert fixed_incident.university_name == original_incident.university_name
        assert fixed_incident.institution_type == original_incident.institution_type
        assert fixed_incident.country == original_incident.country
        assert fixed_incident.incident_date == original_incident.incident_date
        assert fixed_incident.title == original_incident.title
        assert fixed_incident.status == original_incident.status
        assert fixed_incident.source_confidence == original_incident.source_confidence
        
        # Check that primary_url was moved to all_urls
        assert fixed_incident.primary_url is None
        assert "https://example.com/article" in fixed_incident.all_urls
        assert "https://example.com/other" in fixed_incident.all_urls
    
    def test_csv_roundtrip_preserves_data(self):
        """Test that data is preserved through CSV write/read cycle."""
        from tempfile import TemporaryDirectory
        import csv
        from pathlib import Path
        
        original_incident = BaseIncident(
            incident_id="test_roundtrip",
            source="test_source",
            source_event_id="event_123",
            university_name="Test University",
            victim_raw_name="Test University",
            institution_type="University",
            country="US",
            region=None,
            city=None,
            incident_date="2024-01-15",
            date_precision="day",
            source_published_date="2024-01-16",
            ingested_at="2024-01-17T10:30:00Z",
            title="Test Title",
            subtitle="Test Subtitle",
            primary_url=None,
            all_urls=["https://example.com/1", "https://example.com/2"],
            leak_site_url=None,
            source_detail_url=None,
            screenshot_url=None,
            attack_type_hint="ransomware",
            status="confirmed",
            source_confidence="high",
            notes="Test notes",
        )
        
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "roundtrip.csv"
            write_base_csv(output_path, [original_incident])
            
            # Read back
            with output_path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                row = next(reader)
                
                # Check key fields are preserved
                assert row["incident_id"] == "test_roundtrip"
                assert row["university_name"] == "Test University"
                assert row["all_urls"] == "https://example.com/1;https://example.com/2"
                assert row["attack_type_hint"] == "ransomware"
                assert row["status"] == "confirmed"

