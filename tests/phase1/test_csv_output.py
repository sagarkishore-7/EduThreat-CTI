"""Tests for CSV output and data integrity."""

import pytest
import csv
from pathlib import Path
from tempfile import TemporaryDirectory

from src.edu_cti.models import BaseIncident, make_incident_id
from src.edu_cti.pipelines.base_io import write_base_csv, PROC_DIR


class TestCSVOutput:
    """Test CSV output functionality."""
    
    def test_write_base_csv_creates_file(self):
        """Test that write_base_csv creates a CSV file."""
        incidents = [
            BaseIncident(
                incident_id="test_1",
                source="test_source",
                source_event_id=None,
                university_name="Test University 1",
                victim_raw_name="Test University 1",
                institution_type="University",
                country="US",
                region=None,
                city=None,
                incident_date="2024-01-01",
                date_precision="day",
                source_published_date="2024-01-01",
                ingested_at="2024-01-01T00:00:00Z",
                title="Test Incident 1",
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
            ),
            BaseIncident(
                incident_id="test_2",
                source="test_source",
                source_event_id=None,
                university_name="Test University 2",
                victim_raw_name="Test University 2",
                institution_type="School",
                country="UK",
                region=None,
                city=None,
                incident_date="2024-01-02",
                date_precision="day",
                source_published_date="2024-01-02",
                ingested_at="2024-01-02T00:00:00Z",
                title="Test Incident 2",
                subtitle="Test subtitle",
                primary_url=None,
                all_urls=["https://example.com/2", "https://example.com/2b"],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint="ransomware",
                status="confirmed",
                source_confidence="high",
                notes="Test notes",
            ),
        ]
        
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_output.csv"
            rows_written = write_base_csv(output_path, incidents)
            
            # Check that file was created
            assert output_path.exists()
            assert rows_written == 2
            
            # Read and verify contents
            with output_path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                assert len(rows) == 2
                
                # Check first row
                assert rows[0]["incident_id"] == "test_1"
                assert rows[0]["university_name"] == "Test University 1"
                assert rows[0]["all_urls"] == "https://example.com/1"
                assert rows[0]["primary_url"] == ""
                
                # Check second row
                assert rows[1]["incident_id"] == "test_2"
                assert rows[1]["university_name"] == "Test University 2"
                assert rows[1]["all_urls"] == "https://example.com/2;https://example.com/2b"
                assert rows[1]["primary_url"] == ""
                assert rows[1]["attack_type_hint"] == "ransomware"
                assert rows[1]["status"] == "confirmed"
    
    def test_write_base_csv_with_empty_list(self):
        """Test that write_base_csv handles empty list gracefully."""
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "empty_output.csv"
            rows_written = write_base_csv(output_path, [])
            
            # Should return 0 and not create file
            assert rows_written == 0
            assert not output_path.exists()
    
    def test_csv_contains_all_fields(self):
        """Test that CSV contains all expected fields."""
        incident = BaseIncident(
            incident_id="test_all_fields",
            source="test_source",
            source_event_id="event_123",
            university_name="Test University",
            victim_raw_name="Test University Raw",
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
            primary_url=None,
            all_urls=["https://example.com/1", "https://example.com/2"],
            leak_site_url="https://leak.example.com",
            source_detail_url="https://source.example.com",
            screenshot_url="https://screenshot.example.com",
            attack_type_hint="ransomware",
            status="confirmed",
            source_confidence="high",
            notes="Test notes",
        )
        
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "all_fields.csv"
            write_base_csv(output_path, [incident])
            
            with output_path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                assert len(rows) == 1
                row = rows[0]
                
                # Check all fields are present
                expected_fields = [
                    "incident_id", "source", "source_event_id",
                    "university_name", "victim_raw_name",
                    "institution_type", "country", "region", "city",
                    "incident_date", "date_precision", "source_published_date", "ingested_at",
                    "title", "subtitle",
                    "primary_url", "all_urls",
                    "leak_site_url", "source_detail_url", "screenshot_url",
                    "attack_type_hint", "status", "source_confidence", "notes",
                ]
                
                for field in expected_fields:
                    assert field in row, f"Field {field} missing from CSV"
    
    def test_csv_all_urls_format(self):
        """Test that all_urls are properly formatted in CSV."""
        incidents = [
            BaseIncident(
                incident_id="test_empty",
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
                primary_url=None,
                all_urls=[],  # Empty
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            ),
            BaseIncident(
                incident_id="test_single",
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
                primary_url=None,
                all_urls=["https://example.com/1"],  # Single URL
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            ),
            BaseIncident(
                incident_id="test_multiple",
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
                primary_url=None,
                all_urls=["https://example.com/1", "https://example.com/2", "https://example.com/3"],  # Multiple URLs
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            ),
        ]
        
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "urls_test.csv"
            write_base_csv(output_path, incidents)
            
            with output_path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                assert rows[0]["all_urls"] == ""  # Empty
                assert rows[1]["all_urls"] == "https://example.com/1"  # Single
                assert rows[2]["all_urls"] == "https://example.com/1;https://example.com/2;https://example.com/3"  # Multiple
    
    def test_csv_primary_url_always_none_in_phase1(self):
        """Test that primary_url is always None/empty in Phase 1 CSV output."""
        incidents = [
            BaseIncident(
                incident_id=f"test_{i}",
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
                primary_url=None,  # Phase 1: must be None
                all_urls=[f"https://example.com/{i}"],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            )
            for i in range(10)
        ]
        
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "phase1_test.csv"
            write_base_csv(output_path, incidents)
            
            with output_path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                # All rows should have primary_url as empty string (None -> "")
                for row in rows:
                    assert row["primary_url"] == "", "Phase 1 requirement: primary_url must be None/empty"
                    assert row["all_urls"] != "", "At least one URL should be in all_urls"

