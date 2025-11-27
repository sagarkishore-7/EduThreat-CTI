"""Tests for CSV output functionality."""

import pytest
import tempfile
import os
import csv
from pathlib import Path

from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.pipeline.phase1.base_io import write_base_csv


class TestCSVOutput:
    """Test CSV writing functionality."""
    
    def test_write_base_csv_creates_file(self):
        """Test that write_base_csv creates a CSV file."""
        incidents = [
            BaseIncident(
                incident_id="test_csv_1",
                source="test",
                source_event_id="event_1",
                university_name="Test University",
                victim_raw_name="Test University",
                institution_type="University",
                country="US",
                region="California",
                city="Los Angeles",
                incident_date="2024-01-01",
                date_precision="day",
                source_published_date="2024-01-01",
                ingested_at="2024-01-01T00:00:00Z",
                title="Test CSV Incident",
                subtitle="Subtitle",
                primary_url=None,
                all_urls=["https://example.com/csv"],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint="ransomware",
                status="confirmed",
                source_confidence="high",
                notes="Test notes",
            )
        ]
        
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_output.csv"
            write_base_csv(incidents, output_path)
            
            assert output_path.exists()
            
            # Read and verify CSV
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                assert len(rows) == 1
                assert rows[0]["incident_id"] == "test_csv_1"
                assert rows[0]["source"] == "test"
                assert rows[0]["university_name"] == "Test University"
    
    def test_write_base_csv_handles_multiple_incidents(self):
        """Test that write_base_csv handles multiple incidents."""
        incidents = []
        for i in range(5):
            incident = BaseIncident(
                incident_id=f"test_csv_{i}",
                source="test",
                source_event_id=f"event_{i}",
                university_name=f"University {i}",
                victim_raw_name=f"University {i}",
                institution_type="University",
                country="US",
                region=None,
                city=None,
                incident_date="2024-01-01",
                date_precision="day",
                source_published_date="2024-01-01",
                ingested_at="2024-01-01T00:00:00Z",
                title=f"Incident {i}",
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
            incidents.append(incident)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_output.csv"
            write_base_csv(incidents, output_path)
            
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                assert len(rows) == 5
    
    def test_write_base_csv_empty_list(self):
        """Test that write_base_csv handles empty list."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_output.csv"
            write_base_csv([], output_path)
            
            # File should exist but only have headers
            assert output_path.exists()
            
            with open(output_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 0


class TestIncidentID:
    """Test incident ID generation."""
    
    def test_incident_id_deterministic(self):
        """Test incident IDs are deterministic."""
        id1 = make_incident_id("source", "https://example.com/article")
        id2 = make_incident_id("source", "https://example.com/article")
        assert id1 == id2
    
    def test_incident_id_includes_source(self):
        """Test incident IDs include source name."""
        id1 = make_incident_id("mysource", "https://example.com/article")
        assert id1.startswith("mysource_")
