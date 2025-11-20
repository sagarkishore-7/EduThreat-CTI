"""Tests for models module - schema validation and data integrity."""

import pytest
from datetime import datetime
from typing import List

from src.edu_cti.models import BaseIncident, make_incident_id


class TestBaseIncident:
    """Test BaseIncident model schema and validation."""
    
    def test_incident_creation_with_minimal_fields(self):
        """Test creating an incident with only required fields."""
        incident = BaseIncident(
            incident_id="test_123",
            source="test_source",
            source_event_id=None,
            university_name="Test University",
            victim_raw_name="Test University",
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
            all_urls=[],
            leak_site_url=None,
            source_detail_url=None,
            screenshot_url=None,
            attack_type_hint=None,
            status="suspected",
            source_confidence="medium",
            notes=None,
        )
        
        assert incident.incident_id == "test_123"
        assert incident.source == "test_source"
        assert incident.university_name == "Test University"
        assert incident.all_urls == []
        assert incident.primary_url is None
    
    def test_incident_creation_with_all_fields(self):
        """Test creating an incident with all fields populated."""
        incident = BaseIncident(
            incident_id="test_456",
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
            title="Test Incident Title",
            subtitle="Test subtitle",
            primary_url="https://example.com/article",
            all_urls=["https://example.com/article", "https://example.com/article2"],
            leak_site_url="https://leak.example.com/claim",
            source_detail_url="https://source.example.com/detail",
            screenshot_url="https://screenshot.example.com/img.png",
            attack_type_hint="ransomware",
            status="confirmed",
            source_confidence="high",
            notes="Test notes",
        )
        
        assert incident.incident_id == "test_456"
        assert incident.institution_type == "University"
        assert incident.country == "US"
        assert len(incident.all_urls) == 2
        assert incident.status == "confirmed"
        assert incident.source_confidence == "high"
    
    def test_to_dict_method(self):
        """Test that to_dict() correctly serializes all fields."""
        incident = BaseIncident(
            incident_id="test_789",
            source="test_source",
            source_event_id=None,
            university_name="Test University",
            victim_raw_name="Test University",
            institution_type=None,
            country=None,
            region=None,
            city=None,
            incident_date="2024-01-01",
            date_precision="day",
            source_published_date="2024-01-01",
            ingested_at="2024-01-01T00:00:00Z",
            title="Test Title",
            subtitle=None,
            primary_url=None,
            all_urls=["https://example.com/1", "https://example.com/2"],
            leak_site_url=None,
            source_detail_url=None,
            screenshot_url=None,
            attack_type_hint=None,
            status="suspected",
            source_confidence="medium",
            notes=None,
        )
        
        d = incident.to_dict()
        
        # Check that all expected fields are present
        assert "incident_id" in d
        assert "source" in d
        assert "university_name" in d
        assert "all_urls" in d
        
        # Check that all_urls is serialized as semicolon-separated string
        assert d["all_urls"] == "https://example.com/1;https://example.com/2"
        
        # Check that primary_url is None
        assert d["primary_url"] is None
    
    def test_to_dict_with_empty_all_urls(self):
        """Test to_dict() with empty all_urls list."""
        incident = BaseIncident(
            incident_id="test_empty",
            source="test_source",
            source_event_id=None,
            university_name="Test University",
            victim_raw_name="Test University",
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
            all_urls=[],
            leak_site_url=None,
            source_detail_url=None,
            screenshot_url=None,
            attack_type_hint=None,
            status="suspected",
            source_confidence="medium",
            notes=None,
        )
        
        d = incident.to_dict()
        assert d["all_urls"] == ""
    
    def test_to_dict_with_none_all_urls(self):
        """Test to_dict() handles None all_urls (shouldn't happen but test for safety)."""
        # This shouldn't happen in practice, but test edge case
        incident = BaseIncident(
            incident_id="test_none",
            source="test_source",
            source_event_id=None,
            university_name="Test University",
            victim_raw_name="Test University",
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
            all_urls=[],  # Empty list is the default
            leak_site_url=None,
            source_detail_url=None,
            screenshot_url=None,
            attack_type_hint=None,
            status="suspected",
            source_confidence="medium",
            notes=None,
        )
        
        d = incident.to_dict()
        assert d["all_urls"] == ""
    
    def test_phase1_requirement_primary_url_none(self):
        """Test that Phase 1 requirement: primary_url should be None."""
        incident = BaseIncident(
            incident_id="test_phase1",
            source="test_source",
            source_event_id=None,
            university_name="Test University",
            victim_raw_name="Test University",
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
            all_urls=["https://example.com/article"],  # All URLs in all_urls
            leak_site_url=None,
            source_detail_url=None,
            screenshot_url=None,
            attack_type_hint=None,
            status="suspected",
            source_confidence="medium",
            notes=None,
        )
        
        assert incident.primary_url is None
        assert len(incident.all_urls) > 0


class TestMakeIncidentId:
    """Test incident ID generation."""
    
    def test_make_incident_id_stable(self):
        """Test that make_incident_id generates stable IDs."""
        id1 = make_incident_id("source1", "unique_string_123")
        id2 = make_incident_id("source1", "unique_string_123")
        
        # Same source + unique_string should generate same ID
        assert id1 == id2
    
    def test_make_incident_id_unique(self):
        """Test that different inputs generate different IDs."""
        id1 = make_incident_id("source1", "unique_string_123")
        id2 = make_incident_id("source1", "unique_string_456")
        id3 = make_incident_id("source2", "unique_string_123")
        
        # Different unique_strings should generate different IDs
        assert id1 != id2
        # Different sources with same unique_string should generate different IDs
        assert id1 != id3
    
    def test_make_incident_id_format(self):
        """Test that incident IDs have the expected format."""
        incident_id = make_incident_id("test_source", "unique_string")
        
        # Should start with source name and underscore
        assert incident_id.startswith("test_source_")
        # Should have hexadecimal suffix (16 chars from SHA256)
        assert len(incident_id) == len("test_source_") + 16
        # Suffix should be valid hex (lowercase from hashlib.sha256.hexdigest())
        # Use rsplit to get the last part (in case source name contains underscores)
        suffix = incident_id.rsplit("_", 1)[1]
        assert len(suffix) == 16, f"Suffix should be 16 chars, got {len(suffix)}"
        assert all(c in "0123456789abcdef" for c in suffix), f"Suffix {suffix} contains invalid hex characters"


class TestSchemaCompliance:
    """Test schema compliance and data validation."""
    
    def test_required_fields_present(self):
        """Test that all required fields can be set."""
        incident = BaseIncident(
            incident_id="test",
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
            all_urls=[],
            leak_site_url=None,
            source_detail_url=None,
            screenshot_url=None,
            attack_type_hint=None,
            status="suspected",
            source_confidence="medium",
            notes=None,
        )
        
        # All fields should be accessible
        assert hasattr(incident, "incident_id")
        assert hasattr(incident, "source")
        assert hasattr(incident, "university_name")
        assert hasattr(incident, "all_urls")
        assert hasattr(incident, "primary_url")
        assert hasattr(incident, "to_dict")
    
    def test_date_precision_values(self):
        """Test that date_precision accepts valid values."""
        valid_precisions = ["day", "month", "year", "unknown"]
        
        for precision in valid_precisions:
            incident = BaseIncident(
                incident_id="test",
                source="test",
                source_event_id=None,
                university_name="Test",
                victim_raw_name="Test",
                institution_type=None,
                country=None,
                region=None,
                city=None,
                incident_date=None,
                date_precision=precision,
                source_published_date=None,
                ingested_at="2024-01-01T00:00:00Z",
                title=None,
                subtitle=None,
                primary_url=None,
                all_urls=[],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            )
            assert incident.date_precision == precision
    
    def test_status_values(self):
        """Test that status accepts valid values."""
        valid_statuses = ["suspected", "confirmed"]
        
        for status in valid_statuses:
            incident = BaseIncident(
                incident_id="test",
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
                all_urls=[],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status=status,
                source_confidence="medium",
                notes=None,
            )
            assert incident.status == status
    
    def test_source_confidence_values(self):
        """Test that source_confidence accepts valid values."""
        valid_confidences = ["low", "medium", "high"]
        
        for confidence in valid_confidences:
            incident = BaseIncident(
                incident_id="test",
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
                all_urls=[],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence=confidence,
                notes=None,
            )
            assert incident.source_confidence == confidence

