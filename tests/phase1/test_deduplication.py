"""Tests for cross-source deduplication functionality."""

import pytest

from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.deduplication import (
    normalize_url,
    extract_urls_from_incident,
    merge_incidents,
    deduplicate_by_urls,
)


class TestURLNormalization:
    """Test URL normalization for deduplication."""
    
    def test_normalize_url_removes_trailing_slash(self):
        """Test that trailing slashes are removed."""
        url = "https://example.com/article/"
        normalized = normalize_url(url)
        assert not normalized.endswith("/")
    
    def test_normalize_url_removes_www(self):
        """Test that www. prefix is removed."""
        url = "https://www.example.com/article"
        normalized = normalize_url(url)
        assert "www." not in normalized
    
    def test_normalize_url_removes_fragment(self):
        """Test that URL fragments are removed."""
        url = "https://example.com/article#section"
        normalized = normalize_url(url)
        assert "#" not in normalized
    
    def test_normalize_url_lowercase(self):
        """Test that URLs are lowercased."""
        url = "https://EXAMPLE.COM/Article"
        normalized = normalize_url(url)
        assert normalized == normalized.lower()


class TestExtractURLs:
    """Test extracting URLs from incidents."""
    
    def test_extract_urls_from_all_urls(self):
        """Test extracting URLs from all_urls field."""
        incident = BaseIncident(
            incident_id="test_1",
            source="test",
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
            all_urls=["https://example.com/article", "https://other.com/article"],
            leak_site_url=None,
            source_detail_url=None,
            screenshot_url=None,
            attack_type_hint=None,
            status="suspected",
            source_confidence="medium",
            notes=None,
        )
        
        urls = extract_urls_from_incident(incident)
        assert len(urls) == 2


class TestDeduplication:
    """Test deduplication by URLs."""
    
    def test_deduplicate_by_urls_finds_duplicates(self):
        """Test that deduplication finds incidents with same URLs."""
        incident1 = BaseIncident(
            incident_id="source1_123",
            source="source1",
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
            all_urls=["https://example.com/article"],
            leak_site_url=None,
            source_detail_url=None,
            screenshot_url=None,
            attack_type_hint=None,
            status="suspected",
            source_confidence="medium",
            notes=None,
        )
        
        incident2 = BaseIncident(
            incident_id="source2_456",
            source="source2",
            source_event_id="event_2",
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
            title="Same Incident",
            subtitle=None,
            primary_url=None,
            all_urls=["https://www.example.com/article/"],  # Same URL normalized
            leak_site_url=None,
            source_detail_url=None,
            screenshot_url=None,
            attack_type_hint=None,
            status="suspected",
            source_confidence="high",
            notes=None,
        )
        
        unique, stats = deduplicate_by_urls([incident1, incident2])
        
        # Should merge into one incident
        assert len(unique) == 1
        
    def test_deduplicate_keeps_unique_incidents(self):
        """Test that unique incidents are kept."""
        incident1 = BaseIncident(
            incident_id="source1_123",
            source="source1",
            source_event_id="event_1",
            university_name="University A",
            victim_raw_name="University A",
            institution_type="University",
            country="US",
            region=None,
            city=None,
            incident_date="2024-01-01",
            date_precision="day",
            source_published_date="2024-01-01",
            ingested_at="2024-01-01T00:00:00Z",
            title="Incident A",
            subtitle=None,
            primary_url=None,
            all_urls=["https://example.com/article-a"],
            leak_site_url=None,
            source_detail_url=None,
            screenshot_url=None,
            attack_type_hint=None,
            status="suspected",
            source_confidence="medium",
            notes=None,
        )
        
        incident2 = BaseIncident(
            incident_id="source2_456",
            source="source2",
            source_event_id="event_2",
            university_name="University B",
            victim_raw_name="University B",
            institution_type="University",
            country="US",
            region=None,
            city=None,
            incident_date="2024-01-02",
            date_precision="day",
            source_published_date="2024-01-02",
            ingested_at="2024-01-02T00:00:00Z",
            title="Incident B",
            subtitle=None,
            primary_url=None,
            all_urls=["https://example.com/article-b"],  # Different URL
            leak_site_url=None,
            source_detail_url=None,
            screenshot_url=None,
            attack_type_hint=None,
            status="suspected",
            source_confidence="high",
            notes=None,
        )
        
        unique, stats = deduplicate_by_urls([incident1, incident2])
        
        # Should keep both incidents
        assert len(unique) == 2
