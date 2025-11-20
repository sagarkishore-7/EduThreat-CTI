"""Tests for deduplication module."""

import pytest

from src.edu_cti.models import BaseIncident, make_incident_id
from src.edu_cti.deduplication import (
    normalize_url,
    extract_urls_from_incident,
    deduplicate_by_urls,
    merge_incidents,
)


def test_normalize_url():
    """Test URL normalization."""
    assert normalize_url("https://example.com/article") == "https://example.com/article"
    assert normalize_url("https://example.com/article/") == "https://example.com/article"
    assert normalize_url("https://www.example.com/article") == "https://example.com/article"
    assert normalize_url("https://example.com/article#section") == "https://example.com/article"
    assert normalize_url("https://example.com/article?param=value") == "https://example.com/article?param=value"


def test_extract_urls_from_incident():
    """Test URL extraction from incidents."""
    incident = BaseIncident(
        incident_id="test_123",
        source="test",
        source_event_id=None,
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
        all_urls=["https://example.com/article", "https://example.com/article2"],
        leak_site_url=None,
        source_detail_url="https://example.com/detail",
        screenshot_url=None,
        attack_type_hint=None,
        status="suspected",
        source_confidence="medium",
        notes=None,
    )
    
    urls = extract_urls_from_incident(incident)
    assert len(urls) == 3  # 2 from all_urls + 1 from source_detail_url
    assert "https://example.com/article" in urls
    assert "https://example.com/article2" in urls
    assert "https://example.com/detail" in urls


def test_deduplicate_by_urls():
    """Test cross-source deduplication."""
    # Create two incidents with the same URL
    incident1 = BaseIncident(
        incident_id="test_1",
        source="source1",
        source_event_id=None,
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
        incident_id="test_2",
        source="source2",
        source_event_id=None,
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
        source_confidence="high",
        notes=None,
    )
    
    # Create a third incident with different URL
    incident3 = BaseIncident(
        incident_id="test_3",
        source="source1",
        source_event_id=None,
        university_name="Another University",
        victim_raw_name="Another University",
        institution_type="University",
        country="US",
        region=None,
        city=None,
        incident_date="2024-01-02",
        date_precision="day",
        source_published_date="2024-01-02",
        ingested_at="2024-01-02T00:00:00Z",
        title="Another Incident",
        subtitle=None,
        primary_url=None,
        all_urls=["https://example.com/different"],
        leak_site_url=None,
        source_detail_url=None,
        screenshot_url=None,
        attack_type_hint=None,
        status="suspected",
        source_confidence="medium",
        notes=None,
    )
    
    incidents = [incident1, incident2, incident3]
    deduplicated, stats = deduplicate_by_urls(incidents)
    
    # Should have 2 incidents (1 merged + 1 standalone)
    assert stats["total_input"] == 3
    assert stats["total_output"] == 2
    assert stats["duplicates_merged"] == 1
    assert stats["incidents_removed"] == 1
    
    # Check that merged incident has high confidence (from incident2)
    merged = next(inc for inc in deduplicated if "merged_from" in (inc.notes or ""))
    assert merged.source_confidence == "high"
    assert "source1" in merged.notes
    assert "source2" in merged.notes

