"""
Tests for Phase 2: Post-enrichment deduplication

Tests institution name normalization and duplicate detection.
"""

import pytest
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from src.edu_cti.core.db import get_connection, init_db, insert_incident
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.pipeline.phase2.deduplication import (
    normalize_institution_name,
    parse_incident_date,
    dates_within_window,
    find_duplicate_institutions,
    deduplicate_by_institution,
)
from src.edu_cti.pipeline.phase2.schemas import CTIEnrichmentResult
from src.edu_cti.pipeline.phase2.db import (
    save_enrichment_result,
    get_enrichment_result,
)


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    yield conn, db_path
    conn.close()


class TestInstitutionNameNormalization:
    """Tests for institution name normalization."""
    
    def test_normalize_university_name(self):
        """Test normalizing university names."""
        assert normalize_institution_name("University of California, Berkeley") == "california berkeley"
        assert normalize_institution_name("UC Berkeley") == "berkeley"
        assert normalize_institution_name("The University of Texas at Austin") == "texas austin"
    
    def test_normalize_removes_common_words(self):
        """Test that normalization removes common words."""
        assert normalize_institution_name("The University of California") == "california"
        assert normalize_institution_name("California State University") == "california state"
    
    def test_normalize_lowercase(self):
        """Test that normalization converts to lowercase."""
        assert normalize_institution_name("MIT") == "mit"
        assert normalize_institution_name("Stanford University") == "stanford"
    
    def test_normalize_handles_punctuation(self):
        """Test that normalization handles punctuation."""
        assert normalize_institution_name("University of California, Los Angeles") == "california los angeles"
        assert normalize_institution_name("UCLA") == "ucla"


class TestDateParsing:
    """Tests for date parsing."""
    
    def test_parse_standard_date(self):
        """Test parsing standard YYYY-MM-DD format."""
        result = parse_incident_date("2025-01-15")
        assert result is not None
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15
    
    def test_parse_invalid_date(self):
        """Test parsing invalid date."""
        result = parse_incident_date("invalid-date")
        # Should handle gracefully, might return None or try to parse
        # The exact behavior depends on dateutil availability
    
    def test_parse_none_date(self):
        """Test parsing None date."""
        result = parse_incident_date(None)
        assert result is None


class TestDateWindow:
    """Tests for date window checking."""
    
    def test_dates_within_window(self):
        """Test that dates within window are detected."""
        date1 = datetime(2025, 1, 15)
        date2 = datetime(2025, 1, 20)  # 5 days later
        
        assert dates_within_window(date1, date2, days=14) is True
        assert dates_within_window(date1, date2, days=3) is False
    
    def test_dates_outside_window(self):
        """Test that dates outside window are not detected."""
        date1 = datetime(2025, 1, 15)
        date2 = datetime(2025, 2, 1)  # 17 days later
        
        assert dates_within_window(date1, date2, days=14) is False


class TestDeduplication:
    """Tests for post-enrichment deduplication."""
    
    def test_find_duplicate_institutions(self, temp_db):
        """Test finding duplicate institutions."""
        conn, _ = temp_db
        
        # Create two incidents with same normalized institution name
        incident1 = BaseIncident(
            incident_id=make_incident_id("source1", "url1", "2025-01-15"),
            title="UC Berkeley Attack",
            university_name="University of California, Berkeley",
            incident_date="2025-01-15",
            all_urls=["https://example.com/article1"],
        )
        incident2 = BaseIncident(
            incident_id=make_incident_id("source2", "url2", "2025-01-18"),
            title="Berkeley University Attack",
            university_name="UC Berkeley",
            incident_date="2025-01-18",
            all_urls=["https://example.com/article2"],
        )
        
        # Insert incidents
        insert_incident(conn, incident1)
        insert_incident(conn, incident2)
        
        # Enrich both incidents
        enrichment1 = CTIEnrichmentResult(
            primary_url="https://example.com/article1",
            extraction_confidence=0.85,
            enriched_summary="Summary 1",
            timeline=[],
            mitre_attack_techniques=[],
            is_education_related=True,
            education_relevance_confidence=0.9,
        )
        enrichment2 = CTIEnrichmentResult(
            primary_url="https://example.com/article2",
            extraction_confidence=0.75,
            enriched_summary="Summary 2",
            timeline=[],
            mitre_attack_techniques=[],
            is_education_related=True,
            education_relevance_confidence=0.9,
        )
        
        save_enrichment_result(conn, incident1.incident_id, enrichment1)
        save_enrichment_result(conn, incident2.incident_id, enrichment2)
        
        # Find duplicates
        duplicates = find_duplicate_institutions(
            conn,
            incident1.incident_id,
            "University of California, Berkeley",
            "2025-01-15",
            window_days=14,
        )
        
        assert len(duplicates) == 1
        assert duplicates[0]["incident_id"] == incident2.incident_id
    
    def test_deduplicate_by_institution_keeps_highest_confidence(self, temp_db):
        """Test that deduplication keeps incident with highest confidence."""
        conn, _ = temp_db
        
        # Create three incidents with same institution
        incident1 = BaseIncident(
            incident_id=make_incident_id("source1", "url1", "2025-01-15"),
            title="Test University Attack 1",
            university_name="Test University",
            incident_date="2025-01-15",
            all_urls=["https://example.com/article1"],
        )
        incident2 = BaseIncident(
            incident_id=make_incident_id("source2", "url2", "2025-01-16"),
            title="Test University Attack 2",
            university_name="Test University",
            incident_date="2025-01-16",
            all_urls=["https://example.com/article2"],
        )
        incident3 = BaseIncident(
            incident_id=make_incident_id("source3", "url3", "2025-01-17"),
            title="Test University Attack 3",
            university_name="Test University",
            incident_date="2025-01-17",
            all_urls=["https://example.com/article3"],
        )
        
        # Insert all incidents
        insert_incident(conn, incident1)
        insert_incident(conn, incident2)
        insert_incident(conn, incident3)
        
        # Enrich with different confidence levels
        # incident1: 0.70 (lowest)
        # incident2: 0.90 (highest - should be kept)
        # incident3: 0.80 (middle)
        
        enrichment1 = CTIEnrichmentResult(
            primary_url="https://example.com/article1",
            extraction_confidence=0.70,
            enriched_summary="Summary 1",
            timeline=[],
            mitre_attack_techniques=[],
            is_education_related=True,
            education_relevance_confidence=0.9,
        )
        enrichment2 = CTIEnrichmentResult(
            primary_url="https://example.com/article2",
            extraction_confidence=0.90,
            enriched_summary="Summary 2",
            timeline=[],
            mitre_attack_techniques=[],
            is_education_related=True,
            education_relevance_confidence=0.9,
        )
        enrichment3 = CTIEnrichmentResult(
            primary_url="https://example.com/article3",
            extraction_confidence=0.80,
            enriched_summary="Summary 3",
            timeline=[],
            mitre_attack_techniques=[],
            is_education_related=True,
            education_relevance_confidence=0.9,
        )
        
        save_enrichment_result(conn, incident1.incident_id, enrichment1)
        save_enrichment_result(conn, incident2.incident_id, enrichment2)
        save_enrichment_result(conn, incident3.incident_id, enrichment3)
        
        # Run deduplication
        stats = deduplicate_by_institution(conn, window_days=14)
        
        # Should remove 2 duplicates, keep 1
        assert stats["removed"] == 2
        assert stats["remaining"] == 1
        
        # Verify incident2 (highest confidence) is still there
        remaining = get_enrichment_result(conn, incident2.incident_id)
        assert remaining is not None
        assert remaining.extraction_confidence == 0.90
        
        # Verify incidents 1 and 3 are removed
        removed1 = get_enrichment_result(conn, incident1.incident_id)
        removed3 = get_enrichment_result(conn, incident3.incident_id)
        assert removed1 is None
        assert removed3 is None

