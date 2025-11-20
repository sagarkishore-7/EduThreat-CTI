"""
Tests for Phase 2: Enrichment Pipeline

Tests LLM enrichment, article fetching, and enrichment database operations.
"""

import pytest
import sqlite3
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, List

from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.db import get_connection, init_db, insert_incident
from src.edu_cti.pipeline.phase2.enrichment import IncidentEnricher
from src.edu_cti.pipeline.phase2.schemas import (
    EducationRelevanceCheck,
    URLConfidenceScore,
    CTIEnrichmentResult,
    TimelineEvent,
    MITREAttackTechnique,
    AttackDynamics,
)
from src.edu_cti.pipeline.phase2.article_fetcher import ArticleFetcher, ArticleContent
from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti.pipeline.phase2.db import (
    get_unenriched_incidents,
    save_enrichment_result,
    get_enrichment_result,
    mark_incident_skipped,
    get_enrichment_stats,
)


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    yield conn, db_path
    conn.close()


@pytest.fixture
def sample_incident():
    """Create a sample incident for testing."""
    return BaseIncident(
        incident_id=make_incident_id(
            source="test_source",
            url="https://example.com/test",
            date="2025-01-15"
        ),
        title="Test University Cyber Attack",
        subtitle="Ransomware attack on university systems",
        university_name="Test University",
        institution_type="university",
        country="United States",
        region="North America",
        incident_date="2025-01-15",
        date_precision="day",
        all_urls=["https://example.com/article1", "https://example.com/article2"],
        attack_type_hint="ransomware",
        status="confirmed",
        source_confidence="high",
    )


@pytest.fixture
def sample_article_content():
    """Create sample article content for testing."""
    return ArticleContent(
        url="https://example.com/article1",
        title="Test University Cyber Attack",
        content="A ransomware attack hit Test University on January 15, 2025...",
        fetch_successful=True,
        fetch_error=None,
    )


class TestArticleFetcher:
    """Tests for article fetching functionality."""
    
    @patch('src.edu_cti.pipeline.phase2.article_fetcher.build_http_client')
    def test_fetch_article_success(self, mock_build_client):
        """Test successful article fetching."""
        mock_client = Mock()
        mock_client.get.return_value.text = """
        <html>
            <head><title>Test Article</title></head>
            <body>
                <article>
                    <h1>Test Article</h1>
                    <p>This is test content about a cyber incident.</p>
                </article>
            </body>
        </html>
        """
        mock_build_client.return_value = mock_client
        
        fetcher = ArticleFetcher()
        result = fetcher.fetch_article_content("https://example.com/article")
        
        assert result.fetch_successful is True
        assert result.title == "Test Article"
        assert "test content" in result.content.lower()
    
    @patch('src.edu_cti.pipeline.phase2.article_fetcher.build_http_client')
    def test_fetch_article_failure(self, mock_build_client):
        """Test article fetching failure handling."""
        mock_client = Mock()
        mock_client.get.side_effect = Exception("Network error")
        mock_build_client.return_value = mock_client
        
        fetcher = ArticleFetcher()
        result = fetcher.fetch_article_content("https://example.com/article")
        
        assert result.fetch_successful is False
        assert result.fetch_error is not None


class TestEnrichmentDatabase:
    """Tests for enrichment database operations."""
    
    def test_get_unenriched_incidents(self, temp_db, sample_incident):
        """Test retrieving unenriched incidents."""
        conn, _ = temp_db
        
        # Insert an unenriched incident
        insert_incident(conn, sample_incident)
        
        # Get unenriched incidents
        unenriched = get_unenriched_incidents(conn)
        
        assert len(unenriched) == 1
        assert unenriched[0]["incident_id"] == sample_incident.incident_id
    
    def test_save_enrichment_result(self, temp_db, sample_incident):
        """Test saving enrichment result."""
        conn, _ = temp_db
        
        # Insert incident first
        insert_incident(conn, sample_incident)
        
        # Create enrichment result
        enrichment = CTIEnrichmentResult(
            primary_url="https://example.com/article1",
            extraction_confidence=0.85,
            enriched_summary="Test university was hit by ransomware...",
            timeline=[
                TimelineEvent(
                    event_date="2025-01-15",
                    event_type="attack_discovered",
                    description="Ransomware attack detected",
                    confidence=0.9,
                )
            ],
            mitre_attack_techniques=[
                MITREAttackTechnique(
                    technique_id="T1486",
                    technique_name="Data Encrypted for Impact",
                    tactic="Impact",
                    confidence=0.85,
                )
            ],
            attack_dynamics=AttackDynamics(
                attack_type="ransomware",
                attack_family="LockBit",
                initial_access_vector="phishing",
            ),
            is_education_related=True,
            education_relevance_confidence=0.9,
        )
        
        # Save enrichment
        saved = save_enrichment_result(conn, sample_incident.incident_id, enrichment)
        
        assert saved is True
        
        # Verify enrichment was saved
        saved_enrichment = get_enrichment_result(conn, sample_incident.incident_id)
        assert saved_enrichment is not None
        assert saved_enrichment.extraction_confidence == 0.85
        assert saved_enrichment.primary_url == "https://example.com/article1"
    
    def test_enrichment_upgrade_logic(self, temp_db, sample_incident):
        """Test enrichment upgrade when new confidence is higher."""
        conn, _ = temp_db
        
        # Insert incident
        insert_incident(conn, sample_incident)
        
        # First enrichment with lower confidence
        enrichment1 = CTIEnrichmentResult(
            primary_url="https://example.com/article1",
            extraction_confidence=0.70,
            enriched_summary="First summary",
            timeline=[],
            mitre_attack_techniques=[],
            is_education_related=True,
            education_relevance_confidence=0.9,
        )
        saved1 = save_enrichment_result(conn, sample_incident.incident_id, enrichment1)
        assert saved1 is True
        
        # Second enrichment with higher confidence - should upgrade
        enrichment2 = CTIEnrichmentResult(
            primary_url="https://example.com/article2",
            extraction_confidence=0.90,
            enriched_summary="Second summary",
            timeline=[],
            mitre_attack_techniques=[],
            is_education_related=True,
            education_relevance_confidence=0.9,
        )
        saved2 = save_enrichment_result(conn, sample_incident.incident_id, enrichment2)
        assert saved2 is True
        
        # Verify upgrade
        final = get_enrichment_result(conn, sample_incident.incident_id)
        assert final.extraction_confidence == 0.90
        assert final.primary_url == "https://example.com/article2"
        
        # Third enrichment with lower confidence - should not upgrade
        enrichment3 = CTIEnrichmentResult(
            primary_url="https://example.com/article3",
            extraction_confidence=0.65,
            enriched_summary="Third summary",
            timeline=[],
            mitre_attack_techniques=[],
            is_education_related=True,
            education_relevance_confidence=0.9,
        )
        saved3 = save_enrichment_result(conn, sample_incident.incident_id, enrichment3)
        assert saved3 is False  # Should not save lower confidence
        
        # Verify original high confidence is still there
        final_after = get_enrichment_result(conn, sample_incident.incident_id)
        assert final_after.extraction_confidence == 0.90
    
    def test_get_enrichment_stats(self, temp_db, sample_incident):
        """Test getting enrichment statistics."""
        conn, _ = temp_db
        
        # Insert incident
        insert_incident(conn, sample_incident)
        
        # Get stats before enrichment
        stats_before = get_enrichment_stats(conn)
        assert stats_before["unenriched_incidents"] == 1
        assert stats_before["enriched_incidents"] == 0
        assert stats_before["ready_for_enrichment"] == 1
        
        # Enrich incident
        enrichment = CTIEnrichmentResult(
            primary_url="https://example.com/article1",
            extraction_confidence=0.85,
            enriched_summary="Test summary",
            timeline=[],
            mitre_attack_techniques=[],
            is_education_related=True,
            education_relevance_confidence=0.9,
        )
        save_enrichment_result(conn, sample_incident.incident_id, enrichment)
        
        # Get stats after enrichment
        stats_after = get_enrichment_stats(conn)
        assert stats_after["unenriched_incidents"] == 0
        assert stats_after["enriched_incidents"] == 1
        assert stats_after["ready_for_enrichment"] == 0


class TestIncidentEnricher:
    """Tests for the main enrichment orchestrator."""
    
    @patch('src.edu_cti.pipeline.phase2.enrichment.OllamaLLMClient')
    @patch('src.edu_cti.pipeline.phase2.enrichment.ArticleFetcher')
    def test_process_incident_education_related(self, mock_fetcher_class, mock_llm_class):
        """Test processing an education-related incident."""
        # Setup mocks
        mock_llm = Mock()
        mock_llm.extract_structured.return_value = EducationRelevanceCheck(
            is_education_related=True,
            confidence=0.9,
            reasoning="Test university incident",
            institution_name="Test University",
        )
        mock_llm_class.return_value = mock_llm
        
        mock_article = ArticleContent(
            url="https://example.com/article",
            title="Test Article",
            content="Test content",
            fetch_successful=True,
        )
        mock_fetcher = Mock()
        mock_fetcher.fetch_article_content.return_value = mock_article
        mock_fetcher_class.return_value = mock_fetcher
        
        # Create enricher
        enricher = IncidentEnricher(llm_client=mock_llm, article_fetcher=mock_fetcher)
        
        # Create sample incident
        incident = BaseIncident(
            incident_id=make_incident_id("test", "https://example.com", "2025-01-15"),
            title="Test Incident",
            all_urls=["https://example.com/article"],
            university_name="Test University",
        )
        
        # Process incident (should skip if not education-related by default)
        result = enricher.process_incident(incident, skip_if_not_education=False)
        
        # Should return None if relevance check fails or enrichment fails
        # In this case, we'd need to mock the full enrichment flow
        # For now, just verify the methods are called correctly
        assert mock_fetcher.fetch_article_content.called
    
    def test_enricher_initialization(self):
        """Test enricher initialization."""
        enricher = IncidentEnricher()
        assert enricher.llm_client is not None
        assert enricher.article_fetcher is not None
        assert enricher.metadata_extractor is not None


class TestEnrichmentSchemas:
    """Tests for enrichment Pydantic schemas."""
    
    def test_education_relevance_check(self):
        """Test EducationRelevanceCheck schema."""
        check = EducationRelevanceCheck(
            is_education_related=True,
            confidence=0.9,
            reasoning="Test reasoning",
            institution_name="Test University",
        )
        
        assert check.is_education_related is True
        assert check.confidence == 0.9
        assert check.institution_name == "Test University"
    
    def test_url_confidence_score(self):
        """Test URLConfidenceScore schema."""
        score = URLConfidenceScore(
            url="https://example.com/article",
            confidence_score=0.85,
            reasoning="Good coverage",
            article_quality="high",
            content_completeness="complete",
            source_reliability="reliable",
        )
        
        assert score.url == "https://example.com/article"
        assert score.confidence_score == 0.85
    
    def test_timeline_event(self):
        """Test TimelineEvent schema."""
        event = TimelineEvent(
            event_date="2025-01-15",
            event_type="attack_discovered",
            description="Attack was discovered",
            confidence=0.9,
        )
        
        assert event.event_date == "2025-01-15"
        assert event.event_type == "attack_discovered"
    
    def test_mitre_attack_technique(self):
        """Test MITREAttackTechnique schema."""
        technique = MITREAttackTechnique(
            technique_id="T1486",
            technique_name="Data Encrypted for Impact",
            tactic="Impact",
            confidence=0.85,
        )
        
        assert technique.technique_id == "T1486"
        assert technique.tactic == "Impact"
    
    def test_attack_dynamics(self):
        """Test AttackDynamics schema."""
        dynamics = AttackDynamics(
            attack_type="ransomware",
            attack_family="LockBit",
            initial_access_vector="phishing",
        )
        
        assert dynamics.attack_type == "ransomware"
        assert dynamics.attack_family == "LockBit"


class TestEnrichmentIntegration:
    """Integration tests for Phase 2 enrichment."""
    
    def test_full_enrichment_flow(self, temp_db, sample_incident):
        """Test the full enrichment flow from incident to enriched result."""
        conn, _ = temp_db
        
        # Insert incident
        insert_incident(conn, sample_incident)
        
        # Verify incident is unenriched
        unenriched = get_unenriched_incidents(conn)
        assert len(unenriched) == 1
        
        # Create and save enrichment (simulating what the pipeline would do)
        enrichment = CTIEnrichmentResult(
            primary_url=sample_incident.all_urls[0],
            extraction_confidence=0.85,
            enriched_summary="Test university was attacked by ransomware",
            timeline=[
                TimelineEvent(
                    event_date="2025-01-15",
                    event_type="attack_discovered",
                    description="Ransomware attack detected",
                    confidence=0.9,
                )
            ],
            mitre_attack_techniques=[
                MITREAttackTechnique(
                    technique_id="T1486",
                    technique_name="Data Encrypted for Impact",
                    tactic="Impact",
                    confidence=0.85,
                )
            ],
            attack_dynamics=AttackDynamics(
                attack_type="ransomware",
                attack_family="LockBit",
                initial_access_vector="phishing",
            ),
            is_education_related=True,
            education_relevance_confidence=0.9,
        )
        
        # Save enrichment
        saved = save_enrichment_result(conn, sample_incident.incident_id, enrichment)
        assert saved is True
        
        # Verify incident is now enriched
        unenriched_after = get_unenriched_incidents(conn)
        assert len(unenriched_after) == 0
        
        # Verify stats
        stats = get_enrichment_stats(conn)
        assert stats["enriched_incidents"] == 1
        assert stats["ready_for_enrichment"] == 0

