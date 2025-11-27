"""
Contributor Test Suite: Source Integration Verification

This test module is designed for contributors adding new sources to EduThreat-CTI.
It verifies that:
1. New source follows the required patterns
2. Phase 1 ingestion works correctly with the source
3. Data is stored properly in the database
4. Phase 2 can consume the ingested data

Usage:
    # Test a specific source after adding it
    pytest tests/phase1/test_source_contribution.py -v -k "test_source_integration" --source-name <your_source>
    
    # Run all contributor verification tests
    pytest tests/phase1/test_source_contribution.py -v
    
    # Test with limited pages (faster)
    pytest tests/phase1/test_source_contribution.py -v --max-pages 2

Example:
    # After adding a new source called "bleepingcomputer"
    pytest tests/phase1/test_source_contribution.py -v -k "test_source_integration" --source-name bleepingcomputer
"""

import os
import sys
import sqlite3
import tempfile
import pytest
from pathlib import Path
from typing import List, Optional, Dict, Any

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.core.db import get_connection, init_db
from src.edu_cti.core.sources import (
    CURATED_SOURCE_REGISTRY,
    NEWS_SOURCE_REGISTRY,
    RSS_SOURCE_REGISTRY,
    get_all_source_names,
)


# ============================================================================
# TEST FIXTURES
# ============================================================================

def pytest_addoption(parser):
    """Add command-line options for contributor tests."""
    parser.addoption(
        "--source-name",
        action="store",
        default=None,
        help="Specific source name to test (e.g., 'darkreading', 'konbriefing')"
    )
    parser.addoption(
        "--max-pages",
        action="store",
        default="1",
        help="Maximum pages to fetch during testing (default: 1)"
    )


@pytest.fixture
def source_name(request):
    """Get source name from command line or use default."""
    return request.config.getoption("--source-name")


@pytest.fixture
def max_pages(request):
    """Get max pages from command line."""
    return int(request.config.getoption("--max-pages"))


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    # Initialize database
    conn = get_connection(db_path)
    init_db(conn)
    conn.close()
    
    yield db_path
    
    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_source_builder(source_name: str):
    """Get the builder function for a source name."""
    if source_name in CURATED_SOURCE_REGISTRY:
        return CURATED_SOURCE_REGISTRY[source_name], "curated"
    elif source_name in NEWS_SOURCE_REGISTRY:
        return NEWS_SOURCE_REGISTRY[source_name], "news"
    elif source_name in RSS_SOURCE_REGISTRY:
        return RSS_SOURCE_REGISTRY[source_name], "rss"
    else:
        return None, None


def validate_incident_structure(incident: BaseIncident) -> Dict[str, Any]:
    """
    Validate that a BaseIncident has the required structure for Phase 2.
    
    Returns:
        Dict with validation results and any issues found
    """
    issues = []
    
    # Required fields that must not be None/empty
    required_fields = [
        ("incident_id", incident.incident_id),
        ("source", incident.source),
        ("title", incident.title),
    ]
    
    for field_name, value in required_fields:
        if not value:
            issues.append(f"Missing required field: {field_name}")
    
    # Validate incident_id format (should be source_hash)
    if incident.incident_id:
        if "_" not in incident.incident_id:
            issues.append(f"incident_id should be in format 'source_hash', got: {incident.incident_id}")
        elif not incident.incident_id.startswith(incident.source):
            issues.append(f"incident_id should start with source name, got: {incident.incident_id}")
    
    # Validate all_urls is a list with at least one URL
    if not incident.all_urls or len(incident.all_urls) == 0:
        issues.append("all_urls must contain at least one URL for Phase 2 article fetching")
    else:
        # Check URLs are valid
        for url in incident.all_urls:
            if not url.startswith(("http://", "https://")):
                issues.append(f"Invalid URL format: {url}")
    
    # Validate primary_url is None (should be set by Phase 2)
    if incident.primary_url is not None:
        issues.append("primary_url should be None in Phase 1 (set by Phase 2)")
    
    # Validate source_confidence
    valid_confidences = ["low", "medium", "high"]
    if incident.source_confidence not in valid_confidences:
        issues.append(f"source_confidence must be one of {valid_confidences}, got: {incident.source_confidence}")
    
    # Validate date_precision
    valid_precisions = ["day", "month", "year", "unknown"]
    if incident.date_precision not in valid_precisions:
        issues.append(f"date_precision must be one of {valid_precisions}, got: {incident.date_precision}")
    
    # Validate status
    valid_statuses = ["suspected", "confirmed"]
    if incident.status not in valid_statuses:
        issues.append(f"status must be one of {valid_statuses}, got: {incident.status}")
    
    return {
        "valid": len(issues) == 0,
        "issues": issues,
        "incident_id": incident.incident_id,
        "has_urls": len(incident.all_urls) if incident.all_urls else 0,
        "has_title": bool(incident.title),
        "has_date": bool(incident.incident_date),
    }


# ============================================================================
# CONTRIBUTOR TESTS
# ============================================================================

class TestSourceRegistry:
    """Test that sources are properly registered."""
    
    def test_all_sources_have_builders(self):
        """Verify all registered sources have callable builders."""
        all_sources = get_all_source_names()
        
        for source_name in all_sources:
            builder, source_type = get_source_builder(source_name)
            assert builder is not None, f"Source '{source_name}' has no builder"
            assert callable(builder), f"Builder for '{source_name}' is not callable"
    
    def test_source_name_conventions(self):
        """Verify source names follow conventions (lowercase, alphanumeric + underscore)."""
        all_sources = get_all_source_names()
        
        for source_name in all_sources:
            assert source_name == source_name.lower(), f"Source name should be lowercase: {source_name}"
            assert source_name.replace("_", "").isalnum(), f"Source name should be alphanumeric: {source_name}"


class TestSourceIntegration:
    """
    Integration tests for individual source verification.
    
    Use --source-name to test a specific source:
        pytest tests/phase1/test_source_contribution.py -v -k "test_source" --source-name darkreading
    """
    
    def test_source_builds_incidents(self, source_name, max_pages):
        """Test that the source builder produces valid incidents."""
        if source_name is None:
            pytest.skip("No --source-name provided, skipping source-specific test")
        
        builder, source_type = get_source_builder(source_name)
        assert builder is not None, f"Source '{source_name}' not found in registries"
        
        print(f"\n📦 Testing source: {source_name} (type: {source_type})")
        print(f"   Fetching up to {max_pages} page(s)...")
        
        # Build incidents
        try:
            incidents = builder(max_pages=max_pages)
        except Exception as e:
            pytest.fail(f"Source builder failed: {e}")
        
        print(f"   Found {len(incidents)} incidents")
        
        # Validate we got at least one incident
        assert len(incidents) > 0, f"Source '{source_name}' produced no incidents"
        
        # Validate each incident
        validation_results = []
        for incident in incidents:
            result = validate_incident_structure(incident)
            validation_results.append(result)
            
            if not result["valid"]:
                print(f"\n   ⚠️ Validation issues for {incident.incident_id}:")
                for issue in result["issues"]:
                    print(f"      - {issue}")
        
        # Report summary
        valid_count = sum(1 for r in validation_results if r["valid"])
        print(f"\n   ✓ Valid incidents: {valid_count}/{len(incidents)}")
        
        # All incidents should be valid
        invalid = [r for r in validation_results if not r["valid"]]
        if invalid:
            issues_summary = "\n".join(
                f"  - {r['incident_id']}: {', '.join(r['issues'])}"
                for r in invalid[:5]  # Show first 5
            )
            pytest.fail(f"Invalid incidents found:\n{issues_summary}")
    
    def test_source_incidents_ingestable(self, source_name, max_pages, temp_db):
        """Test that source incidents can be ingested into the database."""
        if source_name is None:
            pytest.skip("No --source-name provided, skipping source-specific test")
        
        builder, source_type = get_source_builder(source_name)
        assert builder is not None, f"Source '{source_name}' not found"
        
        from src.edu_cti.core.db import upsert_incident, get_connection
        
        # Build incidents
        incidents = builder(max_pages=max_pages)
        assert len(incidents) > 0, "No incidents to test"
        
        print(f"\n💾 Testing database ingestion for {len(incidents)} incidents...")
        
        # Ingest into temp database
        conn = get_connection(temp_db)
        ingested = 0
        errors = []
        
        for incident in incidents:
            try:
                upsert_incident(conn, incident)
                ingested += 1
            except Exception as e:
                errors.append(f"{incident.incident_id}: {e}")
        
        conn.close()
        
        print(f"   ✓ Ingested: {ingested}/{len(incidents)}")
        
        if errors:
            print(f"   ⚠️ Errors: {len(errors)}")
            for err in errors[:3]:
                print(f"      - {err}")
        
        # All incidents should be ingestable
        assert ingested == len(incidents), f"Failed to ingest {len(incidents) - ingested} incidents"
    
    def test_source_incidents_queryable(self, source_name, max_pages, temp_db):
        """Test that ingested incidents can be queried for Phase 2."""
        if source_name is None:
            pytest.skip("No --source-name provided, skipping source-specific test")
        
        builder, source_type = get_source_builder(source_name)
        incidents = builder(max_pages=max_pages)
        
        from src.edu_cti.core.db import upsert_incident, get_connection
        
        # Ingest
        conn = get_connection(temp_db)
        for incident in incidents:
            upsert_incident(conn, incident)
        
        print(f"\n🔍 Testing Phase 2 readiness for {len(incidents)} incidents...")
        
        # Query like Phase 2 would
        cursor = conn.execute("""
            SELECT 
                incident_id, source, title, all_urls, primary_url,
                llm_enriched, source_confidence
            FROM incidents
            WHERE source = ?
            AND llm_enriched = 0
        """, (source_name,))
        
        rows = cursor.fetchall()
        print(f"   ✓ Unenriched incidents ready for Phase 2: {len(rows)}")
        
        # Verify they have URLs for article fetching
        with_urls = 0
        for row in rows:
            all_urls = row[3]
            if all_urls:
                urls = [u.strip() for u in all_urls.split(";") if u.strip()]
                if urls:
                    with_urls += 1
        
        print(f"   ✓ Incidents with fetchable URLs: {with_urls}/{len(rows)}")
        
        conn.close()
        
        assert with_urls == len(rows), "Some incidents missing URLs for Phase 2 article fetching"


class TestAllSourcesBasic:
    """Quick sanity tests for all registered sources."""
    
    @pytest.mark.parametrize("source_name", list(get_all_source_names()))
    def test_source_builds_without_error(self, source_name):
        """Test that each source can build incidents without crashing (1 page only)."""
        builder, source_type = get_source_builder(source_name)
        
        if builder is None:
            pytest.fail(f"No builder found for {source_name}")
        
        try:
            # Try with max_pages first (news sources), then without (curated/rss)
            import inspect
            sig = inspect.signature(builder)
            params = sig.parameters
            
            if "max_pages" in params:
                incidents = builder(max_pages=1)
            elif "max_age_days" in params:
                # RSS sources use max_age_days
                incidents = builder(max_age_days=1)
            else:
                # Curated sources may not have pagination
                incidents = builder()
            
            print(f"\n  ✓ {source_name}: {len(incidents)} incidents")
        except Exception as e:
            # Don't fail on network errors, but report them
            if "connection" in str(e).lower() or "timeout" in str(e).lower():
                pytest.skip(f"Network error (expected in CI): {e}")
            else:
                pytest.fail(f"Source '{source_name}' failed: {e}")


# ============================================================================
# PHASE 2 READINESS VERIFICATION
# ============================================================================

class TestPhase2Readiness:
    """Verify incidents are properly structured for Phase 2 enrichment."""
    
    def test_incident_has_enrichable_content(self, source_name, max_pages):
        """Test that incidents have content that Phase 2 can enrich."""
        if source_name is None:
            pytest.skip("No --source-name provided")
        
        builder, _ = get_source_builder(source_name)
        incidents = builder(max_pages=max_pages)
        
        print(f"\n📊 Phase 2 Readiness Analysis for {source_name}:")
        
        stats = {
            "total": len(incidents),
            "has_title": 0,
            "has_urls": 0,
            "has_date": 0,
            "has_institution": 0,
            "has_country": 0,
        }
        
        for incident in incidents:
            if incident.title:
                stats["has_title"] += 1
            if incident.all_urls and len(incident.all_urls) > 0:
                stats["has_urls"] += 1
            if incident.incident_date:
                stats["has_date"] += 1
            if incident.university_name or incident.victim_raw_name:
                stats["has_institution"] += 1
            if incident.country:
                stats["has_country"] += 1
        
        print(f"   Total incidents: {stats['total']}")
        print(f"   With title:      {stats['has_title']} ({100*stats['has_title']/stats['total']:.0f}%)")
        print(f"   With URLs:       {stats['has_urls']} ({100*stats['has_urls']/stats['total']:.0f}%)")
        print(f"   With date:       {stats['has_date']} ({100*stats['has_date']/stats['total']:.0f}%)")
        print(f"   With institution: {stats['has_institution']} ({100*stats['has_institution']/stats['total']:.0f}%)")
        print(f"   With country:    {stats['has_country']} ({100*stats['has_country']/stats['total']:.0f}%)")
        
        # Minimum requirements for Phase 2
        assert stats["has_urls"] == stats["total"], "All incidents must have URLs for Phase 2"
        assert stats["has_title"] > 0, "At least some incidents should have titles"


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Allow running directly
    import sys
    
    print("=" * 60)
    print("EduThreat-CTI Source Contribution Test Suite")
    print("=" * 60)
    print()
    print("Usage:")
    print("  pytest tests/phase1/test_source_contribution.py -v")
    print("  pytest tests/phase1/test_source_contribution.py -v --source-name <source>")
    print()
    print("Available sources:")
    for source_name in sorted(get_all_source_names()):
        builder, source_type = get_source_builder(source_name)
        print(f"  - {source_name} ({source_type})")
    print()
    
    # Run with pytest
    sys.exit(pytest.main([__file__, "-v"] + sys.argv[1:]))

