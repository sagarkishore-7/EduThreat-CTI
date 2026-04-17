"""Tests for Phase 2 post-enrichment deduplication."""

from datetime import datetime

import pytest

from src.edu_cti.core.db import (
    add_incident_source,
    get_connection,
    init_db,
    insert_incident,
    register_source_event,
)
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.pipeline.phase2.schemas import CTIEnrichmentResult, EducationRelevanceCheck
from src.edu_cti.pipeline.phase2.storage.db import get_enrichment_result, save_enrichment_result
from src.edu_cti.pipeline.phase2.utils.deduplication import (
    dates_within_window,
    deduplicate_by_institution,
    find_duplicate_institutions,
    normalize_institution_name,
    parse_incident_date,
)


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    yield conn, db_path
    conn.close()


def _incident(source: str, url: str, incident_date: str, university_name: str) -> BaseIncident:
    return BaseIncident(
        incident_id=make_incident_id(source, f"{url}|{incident_date}"),
        source=source,
        source_event_id=f"{source}_{incident_date}",
        title=f"{university_name} attack",
        university_name=university_name,
        victim_raw_name=university_name,
        institution_type="university",
        country="United States",
        region=None,
        city=None,
        incident_date=incident_date,
        date_precision="day",
        source_published_date=None,
        ingested_at=None,
        subtitle=None,
        primary_url=None,
        all_urls=[url],
        status="confirmed",
        source_confidence="high",
    )


def _enrichment(summary: str, primary_url: str) -> CTIEnrichmentResult:
    return CTIEnrichmentResult(
        education_relevance=EducationRelevanceCheck(
            is_education_related=True,
            reasoning="Education-sector incident",
            institution_identified="Test University",
        ),
        primary_url=primary_url,
        enriched_summary=summary,
    )


class TestInstitutionNameNormalization:
    """Tests for institution name normalization."""

    def test_normalize_university_name(self):
        assert normalize_institution_name("University of California, Berkeley") == "california berkeley"
        assert normalize_institution_name("UC Berkeley") == "uc berkeley"
        assert normalize_institution_name("The University of Texas at Austin") == "texas at austin"

    def test_normalize_removes_common_words(self):
        assert normalize_institution_name("The University of California") == "california"
        assert normalize_institution_name("California State University") == "california state"

    def test_normalize_lowercase(self):
        assert normalize_institution_name("MIT") == "mit"
        assert normalize_institution_name("Stanford University") == "stanford"

    def test_normalize_handles_punctuation(self):
        assert normalize_institution_name("University of California, Los Angeles") == "california los angeles"
        assert normalize_institution_name("UCLA") == "ucla"

    def test_normalize_cleans_attack_headline_wrappers(self):
        assert (
            normalize_institution_name("Qilin Ransomware Targets Alamo Heights School District")
            == "alamo heights school district"
        )


class TestDateParsing:
    """Tests for date parsing."""

    def test_parse_standard_date(self):
        result = parse_incident_date("2025-01-15")
        assert result is not None
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15

    def test_parse_invalid_date(self):
        result = parse_incident_date("invalid-date")
        assert result is None

    def test_parse_none_date(self):
        assert parse_incident_date(None) is None


class TestDateWindow:
    """Tests for date window checking."""

    def test_dates_within_window(self):
        date1 = datetime(2025, 1, 15)
        date2 = datetime(2025, 1, 20)

        assert dates_within_window(date1, date2, days=14) is True
        assert dates_within_window(date1, date2, days=3) is False

    def test_dates_outside_window(self):
        date1 = datetime(2025, 1, 15)
        date2 = datetime(2025, 2, 1)

        assert dates_within_window(date1, date2, days=14) is False


class TestDeduplication:
    """Tests for post-enrichment deduplication."""

    def test_find_duplicate_institutions(self, temp_db):
        conn, _ = temp_db

        incident1 = _incident(
            "source1",
            "https://example.com/article1",
            "2025-01-15",
            "University of California, Berkeley",
        )
        incident2 = _incident(
            "source2",
            "https://example.com/article2",
            "2025-01-18",
            "University of California, Berkeley",
        )

        insert_incident(conn, incident1)
        insert_incident(conn, incident2)

        save_enrichment_result(
            conn,
            incident1.incident_id,
            _enrichment("Short summary", incident1.all_urls[0]),
        )
        save_enrichment_result(
            conn,
            incident2.incident_id,
            _enrichment("A much longer and more detailed summary for confidence proxy", incident2.all_urls[0]),
        )

        duplicates = find_duplicate_institutions(
            conn,
            incident1.incident_id,
            incident1.university_name,
            incident1.incident_date,
            window_days=14,
        )

        assert len(duplicates) == 1
        assert duplicates[0]["incident_id"] == incident2.incident_id

    def test_deduplicate_by_institution_keeps_highest_summary_confidence(self, temp_db):
        conn, _ = temp_db

        incident1 = _incident("source1", "https://example.com/article1", "2025-01-15", "Test University")
        incident2 = _incident("source2", "https://example.com/article2", "2025-01-16", "Test University")
        incident3 = _incident("source3", "https://example.com/article3", "2025-01-17", "Test University")

        insert_incident(conn, incident1)
        insert_incident(conn, incident2)
        insert_incident(conn, incident3)

        save_enrichment_result(conn, incident1.incident_id, _enrichment("short", incident1.all_urls[0]))
        save_enrichment_result(
            conn,
            incident2.incident_id,
            _enrichment(
                "This is the longest summary and should therefore win the dedup confidence proxy.",
                incident2.all_urls[0],
            ),
        )
        save_enrichment_result(conn, incident3.incident_id, _enrichment("medium length summary", incident3.all_urls[0]))

        stats = deduplicate_by_institution(conn, window_days=14)

        assert stats["removed"] == 2
        assert stats["remaining"] == 1

        assert get_enrichment_result(conn, incident2.incident_id) is not None
        assert get_enrichment_result(conn, incident1.incident_id) is None
        assert get_enrichment_result(conn, incident3.incident_id) is None

    def test_deduplicate_by_institution_merges_source_attribution_for_headline_style_name(self, temp_db):
        conn, _ = temp_db

        incident1 = _incident(
            "oxylabs_news",
            "https://example.com/dexpose",
            "2026-04-09",
            "Qilin Ransomware Targets Alamo Heights School District",
        )
        incident2 = _incident(
            "ransomlook",
            "https://example.com/news",
            "2026-04-09",
            "Alamo Heights Independent School District",
        )

        insert_incident(conn, incident1)
        insert_incident(conn, incident2)
        add_incident_source(conn, incident1.incident_id, incident1.source, incident1.source_event_id, "2026-04-10T00:00:00", "medium")
        add_incident_source(conn, incident2.incident_id, incident2.source, incident2.source_event_id, "2026-04-10T01:00:00", "high")
        register_source_event(conn, incident1.source, incident1.source_event_id, incident1.incident_id, "2026-04-10T00:00:00")
        register_source_event(conn, incident2.source, incident2.source_event_id, incident2.incident_id, "2026-04-10T01:00:00")

        save_enrichment_result(
            conn,
            incident1.incident_id,
            CTIEnrichmentResult(
                education_relevance=EducationRelevanceCheck(
                    is_education_related=True,
                    reasoning="Education-sector incident",
                    institution_identified="Qilin Ransomware Targets Alamo Heights School District",
                ),
                primary_url=incident1.all_urls[0],
                enriched_summary="short",
            ),
        )
        save_enrichment_result(
            conn,
            incident2.incident_id,
            CTIEnrichmentResult(
                education_relevance=EducationRelevanceCheck(
                    is_education_related=True,
                    reasoning="Education-sector incident",
                    institution_identified="Alamo Heights Independent School District",
                ),
                primary_url=incident2.all_urls[0],
                enriched_summary="This summary is longer and should win the merge.",
            ),
        )

        stats = deduplicate_by_institution(conn, window_days=14)

        assert stats["removed"] == 1
        source_count = conn.execute(
            "SELECT COUNT(*) FROM incident_sources WHERE incident_id = ?",
            (incident2.incident_id,),
        ).fetchone()[0]
        assert source_count == 2

        surviving_name = conn.execute(
            "SELECT institution_name FROM incident_enrichments_flat WHERE incident_id = ?",
            (incident2.incident_id,),
        ).fetchone()[0]
        assert surviving_name == "Alamo Heights Independent School District"
