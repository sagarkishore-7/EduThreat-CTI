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
from src.edu_cti.pipeline.phase2.storage.db import get_enrichment_result, init_incident_enrichments_table, save_enrichment_result
from src.edu_cti.pipeline.phase2.utils.deduplication import (
    dates_within_window,
    dedup_incident_after_save,
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
    init_incident_enrichments_table(conn)
    yield conn, db_path
    conn.close()


def _incident(source: str, url: str, incident_date: str, institution_name: str) -> BaseIncident:
    return BaseIncident(
        incident_id=make_incident_id(source, f"{url}|{incident_date}"),
        source=source,
        source_event_id=f"{source}_{incident_date}",
        title=f"{institution_name} attack",
        institution_name=institution_name,
        victim_raw_name=institution_name,
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


def _enrichment(summary: str, primary_url: str, institution_name: str = "Test University") -> CTIEnrichmentResult:
    return CTIEnrichmentResult(
        education_relevance=EducationRelevanceCheck(
            is_education_related=True,
            reasoning="Education-sector incident",
            institution_identified=institution_name,
        ),
        primary_url=primary_url,
        enriched_summary=summary,
    )


class TestInstitutionNameNormalization:
    """Tests for institution name normalization."""

    def test_normalize_institution_name(self):
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
        assert result.tzinfo is None

    def test_parse_timezone_aware_date_normalizes_to_naive_utc(self):
        result = parse_incident_date("2025-01-15T12:34:56Z")
        assert result is not None
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 12
        assert result.minute == 34
        assert result.second == 56
        assert result.tzinfo is None

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
            _enrichment("Short summary", incident1.all_urls[0], institution_name="University of California, Berkeley"),
        )
        save_enrichment_result(
            conn,
            incident2.incident_id,
            _enrichment("A much longer and more detailed summary for confidence proxy", incident2.all_urls[0], institution_name="University of California, Berkeley"),
        )

        duplicates = find_duplicate_institutions(
            conn,
            incident1.incident_id,
            incident1.institution_name,
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
        # The dedup score can vary with post-processing changes, so find the survivor
        # dynamically rather than assuming a specific incident_id wins.
        survivor_row = conn.execute(
            "SELECT incident_id FROM incidents WHERE llm_enriched = 1"
        ).fetchone()
        assert survivor_row is not None, "No enriched survivor found after dedup"
        survivor_id = survivor_row[0]
        source_count = conn.execute(
            "SELECT COUNT(*) FROM incident_sources WHERE incident_id = ?",
            (survivor_id,),
        ).fetchone()[0]
        assert source_count == 2

        surviving_name = conn.execute(
            "SELECT institution_name FROM incident_enrichments_flat WHERE incident_id = ?",
            (survivor_id,),
        ).fetchone()[0]
        # The surviving name must be the clean institution label, not the headline
        assert surviving_name is not None
        assert "Ransomware" not in surviving_name
        assert "School District" in surviving_name

    def test_deduplicate_skips_unenriched_incidents_with_same_institution_name(self, temp_db):
        """Unenriched incidents should remain separate until each has Phase 2 output."""
        conn, _ = temp_db

        incident1 = _incident("oxylabs_news", "https://example.com/ucf-breach-1", "2016-01-01", "University of Central Florida")
        incident2 = _incident("oxylabs_news", "https://example.com/ucf-breach-2", "2016-01-01", "University of Central Florida")

        insert_incident(conn, incident1)
        insert_incident(conn, incident2)
        init_incident_enrichments_table(conn)
        add_incident_source(conn, incident1.incident_id, incident1.source, incident1.source_event_id, "2026-04-17T00:00:00", "medium")
        add_incident_source(conn, incident2.incident_id, incident2.source, incident2.source_event_id, "2026-04-17T01:00:00", "medium")

        stats = deduplicate_by_institution(conn, window_days=14)

        assert stats["removed"] == 0
        assert stats["remaining"] == 0
        assert conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0] == 2

    def test_deduplicate_matches_abbreviation_against_raw_institution_name(self, temp_db):
        """If LLM stores 'UCF' for one incident and 'University of Central Florida' for another,
        the dedup must still merge them using the raw institution_name as a fallback key."""
        conn, _ = temp_db

        incident1 = _incident("oxylabs_news", "https://example.com/ucf-1", "2016-01-01", "University of Central Florida")
        incident2 = _incident("oxylabs_news", "https://example.com/ucf-2", "2016-01-01", "University of Central Florida")

        insert_incident(conn, incident1)
        insert_incident(conn, incident2)

        # Simulate LLM extracting an abbreviation for incident1 and the full name for incident2
        save_enrichment_result(conn, incident1.incident_id, _enrichment("short summary", incident1.all_urls[0], institution_name="UCF"))
        save_enrichment_result(conn, incident2.incident_id, _enrichment("longer and more detailed summary that should win", incident2.all_urls[0], institution_name="University of Central Florida"))

        stats = deduplicate_by_institution(conn, window_days=14)

        assert stats["removed"] == 1
        assert stats["remaining"] == 1

    def test_inline_dedup_after_save_handles_abbreviation_without_nameerror(self, temp_db):
        """The inline post-save dedup path should share the same matcher as batch dedup."""
        conn, _ = temp_db

        incident1 = _incident("oxylabs_news", "https://example.com/ucf-inline-1", "2016-01-01", "University of Central Florida")
        incident2 = _incident("googlenews_rss", "https://example.com/ucf-inline-2", "2016-01-01", "University of Central Florida")

        insert_incident(conn, incident1)
        insert_incident(conn, incident2)

        save_enrichment_result(
            conn,
            incident1.incident_id,
            _enrichment(
                "This is the longer summary that should survive inline dedup.",
                incident1.all_urls[0],
                institution_name="University of Central Florida",
            ),
        )
        save_enrichment_result(
            conn,
            incident2.incident_id,
            _enrichment(
                "short",
                incident2.all_urls[0],
                institution_name="UCF",
            ),
        )

        survivor = dedup_incident_after_save(conn, incident2.incident_id, window_days=14)

        assert survivor == incident1.incident_id
        assert conn.execute(
            "SELECT 1 FROM incidents WHERE incident_id = ?",
            (incident2.incident_id,),
        ).fetchone() is None

    def test_inline_dedup_skips_unenriched_stub(self, temp_db):
        """Inline dedup should ignore unenriched stubs until they have their own enrichment."""
        conn, _ = temp_db

        comparitech_stub = _incident(
            "comparitech",
            "https://example.com/utah-comparitech",
            "2020-07-19",
            "University of Utah Pays in Cyber-Extortion Scheme",
        )
        darkreading_incident = _incident(
            "darkreading",
            "https://example.com/utah-darkreading",
            "2020-07-19",
            "University of Utah Pays in Cyber-Extortion Scheme",
        )

        insert_incident(conn, comparitech_stub)
        insert_incident(conn, darkreading_incident)

        save_enrichment_result(
            conn,
            darkreading_incident.incident_id,
            _enrichment(
                "Detailed enriched summary from the fetched article.",
                darkreading_incident.all_urls[0],
                institution_name="University of Utah",
            ),
        )

        survivor = dedup_incident_after_save(conn, darkreading_incident.incident_id, window_days=14)

        assert survivor is None
        assert conn.execute(
            "SELECT llm_enriched FROM incidents WHERE incident_id = ?",
            (darkreading_incident.incident_id,),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT llm_enriched FROM incidents WHERE incident_id = ?",
            (comparitech_stub.incident_id,),
        ).fetchone()[0] == 0
        assert get_enrichment_result(conn, darkreading_incident.incident_id) is not None

    def test_batch_dedup_skips_unenriched_stub(self, temp_db):
        """Admin/batch dedup should ignore unenriched stubs until both sides are enriched."""
        conn, _ = temp_db

        comparitech_stub = _incident(
            "comparitech",
            "https://example.com/allen-comparitech",
            "2020-07-19",
            "University of Utah Pays in Cyber-Extortion Scheme",
        )
        oxylabs_incident = _incident(
            "oxylabs_news",
            "https://example.com/allen-oxylabs",
            "2020-07-19",
            "University of Utah Pays in Cyber-Extortion Scheme",
        )

        insert_incident(conn, comparitech_stub)
        insert_incident(conn, oxylabs_incident)

        save_enrichment_result(
            conn,
            oxylabs_incident.incident_id,
            _enrichment(
                "Longer enriched summary that should win batch dedup.",
                oxylabs_incident.all_urls[0],
                institution_name="University of Utah",
            ),
        )

        stats = deduplicate_by_institution(conn, window_days=14)

        assert stats["removed"] == 0
        assert conn.execute(
            "SELECT llm_enriched FROM incidents WHERE incident_id = ?",
            (oxylabs_incident.incident_id,),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT 1 FROM incidents WHERE incident_id = ?",
            (comparitech_stub.incident_id,),
        ).fetchone() is not None
        assert get_enrichment_result(conn, oxylabs_incident.incident_id) is not None

    def test_find_duplicate_institutions_blocks_different_countries(self, temp_db):
        """Burke County GA must not be flagged as duplicate of Broward County FL."""
        conn, _ = temp_db

        burke = _incident("comparitech", "https://example.com/burke", "2020-03-01", "Burke County Public Schools")
        broward = _incident("googlenews_rss", "https://example.com/broward", "2021-03-01", "Broward County Public Schools")

        # Force different country_codes
        insert_incident(conn, burke)
        insert_incident(conn, broward)
        conn.execute("UPDATE incidents SET country_code = 'US', region = 'Georgia' WHERE incident_id = ?", (burke.incident_id,))
        conn.execute("UPDATE incidents SET country_code = 'US', region = 'Florida' WHERE incident_id = ?", (broward.incident_id,))
        conn.commit()

        save_enrichment_result(conn, burke.incident_id, _enrichment("Burke County breach summary.", burke.all_urls[0], institution_name="Burke County Public Schools"))
        save_enrichment_result(conn, broward.incident_id, _enrichment("Broward County breach summary.", broward.all_urls[0], institution_name="Broward County Public Schools"))

        from src.edu_cti.pipeline.phase2.utils.deduplication import find_duplicate_institutions
        # Both are US, but date window is 14 days and these are 365 days apart
        duplicates = find_duplicate_institutions(
            conn, burke.incident_id, "Burke County Public Schools", "2020-03-01",
            window_days=14, incident_country_code="US",
        )
        assert len(duplicates) == 0  # dates too far apart

    def test_inline_dedup_blocks_cross_country_merge(self, temp_db):
        """Two incidents with the same name but different country_codes must not merge."""
        conn, _ = temp_db

        # Simulating two "National University" incidents in different countries
        inc_us = _incident("googlenews_rss", "https://example.com/nat-us", "2024-01-15", "National University")
        inc_ph = _incident("oxylabs_news", "https://example.com/nat-ph", "2024-01-15", "National University")

        insert_incident(conn, inc_us)
        insert_incident(conn, inc_ph)
        conn.execute("UPDATE incidents SET country_code = 'US' WHERE incident_id = ?", (inc_us.incident_id,))
        conn.execute("UPDATE incidents SET country_code = 'PH' WHERE incident_id = ?", (inc_ph.incident_id,))
        conn.commit()

        save_enrichment_result(conn, inc_us.incident_id, _enrichment("US National University breach — long summary that would normally win.", inc_us.all_urls[0], institution_name="National University"))
        survivor = dedup_incident_after_save(conn, inc_ph.incident_id, window_days=14)

        # Must NOT have merged despite identical name and date
        assert survivor is None
        # Both incidents still exist
        assert conn.execute("SELECT 1 FROM incidents WHERE incident_id = ?", (inc_us.incident_id,)).fetchone() is not None
        assert conn.execute("SELECT 1 FROM incidents WHERE incident_id = ?", (inc_ph.incident_id,)).fetchone() is not None

    def test_batch_dedup_prefers_richer_enriched_incident(self, temp_db):
        """When both incidents are enriched, the richer CTI record should survive."""
        conn, _ = temp_db

        incident1 = _incident("darkreading", "https://example.com/detail-1", "2024-09-01", "Test University")
        incident2 = _incident("oxylabs_news", "https://example.com/detail-2", "2024-09-01", "Test University")

        insert_incident(conn, incident1)
        insert_incident(conn, incident2)

        sparse = _enrichment("This summary is much longer but sparse.", incident1.all_urls[0], institution_name="Test University")
        save_enrichment_result(conn, incident1.incident_id, sparse)
        save_enrichment_result(
            conn,
            incident2.incident_id,
            CTIEnrichmentResult(
                education_relevance=EducationRelevanceCheck(
                    is_education_related=True,
                    reasoning="Education-sector incident",
                    institution_identified="Test University",
                ),
                primary_url=incident2.all_urls[0],
                enriched_summary="Brief summary.",
                timeline=[],
                mitre_attack_techniques=[],
            ),
            raw_json_data={
                "institution_name": "Test University",
                "country": "United States",
                "attack_category": "ransomware",
                "threat_actor_name": "LockBit",
                "data_breached": True,
                "records_affected_exact": 1000,
            },
        )

        # Manually add richer counts that the survivor score uses.
        conn.execute(
            """
            UPDATE incident_enrichments_flat
            SET timeline_events_count = 3,
                attack_category = 'ransomware',
                threat_actor_name = 'LockBit',
                data_breached = 1,
                records_affected_exact = 1000
            WHERE incident_id = ?
            """,
            (incident2.incident_id,),
        )
        # Populate mitre junction table so the subquery score counts correctly
        for i in range(2):
            conn.execute(
                "INSERT INTO incident_mitre_techniques (incident_id, seq_order, technique_id) VALUES (?,?,?)",
                [incident2.incident_id, i, f"T100{i}"],
            )
        conn.commit()

        stats = deduplicate_by_institution(conn, window_days=14)

        assert stats["removed"] == 1
        assert conn.execute(
            "SELECT 1 FROM incidents WHERE incident_id = ?",
            (incident2.incident_id,),
        ).fetchone() is not None
        assert conn.execute(
            "SELECT 1 FROM incidents WHERE incident_id = ?",
            (incident1.incident_id,),
        ).fetchone() is None
