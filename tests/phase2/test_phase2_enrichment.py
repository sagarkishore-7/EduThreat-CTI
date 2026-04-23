"""Tests for Phase 2 enrichment, storage, and recovery behavior."""

import importlib
from dataclasses import replace
from types import SimpleNamespace
from typing import Optional
from unittest.mock import Mock, patch

import pytest
from bs4 import BeautifulSoup

from src.edu_cti.core.config import SERP_MAX_ATTEMPTS
from src.edu_cti.core.db import get_connection, init_db, insert_incident
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.pipeline.phase2.enrichment import IncidentEnricher
from src.edu_cti.pipeline.phase2.schemas import (
    AttackDynamics,
    CTIEnrichmentResult,
    EducationRelevanceCheck,
    MITREAttackTechnique,
    TimelineEvent,
)
from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleContent, ArticleFetcher
from src.edu_cti.pipeline.phase2.storage.article_storage import init_articles_table, save_article
from src.edu_cti.pipeline.phase2.storage.db import (
    checkpoint_clear,
    checkpoint_get_fetched,
    checkpoint_mark,
    delete_incident,
    get_enrichment_result,
    get_enrichment_stats,
    get_unenriched_incidents,
    init_incident_enrichments_table,
    revert_enrichment_before_date,
    save_enrichment_result,
)
from src.edu_cti.pipeline.phase2.utils.fetching_strategy import SmartArticleFetchingStrategy


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    init_incident_enrichments_table(conn)
    init_articles_table(conn)
    yield conn, db_path
    conn.close()


@pytest.fixture
def sample_incident():
    """Create a sample incident for testing."""
    return BaseIncident(
        incident_id=make_incident_id("test_source", "https://example.com/test|2025-01-15"),
        source="test_source",
        source_event_id="test_source_event",
        victim_raw_name="Test University",
        title="Test University Cyber Attack",
        subtitle="Ransomware attack on university systems",
        institution_name="Test University",
        institution_type="university",
        country="United States",
        city=None,
        region="North America",
        incident_date="2025-01-15",
        date_precision="day",
        source_published_date=None,
        ingested_at=None,
        primary_url=None,
        all_urls=["https://example.com/article1", "https://example.com/article2"],
        attack_type_hint="ransomware",
        status="confirmed",
        source_confidence="high",
    )


def _sample_enrichment_result(
    primary_url: str = "https://example.com/article1",
    summary: str = "Test university was hit by ransomware.",
    is_education_related: bool = True,
    institution_name: Optional[str] = "Test University",
) -> CTIEnrichmentResult:
    return CTIEnrichmentResult(
        education_relevance=EducationRelevanceCheck(
            is_education_related=is_education_related,
            reasoning="Education-targeted incident" if is_education_related else "Not education-related",
            institution_identified=institution_name,
        ),
        primary_url=primary_url,
        enriched_summary=summary,
        timeline=[
            TimelineEvent(
                date="2025-01-15",
                event_type="discovery",
                event_description="Ransomware attack detected",
            )
        ],
        mitre_attack_techniques=[
            MITREAttackTechnique(
                technique_id="T1486",
                technique_name="Data Encrypted for Impact",
                tactic="Impact",
            )
        ],
        attack_dynamics=AttackDynamics(
            attack_vector="malicious_attachment",
            ransomware_family="LockBit",
            data_exfiltration=True,
        ),
    )


def _save_sample_article(
    conn,
    incident_id: str,
    url: str,
    title: str = "Test Article",
    *,
    author: Optional[str] = None,
    publish_date: Optional[str] = None,
) -> None:
    content = (
        "This is a detailed article about an education-sector cyber incident. "
        "It contains enough text to satisfy the Phase 2 content-length filter."
    )
    save_article(
        conn,
        incident_id=incident_id,
        url=url,
        article=ArticleContent(
            url=url,
            title=title,
            content=content,
            author=author,
            publish_date=publish_date,
            fetch_successful=True,
            content_length=len(content),
        ),
    )


class TestArticleFetcher:
    """Tests for article fetching functionality."""

    def test_extracts_structured_metadata_from_json_ld(self):
        fetcher = ArticleFetcher(http_client=Mock())
        soup = BeautifulSoup(
            """
            <html><head>
            <script type="application/ld+json">
            {
              "@context": "https://schema.org",
              "@type": "ReportageNewsArticle",
              "datePublished": "2018-09-14T00:09:33.000Z",
              "author": [{"@type": "Person", "name": "By Sean Coughlan"}]
            }
            </script>
            </head><body></body></html>
            """,
            "html.parser",
        )

        assert fetcher._extract_publish_date(soup) == "2018-09-14"
        assert fetcher._extract_author(soup) == "Sean Coughlan"

    def test_extracts_structured_metadata_from_next_data(self):
        fetcher = ArticleFetcher(http_client=Mock())
        soup = BeautifulSoup(
            """
            <html><head>
            <script id="__NEXT_DATA__" type="application/json">
            {
              "props": {
                "pageProps": {
                  "metadata": {
                    "firstPublished": 1731672000000,
                    "contributor": "Jane Doe"
                  }
                }
              }
            }
            </script>
            </head><body></body></html>
            """,
            "html.parser",
        )

        assert fetcher._extract_publish_date(soup) == "2024-11-15"
        assert fetcher._extract_author(soup) == "Jane Doe"

    def test_fetch_article_success(self):
        """Fetcher should return the first successful article from the fallback chain."""
        fetcher = ArticleFetcher(http_client=Mock())
        success = ArticleContent(
            url="https://example.com/article",
            title="Test Article",
            content="This is test content about a cyber incident.",
            fetch_successful=True,
            content_length=42,
        )
        failed = ArticleContent(
            url="https://example.com/article",
            title="",
            content="",
            fetch_successful=False,
            error_message="newspaper failed",
        )

        with patch.object(fetcher, "_fetch_with_newspaper", return_value=failed), patch.object(
            fetcher, "_fetch_with_browser", return_value=success
        ):
            result = fetcher.fetch_article("https://example.com/article")

        assert result.fetch_successful is True
        assert result.title == "Test Article"
        assert "test content" in result.content.lower()

    def test_fetch_article_failure(self):
        """Fetcher should return an error result if every tier fails."""
        fetcher = ArticleFetcher(http_client=Mock())
        failed = ArticleContent(
            url="https://example.com/article",
            title="",
            content="",
            fetch_successful=False,
            error_message="failed",
        )

        with patch.object(fetcher, "_fetch_with_newspaper", return_value=failed), patch.object(
            fetcher, "_fetch_with_browser", return_value=failed
        ), patch.object(fetcher, "_fetch_with_oxylabs", return_value=failed), patch.object(
            fetcher, "_fetch_from_archive", return_value=failed
        ):
            result = fetcher.fetch_article("https://example.com/article")

        assert result.fetch_successful is False
        assert "All fetch methods failed" in result.error_message


class TestEnrichmentDatabase:
    """Tests for enrichment database operations."""

    def test_get_unenriched_incidents(self, temp_db, sample_incident):
        """Test retrieving unenriched incidents."""
        conn, _ = temp_db
        insert_incident(conn, sample_incident)

        unenriched = get_unenriched_incidents(conn)

        assert len(unenriched) == 1
        assert unenriched[0]["incident_id"] == sample_incident.incident_id

    def test_save_enrichment_result(self, temp_db, sample_incident):
        """Saving an enrichment should persist the structured result."""
        conn, _ = temp_db
        insert_incident(conn, sample_incident)

        enrichment = _sample_enrichment_result(primary_url="https://example.com/article1")
        saved = save_enrichment_result(conn, sample_incident.incident_id, enrichment)

        assert saved is True
        saved_enrichment = get_enrichment_result(conn, sample_incident.incident_id)
        assert saved_enrichment is not None
        assert saved_enrichment.primary_url == "https://example.com/article1"
        assert saved_enrichment.enriched_summary == "Test university was hit by ransomware."

    def test_save_enrichment_result_skips_schema_bootstrap_on_hot_path(self, temp_db, sample_incident):
        """Per-incident saves should not re-run schema DDL during enrichment."""
        conn, _ = temp_db
        insert_incident(conn, sample_incident)

        with patch(
            "src.edu_cti.pipeline.phase2.storage.db.init_incident_enrichments_table",
            side_effect=AssertionError("schema bootstrap should happen before worker writes"),
        ):
            saved = save_enrichment_result(
                conn,
                sample_incident.incident_id,
                _sample_enrichment_result(primary_url="https://example.com/article1"),
            )

        assert saved is True


class TestFetchingStrategy:
    def test_get_random_incidents_strips_internal_placeholder_urls(self, temp_db):
        conn, _ = temp_db
        incident = BaseIncident(
            incident_id="comparitech_placeholder_case",
            source="comparitech",
            source_event_id="comparitech_placeholder_case",
            victim_raw_name="Penncrest School District",
            title="Ransomware attack on Penncrest School District (2023)",
            subtitle=None,
            institution_name="Penncrest School District",
            institution_type="School",
            country="US",
            city=None,
            region="PA",
            incident_date="2023",
            date_precision="year",
            source_published_date=None,
            ingested_at=None,
            primary_url=None,
            all_urls=["comparitech://synthetic/comparitech_placeholder_case"],
            attack_type_hint="ransomware",
            status="confirmed",
            source_confidence="high",
            notes="Ransomware: Qilin",
        )
        insert_incident(conn, incident)

        strategy = SmartArticleFetchingStrategy(conn)
        selected = strategy.get_random_incidents_for_enrichment(limit=5)

        assert len(selected) == 1
        assert selected[0]["incident_id"] == incident.incident_id
        assert selected[0]["all_urls"] == []

    def test_fetch_articles_skips_internal_placeholder_and_goes_straight_to_serp(self, temp_db):
        conn, _ = temp_db
        insert_incident(
            conn,
            BaseIncident(
                incident_id="comparitech_5569601cfc8fe0b9",
                source="comparitech",
                source_event_id="comparitech_5569601cfc8fe0b9",
                victim_raw_name="Penncrest School District",
                title="Ransomware attack on Penncrest School District (2023)",
                subtitle=None,
                institution_name="Penncrest School District",
                institution_type="School",
                country="US",
                city=None,
                region="PA",
                incident_date="2023",
                date_precision="year",
                source_published_date=None,
                ingested_at=None,
                primary_url=None,
                all_urls=["comparitech://synthetic/comparitech_5569601cfc8fe0b9"],
                attack_type_hint="ransomware",
                status="confirmed",
                source_confidence="high",
                notes="Ransomware: Qilin",
            ),
        )
        fetcher = Mock()
        fetcher.fetch_article.return_value = ArticleContent(
            url="https://therecord.media/penncrest-ransomware",
            title="Penncrest ransomware story",
            content="Detailed coverage of the Penncrest School District ransomware incident.",
            fetch_successful=True,
            content_length=72,
        )
        strategy = SmartArticleFetchingStrategy(conn, article_fetcher=fetcher)
        strategy.rate_limiter.wait_if_needed = Mock()

        incident = {
            "incident_id": "comparitech_5569601cfc8fe0b9",
            "all_urls": ["comparitech://synthetic/comparitech_5569601cfc8fe0b9"],
            "institution_name": "Penncrest School District",
            "victim_raw_name": "Penncrest School District",
            "title": "Ransomware attack on Penncrest School District (2023)",
            "source_published_date": None,
            "attack_type_hint": "ransomware",
            "notes": "Ransomware: Qilin",
            "incident_date": "2023",
            "city": None,
            "region": "PA",
            "country": "US",
            "has_articles": False,
        }

        with patch(
            "src.edu_cti.pipeline.phase2.utils.fetching_strategy.discover_articles_via_serp",
            return_value=["https://therecord.media/penncrest-ransomware"],
        ) as discover_mock:
            results = strategy.fetch_articles_for_incidents([incident])

        discover_mock.assert_called_once()
        fetcher.fetch_article.assert_called_once_with("https://therecord.media/penncrest-ransomware")
        assert results[incident["incident_id"]]
        assert results[incident["incident_id"]][0].url == "https://therecord.media/penncrest-ransomware"

    def test_save_enrichment_result_replaces_existing_entry(self, temp_db, sample_incident):
        """Later saves should replace the existing enrichment snapshot."""
        conn, _ = temp_db
        insert_incident(conn, sample_incident)

        first = _sample_enrichment_result(
            primary_url="https://example.com/article1",
            summary="First summary",
        )
        second = _sample_enrichment_result(
            primary_url="https://example.com/article2",
            summary="Second summary",
        )

        assert save_enrichment_result(conn, sample_incident.incident_id, first) is True
        assert save_enrichment_result(conn, sample_incident.incident_id, second) is True

        final = get_enrichment_result(conn, sample_incident.incident_id)
        assert final.primary_url == "https://example.com/article2"
        assert final.enriched_summary == "Second summary"

    def test_save_enrichment_result_duplicate_cleanup_removes_old_related_rows(self, temp_db, sample_incident):
        conn, _ = temp_db

        # Use all_urls=[] so URL-redirect logic never fires (test is about dedup, not URL matching)
        survivor = replace(sample_incident, all_urls=[])
        survivor.incident_id = make_incident_id("test_source", "https://example.com/survivor|2025-01-15")
        survivor.source_event_id = "survivor_event"
        insert_incident(conn, survivor)
        assert save_enrichment_result(
            conn,
            survivor.incident_id,
            _sample_enrichment_result(primary_url="https://example.com/shared"),
        ) is True

        duplicate = replace(sample_incident, all_urls=[])
        duplicate.incident_id = make_incident_id("test_source", "https://example.com/duplicate|2025-01-15")
        duplicate.source_event_id = "duplicate_event"
        insert_incident(conn, duplicate)

        assert save_enrichment_result(
            conn,
            duplicate.incident_id,
            _sample_enrichment_result(primary_url="https://example.com/old"),
        ) is True
        _save_sample_article(conn, duplicate.incident_id, "https://example.com/old")

        save_enrichment_result(
            conn,
            duplicate.incident_id,
            _sample_enrichment_result(primary_url="https://example.com/shared"),
        )

        assert conn.execute(
            "SELECT 1 FROM incidents WHERE incident_id = ?",
            (duplicate.incident_id,),
        ).fetchone() is None
        assert conn.execute(
            "SELECT 1 FROM incident_enrichments WHERE incident_id = ?",
            (duplicate.incident_id,),
        ).fetchone() is None
        assert conn.execute(
            "SELECT 1 FROM incident_enrichments_flat WHERE incident_id = ?",
            (duplicate.incident_id,),
        ).fetchone() is None
        assert conn.execute(
            "SELECT 1 FROM articles WHERE incident_id = ?",
            (duplicate.incident_id,),
        ).fetchone() is None

    def test_revert_enrichment_before_date_purges_orphan_rows(self, temp_db):
        conn, _ = temp_db
        now = "2026-01-01T00:00:00"

        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            """
            INSERT INTO incident_enrichments_flat
            (incident_id, is_education_related, institution_name, created_at, updated_at, enriched_summary)
            VALUES (?, 1, ?, ?, ?, ?)
            """,
            ("orphan_incident_1", "Ghost University", now, now, "orphan flat"),
        )
        conn.execute(
            """
            INSERT INTO incident_enrichments
            (incident_id, enrichment_data, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            ("orphan_incident_1", "{}", now, now),
        )
        conn.commit()
        conn.execute("PRAGMA foreign_keys = ON")

        reverted = revert_enrichment_before_date(conn, "2099-01-01")

        assert reverted == 0
        assert conn.execute(
            "SELECT 1 FROM incident_enrichments_flat WHERE incident_id = ?",
            ("orphan_incident_1",),
        ).fetchone() is None
        assert conn.execute(
            "SELECT 1 FROM incident_enrichments WHERE incident_id = ?",
            ("orphan_incident_1",),
        ).fetchone() is None

    def test_save_enrichment_result_cleans_headline_style_institution_name(self, temp_db, sample_incident):
        conn, _ = temp_db
        sample_incident.institution_name = "Qilin Ransomware Targets Alamo Heights School District"
        sample_incident.victim_raw_name = None
        sample_incident.title = "Qilin Ransomware Targets Alamo Heights School District"
        insert_incident(conn, sample_incident)

        enrichment = _sample_enrichment_result(
            institution_name="Qilin Ransomware Targets Alamo Heights School District",
        )

        assert save_enrichment_result(conn, sample_incident.incident_id, enrichment) is True

        incident_row = conn.execute(
            "SELECT institution_name FROM incidents WHERE incident_id = ?",
            (sample_incident.incident_id,),
        ).fetchone()
        flat_row = conn.execute(
            "SELECT institution_name FROM incident_enrichments_flat WHERE incident_id = ?",
            (sample_incident.incident_id,),
        ).fetchone()

        assert incident_row["institution_name"] == "Alamo Heights School District"
        assert flat_row["institution_name"] == "Alamo Heights School District"

    def test_save_enrichment_result_uses_reasoning_when_structured_name_is_missing(self, temp_db, sample_incident):
        conn, _ = temp_db
        sample_incident.institution_name = "Hackers expose Victorian student details in data breach"
        sample_incident.victim_raw_name = None
        sample_incident.title = "Hackers expose Victorian student details in data breach"
        sample_incident.subtitle = (
            "Victorian Department of Education data breach affects hundreds of thousands of students."
        )
        insert_incident(conn, sample_incident)

        enrichment = _sample_enrichment_result(institution_name=None)
        enrichment.education_relevance.reasoning = (
            "Victim is Victorian Department of Education, which manages government schools and student data."
        )

        assert save_enrichment_result(conn, sample_incident.incident_id, enrichment) is True

        incident_row = conn.execute(
            "SELECT institution_name FROM incidents WHERE incident_id = ?",
            (sample_incident.incident_id,),
        ).fetchone()
        flat_row = conn.execute(
            "SELECT institution_name FROM incident_enrichments_flat WHERE incident_id = ?",
            (sample_incident.incident_id,),
        ).fetchone()

        assert incident_row["institution_name"] == "Victorian Department of Education"
        assert flat_row["institution_name"] == "Victorian Department of Education"

    def test_save_enrichment_result_coerces_list_scalar_raw_json_fields(self, temp_db, sample_incident):
        """List-typed scalar LLM fields should be flattened before writing to SQLite."""
        conn, _ = temp_db
        insert_incident(conn, sample_incident)

        enrichment = _sample_enrichment_result()
        raw_json_data = {
            "country": ["United States"],
            "institution_type": ["university"],
            "attack_category": ["ransomware"],
            "attack_vector": ["phishing"],
            "ransomware_family_or_group": ["LockBit"],
            "was_ransom_demanded": [True],
        }

        assert save_enrichment_result(
            conn,
            sample_incident.incident_id,
            enrichment,
            raw_json_data=raw_json_data,
        )

        row = conn.execute(
            """
            SELECT country, institution_type, attack_category, attack_vector, ransomware_family
            FROM incident_enrichments_flat
            WHERE incident_id = ?
            """,
            (sample_incident.incident_id,),
        ).fetchone()

        assert row["country"] == "United States"
        assert row["institution_type"] == "university_public"  # normalized from "university"
        assert row["attack_category"] == "ransomware"
        assert row["attack_vector"] == "phishing"
        assert row["ransomware_family"] == "LockBit"

    def test_save_enrichment_result_backfills_source_published_date_from_article_metadata(
        self, temp_db, sample_incident
    ):
        conn, _ = temp_db
        sample_incident.source_published_date = None
        insert_incident(conn, sample_incident)
        _save_sample_article(
            conn,
            sample_incident.incident_id,
            "https://example.com/article1",
            publish_date="2025-01-20",
        )

        assert save_enrichment_result(
            conn,
            sample_incident.incident_id,
            _sample_enrichment_result(primary_url="https://example.com/article1"),
            raw_json_data={"institution_name": "Test University"},
        ) is True

        row = conn.execute(
            "SELECT source_published_date FROM incidents WHERE incident_id = ?",
            (sample_incident.incident_id,),
        ).fetchone()

        assert row["source_published_date"] == "2025-01-20"

    def test_save_enrichment_result_derives_country_from_article_context(self, temp_db):
        """Explicit country evidence in the fetched article should backfill missing LLM output."""
        conn, _ = temp_db
        incident = BaseIncident(
            incident_id="country_fallback_incident",
            source="googlenews_rss",
            source_event_id="https://example.com/georgia-tech",
            victim_raw_name=None,
            title="Georgia Tech Security Breach Exposes 1.3 Million Records",
            subtitle="Georgia Tech Security Breach Exposes 1.3 Million Records Security Magazine",
            institution_name="Georgia Tech",
            institution_type=None,
            country=None,
            city=None,
            region=None,
            incident_date="2019-03-31",
            date_precision="day",
            source_published_date="2019-03-31",
            ingested_at=None,
            primary_url=None,
            all_urls=["https://example.com/georgia-tech"],
            attack_type_hint="data breach",
            status="confirmed",
            source_confidence="medium",
        )
        insert_incident(conn, incident)
        save_article(
            conn,
            incident.incident_id,
            "https://example.com/georgia-tech",
            ArticleContent(
                url="https://example.com/georgia-tech",
                title="Georgia Tech Security Breach Exposes 1.3 Million Records",
                content=(
                    "Georgia Institute of Technology confirmed the breach. "
                    "The U.S. Department of Education and University System of Georgia "
                    "have been notified."
                ),
                fetch_successful=True,
                content_length=160,
            ),
            is_primary=True,
        )

        assert save_enrichment_result(conn, incident.incident_id, _sample_enrichment_result())

        incident_row = conn.execute(
            "SELECT country, country_code FROM incidents WHERE incident_id = ?",
            (incident.incident_id,),
        ).fetchone()
        flat_row = conn.execute(
            """
            SELECT country, country_code
            FROM incident_enrichments_flat
            WHERE incident_id = ?
            """,
            (incident.incident_id,),
        ).fetchone()

        assert incident_row["country"] == "United States"
        assert incident_row["country_code"] == "US"
        assert flat_row["country"] == "United States"
        assert flat_row["country_code"] == "US"

    def test_save_enrichment_result_backfills_curated_ransom_fields_from_notes(
        self, temp_db, sample_incident
    ):
        conn, _ = temp_db
        sample_incident.source = "comparitech"
        sample_incident.notes = (
            "Ransomware: Qilin | Ransom paid: Yes | "
            "Ransom amount: $457,059 | Records affected: 12,345"
        )
        insert_incident(conn, sample_incident)

        enrichment = _sample_enrichment_result()
        enrichment.attack_dynamics.ransomware_family = None

        assert save_enrichment_result(conn, sample_incident.incident_id, enrichment) is True

        row = conn.execute(
            """
            SELECT ransomware_family, was_ransom_demanded, ransom_amount, ransom_currency,
                   ransom_paid, ransom_paid_amount, records_affected_exact
            FROM incident_enrichments_flat
            WHERE incident_id = ?
            """,
            (sample_incident.incident_id,),
        ).fetchone()

        assert row["ransomware_family"] == "Qilin"
        assert row["was_ransom_demanded"] == 1
        assert row["ransom_amount"] == 457059
        assert row["ransom_currency"] == "USD"
        assert row["ransom_paid"] == 1
        assert row["ransom_paid_amount"] == 457059
        assert row["records_affected_exact"] == 12345

    def test_get_enrichment_stats(self, temp_db, sample_incident):
        """Enrichment stats should move incidents from unenriched to enriched."""
        conn, _ = temp_db
        insert_incident(conn, sample_incident)

        stats_before = get_enrichment_stats(conn)
        assert stats_before["unenriched_incidents"] == 1
        assert stats_before["enriched_incidents"] == 0
        assert stats_before["ready_for_enrichment"] == 1

        enrichment = _sample_enrichment_result()
        save_enrichment_result(conn, sample_incident.incident_id, enrichment)

        stats_after = get_enrichment_stats(conn)
        assert stats_after["unenriched_incidents"] == 0
        assert stats_after["enriched_incidents"] == 1
        assert stats_after["ready_for_enrichment"] == 0

    def test_get_enrichment_stats_counts_only_actionable_incidents(self, temp_db, sample_incident):
        """Only incidents Phase 2 can work should count as ready."""
        conn, _ = temp_db
        insert_incident(conn, sample_incident)

        serp_ready = BaseIncident(
            incident_id="serp_ready_incident",
            source="test_source",
            source_event_id="serp_ready_event",
            victim_raw_name="Searchable College",
            institution_type=None,
            country=None,
            region=None,
            city=None,
            date_precision="day",
            source_published_date=None,
            ingested_at=None,
            title="SERP-ready incident",
            subtitle=None,
            institution_name="Searchable College",
            incident_date="2025-01-16",
            primary_url=None,
            all_urls=[],
        )
        insert_incident(conn, serp_ready)

        article_backed = BaseIncident(
            incident_id="article_backed_incident",
            source="test_source",
            source_event_id="article_backed_event",
            victim_raw_name="Recovered College",
            institution_type=None,
            country=None,
            region=None,
            city=None,
            date_precision="day",
            source_published_date=None,
            ingested_at=None,
            title="Article-backed incident",
            subtitle=None,
            institution_name="Recovered College",
            incident_date="2025-01-17",
            primary_url=None,
            all_urls=[],
        )
        insert_incident(conn, article_backed)
        _save_sample_article(conn, article_backed.incident_id, "https://example.com/recovered")

        serp_exhausted = BaseIncident(
            incident_id="serp_exhausted_incident",
            source="test_source",
            source_event_id="serp_exhausted_event",
            victim_raw_name="Exhausted College",
            institution_type=None,
            country=None,
            region=None,
            city=None,
            date_precision="day",
            source_published_date=None,
            ingested_at=None,
            title="SERP exhausted incident",
            subtitle=None,
            institution_name="Exhausted College",
            incident_date="2025-01-18",
            primary_url=None,
            all_urls=[],
        )
        insert_incident(conn, serp_exhausted)
        conn.execute(
            "UPDATE incidents SET serp_attempt_count = ? WHERE incident_id = ?",
            (SERP_MAX_ATTEMPTS, serp_exhausted.incident_id),
        )

        nameless = BaseIncident(
            incident_id="nameless_incident",
            source="test_source",
            source_event_id="nameless_event",
            institution_type=None,
            country=None,
            region=None,
            city=None,
            date_precision="day",
            source_published_date=None,
            ingested_at=None,
            title="Nameless incident",
            subtitle=None,
            institution_name="",
            victim_raw_name=None,
            incident_date="2025-01-19",
            primary_url=None,
            all_urls=[],
        )
        insert_incident(conn, nameless)
        conn.commit()

        stats = get_enrichment_stats(conn)
        assert stats["unenriched_incidents"] == 5
        assert stats["ready_for_enrichment"] == 3

    def test_checkpoint_clear_removes_fetch_checkpoint(self, temp_db, sample_incident):
        """Fetch checkpoints should clear once an incident reaches a terminal outcome."""
        conn, _ = temp_db
        insert_incident(conn, sample_incident)
        checkpoint_mark(conn, sample_incident.incident_id)
        assert sample_incident.incident_id in checkpoint_get_fetched(conn)

        checkpoint_clear(conn, sample_incident.incident_id)

        assert sample_incident.incident_id not in checkpoint_get_fetched(conn)

    def test_phase2_main_keeps_checkpointed_incidents_in_batch(self, temp_db, sample_incident):
        """Checkpointed incidents should resume instead of disappearing from the next run."""
        conn, db_path = temp_db
        insert_incident(conn, sample_incident)
        checkpoint_mark(conn, sample_incident.incident_id)

        phase2_main = importlib.import_module("src.edu_cti.pipeline.phase2.__main__")
        args = SimpleNamespace(
            limit=None,
            batch_size=1,
            skip_non_education=True,
            rate_limit_delay=0.0,
            log_level="INFO",
            log_file=None,
            workers=1,
            export_csv=False,
            csv_output=None,
        )
        captured = {}

        def _test_connection():
            return get_connection(db_path)

        def _capture_fetch(conn, unenriched, incident_queue, limit=None, **kwargs):
            captured["incident_ids"] = [incident["incident_id"] for incident in unenriched]
            return {"processed": 0, "articles_fetched": 0, "errors": 0}

        with patch.object(phase2_main, "parse_args", return_value=args), patch.object(
            phase2_main, "get_connection", side_effect=_test_connection
        ), patch.object(phase2_main, "init_db", side_effect=init_db), patch.object(
            phase2_main, "init_incident_enrichments_table"
        ) as init_enrichment_tables, patch.object(
            phase2_main, "init_articles_table"
        ) as init_article_tables, patch.object(
            phase2_main, "configure_logging"
        ), patch.object(
            phase2_main, "fetch_articles_phase", side_effect=_capture_fetch
        ), patch.object(
            phase2_main,
            "enrich_articles_phase",
            return_value={"processed": 0, "enriched": 0, "skipped": 0, "errors": 0},
        ), patch.object(
            phase2_main, "OllamaLLMClient", return_value=Mock()
        ), patch.object(
            phase2_main, "IncidentEnricher", return_value=Mock()
        ), patch.object(
            phase2_main,
            "deduplicate_by_institution",
            return_value={"checked": 0, "removed": 0, "remaining": 0},
        ):
            phase2_main.main()

        init_enrichment_tables.assert_called_once()
        init_article_tables.assert_called_once()
        assert captured["incident_ids"] == [sample_incident.incident_id]

    def test_delete_incident_soft_deletes_and_preserves_row(self, temp_db, sample_incident):
        """Soft-deleting should preserve the incident row while clearing derived artifacts."""
        conn, _ = temp_db
        insert_incident(conn, sample_incident)
        _save_sample_article(conn, sample_incident.incident_id, sample_incident.all_urls[0])
        save_enrichment_result(conn, sample_incident.incident_id, _sample_enrichment_result())

        assert delete_incident(conn, sample_incident.incident_id, reason="not_education_related") is True

        incident_row = conn.execute(
            """
            SELECT llm_excluded, llm_excluded_reason, llm_enriched
            FROM incidents WHERE incident_id = ?
            """,
            (sample_incident.incident_id,),
        ).fetchone()
        articles_count = conn.execute(
            "SELECT COUNT(*) AS count FROM articles WHERE incident_id = ?",
            (sample_incident.incident_id,),
        ).fetchone()["count"]
        flat_count = conn.execute(
            "SELECT COUNT(*) AS count FROM incident_enrichments_flat WHERE incident_id = ?",
            (sample_incident.incident_id,),
        ).fetchone()["count"]

        assert incident_row["llm_excluded"] == 1
        assert incident_row["llm_excluded_reason"] == "not_education_related"
        assert incident_row["llm_enriched"] == 1
        assert articles_count == 0
        assert flat_count == 0

    def test_record_serp_failure_soft_excludes_after_max_attempts(self, temp_db, sample_incident):
        """SERP exhaustion should soft-exclude incidents instead of deleting them."""
        from src.edu_cti.pipeline.phase2.__main__ import _record_serp_failure

        conn, _ = temp_db
        insert_incident(conn, sample_incident)

        for _ in range(SERP_MAX_ATTEMPTS - 1):
            assert _record_serp_failure(conn, sample_incident.incident_id) is False

        assert _record_serp_failure(conn, sample_incident.incident_id) is True

        row = conn.execute(
            """
            SELECT serp_attempt_count, llm_excluded, llm_excluded_reason, llm_enriched
            FROM incidents WHERE incident_id = ?
            """,
            (sample_incident.incident_id,),
        ).fetchone()

        assert row["serp_attempt_count"] == SERP_MAX_ATTEMPTS
        assert row["llm_excluded"] == 1
        assert row["llm_excluded_reason"] == "serp_exhausted"
        assert row["llm_enriched"] == 1


class TestIncidentEnricher:
    """Tests for the main enrichment orchestrator."""

    def test_enrich_article_prompt_includes_article_metadata(self, sample_incident):
        llm_client = Mock()
        llm_client.extract_json.return_value = """
        {
          "is_edu_cyber_incident": true,
          "education_relevance_reasoning": "Education-targeted incident",
          "institution_name": "Test University",
          "publication_date": "2025-01-20",
          "timeline": [
            {
              "date": "2025-01-15",
              "date_precision": "day",
              "event_description": "Attack discovered",
              "event_type": "discovery"
            }
          ]
        }
        """

        enricher = IncidentEnricher(llm_client=llm_client)
        article = ArticleContent(
            url="https://example.com/article1",
            title="Test Article",
            content="This is a detailed article about a university cyber incident.",
            author="Alex Reporter",
            publish_date="2025-01-20",
            fetch_successful=True,
            content_length=64,
        )

        result, raw_json = enricher._enrich_article(
            sample_incident,
            {"https://example.com/article1": article},
        )

        assert result is not None
        assert raw_json["publication_date"] == "2025-01-20"
        user_prompt = llm_client.extract_json.call_args.kwargs["user_prompt"]
        assert "Article Publish Date: 2025-01-20" in user_prompt
        assert "Article Author: Alex Reporter" in user_prompt

    def test_process_incident_marks_primary_article_and_cleans_up(self, temp_db, sample_incident):
        """The chosen primary article should be kept and non-primary articles removed."""
        conn, _ = temp_db
        insert_incident(conn, sample_incident)
        _save_sample_article(conn, sample_incident.incident_id, sample_incident.all_urls[0], "Article 1")
        _save_sample_article(conn, sample_incident.incident_id, sample_incident.all_urls[1], "Article 2")

        enricher = IncidentEnricher(llm_client=Mock())
        expected = _sample_enrichment_result(primary_url=sample_incident.all_urls[1])

        with patch.object(
            enricher,
            "_process_multiple_articles",
            return_value=(expected, {"institution_name": "Test University"}),
        ):
            result, _ = enricher.process_incident(
                sample_incident,
                skip_if_not_education=False,
                conn=conn,
            )

        remaining = conn.execute(
            "SELECT url, is_primary FROM articles WHERE incident_id = ?",
            (sample_incident.incident_id,),
        ).fetchall()

        assert result.primary_url == sample_incident.all_urls[1]
        assert len(remaining) == 1
        assert remaining[0]["url"] == sample_incident.all_urls[1]
        assert remaining[0]["is_primary"] == 1

    def test_process_incident_returns_non_education_marker_for_single_article(
        self, temp_db, sample_incident
    ):
        """Non-education incidents should return a marker instead of being retried forever."""
        conn, _ = temp_db
        insert_incident(conn, sample_incident)
        _save_sample_article(conn, sample_incident.incident_id, sample_incident.all_urls[0])

        enricher = IncidentEnricher(llm_client=Mock())
        non_edu = _sample_enrichment_result(
            primary_url=sample_incident.all_urls[0],
            is_education_related=False,
            institution_name=None,
        )

        with patch.object(
            enricher,
            "_enrich_article",
            return_value=(non_edu, {"institution_name": None}),
        ):
            result, raw_json = enricher.process_incident(
                sample_incident,
                skip_if_not_education=True,
                conn=conn,
            )

        assert result is None
        assert raw_json["_not_education_related"] is True

    def test_process_incident_skips_article_schema_bootstrap_on_hot_path(
        self, temp_db, sample_incident
    ):
        """Worker article reads should not re-run schema DDL."""
        conn, _ = temp_db
        insert_incident(conn, sample_incident)
        _save_sample_article(conn, sample_incident.incident_id, sample_incident.all_urls[0])

        enricher = IncidentEnricher(llm_client=Mock())
        expected = _sample_enrichment_result(primary_url=sample_incident.all_urls[0])

        with patch(
            "src.edu_cti.pipeline.phase2.storage.article_storage.init_articles_table",
            side_effect=AssertionError("article schema should be initialized before worker hot path"),
        ), patch.object(
            enricher,
            "_enrich_article",
            return_value=(expected, {"institution_name": "Test University"}),
        ):
            result, _ = enricher.process_incident(
                sample_incident,
                skip_if_not_education=False,
                conn=conn,
            )

        assert result is not None

    def test_enricher_requires_llm_client(self):
        """IncidentEnricher should require an LLM client."""
        with pytest.raises(ValueError, match="llm_client is required"):
            IncidentEnricher()

    def test_parse_json_response_repairs_trailing_commas_and_non_ascii_spam(self):
        """The JSON repair path should handle lottery-spam lines seen in production."""
        enricher = IncidentEnricher(llm_client=Mock())
        raw_response = """
        {
          "is_edu_cyber_incident": true,
          "education_relevance_reasoning": "Valid education incident",
          "institution_name": "Test University",
          极 "timeline": [],
        }
        """

        parsed = enricher._parse_json_response(raw_response)

        assert parsed["is_edu_cyber_incident"] is True
        assert parsed["institution_name"] == "Test University"
        assert parsed["timeline"] == []

    def test_enrich_article_strips_cti_fields_when_llm_marks_non_education(self, sample_incident):
        llm_client = Mock()
        llm_client.extract_json.return_value = """
        {
          "is_edu_cyber_incident": false,
          "education_relevance_reasoning": "This article is about a non-education cyber incident.",
          "institution_name": "SolarWinds",
          "attack_category": "espionage",
          "ransom_paid": true,
          "ransom_amount": 12345,
          "timeline": [
            {
              "date": "2021-04-16",
              "date_precision": "day",
              "event_description": "Attack disclosed",
              "event_type": "disclosure"
            }
          ]
        }
        """

        enricher = IncidentEnricher(llm_client=llm_client)
        article = ArticleContent(
            url="https://example.com/non-edu",
            title="Non-education cyber incident",
            content="This article covers a cyber incident unrelated to the education sector.",
            fetch_successful=True,
            content_length=72,
        )

        result, raw_json = enricher._enrich_article(
            sample_incident,
            {"https://example.com/non-edu": article},
        )

        assert result is not None
        assert result.education_relevance.is_education_related is False
        assert result.attack_dynamics is None
        assert result.timeline is None
        assert raw_json["is_edu_cyber_incident"] is False
        assert raw_json["education_relevance_reasoning"] == (
            "This article is about a non-education cyber incident."
        )
        assert "institution_name" not in raw_json
        assert "attack_category" not in raw_json
        assert "timeline" not in raw_json


class TestEnrichmentSchemas:
    """Tests for enrichment Pydantic schemas."""

    def test_education_relevance_check(self):
        check = EducationRelevanceCheck(
            is_education_related=True,
            reasoning="Test reasoning",
            institution_identified="Test University",
        )

        assert check.is_education_related is True
        assert check.reasoning == "Test reasoning"
        assert check.institution_identified == "Test University"

    def test_timeline_event(self):
        event = TimelineEvent(
            date="2025-01-15",
            event_type="discovery",
            event_description="Attack was discovered",
        )

        assert event.date == "2025-01-15"
        assert event.event_type == "discovery"

    def test_mitre_attack_technique(self):
        technique = MITREAttackTechnique(
            technique_id="T1486",
            technique_name="Data Encrypted for Impact",
            tactic="Impact",
        )

        assert technique.technique_id == "T1486"
        assert technique.tactic == "Impact"

    def test_attack_dynamics(self):
        dynamics = AttackDynamics(
            attack_vector="malicious_attachment",
            ransomware_family="LockBit",
            data_exfiltration=True,
        )

        assert dynamics.attack_vector == "malicious_attachment"
        assert dynamics.ransomware_family == "LockBit"
        assert dynamics.data_exfiltration is True


class TestEnrichmentIntegration:
    """Integration tests for Phase 2 enrichment persistence."""

    def test_full_enrichment_flow(self, temp_db, sample_incident):
        """An incident should move from unenriched to enriched after save."""
        conn, _ = temp_db
        insert_incident(conn, sample_incident)

        unenriched = get_unenriched_incidents(conn)
        assert len(unenriched) == 1

        enrichment = _sample_enrichment_result(primary_url=sample_incident.all_urls[0])
        saved = save_enrichment_result(conn, sample_incident.incident_id, enrichment)

        assert saved is True
        assert get_unenriched_incidents(conn) == []

        stats = get_enrichment_stats(conn)
        assert stats["enriched_incidents"] == 1
        assert stats["ready_for_enrichment"] == 0
