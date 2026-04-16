"""Tests for Phase 2 enrichment, storage, and recovery behavior."""

import importlib
from types import SimpleNamespace
from typing import Optional
from unittest.mock import Mock, patch

import pytest

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
    save_enrichment_result,
)


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
        university_name="Test University",
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


def _save_sample_article(conn, incident_id: str, url: str, title: str = "Test Article") -> None:
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
            fetch_successful=True,
            content_length=len(content),
        ),
    )


class TestArticleFetcher:
    """Tests for article fetching functionality."""

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
        assert row["institution_type"] == "university"
        assert row["attack_category"] == "ransomware"
        assert row["attack_vector"] == "phishing"
        assert row["ransomware_family"] == "LockBit"

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
            university_name="Georgia Tech",
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
            university_name="Searchable College",
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
            university_name="Recovered College",
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
            university_name="Exhausted College",
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
            university_name="",
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
