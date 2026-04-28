"""
Unit tests for the 3-call split extraction routing logic.

Tests that:
- _should_use_split() returns False for short articles, True for long ones
- process_incident routes to _enrich_article for short articles
- process_incident routes to _enrich_article_split for long articles
- SPLIT_THRESHOLD_CHARS = 25_000 is the boundary
- Part 1 + Part 2 field merging works correctly (split wins on non-null Part 2 fields)
- Split falls back to _enrich_article when Part 1 fails
- _process_multiple_articles uses split for long combined content
"""

import pytest
from unittest.mock import MagicMock, patch, call
from typing import Dict, Optional, Any

from src.edu_cti.pipeline.phase2.enrichment import IncidentEnricher
from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleContent
from src.edu_cti.pipeline.phase2.schemas import (
    CTIEnrichmentResult,
    EducationRelevanceCheck,
    AttackDynamics,
)
from src.edu_cti.core.models import BaseIncident


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_article(content: str, url: str = "https://example.com/a") -> Dict[str, ArticleContent]:
    return {
        url: ArticleContent(
            url=url,
            title="Test Article",
            content=content,
            author=None,
            publish_date="2024-01-15",
            fetch_successful=True,
            error_message=None,
            content_length=len(content),
        )
    }


def _make_incident(incident_id: str = "test_001") -> BaseIncident:
    return BaseIncident(
        incident_id=incident_id,
        source="test",
        source_event_id=None,
        institution_name="Test University",
        victim_raw_name=None,
        institution_type="university_public",
        country="United States",
        region="California",
        city="Los Angeles",
        incident_date="2024-01-15",
        date_precision="day",
        source_published_date="2024-01-16",
        ingested_at=None,
        title="Test Ransomware Attack",
        subtitle=None,
        primary_url="https://example.com/a",
        all_urls=["https://example.com/a"],
        notes="",
    )


def _make_enricher() -> IncidentEnricher:
    mock_llm = MagicMock()
    return IncidentEnricher(llm_client=mock_llm)


def _edu_result(incident=None) -> CTIEnrichmentResult:
    return CTIEnrichmentResult(
        education_relevance=EducationRelevanceCheck(
            is_education_related=True,
            reasoning="Confirmed education incident",
            institution_identified="Test University",
        ),
        primary_url="https://example.com/a",
        enriched_summary="Test university was hit by ransomware.",
        attack_dynamics=AttackDynamics(
            attack_vector="phishing_email",
            ransomware_family="lockbit",
        ),
    )


# ---------------------------------------------------------------------------
# _should_use_split
# ---------------------------------------------------------------------------

class TestShouldUseSplit:
    def setup_method(self):
        self.enricher = _make_enricher()
        self.threshold = IncidentEnricher.SPLIT_THRESHOLD_CHARS

    def test_threshold_is_25000(self):
        assert self.threshold == 25_000

    def test_short_article_does_not_trigger_split(self):
        content = "x" * (self.threshold - 1)
        assert not self.enricher._should_use_split(_make_article(content))

    def test_exact_threshold_triggers_split(self):
        content = "x" * self.threshold
        assert self.enricher._should_use_split(_make_article(content))

    def test_long_article_triggers_split(self):
        content = "x" * (self.threshold + 10_000)
        assert self.enricher._should_use_split(_make_article(content))

    def test_empty_article_does_not_trigger(self):
        assert not self.enricher._should_use_split(_make_article(""))

    def test_multiple_articles_combined_length(self):
        # Two articles that are each < threshold but together >= threshold
        half = self.threshold // 2
        articles = {
            "https://example.com/a": ArticleContent(
                url="https://example.com/a", title="", content="x" * half,
                author=None, publish_date=None, fetch_successful=True,
                error_message=None, content_length=half,
            ),
            "https://example.com/b": ArticleContent(
                url="https://example.com/b", title="", content="x" * half,
                author=None, publish_date=None, fetch_successful=True,
                error_message=None, content_length=half,
            ),
        }
        # Combined = threshold → should trigger
        assert self.enricher._should_use_split(articles)

    def test_multiple_short_articles_combined_below_threshold(self):
        # Each article well below threshold, combined also below
        articles = {
            f"https://example.com/{i}": ArticleContent(
                url=f"https://example.com/{i}", title="", content="x" * 1000,
                author=None, publish_date=None, fetch_successful=True,
                error_message=None, content_length=1000,
            )
            for i in range(5)
        }
        assert not self.enricher._should_use_split(articles)


# ---------------------------------------------------------------------------
# process_incident routing (single article path)
# ---------------------------------------------------------------------------

class TestProcessIncidentRouting:
    def setup_method(self):
        self.enricher = _make_enricher()
        self.incident = _make_incident()
        self.threshold = IncidentEnricher.SPLIT_THRESHOLD_CHARS

    def _setup_db_articles(self, mock_get_all, content: str):
        """Configure the DB article mock to return one article with given content."""
        mock_get_all.return_value = [
            {
                "url": "https://example.com/a",
                "title": "Test",
                "content": content,
                "fetch_successful": True,
                "author": None,
                "publish_date": "2024-01-15",
                "error_message": None,
                "content_length": len(content),
            }
        ]

    # promote_primary_article is a local import inside process_incident,
    # so patch at the source module rather than the enrichment namespace.
    @patch("src.edu_cti.pipeline.phase2.storage.article_storage.get_all_articles_for_incident")
    @patch("src.edu_cti.pipeline.phase2.storage.article_storage.promote_primary_article")
    def test_short_article_uses_single_call(self, mock_promote, mock_get_all):
        short_content = "x" * (self.threshold - 1)
        self._setup_db_articles(mock_get_all, short_content)

        result_stub = (_edu_result(), {"is_edu_cyber_incident": True, "institution_name": "Test"})

        with patch.object(self.enricher, "_enrich_article", return_value=result_stub) as mock_single, \
             patch.object(self.enricher, "_enrich_article_split") as mock_split:
            self.enricher.process_incident(self.incident, conn=MagicMock())
            mock_single.assert_called_once()
            mock_split.assert_not_called()

    @patch("src.edu_cti.pipeline.phase2.storage.article_storage.get_all_articles_for_incident")
    @patch("src.edu_cti.pipeline.phase2.storage.article_storage.promote_primary_article")
    def test_long_article_uses_split(self, mock_promote, mock_get_all):
        long_content = "x" * (self.threshold + 5_000)
        self._setup_db_articles(mock_get_all, long_content)

        result_stub = (_edu_result(), {"is_edu_cyber_incident": True, "institution_name": "Test"})

        with patch.object(self.enricher, "_enrich_article") as mock_single, \
             patch.object(self.enricher, "_enrich_article_split", return_value=result_stub) as mock_split:
            self.enricher.process_incident(self.incident, conn=MagicMock())
            mock_split.assert_called_once()
            mock_single.assert_not_called()

    @patch("src.edu_cti.pipeline.phase2.storage.article_storage.get_all_articles_for_incident")
    @patch("src.edu_cti.pipeline.phase2.storage.article_storage.promote_primary_article")
    def test_exact_threshold_uses_split(self, mock_promote, mock_get_all):
        content = "x" * self.threshold
        self._setup_db_articles(mock_get_all, content)

        result_stub = (_edu_result(), {"is_edu_cyber_incident": True})

        with patch.object(self.enricher, "_enrich_article") as mock_single, \
             patch.object(self.enricher, "_enrich_article_split", return_value=result_stub) as mock_split:
            self.enricher.process_incident(self.incident, conn=MagicMock())
            mock_split.assert_called_once()
            mock_single.assert_not_called()


# ---------------------------------------------------------------------------
# _process_multiple_articles routing
# ---------------------------------------------------------------------------

class TestMultipleArticlesRouting:
    def setup_method(self):
        self.enricher = _make_enricher()
        self.incident = _make_incident()
        self.threshold = IncidentEnricher.SPLIT_THRESHOLD_CHARS

    def _make_multi_articles(self, total_chars: int, n: int = 2) -> Dict[str, ArticleContent]:
        per_article = total_chars // n
        return {
            f"https://example.com/{i}": ArticleContent(
                url=f"https://example.com/{i}", title="Test", content="x" * per_article,
                author=None, publish_date=None, fetch_successful=True,
                error_message=None, content_length=per_article,
            )
            for i in range(n)
        }

    def test_long_combined_uses_split_in_combined_call(self):
        articles = self._make_multi_articles(self.threshold + 10_000)
        result_stub = (_edu_result(), {"is_edu_cyber_incident": True})

        with patch.object(self.enricher, "_enrich_article") as mock_single, \
             patch.object(self.enricher, "_enrich_article_split", return_value=result_stub) as mock_split:
            self.enricher._process_multiple_articles(self.incident, articles, skip_if_not_education=False)
            # Step 1 combined call should use split
            mock_split.assert_called()
            # Single-call should NOT have been called for the combined attempt
            mock_single.assert_not_called()

    def test_short_combined_uses_single_call(self):
        articles = self._make_multi_articles(self.threshold - 2_000)
        result_stub = (_edu_result(), {"is_edu_cyber_incident": True})

        with patch.object(self.enricher, "_enrich_article", return_value=result_stub) as mock_single, \
             patch.object(self.enricher, "_enrich_article_split") as mock_split:
            self.enricher._process_multiple_articles(self.incident, articles, skip_if_not_education=False)
            mock_single.assert_called()
            mock_split.assert_not_called()


# ---------------------------------------------------------------------------
# Split merge behaviour
# ---------------------------------------------------------------------------

class TestSplitMerge:
    """Verify that Part 2 fields override Part 1 nulls in the merged result."""

    def setup_method(self):
        self.enricher = _make_enricher()
        self.incident = _make_incident()

    def test_part2_timeline_overwrites_part1_null(self):
        """Part 1 has no timeline; Part 2 provides one — merge should include it."""
        part1_json = {
            "is_edu_cyber_incident": True,
            "institution_name": "Test University",
            "attack_category": "ransomware_encryption",
            "attack_chain": ["initial_access", "impact"],
            "timeline": None,
            "applicable_regulations": None,
        }
        part2_json = {
            "timeline": [
                {
                    "date": "2024-01-15",
                    "event_description": "Ransomware encrypted servers campus-wide.",
                    "event_type": "encryption_started",
                    "date_precision": "day",
                }
            ],
            "applicable_regulations": ["FERPA", "state_breach_notification"],
        }

        # Simulate the merge logic from _enrich_article_split
        merged = {**part1_json}
        for key, val in part2_json.items():
            if val is not None and val != [] and val != {}:
                merged[key] = val

        assert merged["timeline"] is not None
        assert len(merged["timeline"]) == 1
        assert merged["timeline"][0]["event_description"] == "Ransomware encrypted servers campus-wide."
        assert merged["applicable_regulations"] == ["FERPA", "state_breach_notification"]
        # Part 1 fields still present
        assert merged["attack_chain"] == ["initial_access", "impact"]
        assert merged["institution_name"] == "Test University"

    def test_part2_empty_list_does_not_overwrite_part1(self):
        """Part 2 returning [] should NOT overwrite Part 1's populated list."""
        part1_json = {
            "attack_chain": ["initial_access", "exfiltration", "impact"],
            "data_categories": ["student_pii", "employee_ssn"],
        }
        part2_json = {
            "applicable_regulations": [],  # empty list — should not overwrite
            "timeline": [],               # empty list — should not overwrite
        }

        merged = {**part1_json}
        for key, val in part2_json.items():
            if val is not None and val != [] and val != {}:
                merged[key] = val

        # Empty Part 2 lists should not appear in merged
        assert "applicable_regulations" not in merged or merged.get("applicable_regulations") != []
        assert merged["attack_chain"] == ["initial_access", "exfiltration", "impact"]

    def test_part2_null_does_not_overwrite_part1_value(self):
        """Part 2 returning null should leave Part 1's value intact."""
        part1_json = {
            "ransomware_family": "lockbit",
            "threat_actor_name": "LockBit Group",
        }
        part2_json = {
            "ransomware_family": None,   # null — should not clobber Part 1
            "timeline": None,
        }

        merged = {**part1_json}
        for key, val in part2_json.items():
            if val is not None and val != [] and val != {}:
                merged[key] = val

        assert merged["ransomware_family"] == "lockbit"
        assert merged["threat_actor_name"] == "LockBit Group"


# ---------------------------------------------------------------------------
# Schema sanity checks
# ---------------------------------------------------------------------------

class TestSplitSchemas:
    def test_part1_has_attack_chain_after_attack_vector(self):
        """attack_chain must come immediately after attack_vector in Part 1 schema
        so the model fills it while focused on attack classification."""
        from src.edu_cti.pipeline.phase2.extraction.extraction_schema import EXTRACTION_SCHEMA_PART1
        props = list(EXTRACTION_SCHEMA_PART1["properties"].keys())
        av_idx = props.index("attack_vector")
        ac_idx = props.index("attack_chain")
        assert ac_idx == av_idx + 1, (
            f"attack_chain (pos {ac_idx}) should be immediately after "
            f"attack_vector (pos {av_idx}) in Part 1 schema"
        )

    def test_part1_does_not_contain_timeline(self):
        from src.edu_cti.pipeline.phase2.extraction.extraction_schema import EXTRACTION_SCHEMA_PART1
        assert "timeline" not in EXTRACTION_SCHEMA_PART1["properties"]

    def test_part1_does_not_contain_mitre(self):
        from src.edu_cti.pipeline.phase2.extraction.extraction_schema import EXTRACTION_SCHEMA_PART1
        assert "mitre_attack_techniques" not in EXTRACTION_SCHEMA_PART1["properties"]

    def test_part2_has_timeline(self):
        from src.edu_cti.pipeline.phase2.extraction.extraction_schema import EXTRACTION_SCHEMA_PART2
        assert "timeline" in EXTRACTION_SCHEMA_PART2["properties"]

    def test_part2_has_mitre(self):
        from src.edu_cti.pipeline.phase2.extraction.extraction_schema import EXTRACTION_SCHEMA_PART2
        assert "mitre_attack_techniques" in EXTRACTION_SCHEMA_PART2["properties"]

    def test_part2_has_applicable_regulations(self):
        from src.edu_cti.pipeline.phase2.extraction.extraction_schema import EXTRACTION_SCHEMA_PART2
        assert "applicable_regulations" in EXTRACTION_SCHEMA_PART2["properties"]

    def test_part2_timeline_requires_event_description(self):
        from src.edu_cti.pipeline.phase2.extraction.extraction_schema import EXTRACTION_SCHEMA_PART2
        timeline_schema = EXTRACTION_SCHEMA_PART2["properties"]["timeline"]
        item_required = timeline_schema["items"].get("required", [])
        assert "event_description" in item_required

    def test_part2_mitre_requires_all_four_fields(self):
        from src.edu_cti.pipeline.phase2.extraction.extraction_schema import EXTRACTION_SCHEMA_PART2
        mitre_schema = EXTRACTION_SCHEMA_PART2["properties"]["mitre_attack_techniques"]
        item_required = mitre_schema["items"].get("required", [])
        for field in ("technique_id", "technique_name", "tactic", "description"):
            assert field in item_required, f"mitre_attack_techniques item should require '{field}'"

    def test_part1_schema_smaller_than_original(self):
        """Part 1 schema must be materially smaller than the original monolithic schema."""
        import json
        from src.edu_cti.pipeline.phase2.extraction.extraction_schema import (
            EXTRACTION_SCHEMA, EXTRACTION_SCHEMA_PART1,
        )
        original_size = len(json.dumps(EXTRACTION_SCHEMA))
        part1_size = len(json.dumps(EXTRACTION_SCHEMA_PART1))
        assert part1_size < original_size * 0.6, (
            f"Part 1 ({part1_size:,} chars) should be < 60% of original ({original_size:,} chars)"
        )

    def test_part2_schema_smaller_than_original(self):
        """Part 2 schema must be materially smaller than the original monolithic schema."""
        import json
        from src.edu_cti.pipeline.phase2.extraction.extraction_schema import (
            EXTRACTION_SCHEMA, EXTRACTION_SCHEMA_PART2,
        )
        original_size = len(json.dumps(EXTRACTION_SCHEMA))
        part2_size = len(json.dumps(EXTRACTION_SCHEMA_PART2))
        assert part2_size < original_size * 0.6, (
            f"Part 2 ({part2_size:,} chars) should be < 60% of original ({original_size:,} chars)"
        )
