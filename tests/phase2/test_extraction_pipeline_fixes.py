"""
Tests for Phase 2 extraction pipeline bug-fixes (April 2026).

Covers five classes of bugs that caused structured CTI fields to be silently
discarded or never generated:

1. MITRE ATT&CK regex pattern constraint blocked Ollama GBNF generation.
2. MITRE mapper hardcoded tactic / description / sub_techniques as None.
3. Extraction schema used ``data_categories`` but mapper read ``data_types``.
4. Regulatory-impact dict stored FERPA breach under wrong key ``ferc_breach``.
5. ``discovery_date`` extracted by LLM but never persisted to the DB.
6. Twelve regex ``pattern`` constraints on date / CVE / country-code fields
   blocked Ollama GBNF from generating those fields.
7. Five timeline ``event_type`` enum values present in the Pydantic model but
   absent from the extraction schema, causing validation failures.
8. ``enriched_summary`` fallback generates a meaningful sentence when the LLM
   returns an empty string (e.g. headline-only articles).
"""

import json
import sqlite3
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from src.edu_cti.core.db import get_connection, init_db, insert_incident
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.pipeline.phase2.extraction.extraction_schema import EXTRACTION_SCHEMA
from src.edu_cti.pipeline.phase2.extraction.json_to_schema_mapper import (
    _build_summary,
    json_to_cti_enrichment,
)
from src.edu_cti.pipeline.phase2.schemas import (
    CTIEnrichmentResult,
    EducationRelevanceCheck,
    MITREAttackTechnique,
)
from src.edu_cti.pipeline.phase2.storage.db import (
    init_incident_enrichments_table,
    save_enrichment_result,
)
from src.edu_cti.pipeline.phase2.storage.article_storage import init_articles_table


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_minimal_json(**overrides) -> Dict[str, Any]:
    """Return a minimal LLM-output dict for a phishing incident."""
    base = {
        "is_edu_cyber_incident": True,
        "education_relevance_reasoning": "Targets a university",
        "institution_name": "Riverdale University",
        "attack_category": "phishing_credential_harvest",
        "incident_date": "2024-03-15",
        "discovery_date": "2024-03-20",
        "enriched_summary": (
            "Riverdale University suffered a phishing campaign in March 2024 that "
            "compromised staff credentials, exposing student and faculty records."
        ),
        "education_relevance": {
            "is_education_related": True,
            "reasoning": "Targets a university",
        },
    }
    base.update(overrides)
    return base


def _make_enrichment_result(**overrides) -> CTIEnrichmentResult:
    """Build a minimal CTIEnrichmentResult."""
    defaults = dict(
        education_relevance=EducationRelevanceCheck(
            is_education_related=True,
            reasoning="University target",
        ),
        primary_url="https://example.com/article",
        enriched_summary="Riverdale University experienced a phishing attack.",
    )
    defaults.update(overrides)
    return CTIEnrichmentResult(**defaults)


@pytest.fixture
def temp_db(tmp_path):
    """Temporary SQLite database with all required tables."""
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    init_incident_enrichments_table(conn)
    init_articles_table(conn)
    # Ensure discovery_date column exists (migration may not have run yet)
    cur = conn.execute("PRAGMA table_info(incidents)")
    cols = [r[1] for r in cur.fetchall()]
    if "discovery_date" not in cols:
        conn.execute("ALTER TABLE incidents ADD COLUMN discovery_date TEXT")
        conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def sample_incident():
    return BaseIncident(
        incident_id=make_incident_id("test", "https://example.com|2024-03-15"),
        source="test",
        source_event_id="test_001",
        title="Riverdale University Phishing Attack",
        victim_raw_name="Riverdale University",
        institution_name="Riverdale University",
        institution_type="university_public",
        country="United States",
        region="New York",
        city="Riverdale",
        incident_date="2024-03-15",
        date_precision="day",
        source_published_date="2024-03-22",
        ingested_at=None,
        subtitle="Staff credentials compromised in phishing campaign",
        primary_url="https://example.com/article",
        all_urls=["https://example.com/article"],
        status="confirmed",
        source_confidence="high",
    )


# ===========================================================================
# 1. Extraction schema — no regex pattern constraints
# ===========================================================================

class TestExtractionSchemaPatterns:
    """Verify no field in the extraction schema uses a 'pattern' constraint.

    Ollama's JSON-schema-to-GBNF converter does not support arbitrary regex
    patterns and silently outputs an empty value (or empty array) for any
    field that carries one.  All pattern constraints have been replaced with
    plain description hints.
    """

    def _collect_patterns(self, node, path=""):
        """Recursively collect every 'pattern' key found in the schema."""
        hits = []
        if isinstance(node, dict):
            if "pattern" in node:
                hits.append(path)
            for key, child in node.items():
                hits.extend(self._collect_patterns(child, f"{path}.{key}"))
        elif isinstance(node, list):
            for i, item in enumerate(node):
                hits.extend(self._collect_patterns(item, f"{path}[{i}]"))
        return hits

    def test_no_pattern_constraints_anywhere(self):
        hits = self._collect_patterns(EXTRACTION_SCHEMA)
        assert hits == [], (
            f"Regex 'pattern' constraints found at: {hits}. "
            "Ollama GBNF does not support pattern constraints — remove them."
        )

    def test_technique_id_is_plain_string(self):
        mitre_items = (
            EXTRACTION_SCHEMA["properties"]["mitre_attack_techniques"]["items"]
        )
        tid = mitre_items["properties"]["technique_id"]
        assert tid["type"] == "string"
        assert "pattern" not in tid

    def test_date_fields_have_no_pattern(self):
        for field in ("incident_date", "discovery_date", "publication_date"):
            prop = EXTRACTION_SCHEMA["properties"][field]
            assert "pattern" not in prop, (
                f"Field '{field}' still has a pattern constraint."
            )

    def test_country_code_has_no_pattern(self):
        prop = EXTRACTION_SCHEMA["properties"]["country_code"]
        assert "pattern" not in prop

    def test_cve_id_has_no_pattern(self):
        vuln_items = (
            EXTRACTION_SCHEMA["properties"]["vulnerabilities_exploited"]["items"]
        )
        assert "pattern" not in vuln_items["properties"]["cve_id"]


# ===========================================================================
# 2. Extraction schema — timeline event_type enum completeness
# ===========================================================================

class TestTimelineEventTypeEnum:
    """All event_type values defined in the Pydantic model must be present
    in the extraction schema so the LLM can generate them without hitting a
    GBNF constraint failure."""

    REQUIRED_EVENT_TYPES = {
        "initial_access", "reconnaissance", "lateral_movement",
        "privilege_escalation", "exploitation", "data_exfiltration",
        "encryption_started", "ransom_demand", "impact", "operational_impact",
        "discovery", "containment", "eradication", "recovery", "disclosure",
        "notification", "investigation", "remediation", "response_action",
        "security_improvement", "law_enforcement_contact", "public_statement",
        "systems_restored", "other",
    }

    def _get_schema_event_types(self):
        timeline_items = EXTRACTION_SCHEMA["properties"]["timeline"]["items"]
        return set(timeline_items["properties"]["event_type"]["enum"])

    def test_all_required_event_types_present(self):
        schema_types = self._get_schema_event_types()
        missing = self.REQUIRED_EVENT_TYPES - schema_types
        assert not missing, (
            f"event_type enum is missing values that are valid in the Pydantic "
            f"model: {missing}"
        )

    def test_exploitation_in_schema(self):
        assert "exploitation" in self._get_schema_event_types()

    def test_impact_in_schema(self):
        assert "impact" in self._get_schema_event_types()

    def test_response_action_in_schema(self):
        assert "response_action" in self._get_schema_event_types()

    def test_security_improvement_in_schema(self):
        assert "security_improvement" in self._get_schema_event_types()


# ===========================================================================
# 3. MITRE ATT&CK mapper — correct field extraction
# ===========================================================================

class TestMITREMapper:
    """json_to_cti_enrichment must correctly map all MITRE technique fields."""

    def _map(self, techniques):
        data = _make_minimal_json(mitre_attack_techniques=techniques)
        return json_to_cti_enrichment(data, "https://example.com")

    def test_technique_id_extracted(self):
        result = self._map([
            {"technique_id": "T1566", "technique_name": "Phishing", "tactic": "initial_access"}
        ])
        assert result.mitre_attack_techniques
        assert result.mitre_attack_techniques[0].technique_id == "T1566"

    def test_technique_name_extracted(self):
        result = self._map([
            {"technique_id": "T1566", "technique_name": "Phishing", "tactic": "initial_access"}
        ])
        assert result.mitre_attack_techniques[0].technique_name == "Phishing"

    def test_tactic_extracted(self):
        """Tactic must NOT be hardcoded None — this was the core bug."""
        result = self._map([
            {"technique_id": "T1486", "technique_name": "Data Encrypted for Impact",
             "tactic": "impact"}
        ])
        assert result.mitre_attack_techniques[0].tactic == "impact"

    def test_description_extracted(self):
        result = self._map([
            {"technique_id": "T1566", "technique_name": "Phishing",
             "tactic": "initial_access",
             "description": "Spear-phishing emails sent to faculty"}
        ])
        assert result.mitre_attack_techniques[0].description == "Spear-phishing emails sent to faculty"

    def test_sub_techniques_extracted(self):
        result = self._map([
            {"technique_id": "T1566", "technique_name": "Phishing",
             "tactic": "initial_access",
             "sub_techniques": ["T1566.001", "T1566.002"]}
        ])
        subs = result.mitre_attack_techniques[0].sub_techniques
        assert subs == ["T1566.001", "T1566.002"]

    def test_multiple_techniques_preserved(self):
        result = self._map([
            {"technique_id": "T1566", "technique_name": "Phishing", "tactic": "initial_access"},
            {"technique_id": "T1078", "technique_name": "Valid Accounts", "tactic": "initial_access"},
            {"technique_id": "T1486", "technique_name": "Data Encrypted for Impact", "tactic": "impact"},
        ])
        assert len(result.mitre_attack_techniques) == 3
        ids = {t.technique_id for t in result.mitre_attack_techniques}
        assert ids == {"T1566", "T1078", "T1486"}

    def test_bare_string_technique_id_handled(self):
        """LLM may return a bare string instead of an object."""
        result = self._map(["T1566", "T1486"])
        assert len(result.mitre_attack_techniques) == 2
        assert result.mitre_attack_techniques[0].technique_id == "T1566"

    def test_empty_mitre_array_returns_none(self):
        result = self._map([])
        assert result.mitre_attack_techniques is None

    def test_missing_technique_id_entry_skipped(self):
        """Entries without technique_id must be skipped, not cause a crash."""
        result = self._map([
            {"technique_name": "Phishing", "tactic": "initial_access"},  # no technique_id
            {"technique_id": "T1486", "technique_name": "Ransomware", "tactic": "impact"},
        ])
        assert len(result.mitre_attack_techniques) == 1
        assert result.mitre_attack_techniques[0].technique_id == "T1486"

    def test_alternate_key_names_handled(self):
        """LLM may use 'id' or 'name' instead of canonical keys."""
        result = self._map([
            {"id": "T1566", "name": "Phishing", "tactic": "initial_access"}
        ])
        assert result.mitre_attack_techniques[0].technique_id == "T1566"
        assert result.mitre_attack_techniques[0].technique_name == "Phishing"


# ===========================================================================
# 4. data_categories → data_types field-name fix
# ===========================================================================

class TestDataCategoriesMapping:
    """Schema uses 'data_categories'; mapper was reading 'data_types'.
    Breach category data (student records, PII, etc.) was silently lost."""

    def test_data_categories_key_is_used(self):
        data = _make_minimal_json(
            data_categories=["student_records", "pii", "financial_records"],
            data_breached=True,
        )
        result = json_to_cti_enrichment(data, "https://example.com")
        assert result.data_impact is not None
        stored = result.data_impact.get("data_types_affected") or []
        assert "student_records" in stored
        assert "pii" in stored

    def test_data_types_fallback_still_works(self):
        """Legacy 'data_types' key must still be accepted as a fallback."""
        data = _make_minimal_json(
            data_types=["financial_records"],
            data_breached=True,
        )
        result = json_to_cti_enrichment(data, "https://example.com")
        assert result.data_impact is not None
        stored = result.data_impact.get("data_types_affected") or []
        assert "financial_records" in stored

    def test_data_categories_preferred_over_data_types(self):
        """When both keys exist, data_categories takes precedence."""
        data = _make_minimal_json(
            data_categories=["medical_records"],
            data_types=["administrative_data"],
            data_breached=True,
        )
        result = json_to_cti_enrichment(data, "https://example.com")
        stored = result.data_impact.get("data_types_affected") or []
        assert "medical_records" in stored

    def test_no_data_impact_when_no_breach_and_no_categories(self):
        data = _make_minimal_json()
        result = json_to_cti_enrichment(data, "https://example.com")
        assert result.data_impact is None


# ===========================================================================
# 5. ferpa_breach key fix in regulatory_impact
# ===========================================================================

class TestFerpaBreach:
    """Regulatory impact dict must store FERPA breach under 'ferpa_breach',
    not the misspelled 'ferc_breach' key the DB flatten function expects."""

    def test_ferpa_breach_key_correct(self):
        data = _make_minimal_json(ferpa_breach=True)
        result = json_to_cti_enrichment(data, "https://example.com")
        reg = result.regulatory_impact or {}
        assert "ferpa_breach" in reg, (
            "regulatory_impact must use key 'ferpa_breach', not 'ferc_breach'"
        )
        assert "ferc_breach" not in reg

    def test_ferpa_breach_value_preserved(self):
        data = _make_minimal_json(ferpa_breach=True)
        result = json_to_cti_enrichment(data, "https://example.com")
        assert result.regulatory_impact["ferpa_breach"] is True

    def test_ferpa_breach_false_preserved(self):
        data = _make_minimal_json(ferpa_breach=False)
        result = json_to_cti_enrichment(data, "https://example.com")
        assert result.regulatory_impact["ferpa_breach"] is False


# ===========================================================================
# 6. enriched_summary fallback (_build_summary)
# ===========================================================================

class TestBuildSummary:
    """When the LLM returns an empty enriched_summary, _build_summary must
    construct a meaningful fallback from available metadata fields."""

    def test_llm_summary_returned_when_present(self):
        data = {"enriched_summary": "Full LLM summary here.", "institution_name": "X"}
        assert _build_summary(data) == "Full LLM summary here."

    def test_fallback_used_when_empty_string(self):
        data = {
            "enriched_summary": "",
            "institution_name": "Riverdale University",
            "attack_category": "ransomware_encryption",
            "incident_date": "2024-03-15",
            "country": "United States",
        }
        summary = _build_summary(data)
        assert "Riverdale University" in summary
        assert len(summary) > 20

    def test_fallback_used_when_none(self):
        data = {
            "enriched_summary": None,
            "institution_name": "Test College",
        }
        summary = _build_summary(data)
        assert "Test College" in summary

    def test_fallback_includes_attack_type(self):
        data = {
            "enriched_summary": "",
            "institution_name": "Apex University",
            "attack_category": "phishing_credential_harvest",
        }
        summary = _build_summary(data)
        assert "phishing" in summary.lower()

    def test_fallback_includes_threat_actor(self):
        data = {
            "enriched_summary": "",
            "institution_name": "Apex University",
            "threat_actor_name": "LockBit",
        }
        summary = _build_summary(data)
        assert "LockBit" in summary

    def test_fallback_includes_ransomware_family(self):
        data = {
            "enriched_summary": "",
            "institution_name": "Apex University",
            "ransomware_family": "BlackCat",
        }
        summary = _build_summary(data)
        assert "BlackCat" in summary

    def test_fallback_when_no_metadata(self):
        """Should not crash even with no metadata at all."""
        data = {"enriched_summary": ""}
        summary = _build_summary(data)
        assert isinstance(summary, str)
        assert len(summary) > 0

    def test_whitespace_only_summary_triggers_fallback(self):
        data = {
            "enriched_summary": "   ",
            "institution_name": "Blanks College",
        }
        summary = _build_summary(data)
        assert "Blanks College" in summary


# ===========================================================================
# 7. discovery_date persistence in the DB
# ===========================================================================

class TestDiscoveryDatePersistence:
    """discovery_date extracted by the LLM must be written to the incidents
    table and returned by the API detail query."""

    def test_discovery_date_column_exists(self, temp_db):
        cur = temp_db.execute("PRAGMA table_info(incidents)")
        cols = [r[1] for r in cur.fetchall()]
        assert "discovery_date" in cols, (
            "incidents table is missing the discovery_date column. "
            "Run init_db() to apply the migration."
        )

    def test_discovery_date_written_on_save(self, temp_db, sample_incident):
        insert_incident(temp_db, sample_incident)
        enrichment = _make_enrichment_result()
        raw_json = {
            "incident_date": "2024-03-15",
            "discovery_date": "2024-03-20",
        }

        with patch(
            "src.edu_cti.pipeline.phase2.storage.db._derive_country_from_context",
            return_value=(None, None),
        ), patch(
            "src.edu_cti.pipeline.phase2.storage.db._extract_institution_from_reasoning",
            return_value=None,
        ), patch(
            "src.edu_cti.pipeline.phase2.storage.db._get_primary_article_metadata",
            return_value={},
        ):
            save_enrichment_result(
                temp_db,
                sample_incident.incident_id,
                enrichment,
                raw_json_data=raw_json,
            )

        temp_db.commit()
        row = temp_db.execute(
            "SELECT discovery_date FROM incidents WHERE incident_id = ?",
            (sample_incident.incident_id,),
        ).fetchone()
        assert row is not None
        assert row[0] == "2024-03-20"

    def test_missing_discovery_date_in_raw_json_is_handled(self, temp_db, sample_incident):
        """save_enrichment_result must not crash when discovery_date is absent."""
        insert_incident(temp_db, sample_incident)
        enrichment = _make_enrichment_result()

        with patch(
            "src.edu_cti.pipeline.phase2.storage.db._derive_country_from_context",
            return_value=(None, None),
        ), patch(
            "src.edu_cti.pipeline.phase2.storage.db._extract_institution_from_reasoning",
            return_value=None,
        ), patch(
            "src.edu_cti.pipeline.phase2.storage.db._get_primary_article_metadata",
            return_value={},
        ):
            save_enrichment_result(
                temp_db,
                sample_incident.incident_id,
                enrichment,
                raw_json_data={"incident_date": "2024-03-15"},
            )

        temp_db.commit()
        row = temp_db.execute(
            "SELECT discovery_date FROM incidents WHERE incident_id = ?",
            (sample_incident.incident_id,),
        ).fetchone()
        # discovery_date should remain NULL when not present in raw JSON
        assert row is not None
        assert row[0] is None


# ===========================================================================
# 8. End-to-end: full phishing incident JSON round-trip
# ===========================================================================

class TestEndToEndPhishingIncident:
    """Simulate a complete phishing incident through the mapper and verify
    all CTI intelligence fields are populated."""

    PHISHING_JSON = {
        "is_edu_cyber_incident": True,
        "education_relevance_reasoning": "Riverdale University is an educational institution",
        "institution_name": "Riverdale University",
        "institution_type": "university_public",
        "country": "United States",
        "country_code": "US",
        "incident_date": "2024-03-15",
        "discovery_date": "2024-03-20",
        "attack_category": "phishing_credential_harvest",
        "data_categories": ["student_records", "pii", "financial_records"],
        "data_breached": True,
        "ferpa_breach": True,
        "mitre_attack_techniques": [
            {
                "technique_id": "T1566",
                "technique_name": "Phishing",
                "tactic": "initial_access",
                "description": "Spear-phishing emails containing malicious links",
            },
            {
                "technique_id": "T1078",
                "technique_name": "Valid Accounts",
                "tactic": "initial_access",
                "description": "Compromised credentials used for lateral movement",
            },
        ],
        "timeline": [
            {
                "date": "2024-03-10",
                "event_type": "initial_access",
                "event_description": "Phishing emails sent to 200 staff members",
            },
            {
                "date": "2024-03-20",
                "event_type": "discovery",
                "event_description": "IT team detected unusual login activity",
            },
            {
                "date": "2024-03-22",
                "event_type": "containment",
                "event_description": "Affected accounts reset, MFA enforced",
            },
        ],
        "enriched_summary": (
            "Riverdale University was targeted by a spear-phishing campaign in March "
            "2024 that compromised staff credentials. The attackers gained access to "
            "student and financial records. The university contained the breach within "
            "two weeks and implemented mandatory MFA."
        ),
        "education_relevance": {
            "is_education_related": True,
            "reasoning": "Riverdale University is an educational institution",
        },
    }

    @pytest.fixture
    def result(self):
        return json_to_cti_enrichment(self.PHISHING_JSON, "https://example.com/article")

    def test_mitre_count(self, result):
        assert len(result.mitre_attack_techniques) == 2

    def test_mitre_tactic_populated(self, result):
        tactics = {t.tactic for t in result.mitre_attack_techniques}
        assert "initial_access" in tactics

    def test_mitre_description_populated(self, result):
        t1566 = next(t for t in result.mitre_attack_techniques if t.technique_id == "T1566")
        assert t1566.description is not None
        assert "phishing" in t1566.description.lower()

    def test_timeline_count(self, result):
        assert len(result.timeline) == 3

    def test_timeline_event_types(self, result):
        types = {e.event_type for e in result.timeline}
        assert "initial_access" in types
        assert "discovery" in types
        assert "containment" in types

    def test_data_categories_mapped(self, result):
        affected = result.data_impact.get("data_types_affected") or []
        assert "student_records" in affected
        assert "pii" in affected

    def test_ferpa_breach_key(self, result):
        assert result.regulatory_impact is not None
        assert result.regulatory_impact.get("ferpa_breach") is True
        assert "ferc_breach" not in result.regulatory_impact

    def test_summary_populated(self, result):
        assert result.enriched_summary
        assert "Riverdale University" in result.enriched_summary

    def test_education_relevance(self, result):
        assert result.education_relevance.is_education_related is True
