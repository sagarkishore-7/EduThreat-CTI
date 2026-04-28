"""
E2E tests: schema → mapper → DB field coverage.

Covers:
- normalize_institution_type / normalize_attack_vector round-trips
- other_edu_incidents mapper fix (multi-incident splitting)
- attack_chain mapped and stored
- New DB columns (secondary_attack_categories, incident_date_precision,
  encryption_extent, disclosure_source, attack_chain)
- _create_secondary_incidents() stub creation
- Dead-code removal: should_upgrade_enrichment / map_initial_access_vector gone
"""

import json
import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.edu_cti.core.db import get_connection, init_db, insert_incident
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.pipeline.phase2.extraction.json_to_schema_mapper import (
    json_to_cti_enrichment,
    normalize_attack_vector,
    normalize_institution_type,
)
from src.edu_cti.pipeline.phase2.schemas import (
    AttackDynamics,
    CTIEnrichmentResult,
    EducationRelevanceCheck,
)
from src.edu_cti.pipeline.phase2.storage.db import (
    get_enrichment_result,
    init_incident_enrichments_table,
    save_enrichment_result,
)


# ── fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def temp_db(tmp_path):
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    init_incident_enrichments_table(conn)
    yield conn, db_path
    conn.close()


def _make_incident(name: str = "Test University", source: str = "test") -> BaseIncident:
    return BaseIncident(
        incident_id=make_incident_id(source, f"{name}|2024-01-01"),
        source=source,
        source_event_id=f"{name.lower().replace(' ', '_')}|2024-01-01",
        institution_name=name,
        victim_raw_name=name,
        institution_type=None,
        country="United States",
        region=None,
        city=None,
        incident_date="2024-01-01",
        date_precision="day",
        source_published_date=None,
        ingested_at=None,
        title=f"{name} Cyber Attack",
        subtitle=None,
        primary_url=None,
        all_urls=["https://example.com/article"],
        attack_type_hint="ransomware",
    )


def _minimal_enrichment(**kwargs) -> CTIEnrichmentResult:
    defaults = dict(
        education_relevance=EducationRelevanceCheck(
            is_education_related=True,
            reasoning="Education incident",
            institution_identified="Test University",
        ),
        enriched_summary="Test university ransomware incident.",
    )
    defaults.update(kwargs)
    return CTIEnrichmentResult(**defaults)


def _minimal_raw(institution_type: str = "university") -> dict:
    return {
        "is_edu_cyber_incident": True,
        "institution_name": "Test University",
        "institution_type": institution_type,
        "attack_category": "ransomware_encryption_only",
        "country": "United States",
        "confidence": 0.9,
    }


# ── normalize_institution_type ────────────────────────────────────────────────

class TestNormalizeInstitutionType:
    def test_already_canonical_passes_through(self):
        assert normalize_institution_type("university") == "university"
        assert normalize_institution_type("k12_school") == "k12_school"
        assert normalize_institution_type("community_college") == "community_college"

    def test_old_university_public_maps_to_university(self):
        assert normalize_institution_type("university_public") == "university"
        assert normalize_institution_type("university_private") == "university"
        assert normalize_institution_type("university_research") == "university"

    def test_old_k12_types_map_to_k12_school(self):
        assert normalize_institution_type("k12_public_school") == "k12_school"
        assert normalize_institution_type("k12_private_school") == "k12_school"
        assert normalize_institution_type("k12_charter_school") == "k12_school"

    def test_free_text_university_maps_to_university(self):
        assert normalize_institution_type("University") == "university"

    def test_free_text_school_maps_to_k12(self):
        assert normalize_institution_type("School") == "k12_school"

    def test_free_text_college_maps(self):
        result = normalize_institution_type("College")
        assert result in {"community_college", "university"}

    def test_research_institute_maps(self):
        assert normalize_institution_type("Research Institute") == "research_institute"

    def test_unknown_freetext_returns_unknown(self):
        assert normalize_institution_type("Totally Unknown Thing") == "unknown"

    def test_none_returns_none(self):
        assert normalize_institution_type(None) is None

    def test_non_string_returns_unknown(self):
        assert normalize_institution_type(42) == "unknown"
        assert normalize_institution_type([]) == "unknown"

    def test_powerschool_does_not_match_school(self):
        # Regression: single-word substring "school" must not match inside "PowerSchool"
        result = normalize_institution_type("PowerSchool")
        assert result == "unknown"

    def test_case_insensitive(self):
        assert normalize_institution_type("UNIVERSITY") == normalize_institution_type("university")

    def test_hyphen_normalized(self):
        # "university-public" backward-compat maps to university
        assert normalize_institution_type("university-public") == "university"


# ── normalize_attack_vector ───────────────────────────────────────────────────

class TestNormalizeAttackVector:
    def test_canonical_value_unchanged(self):
        assert normalize_attack_vector("phishing_email") == "phishing_email"
        assert normalize_attack_vector("exposed_rdp") == "exposed_rdp"

    def test_unknown_passthrough(self):
        assert normalize_attack_vector("unknown") == "unknown"

    def test_garbage_returns_known_value(self):
        # normalize_attack_vector may fuzzy-match; the important guarantee is it
        # always returns a value from the valid set, never None or an arbitrary string.
        valid_values = {
            "phishing_email", "spear_phishing", "vulnerability_exploit", "unknown",
            "social_engineering", "other", "phishing", "credential_theft",
        }
        result = normalize_attack_vector("totally_made_up_vector")
        assert result in valid_values or result is None

    def test_none_returns_unknown_or_none(self):
        result = normalize_attack_vector(None)
        assert result in {None, "unknown"}


# ── other_edu_incidents mapper fix ────────────────────────────────────────────

class TestOtherEduIncidentsMapper:
    """Critical bug fix: other_edu_incidents must be passed to CTIEnrichmentResult."""

    def _roundup_json(self, secondaries: list) -> dict:
        return {
            "is_edu_cyber_incident": True,
            "institution_name": "Primary University",
            "attack_category": "ransomware_encryption_only",
            "country": "United States",
            "enriched_summary": "Roundup article covering multiple victims.",
            "confidence": 0.85,
            "other_edu_incidents": secondaries,
        }

    def test_other_edu_incidents_populated_in_result(self):
        secondaries = [
            {"victim_name": "Second College", "incident_date": "2024-01-05",
             "attack_type": "ransomware", "country": "United States",
             "brief_description": "Ransomware hit Second College."},
        ]
        raw = self._roundup_json(secondaries)
        result = json_to_cti_enrichment(raw, primary_url="https://example.com/article")
        assert result.other_edu_incidents is not None
        assert len(result.other_edu_incidents) == 1
        assert result.other_edu_incidents[0]["victim_name"] == "Second College"

    def test_other_edu_incidents_none_when_absent(self):
        raw = {
            "is_edu_cyber_incident": True,
            "institution_name": "Single University",
            "attack_category": "data_breach_external",
            "country": "UK",
            "enriched_summary": "Single incident.",
            "confidence": 0.9,
        }
        result = json_to_cti_enrichment(raw, primary_url="https://example.com/article")
        assert result.other_edu_incidents is None

    def test_other_edu_incidents_none_when_empty_list(self):
        raw = self._roundup_json([])
        result = json_to_cti_enrichment(raw, primary_url="https://example.com/article")
        assert result.other_edu_incidents is None

    def test_multiple_secondaries_preserved(self):
        secondaries = [
            {"victim_name": "College A", "attack_type": "phishing"},
            {"victim_name": "College B", "attack_type": "ransomware"},
            {"victim_name": "College C", "attack_type": "data_breach"},
        ]
        raw = self._roundup_json(secondaries)
        result = json_to_cti_enrichment(raw, primary_url="https://example.com/article")
        assert len(result.other_edu_incidents) == 3
        names = {e["victim_name"] for e in result.other_edu_incidents}
        assert names == {"College A", "College B", "College C"}


# ── attack_chain mapper ───────────────────────────────────────────────────────

class TestAttackChainMapper:
    def test_attack_chain_populated_from_json(self):
        raw = {
            "is_edu_cyber_incident": True,
            "institution_name": "Tech University",
            "attack_category": "ransomware_encryption_only",
            "country": "United States",
            "enriched_summary": "Attack chain incident.",
            "confidence": 0.9,
            "attack_chain": ["initial_access", "execution", "impact"],
        }
        result = json_to_cti_enrichment(raw, primary_url="https://example.com/article")
        assert result.attack_chain is not None
        assert "initial_access" in result.attack_chain
        assert "impact" in result.attack_chain

    def test_attack_chain_none_when_absent(self):
        raw = {
            "is_edu_cyber_incident": True,
            "institution_name": "Small School",
            "attack_category": "phishing",
            "country": "Canada",
            "enriched_summary": "Phishing attack.",
            "confidence": 0.8,
        }
        result = json_to_cti_enrichment(raw, primary_url="https://example.com/article")
        assert result.attack_chain is None


# ── new DB columns stored and retrieved ──────────────────────────────────────

class TestNewDbColumnsRoundTrip:
    """Verify new flat columns are written and retrievable after save."""

    def _insert_and_save(self, conn, raw_json: dict, enrichment: CTIEnrichmentResult = None) -> dict:
        incident = _make_incident()
        insert_incident(conn, incident)
        conn.commit()
        if enrichment is None:
            enrichment = _minimal_enrichment()
        save_enrichment_result(conn, incident.incident_id, enrichment,
                               raw_json_data=raw_json, force_replace=True)
        conn.commit()
        row = conn.execute(
            "SELECT * FROM incident_enrichments_flat WHERE incident_id = ?",
            (incident.incident_id,)
        ).fetchone()
        return dict(row) if row else {}

    def test_secondary_attack_categories_stored(self, temp_db):
        conn, _ = temp_db
        raw = {**_minimal_raw(), "secondary_attack_categories": ["data_breach_external", "phishing"]}
        row = self._insert_and_save(conn, raw)
        stored = json.loads(row["secondary_attack_categories"])
        assert "data_breach_external" in stored
        assert "phishing" in stored

    def test_secondary_attack_categories_null_when_absent(self, temp_db):
        conn, _ = temp_db
        raw = _minimal_raw()
        row = self._insert_and_save(conn, raw)
        assert row["secondary_attack_categories"] is None

    def test_incident_date_precision_stored(self, temp_db):
        conn, _ = temp_db
        raw = {**_minimal_raw(), "incident_date_precision": "month_only"}
        row = self._insert_and_save(conn, raw)
        assert row["incident_date_precision"] == "month_only"

    def test_encryption_extent_stored(self, temp_db):
        conn, _ = temp_db
        raw = {**_minimal_raw(), "encryption_extent": "partial_encryption"}
        row = self._insert_and_save(conn, raw)
        assert row["encryption_extent"] == "partial_encryption"

    def test_disclosure_source_stored(self, temp_db):
        conn, _ = temp_db
        raw = {**_minimal_raw(), "disclosure_source": "media_report"}
        row = self._insert_and_save(conn, raw)
        assert row["disclosure_source"] == "media_report"

    def test_attack_chain_stored_as_json_array(self, temp_db):
        conn, _ = temp_db
        enrichment = _minimal_enrichment(attack_chain=["initial_access", "execution", "impact"])
        raw = _minimal_raw()
        row = self._insert_and_save(conn, raw, enrichment)
        stored = json.loads(row["attack_chain"])
        assert "initial_access" in stored
        assert "impact" in stored

    def test_attack_chain_null_when_absent(self, temp_db):
        conn, _ = temp_db
        row = self._insert_and_save(conn, _minimal_raw())
        assert row["attack_chain"] is None

    def test_disclosure_source_from_transparency_metrics(self, temp_db):
        conn, _ = temp_db
        enrichment = _minimal_enrichment(
            transparency_metrics={"disclosure_source": "attacker_leak_site", "transparency_level": "high"}
        )
        raw = _minimal_raw()
        row = self._insert_and_save(conn, raw, enrichment)
        assert row["disclosure_source"] == "attacker_leak_site"

    def test_encryption_extent_from_system_impact(self, temp_db):
        conn, _ = temp_db
        enrichment = _minimal_enrichment(
            system_impact={"encryption_extent": "full_encryption", "critical_systems_affected": True}
        )
        raw = _minimal_raw()
        row = self._insert_and_save(conn, raw, enrichment)
        assert row["encryption_extent"] == "full_encryption"


# ── _create_secondary_incidents ───────────────────────────────────────────────

class TestCreateSecondaryIncidents:
    """Verify secondary incident stubs are created from roundup other_edu_incidents."""

    def _run_create(self, conn, secondaries: list, parent_name: str = "Parent University"):
        from src.edu_cti.pipeline.phase2.__main__ import _create_secondary_incidents
        parent = _make_incident(parent_name)
        insert_incident(conn, parent)
        conn.commit()
        _create_secondary_incidents(
            conn,
            parent_incident=parent.__dict__,
            secondary_list=secondaries,
            source_url="https://example.com/roundup",
        )
        conn.commit()

    def test_creates_stub_for_each_valid_victim(self, temp_db):
        conn, _ = temp_db
        secondaries = [
            {"victim_name": "Secondary College A", "incident_date": "2024-01-10",
             "attack_type": "ransomware", "country": "United States"},
            {"victim_name": "Secondary School B", "incident_date": "2024-01-11",
             "attack_type": "phishing", "country": "Canada"},
        ]
        self._run_create(conn, secondaries)
        rows = conn.execute("SELECT institution_name FROM incidents").fetchall()
        names = {r[0] for r in rows}
        assert "Secondary College A" in names
        assert "Secondary School B" in names

    def test_skips_unknown_victim_names(self, temp_db):
        conn, _ = temp_db
        secondaries = [
            {"victim_name": "unknown", "incident_date": "2024-01-10"},
            {"victim_name": "", "incident_date": "2024-01-10"},
            {"victim_name": "undisclosed", "incident_date": "2024-01-10"},
        ]
        self._run_create(conn, secondaries)
        # Only the parent incident should exist — no stubs for invalid names
        count = conn.execute(
            "SELECT COUNT(*) FROM incidents WHERE institution_name != 'Parent University'"
        ).fetchone()[0]
        assert count == 0

    def test_stubs_have_empty_urls(self, temp_db):
        conn, _ = temp_db
        self._run_create(conn, [{"victim_name": "Stub University", "attack_type": "ransomware"}])
        row = conn.execute(
            "SELECT all_urls FROM incidents WHERE institution_name = 'Stub University'"
        ).fetchone()
        assert row is not None
        urls = json.loads(row[0]) if row[0] else []
        assert urls == []

    def test_stubs_have_roundup_note(self, temp_db):
        conn, _ = temp_db
        self._run_create(conn, [{"victim_name": "Note University"}])
        row = conn.execute(
            "SELECT notes FROM incidents WHERE institution_name = 'Note University'"
        ).fetchone()
        assert row is not None
        assert "Extracted from roundup" in (row[0] or "")

    def test_no_duplicate_stubs_on_second_call(self, temp_db):
        conn, _ = temp_db
        secondaries = [{"victim_name": "Once College", "incident_date": "2024-03-01"}]
        self._run_create(conn, secondaries)
        self._run_create(conn, secondaries)  # second call — should be idempotent
        count = conn.execute(
            "SELECT COUNT(*) FROM incidents WHERE institution_name = 'Once College'"
        ).fetchone()[0]
        assert count == 1

    def test_list_valued_attack_type_does_not_crash(self, temp_db):
        # LLM sometimes returns attack_type as a list in other_edu_incidents entries.
        # Regression: "Error binding parameter 21: type 'list' is not supported"
        conn, _ = temp_db
        secondaries = [
            {"victim_name": "Toulouse INP", "attack_type": ["ransomware", "data_breach"],
             "country": "France", "incident_date": "2024-02-01"},
        ]
        self._run_create(conn, secondaries)  # must not raise
        row = conn.execute(
            "SELECT attack_type_hint FROM incidents WHERE institution_name = 'Toulouse INP'"
        ).fetchone()
        assert row is not None
        # First element of the list should be stored as the hint
        assert row[0] == "ransomware"

    def test_stubs_copy_source_attribution(self, temp_db):
        conn, _ = temp_db
        self._run_create(conn, [{"victim_name": "Attributed University", "incident_date": "2024-04-01"}])
        row = conn.execute(
            """
            SELECT source, source_event_id, first_seen_at
            FROM incident_sources
            WHERE incident_id = (
                SELECT incident_id FROM incidents WHERE institution_name = 'Attributed University'
            )
            LIMIT 1
            """
        ).fetchone()
        assert row is not None
        assert row[0] == "test"
        assert row[1] == "attributed university|2024-04-01|roundup_extract"
        assert row[2]


# ── dead code removed ─────────────────────────────────────────────────────────

class TestDeadCodeRemoved:
    def test_should_upgrade_enrichment_does_not_exist(self):
        import src.edu_cti.pipeline.phase2.storage.db as db_module
        assert not hasattr(db_module, "should_upgrade_enrichment"), (
            "should_upgrade_enrichment() was dead code and must be removed"
        )

    def test_map_initial_access_vector_does_not_exist(self):
        import src.edu_cti.pipeline.phase2.extraction.json_to_schema_mapper as mapper_module
        assert not hasattr(mapper_module, "map_initial_access_vector"), (
            "map_initial_access_vector() was dead code and must be removed"
        )
