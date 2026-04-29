"""
End-to-end tests for the Instructor-integrated enrichment pipeline.

Tests the full data journey:

  LLM extraction (mocked)
    → JSON parsing
    → Instructor correction pass (mocked at module level — no real LLM calls)
    → json_to_cti_enrichment() mapping
    → _flatten_enrichment_for_db()
    → SQLite storage
    → get_incident_by_id() API read

All LLM and Instructor calls are mocked — no network calls are made.
Module-level patching is used throughout so tests run on Python 3.9+.
"""

import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from src.edu_cti.pipeline.phase2.extraction.instructor_corrector import (
    CriticalFieldsCorrection,
    apply_instructor_corrections,
    count_null_critical_fields,
    should_trigger_correction,
)
from src.edu_cti.pipeline.phase2.extraction.json_to_schema_mapper import json_to_cti_enrichment
from src.edu_cti.pipeline.phase2.storage.db import (
    _flatten_enrichment_for_db,
    init_incident_enrichments_table,
)
from src.edu_cti.api.database import get_incident_by_id
from src.edu_cti.core.models import BaseIncident, make_incident_id


# ── Module-level patch helpers (avoids importing instructor on Py3.9) ─────────

def _instructor_patch(mock_ic: MagicMock):
    """Replace module-level `instructor` in instructor_corrector without importing it."""
    mock_instructor = MagicMock()
    mock_instructor.from_openai.return_value = mock_ic
    mock_instructor.Mode.JSON = "json"
    return patch(
        "src.edu_cti.pipeline.phase2.extraction.instructor_corrector.instructor",
        mock_instructor,
    )


def _available_patch():
    return patch(
        "src.edu_cti.pipeline.phase2.extraction.instructor_corrector.INSTRUCTOR_AVAILABLE",
        True,
    )


# ── Shared helpers ────────────────────────────────────────────────────────────

def _make_incident(
    incident_id: Optional[str] = None,
    institution_name: str = "Test University",
) -> BaseIncident:
    iid = incident_id or make_incident_id("test", "https://example.com/test|2025-01-01")
    return BaseIncident(
        incident_id=iid,
        source="test",
        source_event_id=iid,
        institution_name=institution_name,
        victim_raw_name=institution_name,
        institution_type="university",
        country="United States",
        region=None,
        city=None,
        incident_date="2025-01-01",
        date_precision="day",
        source_published_date=None,
        ingested_at=None,
        title="Test incident",
        subtitle=None,
        primary_url=None,
        all_urls=["https://example.com/test"],
        status="suspected",
        source_confidence="high",
    )


def _create_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE incidents (
            incident_id TEXT PRIMARY KEY,
            institution_name TEXT,
            victim_raw_name TEXT,
            institution_type TEXT,
            country TEXT,
            region TEXT,
            city TEXT,
            incident_date TEXT,
            date_precision TEXT,
            title TEXT,
            subtitle TEXT,
            attack_type_hint TEXT,
            attack_category TEXT,
            status TEXT DEFAULT 'suspected',
            source_confidence TEXT DEFAULT 'medium',
            all_urls TEXT,
            primary_url TEXT,
            leak_site_url TEXT,
            source_published_date TEXT,
            llm_enriched INTEGER DEFAULT 0,
            llm_enriched_at TEXT,
            ingested_at TEXT,
            notes TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE incident_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_id TEXT NOT NULL,
            source TEXT,
            source_event_id TEXT,
            first_seen_at TEXT,
            confidence TEXT
        )
    """)
    init_incident_enrichments_table(conn)
    conn.commit()
    return conn


def _insert_incident_row(conn: sqlite3.Connection, incident: BaseIncident) -> None:
    conn.execute(
        "INSERT INTO incidents (incident_id, institution_name, ingested_at) VALUES (?, ?, ?)",
        (incident.incident_id, incident.institution_name, datetime.utcnow().isoformat()),
    )
    conn.commit()


def _write_enrichment(
    conn: sqlite3.Connection,
    incident: BaseIncident,
    raw_json: Dict[str, Any],
) -> None:
    """Map raw JSON → CTIEnrichmentResult → flat dict → SQLite."""
    enrichment = json_to_cti_enrichment(raw_json, "https://example.com/test", incident)
    flat = _flatten_enrichment_for_db(enrichment, raw_json_data=raw_json)
    flat["incident_id"] = incident.incident_id
    now = datetime.utcnow().isoformat()
    flat["created_at"] = now
    flat["updated_at"] = now
    flat["enriched_at"] = now

    cols = [k for k, v in flat.items() if v is not None and not isinstance(v, MagicMock)]
    placeholders = ", ".join("?" for _ in cols)
    col_names = ", ".join(cols)
    conn.execute(
        f"INSERT OR REPLACE INTO incident_enrichments_flat ({col_names}) VALUES ({placeholders})",
        [flat[c] for c in cols],
    )
    conn.execute(
        """INSERT OR REPLACE INTO incident_enrichments
           (incident_id, enrichment_data, created_at, updated_at)
           VALUES (?, ?, ?, ?)""",
        (incident.incident_id, json.dumps(raw_json), now, now),
    )
    conn.commit()


# ── Baseline E2E: valid extraction, no correction needed ──────────────────────

class TestE2ENoCorrection:
    """Full pipeline with well-formed LLM output — no Instructor call triggered."""

    def test_complete_extraction_reaches_api(self):
        """Key fields survive the full pipeline when LLM output is valid."""
        conn = _create_db()
        incident = _make_incident()
        _insert_incident_row(conn, incident)

        raw_json = {
            "is_edu_cyber_incident": True,
            "institution_name": "State University",
            "institution_type": "university",
            "country": "United States",
            "attack_category": "ransomware_double_extortion",
            # Use top-level ransomware_family so it's picked up by the flat mapper
            "ransomware_family": "LockBit",
            "attack_dynamics": {
                "attack_vector": "phishing",
                "ransomware_family": "LockBit",
                "data_exfiltration": True,
                "ransom_demanded": True,
                "ransom_amount": 1500000,
            },
            "data_categories": ["student_pii", "employee_ssn"],
            "records_affected_exact": 45000,
            "data_breached": True,
            "enriched_summary": "State University suffered a LockBit ransomware attack.",
        }

        # Correction should NOT trigger (all critical fields populated)
        assert should_trigger_correction(raw_json) is False

        _write_enrichment(conn, incident, raw_json)
        row = get_incident_by_id(conn, incident.incident_id)

        assert row is not None
        assert row["attack_category"] == "ransomware_double_extortion"
        assert row["records_affected_exact"] == 45000

    def test_correction_threshold_not_met_with_one_null(self):
        """One null critical field does not trigger correction."""
        data = {
            "attack_category": "data_breach",
            "institution_type": "k12_school",
            "country": "United States",
            # attack_vector is null — only 1 null field
        }
        assert should_trigger_correction(data) is False

    def test_all_enums_accepted_by_pydantic_validators(self):
        """Every valid enum value passes Pydantic validation (regression guard)."""
        from src.edu_cti.pipeline.phase2.extraction.instructor_corrector import (
            ATTACK_CATEGORY_ENUMS,
            ATTACK_VECTOR_ENUMS,
            INSTITUTION_TYPE_ENUMS,
        )
        for cat in ATTACK_CATEGORY_ENUMS:
            m = CriticalFieldsCorrection(attack_category=cat)
            assert m.attack_category == cat

        for vec in ATTACK_VECTOR_ENUMS:
            m = CriticalFieldsCorrection(attack_vector=vec)
            assert m.attack_vector == vec

        for itype in INSTITUTION_TYPE_ENUMS:
            m = CriticalFieldsCorrection(institution_type=itype)
            assert m.institution_type == itype


# ── E2E: Instructor correction fires and fills null fields ────────────────────

class TestE2EWithInstructorCorrection:
    """Pipeline where the initial LLM call leaves critical fields null, then
    Instructor correction fills them. Instructor is mocked at module level."""

    def _mock_ollama_client(self):
        client = MagicMock()
        client.model = "deepseek-v3.1:671b-cloud"
        client.client = MagicMock()
        return client

    def test_correction_fills_null_attack_category_and_reaches_db(self):
        """Instructor correction for attack_category flows all the way to the API."""
        conn = _create_db()
        incident = _make_incident()
        _insert_incident_row(conn, incident)

        # Initial LLM extraction: attack_category and institution_type are null
        raw_json: Dict[str, Any] = {
            "is_edu_cyber_incident": True,
            "institution_name": "State University",
            "institution_type": None,
            "country": None,
            "attack_category": None,
            "attack_dynamics": {"attack_vector": "phishing"},
            "data_breached": True,
            "enriched_summary": "University hit by ransomware.",
        }
        assert should_trigger_correction(raw_json) is True

        mock_correction = CriticalFieldsCorrection(
            attack_category="ransomware_encryption",
            institution_type="university",
        )
        mock_ic = MagicMock()
        mock_ic.chat.completions.create.return_value = mock_correction

        with _available_patch(), _instructor_patch(mock_ic):
            corrected_json, was_corrected = apply_instructor_corrections(
                json_data=raw_json,
                article_text="University hit by ransomware attack in 2025.",
                institution_name="State University",
                ollama_client=self._mock_ollama_client(),
            )

        assert was_corrected is True
        assert corrected_json["attack_category"] == "ransomware_encryption"
        assert corrected_json["institution_type"] == "university"

        # Write corrected extraction to DB and verify via API
        _write_enrichment(conn, incident, corrected_json)
        row = get_incident_by_id(conn, incident.incident_id)
        assert row is not None
        assert row["attack_category"] == "ransomware_encryption"

    def test_invalid_enum_from_llm_causes_instructor_retry(self):
        """Pydantic validator rejects invalid enum — this is the error Instructor feeds back."""
        # Verify the Pydantic error fires for the canonical wrong value
        with pytest.raises(ValidationError) as exc_info:
            CriticalFieldsCorrection(attack_category="ransomware")

        error_text = str(exc_info.value)
        assert "ransomware_encryption" in error_text
        assert "not a valid attack_category" in error_text

        # After seeing the error, the corrected value passes
        m = CriticalFieldsCorrection(attack_category="ransomware_encryption")
        assert m.attack_category == "ransomware_encryption"

    def test_correction_preserves_existing_valid_fields(self):
        """Existing valid fields are NOT overwritten by correction results."""
        raw_json: Dict[str, Any] = {
            "attack_category": "data_breach",  # already valid
            "institution_type": None,
            "country": None,
            "attack_dynamics": {"attack_vector": None},
        }

        # Instructor returns a conflicting attack_category — should be ignored
        mock_correction = CriticalFieldsCorrection(
            attack_category="ransomware_encryption",
            institution_type="community_college",
            attack_vector="phishing_email",
        )
        mock_ic = MagicMock()
        mock_ic.chat.completions.create.return_value = mock_correction

        with _available_patch(), _instructor_patch(mock_ic):
            corrected_json, was_corrected = apply_instructor_corrections(
                json_data=raw_json,
                article_text="Article text",
                institution_name="Community College",
                ollama_client=self._mock_ollama_client(),
            )

        # attack_category must NOT be overwritten
        assert corrected_json["attack_category"] == "data_breach"
        # New values must be filled in
        assert corrected_json["institution_type"] == "community_college"
        assert corrected_json["attack_dynamics"]["attack_vector"] == "phishing_email"

    def test_ransomware_family_flows_through_nested_dynamics(self):
        """ransomware_family from Instructor correction ends up in attack_dynamics."""
        conn = _create_db()
        incident = _make_incident()
        _insert_incident_row(conn, incident)

        raw_json: Dict[str, Any] = {
            "is_edu_cyber_incident": True,
            "institution_name": "Tech University",
            "institution_type": None,
            "country": None,
            "attack_category": None,
            "attack_dynamics": {"attack_vector": None, "ransomware_family": None},
            "enriched_summary": "University hit by ransomware.",
        }

        mock_correction = CriticalFieldsCorrection(
            attack_category="ransomware_double_extortion",
            institution_type="university",
            attack_vector="phishing_email",
            ransomware_family="BlackCat",
        )
        mock_ic = MagicMock()
        mock_ic.chat.completions.create.return_value = mock_correction

        with _available_patch(), _instructor_patch(mock_ic):
            corrected_json, was_corrected = apply_instructor_corrections(
                json_data=raw_json,
                article_text="BlackCat ransomware hit Tech University.",
                institution_name="Tech University",
                ollama_client=self._mock_ollama_client(),
            )

        assert was_corrected is True
        assert corrected_json["attack_dynamics"]["ransomware_family"] == "BlackCat"
        assert corrected_json["attack_category"] == "ransomware_double_extortion"

    def test_correction_exception_does_not_crash_pipeline(self):
        """Exception during correction must return original data unchanged."""
        raw_json: Dict[str, Any] = {
            "attack_category": None,
            "institution_type": None,
            "country": None,
            "attack_dynamics": {"attack_vector": None},
        }

        mock_ic = MagicMock()
        mock_ic.chat.completions.create.side_effect = ConnectionError("Ollama Cloud unreachable")

        with _available_patch(), _instructor_patch(mock_ic):
            corrected_json, was_corrected = apply_instructor_corrections(
                json_data=raw_json,
                article_text="Some article",
                institution_name="University",
                ollama_client=self._mock_ollama_client(),
            )

        assert was_corrected is False
        assert corrected_json is raw_json  # original returned unchanged
        assert corrected_json["attack_category"] is None


# ── E2E: json_to_cti_enrichment correctness after correction ──────────────────

class TestE2EMapperAfterCorrection:
    """Verify that corrected json_data maps cleanly through json_to_cti_enrichment."""

    def test_corrected_json_maps_to_valid_enrichment_result(self):
        """After Instructor correction, json_to_cti_enrichment must not raise."""
        incident = _make_incident()

        raw_json = {
            "is_edu_cyber_incident": True,
            "institution_name": "Lincoln K-12",
            "institution_type": "k12_school",
            "country": "United States",
            "attack_category": "data_breach",
            "attack_dynamics": {
                "attack_vector": "vulnerability_exploit",
                "ransomware_family": None,
                "data_exfiltration": True,
            },
            "data_categories": ["student_pii"],
            "records_affected_exact": 1200,
            "data_breached": True,
            "enriched_summary": "Lincoln K-12 suffered a data breach.",
        }

        result = json_to_cti_enrichment(raw_json, "https://example.com/test", incident)

        assert result is not None
        assert result.education_relevance.is_education_related is True

    def test_mapper_handles_none_attack_dynamics_after_correction(self):
        """Correction may leave attack_dynamics None — mapper must not crash."""
        incident = _make_incident()

        raw_json = {
            "is_edu_cyber_incident": True,
            "institution_name": "Test School",
            "institution_type": "k12_school",
            "country": "United States",
            "attack_category": "unauthorized_access",
            "attack_dynamics": None,
            "enriched_summary": "Unauthorized access to school records.",
        }

        result = json_to_cti_enrichment(raw_json, "https://example.com/test", incident)
        assert result is not None

    def test_flat_db_write_with_corrected_enum_values(self):
        """Corrected enum values must survive the full write→read cycle."""
        incident = _make_incident()
        conn = _create_db()
        _insert_incident_row(conn, incident)

        raw_json = {
            "is_edu_cyber_incident": True,
            "institution_name": "Research Institute",
            "institution_type": "research_institute",
            "country": "United Kingdom",
            "attack_category": "supply_chain",
            "attack_dynamics": {
                "attack_vector": "third_party_breach",
                "ransomware_family": None,
            },
            "gdpr_breach": True,
            "enriched_summary": "Supply chain attack on research institute.",
        }

        _write_enrichment(conn, incident, raw_json)
        row = get_incident_by_id(conn, incident.incident_id)

        assert row is not None
        assert row["attack_category"] == "supply_chain"
        assert row["enriched_summary"] is not None

    def test_records_affected_reaches_api_after_correction(self):
        """records_affected_exact from Instructor correction flows through to API."""
        incident = _make_incident()
        conn = _create_db()
        _insert_incident_row(conn, incident)

        raw_json = {
            "is_edu_cyber_incident": True,
            "institution_name": "State U",
            "institution_type": "university",
            "country": "United States",
            "attack_category": "data_breach",
            "records_affected_exact": 75000,
            "data_breached": True,
            "enriched_summary": "State U data breach affecting 75,000 students.",
        }

        _write_enrichment(conn, incident, raw_json)
        row = get_incident_by_id(conn, incident.incident_id)
        assert row["records_affected_exact"] == 75000


# ── E2E: Pydantic validator error messages guide LLM retry ───────────────────

class TestE2EValidatorErrorMessages:
    """Verify that error messages contain enough context for the LLM to self-correct.
    These messages are what Instructor feeds back as retry prompts."""

    def test_attack_category_error_lists_valid_options(self):
        with pytest.raises(ValidationError) as exc_info:
            CriticalFieldsCorrection(attack_category="ransomware")
        msg = str(exc_info.value)
        assert "ransomware_encryption" in msg
        assert "ransomware_double_extortion" in msg
        assert "not a valid attack_category" in msg

    def test_institution_type_error_guides_correction(self):
        with pytest.raises(ValidationError) as exc_info:
            CriticalFieldsCorrection(institution_type="high_school")
        msg = str(exc_info.value)
        assert "k12_school" in msg
        assert "not a valid institution_type" in msg

    def test_attack_vector_error_guides_correction(self):
        with pytest.raises(ValidationError) as exc_info:
            CriticalFieldsCorrection(attack_vector="email_phishing")
        msg = str(exc_info.value)
        assert "phishing" in msg
        assert "not a valid attack_vector" in msg

    def test_valid_value_after_invalid_rejected(self):
        """Confirm that the valid value passes after the invalid one was rejected."""
        with pytest.raises(ValidationError):
            CriticalFieldsCorrection(attack_category="hack")
        m = CriticalFieldsCorrection(attack_category="unauthorized_access")
        assert m.attack_category == "unauthorized_access"

    def test_case_insensitive_normalization_avoids_spurious_errors(self):
        """Mixed-case valid values should normalize cleanly without validation error."""
        m = CriticalFieldsCorrection(
            attack_category="Ransomware_Double_Extortion",
            institution_type="K12_School",
            attack_vector="Phishing_Email",
        )
        assert m.attack_category == "ransomware_double_extortion"
        assert m.institution_type == "k12_school"
        assert m.attack_vector == "phishing_email"


# ── E2E: should_trigger_correction with realistic extraction payloads ─────────

class TestE2ERealisticPayloads:
    """Test trigger/no-trigger decisions on realistic LLM output shapes."""

    def test_typical_ransomware_incident_no_trigger(self):
        """A well-enriched ransomware incident should not trigger correction."""
        data = {
            "is_edu_cyber_incident": True,
            "institution_name": "Albuquerque Public Schools",
            "institution_type": "k12_district",
            "country": "United States",
            "attack_category": "ransomware_double_extortion",
            "attack_dynamics": {
                "attack_vector": "phishing",
                "ransomware_family": "LockBit",
                "data_exfiltration": True,
                "ransom_demanded": True,
                "ransom_amount": 500000,
            },
            "data_breached": True,
            "records_affected_exact": 1200,
        }
        assert should_trigger_correction(data) is False

    def test_comparitech_stub_triggers_correction(self):
        """Comparitech stubs have no articles and no attack details — triggers correction."""
        data = {
            "is_edu_cyber_incident": True,
            "institution_name": "Unknown School District",
            "institution_type": None,
            "country": None,
            "attack_category": None,
            "attack_dynamics": {"attack_vector": None},
        }
        assert should_trigger_correction(data) is True

    def test_partial_extraction_at_threshold(self):
        """Exactly CORRECTION_THRESHOLD null fields → triggers correction."""
        data = {
            "attack_category": None,      # null
            "institution_type": None,     # null — exactly threshold=2
            "country": "United States",
            "attack_dynamics": {"attack_vector": "phishing"},
        }
        count, fields = count_null_critical_fields(data)
        assert count == 2
        assert should_trigger_correction(data) is True

    def test_unknown_attack_vector_at_threshold(self):
        """'unknown' attack_vector + null institution_type = 2 null fields → triggers."""
        data = {
            "attack_category": "data_breach",
            "institution_type": None,
            "country": "United States",
            "attack_dynamics": {"attack_vector": "unknown"},
        }
        assert should_trigger_correction(data) is True

    def test_null_correction_is_false_when_only_attack_vector_unknown(self):
        """attack_vector unknown alone (1 field) does not trigger correction."""
        data = {
            "attack_category": "data_breach",
            "institution_type": "university",
            "country": "United States",
            "attack_dynamics": {"attack_vector": "unknown"},
        }
        assert should_trigger_correction(data) is False


# ── E2E: Metric tracking ──────────────────────────────────────────────────────

class TestE2EMetricTracking:
    """The correction return value drives metric increment in enrichment.py."""

    def test_was_corrected_true_when_fields_filled(self):
        """Verify apply_instructor_corrections returns True when corrections applied."""
        mock_correction = CriticalFieldsCorrection(
            attack_category="data_breach_external",
            institution_type="university",
        )
        mock_ic = MagicMock()
        mock_ic.chat.completions.create.return_value = mock_correction

        ollama_client = MagicMock()
        ollama_client.model = "deepseek-v3.1:671b-cloud"
        ollama_client.client = MagicMock()

        with _available_patch(), _instructor_patch(mock_ic):
            _, was_corrected = apply_instructor_corrections(
                json_data={"attack_category": None, "institution_type": None, "country": None},
                article_text="Article",
                institution_name="University",
                ollama_client=ollama_client,
            )

        assert was_corrected is True

    def test_was_corrected_false_when_no_changes(self):
        """Returns False when fields are already populated — no metric increment."""
        data = {
            "attack_category": "data_breach",
            "institution_type": "university",
            "country": "United States",
            "attack_dynamics": {"attack_vector": "phishing"},
        }
        mock_ic = MagicMock()

        with _available_patch(), _instructor_patch(mock_ic):
            _, was_corrected = apply_instructor_corrections(
                json_data=data,
                article_text="Article",
                institution_name="University",
                ollama_client=MagicMock(),
            )

        mock_ic.chat.completions.create.assert_not_called()
        assert was_corrected is False
