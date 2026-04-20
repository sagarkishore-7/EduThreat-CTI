from typing import Optional, Dict, Any
"""
Tests for LLM-to-API field coercion bugs.

Covers three layers where type mismatches cause silent data loss or 500 errors:
  1. Pydantic model validation (data_categories / systems_affected must be lists)
  2. database.py JSON parse layer (DB string → Python type coercion)
  3. db.py flat write layer (LLM JSON → JSON-serialised DB column)
  4. main.py DataImpact constructor guard (belt-and-suspenders)
"""

import json

import pytest

from pydantic import ValidationError

from src.edu_cti.api.models import DataImpact, SystemImpact


# ---------------------------------------------------------------------------
# 1. Pydantic model — validates the contract that caused the 500
# ---------------------------------------------------------------------------

class TestDataImpactPydanticContract:
    """DataImpact rejects bare strings in List[str] fields."""

    def test_list_accepted(self):
        di = DataImpact(data_categories=["student_pii", "employee_ssn"])
        assert di.data_categories == ["student_pii", "employee_ssn"]

    def test_none_accepted(self):
        di = DataImpact(data_categories=None)
        assert di.data_categories is None

    def test_single_element_list(self):
        di = DataImpact(data_categories=["student_pii"])
        assert di.data_categories == ["student_pii"]

    def test_bare_string_rejected(self):
        """LLM emitting a string instead of an array must not silently pass Pydantic."""
        with pytest.raises(ValidationError):
            DataImpact(data_categories="student_pii")

    def test_empty_list_accepted(self):
        di = DataImpact(data_categories=[])
        assert di.data_categories == []


class TestSystemImpactPydanticContract:
    """Same contract for systems_affected."""

    def test_list_accepted(self):
        si = SystemImpact(systems_affected=["email", "lms", "student_portal"])
        assert si.systems_affected == ["email", "lms", "student_portal"]

    def test_none_accepted(self):
        si = SystemImpact(systems_affected=None)
        assert si.systems_affected is None

    def test_bare_string_rejected(self):
        with pytest.raises(ValidationError):
            SystemImpact(systems_affected="email")


# ---------------------------------------------------------------------------
# 2. database.py parse layer — replicates the coercion logic
#    (the exact three-liner from database.py lines 272-277 / 265-270)
# ---------------------------------------------------------------------------

def _parse_json_list(raw_value: Optional[str]):
    """Mirror of the coercion logic in database.py for data_categories and
    systems_affected_codes."""
    if not raw_value:
        return None
    try:
        parsed = json.loads(raw_value)
        return parsed if isinstance(parsed, list) else ([parsed] if isinstance(parsed, str) and parsed else None)
    except Exception:
        return None


class TestDatabaseParseLayer:
    """database.py must coerce both arrays AND bare JSON strings to list."""

    def test_json_array_returned_as_list(self):
        raw = json.dumps(["student_pii", "employee_ssn"])
        assert _parse_json_list(raw) == ["student_pii", "employee_ssn"]

    def test_json_bare_string_coerced_to_single_element_list(self):
        """LLM emits string; json.dumps wraps it; json.loads produces str — must coerce."""
        raw = json.dumps("student_pii")          # produces '"student_pii"'
        result = _parse_json_list(raw)
        assert result == ["student_pii"], f"Expected ['student_pii'], got {result!r}"

    def test_none_returns_none(self):
        assert _parse_json_list(None) is None

    def test_empty_string_returns_none(self):
        assert _parse_json_list("") is None

    def test_invalid_json_returns_none(self):
        assert _parse_json_list("not-json") is None

    def test_empty_json_string_coerces_to_none(self):
        """json.dumps("") → '""'; inner string is falsy after loads — drop it."""
        raw = json.dumps("")
        assert _parse_json_list(raw) is None

    def test_single_item_list_roundtrips(self):
        raw = json.dumps(["only_one"])
        assert _parse_json_list(raw) == ["only_one"]

    def test_pydantic_accepts_coerced_result(self):
        """Coerced value must not trigger ValidationError when fed into DataImpact."""
        raw = json.dumps("student_pii")
        coerced = _parse_json_list(raw)
        di = DataImpact(data_categories=coerced)
        assert di.data_categories == ["student_pii"]

    def test_pydantic_accepts_coerced_systems(self):
        raw = json.dumps("email_system")
        coerced = _parse_json_list(raw)
        si = SystemImpact(systems_affected=coerced)
        assert si.systems_affected == ["email_system"]


# ---------------------------------------------------------------------------
# 3. db.py flat write layer — _flatten_enrichment_for_db stores lists as JSON
# ---------------------------------------------------------------------------

class TestFlatWriteDataCategories:
    """_flatten_enrichment_for_db must serialise data_categories correctly."""

    def _make_enrichment(self, raw_json_data: dict):
        from unittest.mock import MagicMock
        from src.edu_cti.pipeline.phase2.storage.db import _flatten_enrichment_for_db

        # Minimal CTIEnrichmentResult stub
        enrichment = MagicMock()
        enrichment.education_relevance = None
        enrichment.attack_dynamics = None
        enrichment.data_impact = None
        enrichment.system_impact = None
        enrichment.operational_impact = None
        enrichment.user_impact = None
        enrichment.financial_impact = None
        enrichment.regulatory_impact = None
        enrichment.recovery_metrics = None
        enrichment.transparency_metrics = None
        enrichment.timeline = None
        enrichment.mitre_attack_techniques = None
        enrichment.initial_access_description = None

        return _flatten_enrichment_for_db(enrichment, raw_json_data=raw_json_data)

    def test_list_serialised_as_json_array(self):
        flat = self._make_enrichment({"data_categories": ["student_pii", "ssn"]})
        stored = flat["data_categories"]
        assert stored is not None
        parsed = json.loads(stored)
        assert parsed == ["student_pii", "ssn"]

    def test_bare_string_serialised_as_json_string(self):
        """Current behaviour: LLM string is wrapped by json.dumps → JSON string.
        This is the upstream source of the parse-layer bug — ensures the round-trip
        test in TestDatabaseParseLayer covers the real scenario."""
        flat = self._make_enrichment({"data_categories": "student_pii"})
        stored = flat["data_categories"]
        assert stored is not None
        # json.dumps("student_pii") → '"student_pii"'
        assert json.loads(stored) == "student_pii"   # parse layer must then fix this

    def test_missing_field_returns_none(self):
        flat = self._make_enrichment({})
        assert flat["data_categories"] is None

    def test_data_types_affected_fallback(self):
        """When data_categories absent, fall back to data_impact.data_types_affected."""
        from unittest.mock import MagicMock
        from src.edu_cti.pipeline.phase2.storage.db import _flatten_enrichment_for_db

        enrichment = MagicMock()
        enrichment.education_relevance = None
        enrichment.attack_dynamics = None
        enrichment.data_impact = {"data_types_affected": ["financial_records"]}
        enrichment.system_impact = None
        enrichment.operational_impact = None
        enrichment.user_impact = None
        enrichment.financial_impact = None
        enrichment.regulatory_impact = None
        enrichment.recovery_metrics = None
        enrichment.transparency_metrics = None
        enrichment.timeline = None
        enrichment.mitre_attack_techniques = None
        enrichment.initial_access_description = None

        flat = _flatten_enrichment_for_db(enrichment, raw_json_data={})
        stored = flat["data_categories"]
        assert stored is not None
        assert json.loads(stored) == ["financial_records"]


class TestFlatWriteSystemsAffected:
    """systems_affected_codes flat write mirrors data_categories behaviour."""

    def _make_enrichment(self, raw_json_data: dict):
        from unittest.mock import MagicMock
        from src.edu_cti.pipeline.phase2.storage.db import _flatten_enrichment_for_db

        enrichment = MagicMock()
        enrichment.education_relevance = None
        enrichment.attack_dynamics = None
        enrichment.data_impact = None
        enrichment.system_impact = None
        enrichment.operational_impact = None
        enrichment.user_impact = None
        enrichment.financial_impact = None
        enrichment.regulatory_impact = None
        enrichment.recovery_metrics = None
        enrichment.transparency_metrics = None
        enrichment.timeline = None
        enrichment.mitre_attack_techniques = None
        enrichment.initial_access_description = None

        return _flatten_enrichment_for_db(enrichment, raw_json_data=raw_json_data)

    def test_list_roundtrips(self):
        flat = self._make_enrichment({"systems_affected_codes": ["email", "lms"]})
        assert json.loads(flat["systems_affected_codes"]) == ["email", "lms"]

    def test_missing_returns_none(self):
        flat = self._make_enrichment({})
        assert flat["systems_affected_codes"] is None


# ---------------------------------------------------------------------------
# 4. main.py DataImpact constructor guard
# ---------------------------------------------------------------------------

class TestMainPyConstructorGuard:
    """DataImpact in main.py drops data_categories if it's not a list."""

    def _build_data_impact(self, data_categories_value):
        """Replicate the guard from main.py:
            data_categories=v if isinstance(v, list) else None
        """
        v = data_categories_value
        return DataImpact(
            data_categories=v if isinstance(v, list) else None,
        )

    def test_list_passes_through(self):
        di = self._build_data_impact(["student_pii"])
        assert di.data_categories == ["student_pii"]

    def test_string_dropped_to_none(self):
        """If the parse layer somehow lets a string slip through, the constructor
        guard converts it to None rather than raising a 500."""
        di = self._build_data_impact("student_pii")
        assert di.data_categories is None

    def test_none_passes_through(self):
        di = self._build_data_impact(None)
        assert di.data_categories is None

    def test_already_coerced_single_element_list(self):
        di = self._build_data_impact(["student_pii"])
        assert di.data_categories == ["student_pii"]
