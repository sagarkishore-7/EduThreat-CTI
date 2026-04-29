"""
Unit tests for src/edu_cti/pipeline/phase2/extraction/instructor_corrector.py

Tests the Instructor-based correction layer in full isolation — no real LLM calls.
The Pydantic validators and null-field detection logic are exercised directly;
the instructor module is mocked at the module-attribute level so tests work on
Python 3.9 (instructor ≥1.0 requires Python ≥3.10 at import time).
"""

from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from src.edu_cti.pipeline.phase2.extraction.instructor_corrector import (
    ATTACK_CATEGORY_ENUMS,
    ATTACK_VECTOR_ENUMS,
    CORRECTION_THRESHOLD,
    INSTITUTION_TYPE_ENUMS,
    CriticalFieldsCorrection,
    apply_instructor_corrections,
    count_null_critical_fields,
    should_trigger_correction,
)

# ---------------------------------------------------------------------------
# Patch helper — replaces the `instructor` module attribute inside our module
# without trying to import the real instructor package (which requires Py 3.10+).
# ---------------------------------------------------------------------------

def _instructor_patch(mock_ic: MagicMock):
    """Return a context manager that mocks out instructor in instructor_corrector."""
    mock_instructor = MagicMock()
    mock_instructor.from_openai.return_value = mock_ic
    mock_instructor.Mode.JSON = "json"
    return patch(
        "src.edu_cti.pipeline.phase2.extraction.instructor_corrector.instructor",
        mock_instructor,
    )


def _available_patch():
    """Mark INSTRUCTOR_AVAILABLE=True without actually importing instructor."""
    return patch(
        "src.edu_cti.pipeline.phase2.extraction.instructor_corrector.INSTRUCTOR_AVAILABLE",
        True,
    )


# ── Pydantic validator tests ──────────────────────────────────────────────────

class TestCriticalFieldsCorrectionValidators:
    """Pydantic validators enforce enum constraints — wrong values trigger ValueError."""

    def test_valid_attack_category_accepted(self):
        m = CriticalFieldsCorrection(attack_category="ransomware_encryption")
        assert m.attack_category == "ransomware_encryption"

    def test_attack_category_normalized_to_lowercase(self):
        m = CriticalFieldsCorrection(attack_category="Ransomware_Encryption")
        assert m.attack_category == "ransomware_encryption"

    def test_invalid_attack_category_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            CriticalFieldsCorrection(attack_category="ransomware")
        assert "attack_category" in str(exc_info.value)
        assert "ransomware_encryption" in str(exc_info.value)

    def test_valid_institution_type_accepted(self):
        m = CriticalFieldsCorrection(institution_type="k12_school")
        assert m.institution_type == "k12_school"

    def test_institution_type_normalized(self):
        m = CriticalFieldsCorrection(institution_type="K12_School")
        assert m.institution_type == "k12_school"

    def test_invalid_institution_type_raises(self):
        # "High School" → "high_school" which is NOT in the enum (k12_school is)
        with pytest.raises(ValidationError) as exc_info:
            CriticalFieldsCorrection(institution_type="high_school")
        assert "institution_type" in str(exc_info.value)

    def test_institution_type_university_is_valid(self):
        # "University" → "university" which IS in the enum
        m = CriticalFieldsCorrection(institution_type="University")
        assert m.institution_type == "university"

    def test_valid_attack_vector_accepted(self):
        m = CriticalFieldsCorrection(attack_vector="phishing_email")
        assert m.attack_vector == "phishing_email"

    def test_invalid_attack_vector_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            CriticalFieldsCorrection(attack_vector="email_phishing")
        assert "attack_vector" in str(exc_info.value)
        # Error message should list valid options
        assert "phishing" in str(exc_info.value)

    def test_none_values_accepted_for_all_fields(self):
        m = CriticalFieldsCorrection()
        assert m.attack_category is None
        assert m.institution_type is None
        assert m.attack_vector is None
        assert m.ransomware_family is None
        assert m.records_affected_exact is None

    def test_all_valid_enums_pass_attack_category(self):
        for val in ATTACK_CATEGORY_ENUMS:
            m = CriticalFieldsCorrection(attack_category=val)
            assert m.attack_category == val

    def test_all_valid_enums_pass_attack_vector(self):
        for val in ATTACK_VECTOR_ENUMS:
            m = CriticalFieldsCorrection(attack_vector=val)
            assert m.attack_vector == val

    def test_all_valid_enums_pass_institution_type(self):
        for val in INSTITUTION_TYPE_ENUMS:
            m = CriticalFieldsCorrection(institution_type=val)
            assert m.institution_type == val

    def test_attack_category_with_spaces_normalized(self):
        # "data breach external" → "data_breach_external" which is valid
        m = CriticalFieldsCorrection(attack_category="data breach external")
        assert m.attack_category == "data_breach_external"

    def test_ransomware_family_free_text_accepted(self):
        # ransomware_family has no enum constraint — any string passes
        m = CriticalFieldsCorrection(ransomware_family="LockBit 3.0")
        assert m.ransomware_family == "LockBit 3.0"

    def test_records_affected_exact_integer(self):
        m = CriticalFieldsCorrection(records_affected_exact=52000)
        assert m.records_affected_exact == 52000


# ── count_null_critical_fields tests ─────────────────────────────────────────

class TestCountNullCriticalFields:
    """Verify which fields are counted as null/unknown."""

    def test_all_null_returns_four(self):
        count, fields = count_null_critical_fields({})
        assert count == 4
        assert set(fields) == {"attack_category", "institution_type", "country", "attack_vector"}

    def test_all_populated_returns_zero(self):
        data = {
            "attack_category": "ransomware_encryption",
            "institution_type": "university",
            "country": "United States",
            "attack_dynamics": {"attack_vector": "phishing"},
        }
        count, fields = count_null_critical_fields(data)
        assert count == 0
        assert fields == []

    def test_unknown_sentinel_counts_as_null(self):
        data = {
            "attack_category": "unknown",
            "institution_type": "university",
            "country": "United States",
            "attack_dynamics": {"attack_vector": "phishing"},
        }
        count, fields = count_null_critical_fields(data)
        assert count == 1
        assert "attack_category" in fields

    def test_empty_string_counts_as_null(self):
        data = {
            "attack_category": "ransomware_encryption",
            "institution_type": "",
            "country": "United States",
            "attack_dynamics": {"attack_vector": "phishing"},
        }
        count, fields = count_null_critical_fields(data)
        assert count == 1
        assert "institution_type" in fields

    def test_attack_vector_from_nested_attack_dynamics(self):
        """attack_vector in attack_dynamics counts as populated."""
        data = {
            "attack_category": "ransomware_encryption",
            "institution_type": "university",
            "country": "UK",
            "attack_dynamics": {"attack_vector": "phishing"},
        }
        count, fields = count_null_critical_fields(data)
        assert count == 0

    def test_attack_vector_from_flat_field(self):
        """attack_vector at top level also counts as populated."""
        data = {
            "attack_category": "ransomware_encryption",
            "institution_type": "university",
            "country": "UK",
            "attack_vector": "phishing",
        }
        count, fields = count_null_critical_fields(data)
        assert count == 0

    def test_attack_vector_unknown_in_nested_counts_as_null(self):
        data = {
            "attack_category": "ransomware_encryption",
            "institution_type": "university",
            "country": "UK",
            "attack_dynamics": {"attack_vector": "unknown"},
        }
        count, fields = count_null_critical_fields(data)
        assert count == 1
        assert "attack_vector" in fields

    def test_other_sentinel_counts_as_null(self):
        data = {
            "attack_category": "other",
            "institution_type": "university",
            "country": "UK",
            "attack_dynamics": {"attack_vector": "phishing"},
        }
        count, fields = count_null_critical_fields(data)
        assert count == 1
        assert "attack_category" in fields


# ── should_trigger_correction tests ──────────────────────────────────────────

class TestShouldTriggerCorrection:

    def test_triggers_when_threshold_met(self):
        # 2 null fields → should trigger (CORRECTION_THRESHOLD = 2)
        data = {
            "attack_category": None,
            "institution_type": None,
            "country": "United States",
            "attack_dynamics": {"attack_vector": "phishing"},
        }
        assert should_trigger_correction(data) is True

    def test_does_not_trigger_below_threshold(self):
        # 1 null field → should NOT trigger
        data = {
            "attack_category": "ransomware_encryption",
            "institution_type": None,
            "country": "United States",
            "attack_dynamics": {"attack_vector": "phishing"},
        }
        assert should_trigger_correction(data) is False

    def test_triggers_on_all_null(self):
        assert should_trigger_correction({}) is True

    def test_does_not_trigger_when_all_populated(self):
        data = {
            "attack_category": "data_breach",
            "institution_type": "k12_school",
            "country": "United States",
            "attack_dynamics": {"attack_vector": "vulnerability_exploit"},
        }
        assert should_trigger_correction(data) is False

    def test_threshold_value(self):
        assert CORRECTION_THRESHOLD == 2


# ── apply_instructor_corrections tests ───────────────────────────────────────

class TestApplyInstructorCorrections:
    """Test correction application with mocked instructor module."""

    def _mock_ollama_client(self):
        client = MagicMock()
        client.model = "deepseek-v3.1:671b-cloud"
        client.client = MagicMock()
        return client

    def test_returns_original_when_instructor_unavailable(self):
        """When instructor is not installed, original data is returned unchanged."""
        with patch(
            "src.edu_cti.pipeline.phase2.extraction.instructor_corrector.INSTRUCTOR_AVAILABLE",
            False,
        ):
            data = {"attack_category": None, "institution_type": None}
            result, corrected = apply_instructor_corrections(
                json_data=data,
                article_text="Some article text",
                institution_name="Test University",
                ollama_client=self._mock_ollama_client(),
            )
            assert result is data
            assert corrected is False

    def test_no_correction_when_all_fields_populated(self):
        """Should not make an LLM call when no fields need correction."""
        data = {
            "attack_category": "ransomware_encryption",
            "institution_type": "university",
            "country": "United States",
            "attack_dynamics": {"attack_vector": "phishing"},
        }
        ollama_client = self._mock_ollama_client()
        mock_ic = MagicMock()

        with _available_patch(), _instructor_patch(mock_ic):
            result, corrected = apply_instructor_corrections(
                json_data=data,
                article_text="Some article text",
                institution_name="Test University",
                ollama_client=ollama_client,
            )

        mock_ic.chat.completions.create.assert_not_called()
        assert corrected is False

    def test_correction_fills_null_attack_category(self):
        """When attack_category is null, Instructor correction fills it."""
        data = {
            "attack_category": None,
            "institution_type": None,
            "country": "United States",
            "attack_dynamics": {"attack_vector": "phishing"},
        }
        ollama_client = self._mock_ollama_client()

        mock_correction = CriticalFieldsCorrection(
            attack_category="ransomware_encryption",
            institution_type="university",
        )
        mock_ic = MagicMock()
        mock_ic.chat.completions.create.return_value = mock_correction

        with _available_patch(), _instructor_patch(mock_ic):
            result, corrected = apply_instructor_corrections(
                json_data=data,
                article_text="University ransomware attack article text",
                institution_name="State University",
                ollama_client=ollama_client,
            )

        assert corrected is True
        assert result["attack_category"] == "ransomware_encryption"
        assert result["institution_type"] == "university"

    def test_correction_fills_attack_vector_in_nested_dynamics(self):
        """attack_vector correction writes into attack_dynamics dict."""
        data = {
            "attack_category": "ransomware_encryption",
            "institution_type": None,
            "country": None,
            "attack_dynamics": {"attack_vector": None, "ransomware_family": "LockBit"},
        }
        ollama_client = self._mock_ollama_client()

        mock_correction = CriticalFieldsCorrection(
            institution_type="k12_school",
            attack_vector="phishing_email",
        )
        mock_ic = MagicMock()
        mock_ic.chat.completions.create.return_value = mock_correction

        with _available_patch(), _instructor_patch(mock_ic):
            result, corrected = apply_instructor_corrections(
                json_data=data,
                article_text="K-12 school phishing attack",
                institution_name="Lincoln Elementary",
                ollama_client=ollama_client,
            )

        assert corrected is True
        assert result["attack_dynamics"]["attack_vector"] == "phishing_email"
        assert result["institution_type"] == "k12_school"
        # Existing ransomware_family should not be overwritten
        assert result["attack_dynamics"]["ransomware_family"] == "LockBit"

    def test_correction_fills_ransomware_family_in_nested_dynamics(self):
        """ransomware_family correction writes into attack_dynamics."""
        data = {
            "attack_category": None,
            "institution_type": None,
            "country": None,
            "attack_dynamics": {"attack_vector": None, "ransomware_family": None},
        }
        ollama_client = self._mock_ollama_client()

        mock_correction = CriticalFieldsCorrection(
            attack_category="ransomware_double_extortion",
            institution_type="university",
            attack_vector="phishing_email",
            ransomware_family="BlackCat",
        )
        mock_ic = MagicMock()
        mock_ic.chat.completions.create.return_value = mock_correction

        with _available_patch(), _instructor_patch(mock_ic):
            result, corrected = apply_instructor_corrections(
                json_data=data,
                article_text="University BlackCat ransomware attack",
                institution_name="State University",
                ollama_client=ollama_client,
            )

        assert corrected is True
        assert result["attack_dynamics"]["ransomware_family"] == "BlackCat"

    def test_does_not_overwrite_existing_values(self):
        """Correction should not replace already-populated fields."""
        data = {
            "attack_category": "data_breach",
            "institution_type": None,
            "country": None,
            "attack_dynamics": {"attack_vector": None},
        }
        ollama_client = self._mock_ollama_client()

        # Instructor returns a different attack_category — should NOT overwrite
        mock_correction = CriticalFieldsCorrection(
            attack_category="ransomware_encryption",
            institution_type="university",
            attack_vector="phishing_email",
        )
        mock_ic = MagicMock()
        mock_ic.chat.completions.create.return_value = mock_correction

        with _available_patch(), _instructor_patch(mock_ic):
            result, corrected = apply_instructor_corrections(
                json_data=data,
                article_text="Article",
                institution_name="University",
                ollama_client=ollama_client,
            )

        # attack_category must NOT be overwritten
        assert result["attack_category"] == "data_breach"
        # New fields must be filled
        assert result["institution_type"] == "university"

    def test_exception_in_instructor_returns_original_non_fatal(self):
        """Any exception in the correction pass must not crash the pipeline."""
        data = {"attack_category": None, "institution_type": None}
        ollama_client = self._mock_ollama_client()

        mock_ic = MagicMock()
        mock_ic.chat.completions.create.side_effect = RuntimeError("Connection refused")

        with _available_patch(), _instructor_patch(mock_ic):
            result, corrected = apply_instructor_corrections(
                json_data=data,
                article_text="Article",
                institution_name="University",
                ollama_client=ollama_client,
            )

        assert corrected is False
        assert result is data  # original returned unchanged

    def test_correction_uses_correct_model_and_response_model(self):
        """Verify instructor is called with the right model and response_model."""
        data = {
            "attack_category": None,
            "institution_type": None,
            "country": "US",
            "attack_dynamics": {"attack_vector": None},
        }
        ollama_client = self._mock_ollama_client()
        ollama_client.model = "deepseek-v3.1:671b-cloud"

        mock_correction = CriticalFieldsCorrection(
            attack_category="data_breach_external",
            institution_type="university",
        )
        mock_ic = MagicMock()
        mock_ic.chat.completions.create.return_value = mock_correction

        with _available_patch(), _instructor_patch(mock_ic):
            apply_instructor_corrections(
                json_data=data,
                article_text="Article",
                institution_name="University",
                ollama_client=ollama_client,
            )

            call_kwargs = mock_ic.chat.completions.create.call_args
            assert call_kwargs.kwargs["model"] == "deepseek-v3.1:671b-cloud"
            assert call_kwargs.kwargs["response_model"] is CriticalFieldsCorrection
            assert call_kwargs.kwargs["max_retries"] == 3

    def test_article_text_truncated_in_prompt(self):
        """Long article text must be truncated to 8000 chars in the prompt."""
        data = {"attack_category": None, "institution_type": None}
        ollama_client = self._mock_ollama_client()
        long_text = "A" * 50_000

        captured_messages = []

        mock_correction = CriticalFieldsCorrection(attack_category="data_breach_external")

        mock_ic = MagicMock()

        def capture_chat(**kwargs):
            captured_messages.extend(kwargs.get("messages", []))
            return mock_correction

        mock_ic.chat.completions.create.side_effect = capture_chat

        with _available_patch(), _instructor_patch(mock_ic):
            apply_instructor_corrections(
                json_data=data,
                article_text=long_text,
                institution_name="University",
                ollama_client=ollama_client,
            )

        user_prompt = next(m["content"] for m in captured_messages if m["role"] == "user")
        # The article excerpt in the prompt should be capped at 8000 chars
        assert len(user_prompt) < 50_000 + 500
