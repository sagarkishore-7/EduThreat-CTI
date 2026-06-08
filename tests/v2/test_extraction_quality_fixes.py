"""Tests for the deterministic extraction-quality repairs: institution_type
inference and third-party-vendor flag reconciliation."""

from __future__ import annotations

from src.edu_cti_v2.services.enrichment import _apply_extraction_quality_fixes


def test_infers_institution_type_from_name():
    payload = {
        "institution_name": "University of Oxford",
        "institution_type": None,
        "attack_category": "data_breach",
    }
    out = _apply_extraction_quality_fixes(payload)
    assert out["institution_type"] == "university"


def test_does_not_demote_known_institution_type():
    payload = {"institution_name": "Oxford College", "institution_type": "k12_school"}
    out = _apply_extraction_quality_fixes(payload)
    # Existing non-null/known value is preserved (never demoted).
    assert out["institution_type"] == "k12_school"


def test_reconciles_third_party_vendor_impact():
    payload = {
        "institution_name": "University of Oxford",
        "institution_type": "university",
        "attack_category": "third_party_compromise",
        "attack_vector": "supply_chain_compromise",
        "system_impact": {"third_party_vendor_impact": False, "critical_systems_affected": True},
    }
    out = _apply_extraction_quality_fixes(payload)
    assert out["system_impact"]["third_party_vendor_impact"] is True


def test_third_party_flag_untouched_for_non_third_party():
    payload = {
        "attack_category": "ransomware_encryption",
        "system_impact": {"third_party_vendor_impact": False},
    }
    out = _apply_extraction_quality_fixes(payload)
    assert out["system_impact"]["third_party_vendor_impact"] is False


def test_handles_missing_and_non_dict():
    assert _apply_extraction_quality_fixes(None) is None
    assert _apply_extraction_quality_fixes({}) == {}
