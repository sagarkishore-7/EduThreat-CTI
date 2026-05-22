"""
Tests for all post-processing fixes (April 2026).

Covers every function in post_processing.py plus the two downstream fixes
that depend on it:
  - fetching_strategy.discover_articles_via_serp: skips news discovery when
    institution_name is a headline (is_headline_format check).
  - api/database.py get_incident_detail: flat-table values override the raw
    LLM JSON blob for ransomware_family and attack_vector in attack_dynamics.
"""

import json
import sqlite3
import warnings
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from src.edu_cti.pipeline.phase2.utils.post_processing import (
    apply_extraction_date_fallbacks,
    _coerce_iso_date,
    _guard_timeline_dates,
    apply_post_processing,
    extract_ransomware_family,
    infer_confirmed_status,
    infer_institution_type,
    infer_regulatory_impact,
    infer_us_region,
    is_headline_format,
)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. is_headline_format
# ═══════════════════════════════════════════════════════════════════════════════

class TestIsHeadlineFormat:
    def test_short_clean_name_is_not_headline(self):
        assert is_headline_format("University of Michigan") is False

    def test_none_returns_false(self):
        assert is_headline_format(None) is False

    def test_empty_string_returns_false(self):
        assert is_headline_format("") is False

    def test_over_70_chars_is_headline(self):
        long_name = "A" * 71
        assert is_headline_format(long_name) is True

    def test_exactly_70_chars_is_not_headline(self):
        name = "A" * 70
        assert is_headline_format(name) is False

    def test_news_source_suffix_dash_is_headline(self):
        # Pattern: "Headline text - Publication Name"
        assert is_headline_format("Cyberattack forces British high school to close - BBC News") is True

    def test_news_source_suffix_em_dash_is_headline(self):
        assert is_headline_format("Ransomware hits university — The Guardian") is True

    def test_matches_title_exactly_is_headline(self):
        title = "Politie houdt leden radicaal-rechtse groep aan"
        assert is_headline_format(title, title=title) is True

    def test_matches_title_case_insensitive(self):
        title = "Cyberattack Forces British High School"
        name = "cyberattack forces british high school"
        assert is_headline_format(name, title=title) is True

    def test_different_from_title_not_headline(self):
        title = "Cyberattack Forces British High School to Close"
        name = "Higham Lane School"
        assert is_headline_format(name, title=title) is False

    def test_real_headline_tu_eindhoven(self):
        # From monitoring: TU Eindhoven's institution_name was stored as a Dutch news headline.
        # The string is exactly 70 chars so length alone doesn't flag it — but it does match
        # the article title exactly, which is the real detection path.
        name = "Politie houdt leden radicaal-rechtse groep aan die TU Eindhoven hackte"
        title = "Politie houdt leden radicaal-rechtse groep aan die TU Eindhoven hackte"
        assert is_headline_format(name, title=title) is True  # title match

    def test_real_headline_higham_lane(self):
        name = "Cyberattack forces British high school to shut down for a week - Cybernews"
        assert is_headline_format(name) is True  # ends with " - Cybernews"

    def test_broadcaster_with_domain_dot_is_headline(self):
        # Regression: WTVR.com suffix was NOT caught because '.' was missing from char class
        assert is_headline_format("How a recent VCU data breach impacts alumni - WTVR.com") is True

    def test_broadcaster_with_dot_co_uk(self):
        assert is_headline_format("Ransomware cripples UK university - BBC.co.uk") is True

    def test_broadcaster_wptv_dot_com(self):
        assert is_headline_format(
            "Computer hackers demand $40 million ransom from Broward County Public Schools - WPTV.com"
        ) is True

    def test_real_institution_name_not_flagged(self):
        # Real names that include dots should not be caught
        assert is_headline_format("St. Mary's University") is False
        assert is_headline_format("U.S. Naval Academy") is False


# ═══════════════════════════════════════════════════════════════════════════════
# 2. extract_ransomware_family
# ═══════════════════════════════════════════════════════════════════════════════

class TestExtractRansomwareFamily:
    def test_lockbit_in_summary(self):
        assert extract_ransomware_family("LockBit ransomware encrypted servers", None) == "lockbit"

    def test_lockbit_2_in_summary(self):
        assert extract_ransomware_family("LockBit 2.0 group claimed the attack", None) == "lockbit"

    def test_lockbit_3_in_summary(self):
        assert extract_ransomware_family("LockBit 3 variant was used", None) == "lockbit"

    def test_blackcat_alphv(self):
        assert extract_ransomware_family("ALPHV/BlackCat posted data", None) == "blackcat_alphv"

    def test_blackcat_name(self):
        assert extract_ransomware_family("BlackCat ransomware group", None) == "blackcat_alphv"

    def test_cl0p(self):
        assert extract_ransomware_family("Cl0p exploited MOVEit", None) == "cl0p_clop"

    def test_clop(self):
        assert extract_ransomware_family("Clop gang stole files", None) == "cl0p_clop"

    def test_black_basta(self):
        assert extract_ransomware_family("Black Basta group encrypted files", None) == "black_basta"

    def test_blackbasta_nospace(self):
        assert extract_ransomware_family("BlackBasta ransomware", None) == "black_basta"

    def test_rhysida(self):
        assert extract_ransomware_family("Rhysida group attacked the school district", None) == "rhysida"

    def test_akira(self):
        assert extract_ransomware_family("Akira ransomware hit the university", None) == "akira"

    def test_avoslocker(self):
        assert extract_ransomware_family("AvosLocker deployed on campus network", None) == "avoslocker"

    def test_revil(self):
        assert extract_ransomware_family("REvil group demanded $1M ransom", None) == "revil_sodinokibi"

    def test_sodinokibi(self):
        assert extract_ransomware_family("Sodinokibi used in this attack", None) == "revil_sodinokibi"

    def test_found_in_title_not_summary(self):
        assert extract_ransomware_family(None, "Ryuk ransomware hits school district") == "ryuk"

    def test_summary_takes_precedence(self):
        # Black Basta appears first in keyword list before Play — Black Basta wins
        result = extract_ransomware_family("Black Basta encrypted files. Play ransomware also mentioned.", None)
        assert result == "black_basta"

    def test_case_insensitive(self):
        assert extract_ransomware_family("LOCKBIT encrypted servers", None) == "lockbit"

    def test_no_match_returns_none(self):
        assert extract_ransomware_family("Unknown attacker compromised systems", None) is None

    def test_none_both_returns_none(self):
        assert extract_ransomware_family(None, None) is None

    def test_empty_strings_return_none(self):
        assert extract_ransomware_family("", "") is None

    def test_conti(self):
        assert extract_ransomware_family("Conti ransomware group leaked data", None) == "conti"

    def test_pysa(self):
        assert extract_ransomware_family("PYSA encrypted school servers", None) == "pysa"

    def test_8base(self):
        assert extract_ransomware_family("8Base ransomware published stolen records", None) == "8base"


# ═══════════════════════════════════════════════════════════════════════════════
# 3. infer_institution_type
# ═══════════════════════════════════════════════════════════════════════════════

class TestInferInstitutionType:
    def test_unified_school_district(self):
        assert infer_institution_type("Los Angeles Unified School District", None) == "school_district"

    def test_independent_school_district(self):
        assert infer_institution_type("Houston Independent School District", None) == "school_district"

    def test_isd_abbreviation(self):
        assert infer_institution_type("Albuquerque ISD", None) == "school_district"

    def test_lausd_abbreviation(self):
        assert infer_institution_type("LAUSD", None) == "school_district"

    def test_city_public_schools(self):
        assert infer_institution_type("Detroit Public Schools", None) == "school_district"

    def test_county_schools(self):
        assert infer_institution_type("Fresno County Schools", None) == "school_district"

    def test_board_of_education(self):
        assert infer_institution_type("Chicago Board of Education", None) == "school_district"

    def test_high_school(self):
        assert infer_institution_type("Higham Lane High School", None) == "k12_school"

    def test_elementary_school(self):
        assert infer_institution_type("Riverside Elementary School", None) == "k12_school"

    def test_middle_school(self):
        assert infer_institution_type("Jefferson Middle School", None) == "k12_school"

    def test_charter_school(self):
        assert infer_institution_type("Denver Charter School", None) == "k12_school"

    def test_existing_known_type_not_overwritten(self):
        # Never demotes a known type
        assert infer_institution_type("Some High School", "university") == "university"

    def test_existing_unknown_is_upgraded(self):
        assert infer_institution_type("Springfield School District", "unknown") == "school_district"

    def test_no_match_returns_existing(self):
        assert infer_institution_type("University of Oxford", None) is None

    def test_none_name_returns_existing(self):
        assert infer_institution_type(None, None) is None

    def test_unknown_with_no_match_returns_unknown(self):
        assert infer_institution_type("University of Michigan", "unknown") == "unknown"

    def test_case_insensitive_district(self):
        assert infer_institution_type("clark county school district", None) == "school_district"

    # International K-12 patterns
    def test_spanish_primaria(self):
        assert infer_institution_type("Primaria Emiliano Zapata", None) == "k12_school"

    def test_spanish_secundaria(self):
        assert infer_institution_type("Secundaria Técnica 42", None) == "k12_school"

    def test_spanish_preparatoria(self):
        assert infer_institution_type("Preparatoria Regional de Jalisco", None) == "k12_school"

    def test_french_lycee(self):
        assert infer_institution_type("Lycée Henri Matisse", None) == "k12_school"

    def test_french_college(self):
        assert infer_institution_type("Collège Jean Moulin", None) == "k12_school"

    def test_dutch_basisschool(self):
        assert infer_institution_type("Basisschool De Regenboog", None) == "k12_school"

    def test_italian_scuola_elementare(self):
        assert infer_institution_type("Scuola Elementare G. Garibaldi", None) == "k12_school"

    def test_italian_istituto_comprensivo(self):
        assert infer_institution_type("Istituto Comprensivo Salerno 1", None) == "k12_school"

    # International university patterns
    def test_german_universitaet(self):
        assert infer_institution_type("Universität Münster", None) == "university"

    def test_french_universite(self):
        assert infer_institution_type("Université de Paris", None) == "university"

    def test_german_fachhochschule(self):
        assert infer_institution_type("Fachhochschule Münster", None) == "university"

    def test_italian_politecnico(self):
        assert infer_institution_type("Politecnico di Milano", None) == "university"

    def test_intl_type_not_overwritten_when_known(self):
        # Never demotes an already-set type
        assert infer_institution_type("Primaria Emiliano Zapata", "school_district") == "school_district"


# ═══════════════════════════════════════════════════════════════════════════════
# 4. infer_us_region
# ═══════════════════════════════════════════════════════════════════════════════

class TestInferUsRegion:
    def test_boston_is_massachusetts(self):
        assert infer_us_region("Boston", "US") == "Massachusetts"

    def test_case_insensitive(self):
        assert infer_us_region("BOSTON", "US") == "Massachusetts"

    def test_whitespace_stripped(self):
        assert infer_us_region("  Boston  ", "US") == "Massachusetts"

    def test_chicago_is_illinois(self):
        assert infer_us_region("Chicago", "US") == "Illinois"

    def test_austin_is_texas(self):
        assert infer_us_region("Austin", "US") == "Texas"

    def test_albuquerque_is_new_mexico(self):
        assert infer_us_region("Albuquerque", "US") == "New Mexico"

    def test_seattle_is_washington(self):
        assert infer_us_region("Seattle", "US") == "Washington"

    def test_non_us_country_returns_none(self):
        assert infer_us_region("London", "GB") is None

    def test_none_country_code_returns_none(self):
        assert infer_us_region("Boston", None) is None

    def test_none_city_returns_none(self):
        assert infer_us_region(None, "US") is None

    def test_unknown_city_returns_none(self):
        assert infer_us_region("Gotham City", "US") is None

    def test_ann_arbor_is_michigan(self):
        assert infer_us_region("Ann Arbor", "US") == "Michigan"

    def test_multi_word_city(self):
        assert infer_us_region("New York City", "US") == "New York"

    def test_saint_paul(self):
        assert infer_us_region("Saint Paul", "US") == "Minnesota"

    def test_honolulu_is_hawaii(self):
        assert infer_us_region("Honolulu", "US") == "Hawaii"


# ═══════════════════════════════════════════════════════════════════════════════
# 5. infer_regulatory_impact
# ═══════════════════════════════════════════════════════════════════════════════

class TestInferRegulatoryImpact:

    def _base_us_edu(self, **overrides) -> Dict[str, Any]:
        base = {
            "is_education_related": True,
            "institution_type": "university",
            "country_code": "US",
            "data_breached": True,
            "enriched_summary": "",
            "data_categories": json.dumps(["student_pii", "student_ssn"]),
        }
        base.update(overrides)
        return base

    # ── FERPA ──────────────────────────────────────────────────────────────────

    def test_ferpa_inferred_for_us_edu_with_student_data(self):
        flat = self._base_us_edu()
        infer_regulatory_impact(flat)
        assert flat["ferpa_breach"] is True

    def test_ferpa_not_set_when_already_present(self):
        flat = self._base_us_edu(ferpa_breach=False)
        infer_regulatory_impact(flat)
        assert flat["ferpa_breach"] is False  # not overwritten

    def test_ferpa_not_inferred_without_student_data(self):
        flat = self._base_us_edu(data_categories=json.dumps(["financial_records"]))
        infer_regulatory_impact(flat)
        assert flat.get("ferpa_breach") is None

    def test_ferpa_not_inferred_for_non_us(self):
        flat = self._base_us_edu(country_code="GB", data_categories=json.dumps(["student_pii"]))
        infer_regulatory_impact(flat)
        assert flat.get("ferpa_breach") is None

    def test_ferpa_not_inferred_when_no_breach(self):
        flat = self._base_us_edu(data_breached=False)
        infer_regulatory_impact(flat)
        assert flat.get("ferpa_breach") is None

    def test_ferpa_with_list_data_categories(self):
        flat = self._base_us_edu(data_categories=["student_grades", "employee_pii"])
        infer_regulatory_impact(flat)
        assert flat["ferpa_breach"] is True

    # ── GDPR ───────────────────────────────────────────────────────────────────

    def test_gdpr_inferred_for_eu_country(self):
        flat = {
            "is_education_related": True,
            "institution_type": "university",
            "country_code": "DE",
            "data_breached": True,
            "enriched_summary": "",
            "data_categories": "[]",
        }
        infer_regulatory_impact(flat)
        assert flat["gdpr_breach"] is True

    def test_gdpr_inferred_for_gb(self):
        flat = {
            "is_education_related": True,
            "institution_type": "school_district",
            "country_code": "GB",
            "data_breached": True,
            "enriched_summary": "",
            "data_categories": "[]",
        }
        infer_regulatory_impact(flat)
        assert flat["gdpr_breach"] is True

    def test_gdpr_inferred_for_nl(self):
        flat = {
            "is_education_related": True,
            "institution_type": "university",
            "country_code": "NL",
            "data_breached": True,
            "enriched_summary": "",
            "data_categories": "[]",
        }
        infer_regulatory_impact(flat)
        assert flat["gdpr_breach"] is True

    def test_gdpr_not_inferred_for_us(self):
        flat = self._base_us_edu()
        flat.pop("gdpr_breach", None)
        infer_regulatory_impact(flat)
        assert flat.get("gdpr_breach") is None

    def test_gdpr_not_overwritten(self):
        flat = {
            "is_education_related": True,
            "institution_type": "university",
            "country_code": "FR",
            "data_breached": True,
            "enriched_summary": "",
            "data_categories": "[]",
            "gdpr_breach": False,
        }
        infer_regulatory_impact(flat)
        assert flat["gdpr_breach"] is False

    # ── HIPAA ──────────────────────────────────────────────────────────────────

    def test_hipaa_inferred_from_data_categories(self):
        flat = self._base_us_edu(
            institution_type="teaching_hospital",
            data_categories=json.dumps(["medical_records", "student_pii"]),
        )
        infer_regulatory_impact(flat)
        assert flat["hipaa_breach"] is True

    def test_hipaa_inferred_from_summary_keyword(self):
        flat = self._base_us_edu(
            data_categories="[]",
            enriched_summary="University hospital exposed patient health records and PHI.",
        )
        infer_regulatory_impact(flat)
        assert flat["hipaa_breach"] is True

    def test_hipaa_not_inferred_without_health_data(self):
        flat = self._base_us_edu(
            data_categories=json.dumps(["student_pii"]),
            enriched_summary="Students' names and addresses were exposed.",
        )
        infer_regulatory_impact(flat)
        assert flat.get("hipaa_breach") is None

    def test_hipaa_not_inferred_for_non_us(self):
        flat = {
            "is_education_related": True,
            "institution_type": "university",
            "country_code": "GB",
            "data_breached": True,
            "enriched_summary": "Patient health records exposed.",
            "data_categories": json.dumps(["medical_records"]),
        }
        infer_regulatory_impact(flat)
        assert flat.get("hipaa_breach") is None

    # ── breach_notification_required ──────────────────────────────────────────

    def test_breach_notification_required_for_pii(self):
        flat = self._base_us_edu()
        infer_regulatory_impact(flat)
        assert flat["breach_notification_required"] is True

    def test_breach_notification_not_required_without_pii(self):
        flat = self._base_us_edu(data_categories=json.dumps(["internal_communications"]))
        infer_regulatory_impact(flat)
        assert flat.get("breach_notification_required") is None

    # ── notifications_sent ────────────────────────────────────────────────────

    def test_notifications_sent_from_credit_monitoring(self):
        flat = self._base_us_edu(
            enriched_summary="The university is offering credit monitoring services to affected students."
        )
        infer_regulatory_impact(flat)
        assert flat["notifications_sent"] is True

    def test_notifications_sent_from_began_notifying(self):
        flat = self._base_us_edu(
            enriched_summary="The district began notifying affected individuals in March."
        )
        infer_regulatory_impact(flat)
        assert flat["notifications_sent"] is True

    def test_notifications_sent_from_breach_notification_letter(self):
        flat = self._base_us_edu(
            enriched_summary="Victims received a data breach notification letter."
        )
        infer_regulatory_impact(flat)
        assert flat["notifications_sent"] is True

    def test_notifications_not_inferred_without_keywords(self):
        flat = self._base_us_edu(
            enriched_summary="The university confirmed a data breach occurred."
        )
        infer_regulatory_impact(flat)
        assert flat.get("notifications_sent") is None

    def test_not_education_related_skips_all(self):
        flat = {
            "is_education_related": False,
            "country_code": "US",
            "data_breached": True,
            "data_categories": json.dumps(["student_pii"]),
            "enriched_summary": "credit monitoring offered",
        }
        infer_regulatory_impact(flat)
        assert flat.get("ferpa_breach") is None
        assert flat.get("gdpr_breach") is None
        assert flat.get("hipaa_breach") is None
        assert flat.get("notifications_sent") is None


# ═══════════════════════════════════════════════════════════════════════════════
# 6. infer_confirmed_status
# ═══════════════════════════════════════════════════════════════════════════════

class TestInferConfirmedStatus:
    def test_notifying_affected_individuals(self):
        assert infer_confirmed_status("The university began notifying affected individuals.", None) is True

    def test_credit_monitoring_offered(self):
        assert infer_confirmed_status("The institution is offering credit monitoring services.", None) is True

    def test_officially_confirmed(self):
        assert infer_confirmed_status("The district officially confirmed the breach.", None) is True

    def test_publicly_disclosed(self):
        assert infer_confirmed_status("The university publicly disclosed the incident.", None) is True

    def test_announced_a_breach(self):
        assert infer_confirmed_status("The school announced a data breach affecting 10,000 students.", None) is True

    def test_breach_notification_phrase(self):
        assert infer_confirmed_status("Victims received a breach notification letter.", None) is True

    def test_apologized(self):
        assert infer_confirmed_status("The university apologized to affected students.", None) is True

    def test_paid_ransom(self):
        assert infer_confirmed_status("The district paid the ransom to recover files.", None) is True

    def test_ransom_was_paid(self):
        assert infer_confirmed_status("Ransom was paid to restore systems.", None) is True

    def test_acknowledged_the_breach(self):
        assert infer_confirmed_status("The university acknowledged the breach in a statement.", None) is True

    def test_found_in_title(self):
        assert infer_confirmed_status(None, "University confirms data breach notification sent") is True

    def test_no_confirmation_language_returns_false(self):
        assert infer_confirmed_status("Hackers may have accessed student records.", None) is False

    def test_none_both_returns_false(self):
        assert infer_confirmed_status(None, None) is False

    def test_empty_strings_returns_false(self):
        assert infer_confirmed_status("", "") is False

    def test_case_insensitive(self):
        assert infer_confirmed_status("BEGAN NOTIFYING AFFECTED INDIVIDUALS", None) is True

    def test_has_sent_notifications(self):
        assert infer_confirmed_status("The college has sent notifications to all affected users.", None) is True

    # New dark-web publication patterns
    def test_posted_on_dark_web(self):
        assert infer_confirmed_status("Hackers posted the data on the dark web.", None) is True

    def test_published_on_dark_web(self):
        assert infer_confirmed_status("Stolen records were published on dark web forums.", None) is True

    def test_dark_web_leak(self):
        assert infer_confirmed_status("A dark web leak exposed 31 students' health records.", None) is True

    # Payment disclosure patterns
    def test_paid_dollar_amount(self):
        assert infer_confirmed_status("The university paid $457,000 to recover their systems.", None) is True

    def test_ransom_payment_of(self):
        assert infer_confirmed_status("A ransom payment of $1.2M was made to restore files.", None) is True

    # Regulatory / legal patterns
    def test_notified_attorney_general(self):
        assert infer_confirmed_status("The district notified the attorney general of the breach.", None) is True

    def test_regulatory_filing(self):
        assert infer_confirmed_status("A regulatory filing confirmed the incident in March.", None) is True

    def test_class_action_lawsuit(self):
        assert infer_confirmed_status("A class-action lawsuit was filed against the university.", None) is True

    def test_lawsuit_filed(self):
        assert infer_confirmed_status("A lawsuit has been filed on behalf of affected students.", None) is True


# ═══════════════════════════════════════════════════════════════════════════════
# 5b. infer_regulatory_impact — gate: None is_education_related still triggers
# ═══════════════════════════════════════════════════════════════════════════════

class TestRegulatoryGate:
    """The gate should block only when is_education_related is explicitly False."""

    def test_none_is_education_related_does_not_block(self):
        """Synthetic/sparse incidents where LLM returned null should still get FERPA."""
        flat = {
            "is_education_related": None,
            "institution_type": "school_district",
            "country_code": "US",
            "data_breached": True,
            "enriched_summary": "",
            "data_categories": '["student_pii"]',
        }
        infer_regulatory_impact(flat)
        assert flat["ferpa_breach"] is True

    def test_false_is_education_related_blocks(self):
        """Incidents the LLM said are NOT education-related must stay blocked."""
        flat = {
            "is_education_related": False,
            "institution_type": "university",
            "country_code": "US",
            "data_breached": True,
            "enriched_summary": "",
            "data_categories": '["student_pii"]',
        }
        infer_regulatory_impact(flat)
        assert flat.get("ferpa_breach") is None

    def test_none_is_education_related_gdpr_fires_for_eu(self):
        """GDPR should also fire when is_education_related is None for EU incidents."""
        flat = {
            "is_education_related": None,
            "institution_type": "university",
            "country_code": "NL",
            "data_breached": True,
            "enriched_summary": "",
            "data_categories": "[]",
        }
        infer_regulatory_impact(flat)
        assert flat["gdpr_breach"] is True


# ═══════════════════════════════════════════════════════════════════════════════
# 7. apply_post_processing (orchestrator integration)
# ═══════════════════════════════════════════════════════════════════════════════

class TestApplyPostProcessing:

    def _make_incident_row(self, title: str = "Test Incident") -> MagicMock:
        row = MagicMock()
        row.__getitem__ = lambda self, key: title if key == "title" else None
        return row

    def test_ransomware_family_filled_from_summary(self):
        flat = {
            "ransomware_family": None,
            "institution_name": "Springfield University",
            "institution_type": "university",
            "country_code": "US",
            "city": None,
            "data_breached": False,
            "enriched_summary": "LockBit ransomware encrypted university servers.",
            "is_education_related": False,
        }
        apply_post_processing(flat, None, summary="LockBit ransomware encrypted university servers.")
        assert flat["ransomware_family"] == "lockbit"

    def test_ransomware_family_not_overwritten(self):
        flat = {
            "ransomware_family": "akira",
            "institution_name": "Springfield University",
            "institution_type": "university",
            "country_code": "US",
            "city": None,
            "data_breached": False,
            "enriched_summary": "LockBit ransomware encrypted servers.",
            "is_education_related": False,
        }
        apply_post_processing(flat, None, summary="LockBit ransomware encrypted servers.")
        assert flat["ransomware_family"] == "akira"  # not overwritten

    def test_institution_type_upgraded_from_unknown(self):
        flat = {
            "ransomware_family": None,
            "institution_name": "Clark County School District",
            "institution_type": "unknown",
            "country_code": "US",
            "city": None,
            "region": None,
            "data_breached": False,
            "enriched_summary": "",
            "is_education_related": False,
        }
        apply_post_processing(flat, None)
        assert flat["institution_type"] == "school_district"

    def test_region_filled_from_city(self):
        flat = {
            "ransomware_family": None,
            "institution_name": "MIT",
            "institution_type": "university_research",
            "country_code": "US",
            "city": "Boston",
            "region": None,
            "data_breached": False,
            "enriched_summary": "",
            "is_education_related": False,
        }
        apply_post_processing(flat, None)
        assert flat["region"] == "Massachusetts"

    def test_region_not_filled_for_non_us(self):
        flat = {
            "ransomware_family": None,
            "institution_name": "TU Eindhoven",
            "institution_type": "university",
            "country_code": "NL",
            "city": "Eindhoven",
            "region": None,
            "data_breached": False,
            "enriched_summary": "",
            "is_education_related": False,
        }
        apply_post_processing(flat, None)
        assert flat.get("region") is None

    def test_regulatory_impact_filled_for_us_edu_breach(self):
        flat = {
            "ransomware_family": None,
            "institution_name": "State University",
            "institution_type": "university",
            "country_code": "US",
            "city": None,
            "region": None,
            "data_breached": True,
            "data_categories": json.dumps(["student_pii", "student_ssn"]),
            "enriched_summary": "The university began notifying affected students.",
            "is_education_related": True,
        }
        apply_post_processing(flat, None)
        assert flat.get("ferpa_breach") is True
        assert flat.get("notifications_sent") is True
        assert flat.get("breach_notification_required") is True

    def test_title_used_for_ransomware_when_no_summary(self):
        flat = {
            "ransomware_family": None,
            "institution_name": "Some School",
            "institution_type": "k12_school",
            "country_code": "US",
            "city": None,
            "data_breached": False,
            "enriched_summary": None,
            "is_education_related": False,
        }
        incident_row = self._make_incident_row(title="Ryuk ransomware hits school district")
        apply_post_processing(flat, incident_row)
        assert flat["ransomware_family"] == "ryuk"

    def test_all_nulls_handled_gracefully(self):
        flat = {
            "ransomware_family": None,
            "institution_name": None,
            "institution_type": None,
            "country_code": None,
            "city": None,
            "region": None,
            "data_breached": None,
            "enriched_summary": None,
            "is_education_related": None,
        }
        apply_post_processing(flat, None)
        # Should not raise; all values stay None or unchanged
        assert flat["ransomware_family"] is None

    def test_students_affected_propagated_from_records_exact(self):
        """When records_affected_exact is set and data_categories has student data,
        students_affected should be filled."""
        flat = {
            "ransomware_family": None,
            "institution_name": "Primaria Emiliano Zapata",
            "institution_type": "k12_school",
            "country_code": "MX",
            "city": None,
            "region": None,
            "data_breached": True,
            "data_categories": '["student_pii"]',
            "enriched_summary": "31 students' data was leaked.",
            "is_education_related": True,
            "records_affected_exact": 31,
            "students_affected": None,
        }
        apply_post_processing(flat, None)
        assert flat["students_affected"] == 31

    def test_students_affected_not_overwritten(self):
        """LLM-set students_affected must not be overwritten."""
        flat = {
            "ransomware_family": None,
            "institution_name": "Test University",
            "institution_type": "university",
            "country_code": "US",
            "city": None,
            "data_breached": True,
            "data_categories": '["student_pii"]',
            "enriched_summary": "",
            "is_education_related": True,
            "records_affected_exact": 1000,
            "students_affected": 500,
        }
        apply_post_processing(flat, None)
        assert flat["students_affected"] == 500  # not overwritten

    def test_students_affected_not_set_without_student_categories(self):
        """Should not propagate if data_categories has no student keywords."""
        flat = {
            "ransomware_family": None,
            "institution_name": "Test University",
            "institution_type": "university",
            "country_code": "US",
            "city": None,
            "data_breached": True,
            "data_categories": '["employee_pii"]',
            "enriched_summary": "",
            "is_education_related": True,
            "records_affected_exact": 100,
            "students_affected": None,
        }
        apply_post_processing(flat, None)
        assert flat.get("students_affected") is None

    def test_spanish_health_data_keyword_inferred(self):
        """'temas de salud' in summary should add health_records to data_categories."""
        flat = {
            "ransomware_family": None,
            "institution_name": "Primaria Emiliano Zapata",
            "institution_type": "k12_school",
            "country_code": "MX",
            "city": None,
            "data_breached": True,
            "data_categories": '["student_pii"]',
            "enriched_summary": "Los datos incluyen temas de salud personal de los alumnos.",
            "is_education_related": True,
        }
        apply_post_processing(flat, None)
        import json as _json
        cats = _json.loads(flat["data_categories"])
        assert "health_records" in cats

    def test_gdpr_filled_for_eu_breach(self):
        flat = {
            "ransomware_family": None,
            "institution_name": "TU Eindhoven",
            "institution_type": "university",
            "country_code": "NL",
            "city": "Eindhoven",
            "region": None,
            "data_breached": True,
            "data_categories": "[]",
            "enriched_summary": "",
            "is_education_related": True,
        }
        apply_post_processing(flat, None)
        assert flat.get("gdpr_breach") is True


class TestExtractionDateFallbacks:
    def test_coerces_cest_publication_date_without_timezone_warning(self):
        from dateutil.parser import UnknownTimezoneWarning

        with warnings.catch_warnings():
            warnings.simplefilter("error", UnknownTimezoneWarning)
            assert _coerce_iso_date("20 May 2026 18:09 CEST") == "2026-05-20"

    def test_backfills_publication_date_from_article_metadata(self):
        payload = {
            "publication_date": None,
            "incident_date": None,
            "incident_date_precision": "unknown",
            "timeline": [],
        }

        apply_extraction_date_fallbacks(
            payload,
            article_text="The university said systems were impacted recently.",
            article_publish_date="2024-03-20",
            source_published_date=None,
        )

        assert payload["publication_date"] == "2024-03-20"
        assert payload["publication_date_basis"] == "article_metadata_fallback"
        assert payload["source_published_date"] == "2024-03-20"
        assert payload["incident_date"] is None

    def test_derives_incident_date_from_last_weekday(self):
        payload = {
            "publication_date": None,
            "incident_date": None,
            "incident_date_precision": "unknown",
            "timeline": [
                {
                    "event_type": "initial_access",
                    "event_description": "Attackers gained access last Sunday.",
                    "date": None,
                    "date_precision": None,
                }
            ],
        }

        apply_extraction_date_fallbacks(
            payload,
            article_text=(
                "Jonathan Greig March 20th, 2024. "
                "The district said the ransomware attack began last Sunday."
            ),
            article_publish_date="2024-03-20",
            source_published_date=None,
        )

        assert payload["publication_date"] == "2024-03-20"
        assert payload["publication_date_basis"] == "article_metadata_fallback"
        assert payload["incident_date"] == "2024-03-17"
        assert payload["incident_date_basis"] == "deterministic_relative_to_publication_date"
        assert payload["incident_date_precision"] == "approximate"
        assert payload["timeline"][0]["date"] == "2024-03-17"
        assert payload["timeline"][0]["date_precision"] == "approximate"

    def test_does_not_promote_publication_date_to_incident_date_without_support(self):
        payload = {
            "publication_date": None,
            "incident_date": None,
            "incident_date_precision": "unknown",
            "timeline": [],
        }

        apply_extraction_date_fallbacks(
            payload,
            article_text=(
                "Jonathan Greig March 15th, 2024. "
                "The university confirmed it is investigating the incident."
            ),
            article_publish_date="2024-03-15",
            source_published_date=None,
        )

        assert payload["publication_date"] == "2024-03-15"
        assert payload["incident_date"] is None

    def test_marks_existing_llm_dates_with_basis_without_overwriting(self):
        payload = {
            "publication_date": "2024-03-15",
            "incident_date": "2024-03-14",
            "incident_date_precision": "day",
            "timeline": [],
        }

        apply_extraction_date_fallbacks(
            payload,
            article_text="The school disclosed the incident on March 15, 2024 after it happened the day before.",
            article_publish_date="2024-03-15",
            source_published_date=None,
        )

        assert payload["publication_date"] == "2024-03-15"
        assert payload["publication_date_basis"] == "llm_extracted"
        assert payload["incident_date"] == "2024-03-14"
        assert payload["incident_date_basis"] == "llm_extracted"

    def test_shifts_month_only_dates_that_llm_anchors_after_publication(self):
        payload = {
            "publication_date": None,
            "incident_date": "2026-08-01",
            "incident_date_precision": "month_only",
            "timeline": [
                {
                    "event_type": "compromise",
                    "event_description": "Hackers broke in during August.",
                    "date": "2026-08-01",
                    "date_precision": "month_only",
                },
                {
                    "event_type": "regulatory_filing",
                    "event_description": "The report was filed in December.",
                    "date": "2026-12-01",
                    "date_precision": "month_only",
                },
                {
                    "event_type": "public_disclosure",
                    "event_description": "The article was published.",
                    "date": "2026-01-12",
                    "date_precision": "day",
                },
            ],
        }

        apply_extraction_date_fallbacks(
            payload,
            article_text="The breach happened in August and was reported in December.",
            article_publish_date="2026-01-12",
            source_published_date=None,
        )

        assert payload["incident_date"] == "2025-08-01"
        assert payload["incident_date_basis"] == "shifted_previous_year_after_publication"
        assert payload["timeline"][0]["date"] == "2025-08-01"
        assert payload["timeline"][1]["date"] == "2025-12-01"
        assert payload["timeline"][2]["date"] == "2026-01-12"

    def test_discards_exact_dates_that_are_after_publication_window(self):
        payload = {
            "publication_date": None,
            "incident_date": "2026-08-15",
            "incident_date_precision": "day",
            "timeline": [
                {
                    "event_type": "initial_access",
                    "event_description": "The LLM invented an exact future date.",
                    "date": "2026-08-15",
                    "date_precision": "day",
                },
            ],
        }

        apply_extraction_date_fallbacks(
            payload,
            article_text="The university disclosed the breach in January.",
            article_publish_date="2026-01-12",
            source_published_date=None,
        )

        assert payload["incident_date"] is None
        assert payload["incident_date_basis"] == "discarded_after_publication"
        assert payload["timeline"][0]["date"] is None
        assert payload["timeline"][0]["date_repair_basis"] == "discarded_after_publication"

    def test_source_publication_date_wins_over_much_newer_article_metadata(self):
        payload = {
            "publication_date": None,
            "incident_date": "2026-05-28",
            "incident_date_precision": "day",
            "timeline": [
                {
                    "event_type": "data_exfiltration",
                    "event_description": "Leaked student portal records were uploaded.",
                    "date": "2026-05-28",
                    "date_precision": "day",
                },
                {
                    "event_type": "disclosure",
                    "event_description": "The university acknowledged the incident.",
                    "date": "2026-05-18",
                    "date_precision": "approximate",
                },
            ],
        }

        apply_extraction_date_fallbacks(
            payload,
            article_text="The leaked information was uploaded last May 28.",
            article_publish_date="2026-05-18",
            source_published_date="2020-06-17",
        )

        assert payload["publication_date"] == "2020-06-17"
        assert payload["publication_date_basis"] == "source_metadata_fallback"
        assert payload["incident_date"] is None
        assert payload["incident_date_basis"] == "discarded_after_publication"
        assert payload["timeline"][0]["date"] is None
        assert payload["timeline"][1]["date"] == "2020-06-17"

    def test_curated_source_date_does_not_act_as_publication_anchor(self):
        payload = {
            "publication_date": None,
            "incident_date": "2023-10-05",
            "incident_date_precision": "day",
            "timeline": [],
        }

        apply_extraction_date_fallbacks(
            payload,
            article_text="Ransomware attack on a school district in 2023.",
            article_publish_date=None,
            source_published_date="2020-09-09",
            source_name="comparitech",
        )

        assert payload["incident_date"] == "2023-10-05"
        assert payload["incident_date_basis"] == "llm_extracted"
        assert payload["publication_date"] is None


# ═══════════════════════════════════════════════════════════════════════════════
# 8. fetching_strategy: SERP bypass for headline institution_name
# ═══════════════════════════════════════════════════════════════════════════════

class TestSerpHeadlineBypass:
    """
    discover_articles_via_serp must skip the SERP call when institution_name
    looks like a news headline (is_headline_format returns True).
    """

    def _make_incident(self, name: str, title: str = "") -> Dict[str, Any]:
        return {
            "institution_name": name,
            "victim_raw_name": "",
            "title": title,
            "attack_type_hint": "ransomware",
            "incident_date": "2025-01-15",
        }

    @patch("src.edu_cti.pipeline.phase2.utils.fetching_strategy._discover_google_news_rss_with_scrapling")
    @patch("src.edu_cti.pipeline.phase2.utils.fetching_strategy.OxylabsClient")
    def test_serp_skipped_for_headline_name(self, mock_oxylabs_cls, mock_google):
        from src.edu_cti.pipeline.phase2.utils.fetching_strategy import discover_articles_via_serp

        headline = "Cyberattack forces British high school to shut down for a week - Cybernews"
        incident = self._make_incident(headline)
        result = discover_articles_via_serp(incident)

        mock_google.assert_not_called()
        mock_oxylabs_cls.return_value.search_news.assert_not_called()
        assert result == []

    @patch("src.edu_cti.pipeline.phase2.utils.fetching_strategy._discover_google_news_rss_with_scrapling")
    @patch("src.edu_cti.pipeline.phase2.utils.fetching_strategy.OxylabsClient")
    def test_free_google_news_used_for_normal_name(self, mock_oxylabs_cls, mock_google):
        from src.edu_cti.pipeline.phase2.utils.fetching_strategy import discover_articles_via_serp

        mock_google.return_value = ["https://example.com/article"]

        incident = self._make_incident("University of Michigan", "University of Michigan breach")
        result = discover_articles_via_serp(incident)

        assert result == ["https://example.com/article"]
        mock_google.assert_called_once()
        mock_oxylabs_cls.return_value.search_news.assert_not_called()

    @patch("src.edu_cti.pipeline.phase2.utils.fetching_strategy._discover_google_news_rss_with_scrapling")
    @patch("src.edu_cti.pipeline.phase2.utils.fetching_strategy.OxylabsClient")
    def test_title_discovery_retries_with_stripped_loose_headline(self, mock_oxylabs_cls, mock_google):
        from src.edu_cti.pipeline.phase2.utils.fetching_strategy import discover_articles_via_serp

        mock_google.side_effect = [[], [], ["https://example.com/canvas"]]
        incident = self._make_incident(
            "",
            "Canvas cyberattack disrupts schools across Canada - Example News",
        )

        result = discover_articles_via_serp(incident)

        assert result == ["https://example.com/canvas"]
        assert mock_google.call_args_list[0].args[0] == '"Canvas cyberattack disrupts schools across Canada - Example News"'
        assert mock_google.call_args_list[1].args[0] == '"Canvas cyberattack disrupts schools across Canada"'
        assert mock_google.call_args_list[2].args[0] == "Canvas cyberattack disrupts schools across Canada"
        mock_oxylabs_cls.return_value.search_news.assert_not_called()

    @patch("src.edu_cti.pipeline.phase2.utils.fetching_strategy._discover_google_news_rss_with_scrapling")
    @patch("src.edu_cti.pipeline.phase2.utils.fetching_strategy.OxylabsClient")
    def test_serp_skipped_for_very_long_name(self, mock_oxylabs_cls, mock_google):
        from src.edu_cti.pipeline.phase2.utils.fetching_strategy import discover_articles_via_serp

        long_name = "This is a very long institution name that clearly exceeds seventy characters total"
        incident = self._make_incident(long_name)
        result = discover_articles_via_serp(incident)

        mock_google.assert_not_called()
        mock_oxylabs_cls.return_value.search_news.assert_not_called()
        assert result == []

    @patch("src.edu_cti.pipeline.phase2.utils.fetching_strategy._discover_google_news_rss_with_scrapling")
    @patch("src.edu_cti.pipeline.phase2.utils.fetching_strategy.OxylabsClient")
    def test_oxylabs_only_used_when_enabled_and_free_empty(self, mock_oxylabs_cls, mock_google, monkeypatch):
        from src.edu_cti.pipeline.phase2.utils.fetching_strategy import discover_articles_via_serp

        monkeypatch.setenv("EDU_CTI_ENABLE_OXYLABS_SERP", "1")
        mock_google.return_value = []
        mock_oxylabs_cls.return_value.search_news.return_value = [
            {"url": "https://example.com/paid-fallback", "title": "University breach"}
        ]

        incident = self._make_incident("University of Michigan", "University of Michigan breach")
        result = discover_articles_via_serp(incident)

        assert result == ["https://example.com/paid-fallback"]
        mock_oxylabs_cls.return_value.search_news.assert_called_once()

    def test_google_news_rss_fixture_filters_and_caps(self, monkeypatch):
        from src.edu_cti.pipeline.phase2.utils import fetching_strategy as fs

        fixture = """<?xml version="1.0" encoding="UTF-8"?>
        <rss><channel>
          <item><title>A</title><link>https://news.google.com/rss/articles/wrapped</link></item>
          <item><title>B</title><link>https://twitter.com/not-an-article</link></item>
          <item><title>C</title><link>https://example.org/cyber-school</link></item>
        </channel></rss>"""
        monkeypatch.setattr(fs, "_fetch_discovery_url_with_scrapling", lambda _url: fixture)
        monkeypatch.setattr(
            fs,
            "_resolve_google_news_article_url",
            lambda link: "https://example.com/article" if "wrapped" in link else link,
        )

        assert fs._discover_google_news_rss_with_scrapling("university breach", 2) == [
            "https://example.com/article",
            "https://example.org/cyber-school",
        ]

    def test_bing_news_rss_fixture_extracts_apiclick_urls(self, monkeypatch):
        from src.edu_cti.pipeline.phase2.utils import fetching_strategy as fs

        fixture = """<?xml version="1.0" encoding="UTF-8"?>
        <rss><channel>
          <item><title>A</title><link>http://www.bing.com/news/apiclick.aspx?url=https%3A%2F%2Fexample.com%2Farticle</link></item>
          <item><title>B</title><link>https://twitter.com/not-an-article</link></item>
          <item><title>C</title><link>https://example.org/cyber-school</link></item>
        </channel></rss>"""
        monkeypatch.setattr(fs, "_fetch_discovery_url_with_scrapling", lambda _url: fixture)

        assert fs._discover_bing_news_rss_with_scrapling("university breach", 2) == [
            "https://example.com/article",
            "https://example.org/cyber-school",
        ]

    @patch("src.edu_cti.pipeline.phase2.utils.fetching_strategy._discover_google_news_rss_with_scrapling")
    @patch("src.edu_cti.pipeline.phase2.utils.fetching_strategy._discover_bing_news_rss_with_scrapling")
    @patch("src.edu_cti.pipeline.phase2.utils.fetching_strategy.OxylabsClient")
    def test_bing_used_when_google_news_empty(self, mock_oxylabs_cls, mock_bing, mock_google):
        from src.edu_cti.pipeline.phase2.utils.fetching_strategy import discover_articles_via_serp

        mock_google.return_value = []
        mock_bing.return_value = ["https://example.com/bing-article"]

        incident = self._make_incident("University of Michigan", "University of Michigan breach")
        result = discover_articles_via_serp(incident)

        assert result == ["https://example.com/bing-article"]
        mock_google.assert_called_once()
        mock_bing.assert_called_once()
        mock_oxylabs_cls.return_value.search_news.assert_not_called()

    def test_scrapling_discovery_uses_millisecond_env_timeout_as_seconds(self, monkeypatch):
        from src.edu_cti.pipeline.phase2.utils import fetching_strategy as fs

        calls = []

        class FakeScraplingFetcher:
            @staticmethod
            def get(url, **kwargs):
                calls.append(kwargs)

                class Response:
                    status = 200
                    body = b"<rss><channel></channel></rss>"

                return Response()

        monkeypatch.setenv("EDU_CTI_SCRAPLING_DISCOVERY_TIMEOUT_MS", "2500")
        monkeypatch.setattr(fs, "SCRAPLING_DISCOVERY_AVAILABLE", True)
        monkeypatch.setattr(fs, "ScraplingFetcher", FakeScraplingFetcher)

        assert fs._fetch_discovery_url_with_scrapling("https://news.google.com/rss/search?q=test")
        assert calls[0]["timeout"] == 2.5
        assert calls[0]["stealthy_headers"] is True
        assert calls[0]["follow_redirects"] is True

    def test_yahoo_consent_fixture_returns_empty(self, monkeypatch):
        from src.edu_cti.pipeline.phase2.utils import fetching_strategy as fs

        monkeypatch.setattr(
            fs,
            "_fetch_discovery_url_with_scrapling",
            lambda _url: "<html><title>Consent</title><body>consent.yahoo.com privacy dashboard</body></html>",
        )

        assert fs._discover_yahoo_news_with_scrapling("university breach", 5) == []


# ═══════════════════════════════════════════════════════════════════════════════
# 9. API database.py: flat-table values override JSON blob for attack_dynamics
# ═══════════════════════════════════════════════════════════════════════════════

class TestApiAttackDynamicsOverride:
    """
    When get_incident_detail builds the attack_dynamics dict from the raw JSON
    blob, it must override ransomware_family and attack_vector with values from
    the post-processed flat table (incident_enrichments_flat).
    """

    def _setup_db(self) -> sqlite3.Connection:
        """Create an in-memory DB with the schema tables needed for the test."""
        from src.edu_cti.core.db import init_db
        from src.edu_cti.pipeline.phase2.storage.db import init_incident_enrichments_table
        from src.edu_cti.pipeline.phase2.storage.article_storage import init_articles_table

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_db(conn)
        init_incident_enrichments_table(conn)
        init_articles_table(conn)
        return conn

    def _insert_incident(self, conn, incident_id: str = "test_001"):
        # incidents table has no 'source' column (moved to incident_sources)
        conn.execute(
            """
            INSERT INTO incidents (
                incident_id, institution_name, victim_raw_name,
                status, ingested_at, primary_url
            ) VALUES (?,?,?,?,?,?)
            """,
            (incident_id, "Test University", "Test University",
             "suspected", "2024-01-01T00:00:00Z", "https://example.com/article"),
        )
        conn.commit()

    def _insert_enrichment_blob(self, conn, incident_id: str, raw_blob: dict):
        """Insert raw LLM JSON blob (pre-post-processing values)."""
        conn.execute(
            """INSERT INTO incident_enrichments
               (incident_id, final_enrichment_json, created_at, updated_at)
               VALUES (?,?,?,?)""",
            (incident_id, json.dumps(raw_blob), "2024-01-02T00:00:00Z", "2024-01-02T00:00:00Z"),
        )
        conn.commit()

    def _insert_flat_row(self, conn, incident_id: str, ransomware_family: str, attack_vector: str):
        """Insert post-processed flat row."""
        conn.execute(
            """
            INSERT INTO incident_enrichments_flat
                (incident_id, ransomware_family, attack_vector, is_education_related, created_at, updated_at)
            VALUES (?,?,?,?,?,?)
            """,
            (incident_id, ransomware_family, attack_vector, 1, "2024-01-02T00:00:00Z", "2024-01-02T00:00:00Z"),
        )
        conn.commit()

    def test_flat_table_ransomware_overrides_blob(self):
        """
        Scenario: LLM JSON blob has ransomware_family=null, but post-processing
        filled it in the flat table. The API must return the flat-table value.
        """
        from src.edu_cti.api.database import get_incident_by_id as get_incident_detail

        conn = self._setup_db()
        iid = "test_001"
        self._insert_incident(conn, iid)

        # Blob has null ransomware_family
        blob = {
            "attack_dynamics": {
                "ransomware_family": None,
                "attack_vector": None,
                "attack_chain": ["initial_access", "execution", "impact"],
            }
        }
        self._insert_enrichment_blob(conn, iid, blob)

        # Flat table has it filled (post-processing keyword scan result)
        self._insert_flat_row(conn, iid, "lockbit", "phishing_email")

        result = get_incident_detail(conn, iid)

        assert result is not None
        ad = result.get("attack_dynamics", {})
        assert ad.get("ransomware_family") == "LockBit"
        assert ad.get("attack_vector") == "phishing_email"

    def test_blob_value_kept_when_flat_has_none(self):
        """
        When flat table has no value for these fields, the blob value is kept.
        """
        from src.edu_cti.api.database import get_incident_by_id as get_incident_detail

        conn = self._setup_db()
        iid = "test_002"
        self._insert_incident(conn, iid)

        blob = {
            "attack_dynamics": {
                "ransomware_family": "akira",
                "attack_vector": "exposed_vpn",
                "attack_chain": ["initial_access", "impact"],
            }
        }
        self._insert_enrichment_blob(conn, iid, blob)
        self._insert_flat_row(conn, iid, None, None)

        result = get_incident_detail(conn, iid)

        ad = result.get("attack_dynamics", {})
        # flat has None → blob value is kept
        assert ad.get("ransomware_family") == "Akira"
        assert ad.get("attack_vector") == "exposed_vpn"

    def test_flat_overrides_wrong_blob_ransomware(self):
        """
        Scenario from monitoring: LLM stored wrong ransomware_family in blob,
        post-processing keyword scan found the correct one in flat table.
        """
        from src.edu_cti.api.database import get_incident_by_id as get_incident_detail

        conn = self._setup_db()
        iid = "test_003"
        self._insert_incident(conn, iid)

        blob = {
            "attack_dynamics": {
                "ransomware_family": "unknown",
                "attack_vector": "phishing_email",
            }
        }
        self._insert_enrichment_blob(conn, iid, blob)
        self._insert_flat_row(conn, iid, "rhysida", "phishing_email")

        result = get_incident_detail(conn, iid)

        ad = result.get("attack_dynamics", {})
        assert ad.get("ransomware_family") == "Rhysida"


# ═══════════════════════════════════════════════════════════════════════════════
# 9. _guard_timeline_dates
# ═══════════════════════════════════════════════════════════════════════════════

class TestGuardTimelineDates:
    """Timeline event dates that are >90 days after source_published_date are nulled."""

    def _make_incident_row(self, source_published_date: str):
        row = MagicMock()
        row.__getitem__ = lambda self, k: source_published_date if k == "source_published_date" else None
        return row

    def test_future_date_nulled(self):
        """Event date >90 days after source is nulled and precision set to approximate."""
        flat = {
            "timeline_json": json.dumps([
                {"date": "2026-04-23", "date_precision": "day", "event_description": "Attack occurred"},
            ]),
        }
        incident_row = self._make_incident_row("2025-03-10")
        _guard_timeline_dates(flat, incident_row)
        events = json.loads(flat["timeline_json"])
        assert events[0]["date"] is None
        assert events[0]["date_precision"] == "approximate"
        assert events[0]["event_description"] == "Attack occurred"  # description preserved

    def test_recent_date_preserved(self):
        """Event date within 90 days of source is left untouched."""
        flat = {
            "timeline_json": json.dumps([
                {"date": "2025-03-15", "date_precision": "day", "event_description": "Discovery"},
            ]),
        }
        incident_row = self._make_incident_row("2025-03-10")
        _guard_timeline_dates(flat, incident_row)
        events = json.loads(flat["timeline_json"])
        assert events[0]["date"] == "2025-03-15"
        assert events[0]["date_precision"] == "day"

    def test_exactly_90_days_preserved(self):
        """Event exactly 90 days after source is preserved (boundary: >90, not >=90)."""
        flat = {
            "timeline_json": json.dumps([
                {"date": "2025-06-08", "date_precision": "day", "event_description": "Notification"},
            ]),
        }
        incident_row = self._make_incident_row("2025-03-10")  # 90 days later = 2025-06-08
        _guard_timeline_dates(flat, incident_row)
        events = json.loads(flat["timeline_json"])
        assert events[0]["date"] == "2025-06-08"

    def test_mixed_events_only_bad_nulled(self):
        """Only the offending event is nulled; nearby events are preserved."""
        flat = {
            "timeline_json": json.dumps([
                {"date": "2025-03-10", "event_description": "Initial access"},
                {"date": "2026-04-24", "event_description": "Discovery (wrong year)"},
                {"date": "2025-03-15", "event_description": "Containment"},
            ]),
        }
        incident_row = self._make_incident_row("2025-03-10")
        _guard_timeline_dates(flat, incident_row)
        events = json.loads(flat["timeline_json"])
        assert events[0]["date"] == "2025-03-10"
        assert events[1]["date"] is None
        assert events[1]["date_precision"] == "approximate"
        assert events[2]["date"] == "2025-03-15"

    def test_no_source_date_leaves_timeline_unchanged(self):
        """If no source_published_date is available, the timeline is untouched."""
        timeline = [{"date": "2026-04-23", "event_description": "Attack"}]
        flat = {"timeline_json": json.dumps(timeline)}
        row = MagicMock()
        row.__getitem__ = lambda self, k: None
        _guard_timeline_dates(flat, row)
        events = json.loads(flat["timeline_json"])
        assert events[0]["date"] == "2026-04-23"  # unchanged

    def test_empty_timeline_no_error(self):
        """Empty timeline JSON doesn't raise."""
        flat = {"timeline_json": json.dumps([])}
        incident_row = self._make_incident_row("2025-03-10")
        _guard_timeline_dates(flat, incident_row)
        assert json.loads(flat["timeline_json"]) == []

    def test_no_timeline_no_error(self):
        """Missing timeline_json key doesn't raise."""
        flat = {}
        incident_row = self._make_incident_row("2025-03-10")
        _guard_timeline_dates(flat, flat)  # incident_row with no source_published_date
        assert "timeline_json" not in flat

    def test_uses_flat_source_date_when_no_incident_row(self):
        """Falls back to flat_data['source_published_date'] when incident_row is None."""
        flat = {
            "timeline_json": json.dumps([
                {"date": "2026-04-23", "event_description": "Attack"},
            ]),
            "source_published_date": "2025-03-10",
        }
        _guard_timeline_dates(flat, None)
        events = json.loads(flat["timeline_json"])
        assert events[0]["date"] is None
