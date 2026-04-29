"""
Tests for every schema→mapper→flat field mapping identified in the audit.

Each test passes a realistic LLM JSON payload (using schema field names) through
json_to_cti_enrichment() and/or _flatten_enrichment_for_db() and asserts the
correct flat-table value comes out the other end.
"""

import json
import pytest
from src.edu_cti.pipeline.phase2.extraction.json_to_schema_mapper import json_to_cti_enrichment
from src.edu_cti.pipeline.phase2.storage.db import _flatten_enrichment_for_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _base_payload(**overrides):
    """Minimal valid LLM JSON payload with required fields."""
    base = {
        "is_edu_cyber_incident": True,
        "education_relevance_reasoning": "A university was attacked.",
        "institution_name": "Test University",
        "institution_type": "university",
        "attack_category": "ransomware_encryption",
        "attack_vector": "phishing_email",
    }
    base.update(overrides)
    return base


def _enrich_and_flatten(payload):
    enrichment = json_to_cti_enrichment(payload, primary_url="https://example.com/article")
    flat = _flatten_enrichment_for_db(enrichment, payload)
    return enrichment, flat


# ===========================================================================
# 1. OPERATIONAL IMPACT
# ===========================================================================

class TestOperationalImpact:

    def test_teaching_disrupted_from_classes_cancelled(self):
        """teaching_disrupted derives from operational_impacts: classes_cancelled."""
        _, flat = _enrich_and_flatten(_base_payload(
            operational_impacts=["classes_cancelled"]
        ))
        assert flat["teaching_disrupted"] == 1

    def test_teaching_disrupted_from_classes_moved_online(self):
        _, flat = _enrich_and_flatten(_base_payload(
            operational_impacts=["classes_moved_online"]
        ))
        assert flat["teaching_disrupted"] == 1

    def test_teaching_disrupted_from_semester_extended(self):
        _, flat = _enrich_and_flatten(_base_payload(
            operational_impacts=["semester_extended"]
        ))
        assert flat["teaching_disrupted"] == 1

    def test_teaching_not_disrupted_when_no_teaching_ops(self):
        _, flat = _enrich_and_flatten(_base_payload(
            operational_impacts=["payroll_delayed"]
        ))
        assert not flat["teaching_disrupted"]

    def test_research_disrupted_from_research_halted(self):
        _, flat = _enrich_and_flatten(_base_payload(
            operational_impacts=["research_halted"]
        ))
        assert flat["research_disrupted"] == 1

    def test_research_disrupted_from_research_data_lost(self):
        _, flat = _enrich_and_flatten(_base_payload(
            operational_impacts=["research_data_lost"]
        ))
        assert flat["research_disrupted"] == 1

    def test_admissions_disrupted_from_admissions_suspended(self):
        _, flat = _enrich_and_flatten(_base_payload(
            operational_impacts=["admissions_suspended"]
        ))
        assert flat["admissions_disrupted"] == 1

    def test_payroll_disrupted_from_payroll_delayed(self):
        _, flat = _enrich_and_flatten(_base_payload(
            operational_impacts=["payroll_delayed"]
        ))
        assert flat["payroll_disrupted"] == 1

    def test_enrollment_disrupted_from_registration_suspended(self):
        _, flat = _enrich_and_flatten(_base_payload(
            operational_impacts=["registration_suspended"]
        ))
        assert flat["enrollment_disrupted"] == 1

    def test_classes_cancelled(self):
        _, flat = _enrich_and_flatten(_base_payload(
            operational_impacts=["classes_cancelled"]
        ))
        assert flat["classes_cancelled"] == 1

    def test_exams_postponed(self):
        _, flat = _enrich_and_flatten(_base_payload(
            operational_impacts=["exams_postponed"]
        ))
        assert flat["exams_postponed"] == 1

    def test_downtime_days_from_direct_field(self):
        _, flat = _enrich_and_flatten(_base_payload(downtime_days=14))
        assert flat["downtime_days"] == 14

    def test_downtime_days_derived_from_outage_hours(self):
        _, flat = _enrich_and_flatten(_base_payload(outage_duration_hours=48))
        assert flat["downtime_days"] == 2.0

    def test_multiple_ops_all_derived(self):
        _, flat = _enrich_and_flatten(_base_payload(
            operational_impacts=["classes_cancelled", "research_halted", "admissions_suspended", "payroll_delayed"]
        ))
        assert flat["teaching_disrupted"] == 1
        assert flat["research_disrupted"] == 1
        assert flat["admissions_disrupted"] == 1
        assert flat["payroll_disrupted"] == 1


# ===========================================================================
# 2. RECOVERY FIELDS
# ===========================================================================

class TestRecoveryFields:

    def test_recovery_duration_days_schema_field(self):
        """Schema field recovery_duration_days → flat recovery_timeframe_days."""
        _, flat = _enrich_and_flatten(_base_payload(recovery_duration_days=21))
        assert flat["recovery_timeframe_days"] == 21

    def test_recovery_duration_days_wins_over_mttr(self):
        _, flat = _enrich_and_flatten(_base_payload(recovery_duration_days=21, mttr_hours=240))
        assert flat["recovery_timeframe_days"] == 21

    def test_mttr_hours_fallback(self):
        _, flat = _enrich_and_flatten(_base_payload(mttr_hours=240))
        assert flat["recovery_timeframe_days"] == 10.0

    def test_from_backup_from_recovery_method_backup_restore(self):
        _, flat = _enrich_and_flatten(_base_payload(recovery_method="backup_restore"))
        assert flat["from_backup"] == 1

    def test_from_backup_from_partial_backup(self):
        _, flat = _enrich_and_flatten(_base_payload(recovery_method="partial_backup_partial_rebuild"))
        assert flat["from_backup"] == 1

    def test_from_backup_false_for_clean_rebuild(self):
        _, flat = _enrich_and_flatten(_base_payload(recovery_method="clean_rebuild"))
        assert not flat["from_backup"]

    def test_ir_firm_from_ir_firm_engaged(self):
        """Schema uses ir_firm_engaged, not incident_response_firm."""
        _, flat = _enrich_and_flatten(_base_payload(ir_firm_engaged="CrowdStrike"))
        assert flat["incident_response_firm"] == "CrowdStrike"

    def test_forensics_firm_from_forensics_firm_engaged(self):
        """Schema uses forensics_firm_engaged, not forensics_firm."""
        _, flat = _enrich_and_flatten(_base_payload(forensics_firm_engaged="Mandiant"))
        assert flat["forensics_firm"] == "Mandiant"

    def test_mfa_implemented_from_security_improvements_array(self):
        """mfa_implemented is NOT a standalone field — it's in security_improvements array."""
        _, flat = _enrich_and_flatten(_base_payload(
            security_improvements=["mfa_implemented", "network_segmentation"]
        ))
        assert flat["mfa_implemented"] == 1

    def test_mfa_expanded_also_sets_mfa_implemented(self):
        _, flat = _enrich_and_flatten(_base_payload(
            security_improvements=["mfa_expanded"]
        ))
        assert flat["mfa_implemented"] == 1

    def test_mfa_not_set_when_absent_from_improvements(self):
        _, flat = _enrich_and_flatten(_base_payload(
            security_improvements=["network_segmentation"],
            recovery_method="clean_rebuild",
        ))
        assert not flat["mfa_implemented"]


# ===========================================================================
# 3. REGULATORY FIELDS
# ===========================================================================

class TestRegulatoryFields:

    def test_gdpr_from_applicable_regulations(self):
        """Schema: applicable_regulations array → gdpr_breach boolean."""
        _, flat = _enrich_and_flatten(_base_payload(
            applicable_regulations=["GDPR", "state_breach_notification"]
        ))
        assert flat["gdpr_breach"] == 1

    def test_hipaa_from_applicable_regulations(self):
        _, flat = _enrich_and_flatten(_base_payload(
            applicable_regulations=["HIPAA"]
        ))
        assert flat["hipaa_breach"] == 1

    def test_ferpa_from_applicable_regulations(self):
        _, flat = _enrich_and_flatten(_base_payload(
            applicable_regulations=["FERPA"]
        ))
        assert flat["ferpa_breach"] == 1

    def test_no_false_positives_when_regulation_absent(self):
        _, flat = _enrich_and_flatten(_base_payload(
            applicable_regulations=["PCI_DSS"]
        ))
        assert not flat["gdpr_breach"]
        assert not flat["hipaa_breach"]
        assert not flat["ferpa_breach"]

    def test_fine_amount_from_fine_amount_usd(self):
        """Schema uses fine_amount_usd, flat column is fine_amount."""
        _, flat = _enrich_and_flatten(_base_payload(
            fine_imposed=True, fine_amount_usd=500000
        ))
        assert flat["fine_amount"] == 500000

    def test_notifications_sent_from_notification_sent_singular(self):
        """Schema uses notification_sent (singular), flat has notifications_sent (plural)."""
        _, flat = _enrich_and_flatten(_base_payload(notification_sent=True))
        assert flat["notifications_sent"] == 1

    def test_class_action_from_class_action_filed(self):
        """Schema uses class_action_filed, flat has class_action."""
        _, flat = _enrich_and_flatten(_base_payload(class_action_filed=True))
        assert flat["class_action"] == 1

    def test_lawsuits_filed_direct(self):
        _, flat = _enrich_and_flatten(_base_payload(lawsuits_filed=True))
        assert flat["lawsuits_filed"] == 1

    def test_breach_notification_required(self):
        _, flat = _enrich_and_flatten(_base_payload(breach_notification_required=True))
        assert flat["breach_notification_required"] == 1


# ===========================================================================
# 4. FINANCIAL FIELDS
# ===========================================================================

class TestFinancialFields:

    def test_recovery_cost_usd_maps_to_recovery_costs_min(self):
        """Schema: recovery_cost_usd (single) → flat recovery_costs_min."""
        _, flat = _enrich_and_flatten(_base_payload(
            was_ransom_demanded=True, recovery_cost_usd=2000000
        ))
        assert flat["recovery_costs_min"] == 2000000

    def test_legal_cost_usd_maps_to_legal_costs(self):
        _, flat = _enrich_and_flatten(_base_payload(
            was_ransom_demanded=True, legal_cost_usd=150000
        ))
        assert flat["legal_costs"] == 150000

    def test_insurance_payout_usd_maps_to_insurance_claim_amount(self):
        _, flat = _enrich_and_flatten(_base_payload(
            insurance_claim=True, insurance_payout_usd=1000000
        ))
        assert flat["insurance_claim_amount"] == 1000000

    def test_ransom_paid_amount_direct(self):
        _, flat = _enrich_and_flatten(_base_payload(
            was_ransom_demanded=True, ransom_paid=True, ransom_paid_amount=400000
        ))
        assert flat["ransom_paid_amount"] == 400000


# ===========================================================================
# 5. SYSTEM IMPACT FIELD NAMES
# ===========================================================================

class TestSystemImpactFieldNames:

    def test_email_system_affected_from_email_system_enum(self):
        """Schema: systems_affected: ['email_system'] — not 'email'."""
        _, flat = _enrich_and_flatten(_base_payload(
            systems_affected=["email_system", "active_directory"]
        ))
        assert flat["email_system_affected"] == 1

    def test_student_portal_affected_from_student_portal_enum(self):
        """Schema: systems_affected: ['student_portal'] — not 'portal_student_staff'."""
        _, flat = _enrich_and_flatten(_base_payload(
            systems_affected=["student_portal"]
        ))
        assert flat["student_portal_affected"] == 1

    def test_student_portal_from_sis_student_information(self):
        _, flat = _enrich_and_flatten(_base_payload(
            systems_affected=["sis_student_information"]
        ))
        assert flat["student_portal_affected"] == 1

    def test_research_systems_from_research_computing_hpc(self):
        """Schema: systems_affected: ['research_computing_hpc'] — not 'research_hpc'."""
        _, flat = _enrich_and_flatten(_base_payload(
            systems_affected=["research_computing_hpc"]
        ))
        assert flat["research_systems_affected"] == 1

    def test_hospital_systems_from_hospital_systems_enum(self):
        _, flat = _enrich_and_flatten(_base_payload(
            systems_affected=["hospital_systems"]
        ))
        assert flat["hospital_systems_affected"] == 1

    def test_network_compromised_from_core_network(self):
        _, flat = _enrich_and_flatten(_base_payload(
            systems_affected=["core_network"]
        ))
        assert flat["network_compromised"] == 1


# ===========================================================================
# 6. EXISTING FIELDS THAT MUST STILL WORK (REGRESSION)
# ===========================================================================

class TestRegressions:

    def test_ransomware_family_direct(self):
        _, flat = _enrich_and_flatten(_base_payload(ransomware_family="lockbit"))
        assert flat["ransomware_family"] == "lockbit"

    def test_attack_category_direct(self):
        _, flat = _enrich_and_flatten(_base_payload(attack_category="ransomware_encryption"))
        assert flat["attack_category"] == "ransomware_encryption"

    def test_attack_vector_direct(self):
        _, flat = _enrich_and_flatten(_base_payload(attack_vector="phishing_email"))
        assert flat["attack_vector"] == "phishing_email"

    def test_threat_actor_name(self):
        _, flat = _enrich_and_flatten(_base_payload(threat_actor_name="LockBit"))
        assert flat["threat_actor_name"] == "LockBit"

    def test_threat_actor_category(self):
        _, flat = _enrich_and_flatten(_base_payload(threat_actor_category="ransomware_gang"))
        assert flat["threat_actor_category"] == "ransomware_gang"

    def test_threat_actor_motivation(self):
        _, flat = _enrich_and_flatten(_base_payload(threat_actor_motivation="financial_gain"))
        assert flat["threat_actor_motivation"] == "financial_gain"

    def test_incident_severity(self):
        _, flat = _enrich_and_flatten(_base_payload(incident_severity="critical"))
        assert flat["incident_severity"] == "critical"

    def test_records_affected_exact(self):
        _, flat = _enrich_and_flatten(_base_payload(records_affected_exact=45000))
        assert flat["records_affected_exact"] == 45000

    def test_data_categories_stored_as_json(self):
        _, flat = _enrich_and_flatten(_base_payload(data_categories=["student_pii", "employee_ssn"]))
        parsed = json.loads(flat["data_categories"])
        assert "student_pii" in parsed

    def test_ransom_amount_direct(self):
        _, flat = _enrich_and_flatten(_base_payload(was_ransom_demanded=True, ransom_amount=5000000))
        assert flat["ransom_amount"] == 5000000

    def test_disclosure_delay_days(self):
        _, flat = _enrich_and_flatten(_base_payload(disclosure_delay_days=47))
        assert flat["disclosure_delay_days"] == 47

    def test_transparency_level(self):
        _, flat = _enrich_and_flatten(_base_payload(transparency_level="good"))
        assert flat["transparency_level"] == "good"

    def test_students_affected(self):
        _, flat = _enrich_and_flatten(_base_payload(students_affected=12000))
        assert flat["students_affected"] == 12000

    def test_institution_type(self):
        _, flat = _enrich_and_flatten(_base_payload(institution_type="school_district"))
        assert flat["institution_type"] == "school_district"

    def test_data_breach_inferred_from_category(self):
        """data_breached should be inferred when attack_category is a breach type."""
        _, flat = _enrich_and_flatten(_base_payload(attack_category="data_breach_external"))
        assert flat["data_breached"] == 1


# ===========================================================================
# 7. EDGE CASES
# ===========================================================================

class TestEdgeCases:

    def test_empty_security_improvements_no_mfa(self):
        _, flat = _enrich_and_flatten(_base_payload(security_improvements=[]))
        assert not flat["mfa_implemented"]

    def test_applicable_regulations_empty_no_breach_flags(self):
        _, flat = _enrich_and_flatten(_base_payload(applicable_regulations=[]))
        assert not flat["gdpr_breach"]
        assert not flat["hipaa_breach"]
        assert not flat["ferpa_breach"]

    def test_operational_impacts_empty_no_disruption(self):
        _, flat = _enrich_and_flatten(_base_payload(operational_impacts=[]))
        assert not flat["teaching_disrupted"]
        assert not flat["research_disrupted"]
        assert not flat["classes_cancelled"]

    def test_systems_affected_irrelevant_value_does_not_match_email(self):
        """A value unrelated to email must not set email_system_affected."""
        _, flat = _enrich_and_flatten(_base_payload(
            systems_affected=["payroll_system"]
        ))
        assert not flat["email_system_affected"]
        assert not flat["network_compromised"]
        assert not flat["hospital_systems_affected"]

    def test_fine_amount_legacy_name_still_works(self):
        """fine_amount (not usd suffix) should still be accepted as legacy alias."""
        _, flat = _enrich_and_flatten(_base_payload(fine_imposed=True, fine_amount=250000))
        assert flat["fine_amount"] == 250000

    def test_recovery_costs_min_direct_still_works(self):
        """recovery_costs_min set directly should still work."""
        _, flat = _enrich_and_flatten(_base_payload(
            was_ransom_demanded=True, recovery_costs_min=500000
        ))
        assert flat["recovery_costs_min"] == 500000

    def test_full_payload_all_fields_populated(self):
        """Integration: realistic full payload — all critical fields populated."""
        payload = _base_payload(
            ransomware_family="lockbit",
            threat_actor_name="LockBit",
            threat_actor_category="ransomware_gang",
            threat_actor_motivation="financial_gain",
            incident_severity="critical",
            was_ransom_demanded=True,
            ransom_amount=3000000,
            ransom_paid=False,
            records_affected_exact=80000,
            data_categories=["student_pii", "employee_payroll"],
            systems_affected=["email_system", "student_portal", "backup_systems", "file_servers"],
            operational_impacts=["classes_cancelled", "research_halted", "payroll_delayed"],
            applicable_regulations=["FERPA", "HIPAA"],
            breach_notification_required=True,
            notification_sent=True,
            recovery_duration_days=28,
            ir_firm_engaged="CrowdStrike",
            forensics_firm_engaged="Mandiant",
            recovery_method="backup_restore",
            security_improvements=["mfa_implemented", "network_segmentation", "air_gapped_backups"],
            recovery_cost_usd=1500000,
            legal_cost_usd=200000,
            disclosure_delay_days=14,
            transparency_level="good",
        )
        _, flat = _enrich_and_flatten(payload)

        assert flat["ransomware_family"] == "lockbit"
        assert flat["incident_severity"] == "critical"
        assert flat["teaching_disrupted"] == 1
        assert flat["research_disrupted"] == 1
        assert flat["payroll_disrupted"] == 1
        assert flat["classes_cancelled"] == 1
        assert flat["ferpa_breach"] == 1
        assert flat["hipaa_breach"] == 1
        assert not flat["gdpr_breach"]
        assert flat["notifications_sent"] == 1
        assert flat["recovery_timeframe_days"] == 28
        assert flat["incident_response_firm"] == "CrowdStrike"
        assert flat["forensics_firm"] == "Mandiant"
        assert flat["from_backup"] == 1
        assert flat["mfa_implemented"] == 1
        assert flat["email_system_affected"] == 1
        assert flat["student_portal_affected"] == 1
        assert flat["recovery_costs_min"] == 1500000
        assert flat["legal_costs"] == 200000
        assert flat["disclosure_delay_days"] == 14
