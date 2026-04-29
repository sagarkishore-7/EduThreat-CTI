"""
Full field audit: every extraction_schema.py field → mapper → flat dict → SQLite DB.

Two things this file does:
  1. Defines FULL_LLM_JSON — a synthetic LLM response that populates every
     extraction schema field with a realistic value.
  2. Asserts that every DB column in incident_enrichments_flat receives the
     correct value when that payload is passed through the pipeline.

Run with:
    python -m pytest tests/test_full_field_audit.py -v

To see the audit table (which schema fields have no DB column):
    python -m pytest tests/test_full_field_audit.py -v -s
"""

import json
import sqlite3
import pytest
from datetime import datetime

from src.edu_cti.pipeline.phase2.extraction.extraction_schema import EXTRACTION_SCHEMA
from src.edu_cti.pipeline.phase2.extraction.json_to_schema_mapper import json_to_cti_enrichment
from src.edu_cti.pipeline.phase2.storage.db import _flatten_enrichment_for_db, init_incident_enrichments_table
from src.edu_cti.pipeline.phase2.utils.post_processing import apply_post_processing


# ─────────────────────────────────────────────────────────────────────────────
# Complete LLM JSON payload — every extraction_schema.py field has a value.
# ─────────────────────────────────────────────────────────────────────────────

FULL_LLM_JSON = {
    # ── Education relevance ──────────────────────────────────────────────────
    "is_edu_cyber_incident": True,
    "education_relevance_reasoning": (
        "The article reports a confirmed ransomware attack on Westbrook University, "
        "a degree-granting higher education institution in Texas, USA."
    ),

    # ── Institution ─────────────────────────────────────────────────────────
    "institution_name": "Westbrook University",
    "institution_aliases": ["WBU", "Westbrook"],
    "institution_type": "university",
    "country": "United States",
    "region": "Texas",
    "city": "Houston",

    # ── Classification ───────────────────────────────────────────────────────
    "incident_status": "resolved",

    # ── Dates ────────────────────────────────────────────────────────────────
    "incident_date": "2024-03-12",
    "incident_date_precision": "exact",
    "discovery_date": "2024-03-13",
    "dwell_time_days": 1,

    # ── Timeline ─────────────────────────────────────────────────────────────
    "timeline": [
        {
            "date": "2024-03-12",
            "date_precision": "day",
            "event_description": "LockBit ransomware encrypted administrative servers causing campus-wide outage.",
            "event_type": "encryption_started",
            "actor_attribution": "LockBit",
            "indicators": ["lockbit3.exe", "192.0.2.47"],
        },
        {
            "date": "2024-03-13",
            "date_precision": "day",
            "event_description": "IT staff discovered encrypted files and isolated affected systems.",
            "event_type": "discovery",
            "actor_attribution": None,
            "indicators": [],
        },
        {
            "date": "2024-03-14",
            "date_precision": "day",
            "event_description": "University activated incident response plan and engaged CrowdStrike.",
            "event_type": "containment",
            "actor_attribution": None,
            "indicators": [],
        },
        {
            "date": "2024-03-20",
            "date_precision": "day",
            "event_description": "University issued public statement and notified affected individuals.",
            "event_type": "disclosure",
            "actor_attribution": None,
            "indicators": [],
        },
    ],

    # ── Attack classification ────────────────────────────────────────────────
    "attack_category": "ransomware_double_extortion",
    "secondary_attack_categories": ["data_breach_external"],
    "attack_vector": "phishing_email",
    "initial_access_description": (
        "Attacker sent spear-phishing email to an administrator; "
        "clicking the link installed a dropper that deployed LockBit 3.0."
    ),

    # ── Kill chain ───────────────────────────────────────────────────────────
    "attack_chain": [
        "initial_access",
        "execution",
        "persistence",
        "privilege_escalation",
        "lateral_movement",
        "exfiltration",
        "impact",
    ],

    # ── Vulnerabilities ──────────────────────────────────────────────────────
    "vulnerabilities_exploited": [
        {
            "cve_id": "CVE-2023-20269",
            "vulnerability_name": "Cisco ASA VPN Authentication Bypass",
            "vulnerability_type": "authentication_bypass",
            "affected_product": "Cisco Adaptive Security Appliance",
            "cvss_score": 9.1,
        }
    ],

    # ── MITRE ATT&CK ─────────────────────────────────────────────────────────
    "mitre_attack_techniques": [
        {
            "technique_id": "T1566.001",
            "technique_name": "Phishing: Spearphishing Attachment",
            "tactic": "initial_access",
            "description": "Attacker sent targeted phishing email with malicious link to VPN admin.",
            "sub_techniques": ["T1566"],
        },
        {
            "technique_id": "T1486",
            "technique_name": "Data Encrypted for Impact",
            "tactic": "impact",
            "description": "LockBit 3.0 encrypted files across administrative servers and backup systems.",
            "sub_techniques": [],
        },
        {
            "technique_id": "T1041",
            "technique_name": "Exfiltration Over C2 Channel",
            "tactic": "exfiltration",
            "description": "Approximately 200 GB of student and staff records exfiltrated before encryption.",
            "sub_techniques": [],
        },
    ],

    # ── Threat actor ─────────────────────────────────────────────────────────
    "threat_actor_claimed": True,
    "threat_actor_name": "LockBit",
    "threat_actor_aliases": ["LockBit 3.0", "LockBit Black"],
    "threat_actor_category": "ransomware_gang",
    "threat_actor_motivation": "financial_gain",
    "threat_actor_origin_country": "Russia",
    "threat_actor_claim_url": "http://lockbit3uzfmki6x4cjkgclnkgqkgqe4ttyqbfvxmfqbvmwqmfq.onion/westbrook",

    # ── Ransomware / malware ─────────────────────────────────────────────────
    "ransomware_family": "lockbit_3",
    "malware_families": ["lockbit", "cobalt_strike"],
    "attacker_tools": ["cobalt_strike", "mimikatz", "rclone"],
    "attacker_communication_channel": "tor_leak_site",

    # ── Ransom details ───────────────────────────────────────────────────────
    "was_ransom_demanded": True,
    "ransom_amount": 3500000,
    "ransom_amount_exact": 3500000,
    "ransom_currency": "USD",
    "ransom_cryptocurrency": "bitcoin",
    "ransom_paid": False,
    "ransom_paid_amount": None,
    "ransom_negotiated": True,
    "ransom_deadline_given": True,
    "ransom_deadline_days": 7,
    "decryptor_received": False,
    "decryptor_worked": None,

    # ── IOCs ─────────────────────────────────────────────────────────────────
    "iocs": {
        "ip_addresses": ["192.0.2.47", "198.51.100.12"],
        "domains": ["lockbit-cdn.onion", "exfil-drop.io"],
        "urls": ["http://lockbit-cdn.onion/drop"],
        "file_hashes": [
            {"hash_type": "sha256", "hash_value": "abc123def456abc123def456abc123def456abc123def456abc123def456abc1"},
        ],
        "email_addresses": ["phish@evil.io"],
        "cryptocurrency_wallets": ["1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"],
        "file_names": ["lockbit3.exe", "ransom_note.txt"],
        "registry_keys": ["HKLM\\Software\\LockBit\\Config"],
    },

    # ── Data impact ──────────────────────────────────────────────────────────
    "data_breached": True,
    "data_exfiltrated": True,
    "data_encrypted": True,
    "data_destroyed": False,
    "data_published": True,
    "data_sold": False,
    "data_categories": [
        "student_pii",
        "student_ssn",
        "student_grades",
        "student_financial_aid",
        "employee_pii",
        "employee_ssn",
        "employee_payroll",
        "medical_records",
        "health_insurance",
        "usernames_passwords",
    ],
    "records_affected_exact": 44823,
    "pii_records_leaked": 44823,
    "data_volume_gb": 203.5,

    # ── System impact ────────────────────────────────────────────────────────
    "infrastructure_type": "hybrid",
    "cloud_provider": "azure",
    "systems_affected": [
        "email_system",
        "active_directory",
        "student_portal",
        "sis_student_information",
        "backup_systems",
        "file_servers",
        "core_network",
        "payroll_system",
        "erp_system",
        "research_computing_hpc",
        "ehr_emr",
        "hospital_systems",
    ],
    "critical_systems_affected": True,
    "network_compromised": True,
    "domain_admin_compromised": True,
    "backup_compromised": True,
    "encryption_extent": "full_encryption",
    "systems_encrypted_count": 847,
    "servers_affected_count": 120,
    "endpoints_affected_count": 2300,

    # ── Operational impact ───────────────────────────────────────────────────
    "outage_start_date": "2024-03-12",
    "outage_end_date": "2024-03-26",
    "outage_duration_hours": 336,
    "downtime_days": 14,
    "partial_service_days": 7,
    "operational_impacts": [
        "classes_cancelled",
        "exams_postponed",
        "research_halted",
        "payroll_delayed",
        "admissions_suspended",
        "registration_suspended",
        "email_unavailable",
        "student_portal_down",
        "clinical_operations_disrupted",
        "graduation_delayed",
    ],
    "teaching_impacted": True,
    "research_impacted": True,

    # ── User impact ──────────────────────────────────────────────────────────
    "students_affected": 34000,
    "staff_affected": 4200,
    "faculty_affected": 2100,
    "alumni_affected": 8500,
    "applicants_affected": 1200,
    "patients_affected": 3500,
    "donors_affected": 950,
    "total_individuals_affected": 44823,
    "users_affected_exact": 44823,

    # ── Financial impact ──────────────────────────────────────────────────────
    "estimated_total_cost_usd": 8500000,
    "ransom_cost_usd": 0,
    "recovery_cost_usd": 4200000,
    "legal_cost_usd": 750000,
    "notification_cost_usd": 320000,
    "credit_monitoring_cost_usd": 180000,
    "lost_revenue_usd": 1050000,
    "insurance_claim": True,
    "insurance_payout_usd": 2000000,
    "business_impact": "severe",

    # ── Regulatory impact ─────────────────────────────────────────────────────
    "breach_notification_required": True,
    "notification_sent": True,
    "notification_sent_date": "2024-04-01",
    "dpa_notified": True,
    "regulators_notified": ["HHS Office for Civil Rights", "Texas AG", "ICO"],
    "investigation_opened": True,
    "investigating_agencies": ["FBI", "CISA"],
    "fine_imposed": False,
    "fine_amount_usd": None,
    "lawsuits_filed": True,
    "lawsuit_count": 3,
    "class_action_filed": True,
    "settlement_amount_usd": None,

    # ── Response & recovery ───────────────────────────────────────────────────
    "incident_response_activated": True,
    "ir_firm_engaged": "CrowdStrike",
    "forensics_firm_engaged": "Mandiant",
    "legal_counsel_engaged": "Baker McKenzie",
    "pr_firm_engaged": "Edelman",
    "law_enforcement_involved": True,
    "law_enforcement_agencies": ["FBI", "CISA"],
    "recovery_method": "backup_restore",
    "recovery_started_date": "2024-03-14",
    "recovery_completed_date": "2024-03-26",
    "recovery_duration_days": 14,
    "mttd_hours": 28,
    "mttr_hours": 336,
    "security_improvements": [
        "mfa_implemented",
        "network_segmentation",
        "air_gapped_backups",
        "endpoint_detection_response",
        "siem_implemented",
        "privileged_access_management",
    ],
    "backup_status": "available_and_used",
    "backup_age_days": 1,
    # ── Transparency & disclosure ─────────────────────────────────────────────
    "public_disclosure": True,
    "public_disclosure_date": "2024-03-20",
    "disclosure_delay_days": 8,
    "disclosure_source": "institution_statement",
    "transparency_level": "good",
    "official_statement_url": "https://westbrook.edu/security-notice",
    "incident_report_url": "https://ocrportal.hhs.gov/ocr/breach/0001",
    "updates_provided_count": 4,

    # ── Cross-incident analysis ───────────────────────────────────────────────
    "attack_campaign_name": "LockBit Education Wave Q1 2024",
    "related_incidents": ["comparitech_2024_001", "bleepingcomputer_lb_edu"],
    "common_vulnerability_exploited": "CVE-2023-20269",
    "sector_targeting_pattern": "targeted_education_only",

    # ── Research impact ───────────────────────────────────────────────────────
    "research_impacted": True,
    "research_projects_affected": 12,
    "research_data_compromised": True,
    "research_area": "biomedical",
    "publications_delayed": True,
    "grants_affected": True,

    # ── Notes ─────────────────────────────────────────────────────────────────
    "enriched_summary": (
        "Westbrook University in Houston, Texas suffered a ransomware_double_extortion attack "
        "by LockBit 3.0 on 12 March 2024. Attackers used a phishing email to gain initial access, "
        "deployed LockBit 3.0 ransomware, and exfiltrated 203 GB of data including SSNs and medical "
        "records of 44,823 individuals. A $3.5M ransom was demanded but not paid. Systems were restored "
        "from backup within 14 days. CrowdStrike and Mandiant were engaged for incident response."
    ),
    "extraction_notes": "Article published 8 days after incident; ransom amount explicitly stated as $3.5M USD.",
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _run_pipeline(payload=None):
    """Run full mapper + flatten pipeline. Returns (enrichment, flat)."""
    p = payload if payload is not None else FULL_LLM_JSON
    enrichment = json_to_cti_enrichment(p, primary_url="https://bleepingcomputer.com/news/security/westbrook-ransomware")
    flat = _flatten_enrichment_for_db(enrichment, p)
    apply_post_processing(flat, incident_row=None, summary=enrichment.enriched_summary)
    return enrichment, flat


def _write_and_read_db(flat: dict) -> dict:
    """Write flat dict to in-memory SQLite and read it back as a row dict."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_incident_enrichments_table(conn)

    flat["incident_id"] = "test_westbrook_001"
    flat["created_at"] = datetime.utcnow().isoformat()
    flat["updated_at"] = datetime.utcnow().isoformat()
    flat["enriched_at"] = datetime.utcnow().isoformat()

    cols = [k for k, v in flat.items() if v is not None]
    placeholders = ", ".join("?" for _ in cols)
    col_names = ", ".join(cols)
    values = [flat[c] for c in cols]

    conn.execute(
        f"INSERT OR REPLACE INTO incident_enrichments_flat ({col_names}) VALUES ({placeholders})",
        values,
    )
    conn.commit()

    row = conn.execute(
        "SELECT * FROM incident_enrichments_flat WHERE incident_id = ?",
        ("test_westbrook_001",),
    ).fetchone()
    conn.close()
    return dict(row) if row else {}


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestFullFieldAudit:
    """Assert every DB column receives the expected value from FULL_LLM_JSON."""

    @pytest.fixture(scope="class")
    def flat(self):
        _, f = _run_pipeline()
        return f

    @pytest.fixture(scope="class")
    def db_row(self, flat):
        return _write_and_read_db(dict(flat))

    # ── Education & institution ────────────────────────────────────────────
    def test_is_education_related(self, flat):
        assert flat["is_education_related"] == 1

    def test_institution_type(self, flat):
        assert flat["institution_type"] == "university"

    def test_country_normalized(self, flat):
        assert flat["country"] == "United States"

    def test_country_code(self, flat):
        assert flat["country_code"] == "US"

    def test_region(self, flat):
        assert flat["region"] == "Texas"

    def test_city(self, flat):
        assert flat["city"] == "Houston"

    # ── Classification ────────────────────────────────────────────────────
    def test_incident_date_precision(self, flat):
        assert flat["incident_date_precision"] == "exact"

    # ── Attack details ────────────────────────────────────────────────────
    def test_attack_category(self, flat):
        assert flat["attack_category"] == "ransomware_double_extortion"

    def test_attack_vector(self, flat):
        assert flat["attack_vector"] == "phishing_email"

    def test_initial_access_description(self, flat):
        assert flat["initial_access_description"] is not None
        assert "phishing" in flat["initial_access_description"].lower()

    def test_ransomware_family(self, flat):
        # Schema field is "ransomware_family"; mapper also checks "ransomware_family_or_group"
        assert flat["ransomware_family"] == "lockbit_3"

    def test_secondary_attack_categories(self, flat):
        parsed = json.loads(flat["secondary_attack_categories"])
        assert "data_breach_external" in parsed

    def test_attack_chain(self, flat):
        chain = json.loads(flat["attack_chain"])
        assert "initial_access" in chain
        assert "exfiltration" in chain
        assert "impact" in chain

    def test_encryption_extent(self, flat):
        assert flat["encryption_extent"] == "full_encryption"

    # ── Threat actor ──────────────────────────────────────────────────────
    def test_threat_actor_name(self, flat):
        assert flat["threat_actor_name"] == "LockBit"

    def test_threat_actor_category(self, flat):
        assert flat["threat_actor_category"] == "ransomware_gang"

    def test_threat_actor_motivation(self, flat):
        assert flat["threat_actor_motivation"] == "financial_gain"

    def test_threat_actor_origin_country(self, flat):
        assert flat["threat_actor_origin_country"] == "Russia"

    def test_threat_actor_claim_url(self, flat):
        assert flat["threat_actor_claim_url"] is not None
        assert "lockbit" in flat["threat_actor_claim_url"]

    def test_threat_actor_aliases(self, flat):
        aliases = json.loads(flat["threat_actor_aliases"])
        assert "LockBit 3.0" in aliases

    # ── Ransom ────────────────────────────────────────────────────────────
    def test_was_ransom_demanded(self, flat):
        assert flat["was_ransom_demanded"] == 1

    def test_ransom_amount(self, flat):
        assert flat["ransom_amount"] == 3500000

    def test_ransom_currency(self, flat):
        assert flat["ransom_currency"] == "USD"

    def test_ransom_paid_false(self, flat):
        assert flat["ransom_paid"] == 0

    # ── Data impact ───────────────────────────────────────────────────────
    def test_data_breached(self, flat):
        assert flat["data_breached"] == 1

    def test_data_exfiltrated(self, flat):
        assert flat["data_exfiltrated"] == 1

    def test_records_affected_exact(self, flat):
        assert flat["records_affected_exact"] == 44823

    def test_pii_records_leaked(self, flat):
        assert flat["pii_records_leaked"] == 44823

    def test_data_categories(self, flat):
        cats = json.loads(flat["data_categories"])
        assert "student_pii" in cats
        assert "student_ssn" in cats
        assert "employee_payroll" in cats
        assert "medical_records" in cats

    def test_data_volume_gb(self, flat):
        assert flat["data_volume_gb"] == 203.5

    # ── System impact ─────────────────────────────────────────────────────
    def test_systems_affected_codes_json(self, flat):
        systems = json.loads(flat["systems_affected_codes"])
        assert len(systems) > 0

    def test_critical_systems_affected(self, flat):
        assert flat["critical_systems_affected"] == 1

    def test_network_compromised(self, flat):
        assert flat["network_compromised"] == 1

    def test_email_system_affected(self, flat):
        assert flat["email_system_affected"] == 1

    def test_student_portal_affected(self, flat):
        assert flat["student_portal_affected"] == 1

    def test_research_systems_affected(self, flat):
        assert flat["research_systems_affected"] == 1

    def test_hospital_systems_affected(self, flat):
        assert flat["hospital_systems_affected"] == 1

    def test_cloud_services_affected_from_cloud_provider(self, flat):
        # cloud_provider=azure → cloud_services_affected should be True
        assert flat["cloud_services_affected"] == 1

    def test_cloud_provider(self, flat):
        assert flat["cloud_provider"] == "azure"

    # ── Operational impact ────────────────────────────────────────────────
    def test_teaching_disrupted(self, flat):
        # classes_cancelled in operational_impacts → teaching_disrupted
        assert flat["teaching_disrupted"] == 1

    def test_research_disrupted(self, flat):
        # research_halted in operational_impacts → research_disrupted
        assert flat["research_disrupted"] == 1

    def test_admissions_disrupted(self, flat):
        assert flat["admissions_disrupted"] == 1

    def test_enrollment_disrupted(self, flat):
        # registration_suspended in operational_impacts → enrollment_disrupted
        assert flat["enrollment_disrupted"] == 1

    def test_payroll_disrupted(self, flat):
        assert flat["payroll_disrupted"] == 1

    def test_classes_cancelled(self, flat):
        assert flat["classes_cancelled"] == 1

    def test_exams_postponed(self, flat):
        assert flat["exams_postponed"] == 1

    def test_downtime_days(self, flat):
        assert flat["downtime_days"] == 14

    def test_outage_duration_hours(self, flat):
        assert flat["outage_duration_hours"] == 336

    def test_clinical_operations_disrupted(self, flat):
        # clinical_operations_disrupted in operational_impacts
        assert flat["clinical_operations_disrupted"] == 1

    def test_graduation_delayed(self, flat):
        assert flat["graduation_delayed"] == 1

    def test_teaching_impacted_direct(self, flat):
        assert flat["teaching_impacted"] == 1

    def test_research_impacted_direct(self, flat):
        assert flat["research_impacted"] == 1

    # ── User impact ───────────────────────────────────────────────────────
    def test_students_affected(self, flat):
        assert flat["students_affected"] == 34000

    def test_staff_affected(self, flat):
        assert flat["staff_affected"] == 4200

    def test_faculty_affected(self, flat):
        assert flat["faculty_affected"] == 2100

    def test_alumni_affected(self, flat):
        assert flat["alumni_affected"] == 8500

    def test_applicants_affected(self, flat):
        assert flat["applicants_affected"] == 1200

    def test_patients_affected(self, flat):
        assert flat["patients_affected"] == 3500

    def test_users_affected_exact(self, flat):
        assert flat["users_affected_exact"] == 44823

    # ── Financial impact ──────────────────────────────────────────────────
    def test_recovery_costs_min_from_recovery_cost_usd(self, flat):
        assert flat["recovery_costs_min"] == 4200000

    def test_legal_costs_from_legal_cost_usd(self, flat):
        assert flat["legal_costs"] == 750000

    def test_insurance_claim(self, flat):
        assert flat["insurance_claim"] == 1

    def test_insurance_claim_amount_from_insurance_payout_usd(self, flat):
        assert flat["insurance_claim_amount"] == 2000000

    def test_total_cost_estimate(self, flat):
        assert flat["total_cost_estimate"] == 8500000

    def test_business_impact(self, flat):
        assert flat["business_impact"] == "severe"

    # ── Regulatory impact ─────────────────────────────────────────────────
    def test_hipaa_breach(self, flat):
        assert flat["hipaa_breach"] == 1

    def test_ferpa_breach(self, flat):
        assert flat["ferpa_breach"] == 1

    def test_dpa_notified(self, flat):
        assert flat["dpa_notified"] == 1

    def test_breach_notification_required(self, flat):
        assert flat["breach_notification_required"] == 1

    def test_notifications_sent_from_notification_sent(self, flat):
        # Schema: notification_sent (singular) → flat: notifications_sent (plural)
        assert flat["notifications_sent"] == 1

    def test_notifications_sent_date(self, flat):
        assert flat["notifications_sent_date"] == "2024-04-01"

    def test_investigation_opened(self, flat):
        assert flat["investigation_opened"] == 1

    def test_fine_imposed_false(self, flat):
        assert flat["fine_imposed"] == 0

    def test_lawsuits_filed(self, flat):
        assert flat["lawsuits_filed"] == 1

    def test_class_action_from_class_action_filed(self, flat):
        # Schema: class_action_filed → flat: class_action
        assert flat["class_action"] == 1

    def test_regulatory_context_json(self, flat):
        regs = json.loads(flat["regulatory_context"])
        assert "FERPA" in regs
        assert "HIPAA" in regs
        assert "state_breach_notification" in regs

    # ── Recovery ─────────────────────────────────────────────────────────
    def test_recovery_timeframe_days_from_recovery_duration_days(self, flat):
        # Schema: recovery_duration_days → flat: recovery_timeframe_days
        assert flat["recovery_timeframe_days"] == 14

    def test_recovery_started_date(self, flat):
        assert flat["recovery_started_date"] == "2024-03-14"

    def test_recovery_completed_date(self, flat):
        assert flat["recovery_completed_date"] == "2024-03-26"

    def test_from_backup_from_recovery_method(self, flat):
        # recovery_method=backup_restore → from_backup=1
        assert flat["from_backup"] == 1

    def test_mfa_implemented_from_security_improvements(self, flat):
        # mfa_implemented in security_improvements → mfa_implemented=1
        assert flat["mfa_implemented"] == 1

    def test_incident_response_firm_from_ir_firm_engaged(self, flat):
        # Schema: ir_firm_engaged → flat: incident_response_firm
        assert flat["incident_response_firm"] == "CrowdStrike"

    def test_forensics_firm_from_forensics_firm_engaged(self, flat):
        # Schema: forensics_firm_engaged → flat: forensics_firm
        assert flat["forensics_firm"] == "Mandiant"

    def test_mttd_hours(self, flat):
        assert flat["mttd_hours"] == 28

    def test_mttr_hours(self, flat):
        assert flat["mttr_hours"] == 336

    def test_backup_status(self, flat):
        assert flat["backup_status"] == "available_and_used"

    def test_backup_age_days(self, flat):
        assert flat["backup_age_days"] == 1

    def test_law_enforcement_involved(self, flat):
        assert flat["law_enforcement_involved"] == 1

    def test_law_enforcement_agency(self, flat):
        # law_enforcement_agencies list → stored as-is or joined
        assert flat["law_enforcement_agency"] is not None

    def test_dwell_time_days(self, flat):
        assert flat["dwell_time_days"] == 1

    # ── Transparency ──────────────────────────────────────────────────────
    def test_public_disclosure(self, flat):
        assert flat["public_disclosure"] == 1

    def test_public_disclosure_date(self, flat):
        assert flat["public_disclosure_date"] == "2024-03-20"

    def test_disclosure_delay_days(self, flat):
        assert flat["disclosure_delay_days"] == 8

    def test_disclosure_source(self, flat):
        assert flat["disclosure_source"] == "institution_statement"

    def test_transparency_level(self, flat):
        assert flat["transparency_level"] == "good"

    def test_official_statement_url(self, flat):
        assert flat["official_statement_url"] == "https://westbrook.edu/security-notice"

    # ── Timeline (full data in incident_timeline junction table) ──────────
    def test_timeline_events_count(self, flat):
        # Count stored in flat table as a convenience column
        assert flat["timeline_events_count"] == 4

    # ── MITRE ATT&CK (full data in incident_mitre_techniques junction table)
    def test_primary_mitre_technique_id(self, flat):
        # First technique ID stored as convenience column in flat table
        assert flat["primary_mitre_technique_id"] is not None
        assert flat["primary_mitre_technique_id"].startswith("T")

    # ── Vulnerabilities (full data in incident_vulnerabilities junction table)
    def test_primary_cve_id(self, flat):
        # Highest-CVSS CVE stored as convenience column in flat table
        assert flat["primary_cve_id"] == "CVE-2023-20269"

    def test_max_cvss_score(self, flat):
        assert flat["max_cvss_score"] == 9.1

    # ── Threat intelligence ───────────────────────────────────────────────
    def test_malware_families(self, flat):
        families = json.loads(flat["malware_families"])
        assert "lockbit" in families

    def test_attacker_tools(self, flat):
        tools = json.loads(flat["attacker_tools"])
        assert "cobalt_strike" in tools
        assert "mimikatz" in tools

    def test_attack_campaign_name(self, flat):
        assert flat["attack_campaign_name"] == "LockBit Education Wave Q1 2024"

    # ── Research impact ───────────────────────────────────────────────────
    def test_research_projects_affected(self, flat):
        assert flat["research_projects_affected"] == 12

    def test_research_data_compromised(self, flat):
        assert flat["research_data_compromised"] == 1

    def test_publications_delayed(self, flat):
        assert flat["publications_delayed"] == 1

    def test_grants_affected(self, flat):
        assert flat["grants_affected"] == 1

    def test_research_area(self, flat):
        assert flat["research_area"] == "biomedical"

    # ── Enriched summary & notes ──────────────────────────────────────────
    def test_enriched_summary_populated(self, flat):
        assert flat["enriched_summary"] is not None
        assert "Westbrook" in flat["enriched_summary"]

    def test_extraction_notes(self, flat):
        assert flat["extraction_notes"] is not None

    def test_confidence(self, flat):
        assert 0.0 < flat["confidence"] <= 1.0

    # ── DB round-trip ─────────────────────────────────────────────────────
    def test_db_write_and_read_back(self, flat, db_row):
        """Verify the flat dict survives a round-trip through SQLite."""
        assert db_row, "DB row should not be empty"
        assert db_row["incident_id"] == "test_westbrook_001"

    def test_db_attack_category_persisted(self, db_row):
        assert db_row["attack_category"] == "ransomware_double_extortion"

    def test_db_ransomware_family_persisted(self, db_row):
        assert db_row["ransomware_family"] == "lockbit_3"

    def test_db_records_affected_exact_persisted(self, db_row):
        assert db_row["records_affected_exact"] == 44823

    def test_db_hipaa_breach_persisted(self, db_row):
        assert db_row["hipaa_breach"] == 1

    def test_db_ferpa_breach_persisted(self, db_row):
        assert db_row["ferpa_breach"] == 1

    def test_db_dpa_notified_persisted(self, db_row):
        assert db_row["dpa_notified"] == 1

    def test_db_timeline_events_count_persisted(self, db_row):
        assert db_row["timeline_events_count"] == 4

    def test_db_primary_mitre_technique_persisted(self, db_row):
        assert db_row["primary_mitre_technique_id"] is not None

    def test_db_cloud_services_affected_persisted(self, db_row):
        assert db_row["cloud_services_affected"] == 1

    def test_db_attack_chain_persisted(self, db_row):
        chain = json.loads(db_row["attack_chain"])
        assert "initial_access" in chain

    def test_db_regulatory_context_persisted(self, db_row):
        regs = json.loads(db_row["regulatory_context"])
        assert "FERPA" in regs

    def test_db_all_user_counts_persisted(self, db_row):
        assert db_row["students_affected"] == 34000
        assert db_row["staff_affected"] == 4200
        assert db_row["faculty_affected"] == 2100
        assert db_row["alumni_affected"] == 8500
        assert db_row["applicants_affected"] == 1200
        assert db_row["patients_affected"] == 3500


# ─────────────────────────────────────────────────────────────────────────────
# Schema coverage audit (print-only, not a pytest assertion)
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_FIELDS_NOT_IN_DB = {
    # ── Fields that ARE stored, but under a different flat dict key ──────────
    "is_edu_cyber_incident": "stored as is_education_related",
    "education_relevance_reasoning": "stored inside EducationRelevanceCheck (not a DB column)",
    "incident_status": "not stored in enrichments_flat (low analytical value)",
    "incident_date": "stored on the incidents table, not enrichments_flat",
    "timeline": "stored in incident_timeline junction table + timeline_events_count convenience column",
    "vulnerabilities_exploited": "stored in incident_vulnerabilities junction table + primary_cve_id / max_cvss_score convenience columns",
    "mitre_attack_techniques": "stored in incident_mitre_techniques junction table + primary_mitre_technique_id convenience column",
    "ransom_amount_exact": "stored as ransom_amount",
    "systems_affected": "stored as systems_affected_codes",
    "operational_impacts": "decomposed into teaching_disrupted / research_disrupted / classes_cancelled etc.",
    "estimated_total_cost_usd": "stored as total_cost_estimate",
    "recovery_cost_usd": "stored as recovery_costs_min",
    "legal_cost_usd": "stored as legal_costs",
    "notification_cost_usd": "removed (low analytical value)",
    "insurance_payout_usd": "stored as insurance_claim_amount",
    "notification_sent": "stored as notifications_sent (plural)",
    "notification_sent_date": "stored as notifications_sent_date (plural)",
    "fine_amount_usd": "stored as fine_amount",
    "class_action_filed": "stored as class_action",
    "ir_firm_engaged": "stored as incident_response_firm",
    "forensics_firm_engaged": "stored as forensics_firm",
    "law_enforcement_agencies": "stored as law_enforcement_agency",
    "recovery_duration_days": "stored as recovery_timeframe_days",
    # ── Intentionally not stored ─────────────────────────────────────────────
    "institution_aliases": "not needed as separate column",
    "discovery_date": "not stored (derived info rarely in articles)",
    "threat_actor_claimed": "not stored (implicit from threat_actor_claim_url)",
    "attacker_communication_channel": "not stored (low analytical value)",
    "ransom_cryptocurrency": "not stored (low analytical value)",
    "ransom_negotiated": "not stored",
    "ransom_deadline_given": "not stored",
    "ransom_deadline_days": "not stored",
    "decryptor_received": "not stored",
    "decryptor_worked": "not stored",
    "iocs": "not stored in flat (complex object retained only in raw extraction JSON)",
    "data_encrypted": "not stored directly; drives encryption_extent column",
    "data_destroyed": "not stored",
    "data_published": "not stored",
    "data_sold": "not stored",
    "domain_admin_compromised": "not stored",
    "backup_compromised": "not stored",
    "systems_encrypted_count": "not stored",
    "servers_affected_count": "not stored",
    "endpoints_affected_count": "not stored",
    "outage_start_date": "not stored",
    "outage_end_date": "not stored",
    "donors_affected": "not stored",
    "total_individuals_affected": "captured via users_affected_exact",
    "ransom_cost_usd": "captured via ransom_paid_amount",
    "credit_monitoring_cost_usd": "not stored",
    "lost_revenue_usd": "not stored",
    "regulators_notified": "stored in regulatory_context JSON array",
    "investigating_agencies": "not stored (law_enforcement_agency captures agencies)",
    "lawsuit_count": "not stored",
    "settlement_amount_usd": "not stored",
    "incident_response_activated": "not stored (implicit from incident_response_firm)",
    "legal_counsel_engaged": "not stored",
    "pr_firm_engaged": "not stored",
    "recovery_method": "not stored directly; drives from_backup column",
    "security_improvements": "not stored as list; drives mfa_implemented column",
    "incident_report_url": "not stored",
    "updates_provided_count": "not stored",
    "related_incidents": "not stored",
    "common_vulnerability_exploited": "captured via incident_vulnerabilities junction table",
    "sector_targeting_pattern": "not stored",
    "other_edu_incidents": "handled as separate incident records",
}


def test_print_schema_coverage_audit():
    """Prints field coverage audit table. Always passes; useful with -s flag."""
    _, flat = _run_pipeline()
    flat_keys = set(flat.keys())

    print("\n" + "=" * 80)
    print("FIELD AUDIT: extraction_schema.py → mapper → flat dict → DB")
    print("=" * 80)

    all_schema_fields = list(EXTRACTION_SCHEMA["properties"].keys())
    missing_from_flat = []
    present_in_flat = []

    for field in all_schema_fields:
        if field in flat_keys:
            present_in_flat.append(field)
        elif field in SCHEMA_FIELDS_NOT_IN_DB:
            pass  # Intentionally not in DB — skip
        else:
            missing_from_flat.append(field)

    print(f"\n✅  {len(present_in_flat)} schema fields successfully mapped to flat/DB columns")
    print(f"⚪  {len(SCHEMA_FIELDS_NOT_IN_DB)} schema fields intentionally not stored in DB")
    if missing_from_flat:
        print(f"❌  {len(missing_from_flat)} schema fields MISSING from flat dict (possible bug):")
        for f in missing_from_flat:
            print(f"    - {f}")
    else:
        print("✅  No schema fields are missing from the flat dict")

    print("\n── Intentionally not stored in DB ──────────────────────────────────")
    for field, reason in SCHEMA_FIELDS_NOT_IN_DB.items():
        print(f"  {field:45s}  {reason}")

    print("\n── Null values in flat dict ────────────────────────────────────────")
    null_cols = [k for k, v in flat.items() if v is None]
    for col in sorted(null_cols):
        print(f"  {col}")

    print("=" * 80 + "\n")

    # This test always passes — it's for reporting only
    assert True
