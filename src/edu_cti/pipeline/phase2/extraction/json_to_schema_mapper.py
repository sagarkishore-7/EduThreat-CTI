"""
Mapper to convert JSON schema extraction response to CTIEnrichmentResult.

Includes dynamic normalization to handle unexpected LLM outputs gracefully.
"""

from typing import Dict, Any, Optional, List
from src.edu_cti.pipeline.phase2.schemas import (
    CTIEnrichmentResult,
    EducationRelevanceCheck,
    TimelineEvent,
    MITREAttackTechnique,
    AttackDynamics,
)


# ============================================
# DYNAMIC VALUE NORMALIZATION
# ============================================
# Maps unexpected LLM outputs to valid enum values

ATTACK_VECTOR_NORMALIZATION = {
    # Simple/short forms -> full form
    "email": "phishing_email",
    "phish": "phishing",
    "spear_phish": "spear_phishing",
    "spearphishing": "spear_phishing",
    "bec": "business_email_compromise",
    "cred_stuffing": "credential_stuffing",
    "password_spray": "password_spraying",
    "cred_theft": "credential_theft",
    "stolen_creds": "stolen_credentials",
    "vuln_exploit": "vulnerability_exploit",
    "zero_day": "vulnerability_exploit_zero_day",
    "0day": "vulnerability_exploit_zero_day",
    "known_vuln": "vulnerability_exploit_known",
    "rce": "vulnerability_exploit",
    "remote_code_execution": "vulnerability_exploit",
    "unpatched": "unpatched_system",
    "misconfig": "misconfiguration",
    "default_creds": "default_credentials",
    "default_password": "default_credentials",
    "rdp": "exposed_rdp",
    "vpn": "exposed_vpn",
    "ssh": "exposed_ssh",
    "api": "exposed_api",
    "database": "exposed_database",
    "db": "exposed_database",
    "mitm": "man_in_the_middle",
    "social_eng": "social_engineering",
    "se": "social_engineering",
    "insider": "insider_access",
    "internal": "insider_access",
    "cloud_misconfig": "cloud_misconfiguration",
    "api_key": "api_key_exposure",
    "bucket": "storage_bucket_exposure",
    "s3": "storage_bucket_exposure",
    "supply_chain_attack": "supply_chain_compromise",
    "vendor": "third_party_vendor",
    "third_party": "third_party_vendor",
    "dos": "ddos",
    "denial_of_service": "ddos",
    "dns": "dns_hijacking",
    "sim_swap": "sim_swapping",
    "usb": "usb_drop",
    "tailgate": "tailgating",
    "physical": "tailgating",
    "watering_hole_attack": "watering_hole",
    "drive_by": "drive_by_download",
    "malvert": "malvertising",
    "sqli": "sql_injection",
    "xss_attack": "xss",
    "csrf_attack": "csrf",
    "ssrf_attack": "ssrf",
}

ATTACK_CHAIN_NORMALIZATION = {
    # Common LLM variations
    "recon": "reconnaissance",
    "initial": "initial_access",
    "access": "initial_access",
    "exec": "execution",
    "run": "execution",
    "persist": "persistence",
    "priv_esc": "privilege_escalation",
    "privesc": "privilege_escalation",
    "escalation": "privilege_escalation",
    "defense_bypass": "defense_evasion",
    "evasion": "defense_evasion",
    "cred_access": "credential_access",
    "credentials": "credential_access",
    "lateral": "lateral_movement",
    "movement": "lateral_movement",
    "c2": "command_and_control",
    "c&c": "command_and_control",
    "cnc": "command_and_control",
    "exfil": "exfiltration",
    "data_exfil": "data_exfiltration",
    "encrypt": "encryption",
    "ransom": "ransom_demand",
    "contain": "containment",
    "eradicate": "eradication",
    "recover": "recovery",
    "lessons": "lessons_learned",
    "notify": "notification",
    "disclose": "disclosure",
    "detect": "detection",
    "investigate": "investigation",
}

EVENT_TYPE_NORMALIZATION = {
    "access": "initial_access",
    "recon": "reconnaissance",
    "lateral": "lateral_movement",
    "priv_esc": "privilege_escalation",
    "exfil": "data_exfiltration",
    "encrypt": "encryption_started",
    "ransom": "ransom_demand",
    "contain": "containment",
    "eradicate": "eradication",
    "recover": "recovery",
    "disclose": "disclosure",
    "notify": "notification",
    "investigate": "investigation",
    "remediate": "remediation",
    "improve": "security_improvement",
}

INSTITUTION_TYPE_NORMALIZATION = {
    # Generic free-text from ingestion sources → canonical enum values
    "university": "university",
    "public university": "university",
    "state university": "university",
    "private university": "university",
    "research university": "university",
    "r1 university": "university",
    "r2 university": "university",
    "university_public": "university",
    "university_private": "university",
    "university_research": "university",
    "university-public": "university",
    "university-private": "university",
    # Backward-compat: old enum values the LLM may still output → new values
    "college": "community_college",
    "junior college": "community_college",
    "2-year college": "community_college",
    "two-year college": "community_college",
    "technical school": "technical_college",
    "tech college": "technical_college",
    "polytechnic": "technical_college",
    "trade school": "vocational_school",
    "vocational": "vocational_school",
    "school": "k12_school",
    "public school": "k12_school",
    "private school": "k12_school",
    "charter school": "k12_school",
    "k12_public_school": "k12_school",
    "k12_private_school": "k12_school",
    "k12_charter_school": "k12_school",
    "k_12_public_school": "k12_school",
    "k_12_private_school": "k12_school",
    "k_12_charter_school": "k12_school",
    "district": "school_district",
    "school district": "school_district",
    "independent school district": "school_district",
    "isd": "school_district",
    "unified school district": "school_district",
    "board of education": "school_district",
    "research institute": "research_institute",
    "institute": "research_institute",
    "research center": "research_center",
    "research lab": "research_center",
    "laboratory": "research_center",
    "medical school": "medical_school",
    "school of medicine": "medical_school",
    "hospital": "university_hospital",
    "university hospital": "university_hospital",
    "teaching hospital": "teaching_hospital",
    "online school": "online_university",
    "online college": "online_university",
    "public library": "library",
    "academic library": "library",
    "tribal college": "tribal_college",
    "tribal university": "tribal_college",
    "military academy": "military_academy",
    "service academy": "military_academy",
    "edtech": "edtech_platform",
    "education technology": "edtech_platform",
    "ed tech": "edtech_platform",
    "tutoring": "tutoring_service",
    "tutoring company": "tutoring_service",
    "test prep": "tutoring_service",
    "consortium": "consortium",
    "department of education": "education_department",
    "education department": "education_department",
    "ministry of education": "education_ministry",
    "student loan": "student_loan_servicer",
    "loan servicer": "student_loan_servicer",
    "nonprofit": "education_nonprofit",
    "education nonprofit": "education_nonprofit",
    "vendor": "education_vendor",
    "education vendor": "education_vendor",
    "software vendor": "education_vendor",
    "n/a": "unknown",
    "none": "unknown",
    "": "unknown",
}

_VALID_INSTITUTION_TYPES = {
    "university",
    "community_college", "technical_college", "vocational_school",
    "k12_school", "school_district", "research_institute", "research_center",
    "medical_school", "university_hospital", "teaching_hospital",
    "online_university", "library", "tribal_college", "military_academy",
    "edtech_platform", "tutoring_service", "consortium",
    "education_department", "education_ministry", "student_loan_servicer",
    "education_nonprofit", "education_vendor", "unknown",
}

DATE_PRECISION_NORMALIZATION = {
    # Common LLM variations -> valid values
    "exact": "day",
    "precise": "day",
    "specific": "day",
    "daily": "day",
    "full": "day",
    "complete": "day",
    "monthly": "month",
    "yearly": "year",
    "annual": "year",
    "approx": "approximate",
    "estimated": "approximate",
    "rough": "approximate",
    "unknown": "approximate",
    "uncertain": "approximate",
    "unclear": "approximate",
    "vague": "approximate",
    "imprecise": "approximate",
}


def normalize_enum_value(value: Any, normalization_map: Dict[str, str], valid_values: set, fallback: str = "unknown") -> Optional[str]:
    """
    Normalize an enum value to a valid option.
    
    Args:
        value: The raw value from LLM
        normalization_map: Mapping of variations to canonical values
        valid_values: Set of all valid enum values
        fallback: Value to use if no mapping found
    
    Returns:
        Normalized value or fallback
    """
    if value is None:
        return None
    
    if not isinstance(value, str):
        return fallback
    
    # Normalize: lowercase, strip, replace spaces with underscores
    normalized = value.lower().strip().replace(" ", "_").replace("-", "_")
    
    # Check if already valid
    if normalized in valid_values:
        return normalized
    
    # Check normalization map
    if normalized in normalization_map:
        return normalization_map[normalized]
    
    # Try partial matching for compound values
    for key, mapped_value in normalization_map.items():
        if key in normalized or normalized in key:
            return mapped_value
    
    # Fallback
    return fallback


def normalize_institution_type(value: Any) -> Optional[str]:
    """Normalize institution_type to a valid canonical enum value."""
    if value is None:
        return None
    if not isinstance(value, str):
        return "unknown"
    normalized = value.lower().strip().replace("-", "_").replace(" ", "_")
    if normalized in _VALID_INSTITUTION_TYPES:
        return normalized
    # Try normalized form (hyphens/spaces → underscores) in the normalization map
    if normalized in INSTITUTION_TYPE_NORMALIZATION:
        return INSTITUTION_TYPE_NORMALIZATION[normalized]
    # Try direct lookup in normalization map (key already lowercased+stripped)
    key = value.lower().strip()
    if key in INSTITUTION_TYPE_NORMALIZATION:
        return INSTITUTION_TYPE_NORMALIZATION[key]
    # Try substring lookup — only for multi-word map keys to avoid false positives
    # (e.g. "school" substring in "PowerSchool" should NOT match)
    for map_key, canonical in INSTITUTION_TYPE_NORMALIZATION.items():
        if " " in map_key and map_key in key:
            return canonical
    return "unknown"


def normalize_attack_vector(value: Any) -> Optional[str]:
    """Normalize attack_vector to valid enum value."""
    valid_values = {
        "phishing_email", "spear_phishing_email", "malicious_attachment", "malicious_link",
        "business_email_compromise", "stolen_credentials", "credential_stuffing", "brute_force",
        "password_spraying", "credential_phishing", "session_hijacking", "vulnerability_exploit_known",
        "vulnerability_exploit_zero_day", "unpatched_system", "misconfiguration", "default_credentials",
        "drive_by_download", "watering_hole", "malvertising", "sql_injection", "xss", "csrf", "ssrf",
        "path_traversal", "exposed_service", "exposed_rdp", "exposed_vpn", "exposed_ssh",
        "exposed_database", "exposed_api", "man_in_the_middle", "supply_chain_compromise",
        "third_party_vendor", "software_update_compromise", "trusted_relationship", "social_engineering",
        "pretexting", "baiting", "tailgating", "usb_drop", "insider_access", "former_employee",
        "cloud_misconfiguration", "api_key_exposure", "storage_bucket_exposure",
        "dns_hijacking", "sim_swapping", "unknown", "other"
    }
    return normalize_enum_value(value, ATTACK_VECTOR_NORMALIZATION, valid_values, "unknown")


def normalize_attack_chain(values: Any) -> Optional[List[str]]:
    """Normalize attack_chain list to valid enum values."""
    if not values or not isinstance(values, list):
        return None
    
    valid_values = {
        "reconnaissance", "resource_development", "initial_access", "execution", "persistence",
        "privilege_escalation", "defense_evasion", "credential_access", "discovery",
        "lateral_movement", "collection", "command_and_control", "exfiltration", "impact",
        "weaponization", "delivery", "exploitation", "installation", "actions_on_objectives",
        "vulnerability_discovery", "credential_harvesting", "data_analysis", "data_theft",
        "encryption", "ransom_demand", "data_exfiltration", "containment", "eradication",
        "recovery", "lessons_learned", "notification", "disclosure", "detection", "investigation"
    }
    
    normalized = []
    for v in values:
        norm = normalize_enum_value(v, ATTACK_CHAIN_NORMALIZATION, valid_values, None)
        if norm and norm not in normalized:
            normalized.append(norm)
    
    return normalized if normalized else None


def normalize_event_type(value: Any) -> Optional[str]:
    """Normalize event_type to valid enum value."""
    valid_values = {
        "initial_access", "reconnaissance", "lateral_movement", "privilege_escalation",
        "data_exfiltration", "encryption_started", "ransom_demand", "discovery", "exploitation",
        "impact", "operational_impact", "containment", "eradication", "recovery", "disclosure",
        "notification", "investigation", "remediation", "law_enforcement_contact",
        "public_statement", "systems_restored", "response_action", "security_improvement", "other"
    }
    return normalize_enum_value(value, EVENT_TYPE_NORMALIZATION, valid_values, "other")


def normalize_date_precision(value: Any) -> Optional[str]:
    """Normalize date_precision to valid enum value."""
    valid_values = {"day", "month", "year", "approximate"}
    return normalize_enum_value(value, DATE_PRECISION_NORMALIZATION, valid_values, "approximate")


OPERATIONAL_IMPACT_NORMALIZATION = {
    "classes_canceled": "classes_cancelled",
    "class_cancelled": "classes_cancelled",
    "class_cancellation": "classes_cancelled",
    "online_classes": "classes_moved_online",
    "remote_classes": "classes_moved_online",
    "virtual_classes": "classes_moved_online",
    "exam_delayed": "exams_postponed",
    "exam_delay": "exams_postponed",
    "exams_delayed": "exams_postponed",
    "research_stopped": "research_halted",
    "research_paused": "research_halted",
    "enrollment_delayed": "enrollment_disrupted",
    "enrollment_stopped": "enrollment_disrupted",
    "admissions_delayed": "admissions_impacted",
    "admissions_stopped": "admissions_impacted",
    "library_down": "library_services_affected",
    "library_closed": "library_services_affected",
    "payment_down": "payment_systems_offline",
    "payments_down": "payment_systems_offline",
    "email_down": "email_system_down",
    "email_offline": "email_system_down",
    "portal_down": "student_portal_down",
    "student_portal_offline": "student_portal_down",
    "network_down": "network_down",
    "network_offline": "network_down",
    "website_down": "website_down",
    "website_offline": "website_down",
    "site_down": "website_down",
}

BUSINESS_IMPACT_NORMALIZATION = {
    "high": "severe",
    "low": "limited",
    "medium": "moderate",
    "minor": "minimal",
    "major": "severe",
    "significant": "severe",
    "catastrophic": "critical",
    "extreme": "critical",
    "none": "minimal",
    "negligible": "minimal",
}

ENCRYPTION_IMPACT_NORMALIZATION = {
    "complete": "full",
    "total": "full",
    "100%": "full",
    "some": "partial",
    "limited": "partial",
    "partial_encryption": "partial",
    "no_encryption": "none",
    "not_encrypted": "none",
    "unencrypted": "none",
}


def normalize_operational_impact(values: Any) -> Optional[List[str]]:
    """Normalize operational_impact list to valid enum values."""
    if not values or not isinstance(values, list):
        return None
    
    valid_values = {
        "classes_cancelled", "classes_moved_online", "exams_postponed", "research_halted",
        "enrollment_disrupted", "graduation_delayed", "admissions_impacted",
        "campus_closed", "dormitories_affected", "dining_services_disrupted",
        "library_services_affected", "athletic_events_cancelled", "payment_systems_offline",
        "email_system_down", "student_portal_down", "network_down", "website_down", "other"
    }
    
    normalized = []
    for v in values:
        norm = normalize_enum_value(v, OPERATIONAL_IMPACT_NORMALIZATION, valid_values, None)
        if norm and norm not in normalized:
            normalized.append(norm)
    
    return normalized if normalized else None


def normalize_business_impact(value: Any) -> Optional[str]:
    """Normalize business_impact to valid enum value."""
    valid_values = {"critical", "severe", "moderate", "limited", "minimal"}
    return normalize_enum_value(value, BUSINESS_IMPACT_NORMALIZATION, valid_values, None)


def normalize_encryption_impact(value: Any) -> Optional[str]:
    """Normalize encryption_impact to valid enum value."""
    valid_values = {"full", "partial", "none"}
    return normalize_enum_value(value, ENCRYPTION_IMPACT_NORMALIZATION, valid_values, None)


def map_systems_affected_codes(codes: List[str]) -> List[str]:
    """Map extraction schema system codes to CTIEnrichmentResult system codes (extensive mapping)."""
    if not codes:
        return None
    
    mapping = {
        # Email/Communication
        "email": "email_system",
        "email_system": "email_system",
        # Web/Public
        "website_public": "web_servers",
        "public_website": "web_servers",
        # Portals
        "portal_student_staff": "student_portal",
        "student_portal": "student_portal",
        "staff_portal": "student_portal",
        "alumni_portal": "student_portal",
        "applicant_portal": "student_portal",
        # Identity
        "identity_sso": "other",
        "identity_management": "other",
        "active_directory": "other",
        # Network
        "vpn_remote_access": "network_infrastructure",
        "vpn": "network_infrastructure",
        "wifi_network": "network_infrastructure",
        "wired_network_core": "network_infrastructure",
        "core_network": "network_infrastructure",
        "dns_dhcp": "network_infrastructure",
        "dns": "network_infrastructure",
        "dhcp": "network_infrastructure",
        "firewall_gateway": "network_infrastructure",
        "firewall": "network_infrastructure",
        # Academic systems
        "lms": "learning_management_system",
        "lms_learning_management": "learning_management_system",
        "sis": "student_portal",
        "sis_student_information": "student_portal",
        "registration_system": "student_portal",
        "grade_system": "student_portal",
        "library_system": "other",
        "library_systems": "other",
        "exam_proctoring": "other",
        # Administrative
        "erp_finance_hr": "financial_systems",
        "erp_system": "financial_systems",
        "hr_payroll": "payroll_system",
        "hr_system": "payroll_system",
        "payroll_system": "payroll_system",
        "financial_system": "financial_systems",
        "admissions_enrollment": "admissions_system",
        "admissions_system": "admissions_system",
        "financial_aid_system": "financial_systems",
        "procurement": "financial_systems",
        # Storage/Files
        "payment_billing": "financial_systems",
        "file_transfer": "file_servers",
        "cloud_storage": "cloud_services",
        "on_prem_file_share": "file_servers",
        "file_servers": "file_servers",
        # Research
        "research_hpc": "research_systems",
        "research_computing_hpc": "research_systems",
        "research_lab_instruments": "research_systems",
        "research_storage": "research_systems",
        "research_databases": "research_systems",
        "lab_instruments": "research_systems",
        # Healthcare (teaching hospitals)
        "ehr_emr": "hospital_systems",
        "hospital_systems": "hospital_systems",
        "medical_devices": "hospital_systems",
        "pharmacy_system": "hospital_systems",
        # Phone/Communication
        "phone_voip": "other",
        "voip_phone": "other",
        # Other infrastructure
        "printing_copy": "other",
        "printing_system": "other",
        "parking_system": "other",
        "physical_access": "other",
        "cctv_security": "other",
        "backup_infrastructure": "backup_systems",
        "backup_systems": "backup_systems",
        "datacenter_facilities": "other",
        "data_center": "other",
        "virtualization": "other",
        "security_tools": "other",
        "other": "other",
        "unknown": "other",
    }
    
    mapped = []
    for code in codes:
        if code:
            code_lower = code.lower() if isinstance(code, str) else code
            mapped_code = mapping.get(code_lower, mapping.get(code, "other"))
        if mapped_code not in mapped:
            mapped.append(mapped_code)
    return mapped if mapped else None


def map_attack_category_to_vector(category) -> Optional[str]:
    """Map attack category to attack vector (handles extensive new categories)."""
    if not category:
        return None

    # LLM may return a list when the schema allows multiple categories — take the first
    if isinstance(category, list):
        category = category[0] if category else None
        if not category:
            return None

    # Normalize to lowercase for matching
    category_lower = category.lower()
    
    # Ransomware variants — don't derive attack_vector from category; the initial
    # access method is unknown without explicit article evidence.
    if "ransomware" in category_lower:
        return None
    
    # Phishing variants
    if "phishing" in category_lower or "bec" in category_lower or "whaling" in category_lower:
        return "phishing"
    
    # Data breach variants
    if "data_breach" in category_lower or "data_leak" in category_lower or "data_exposure" in category_lower:
        return "other"
    
    # DDoS variants
    if "ddos" in category_lower:
        return "ddos"
    
    # Malware variants
    if "malware" in category_lower or "trojan" in category_lower or "infostealer" in category_lower:
        return "malware"
    
    # Access-based attacks
    if "credential" in category_lower or "brute_force" in category_lower or "unauthorized" in category_lower:
        return "credential_theft"
    
    # Supply chain
    if "supply_chain" in category_lower or "third_party" in category_lower:
        return "supply_chain"
    
    # Insider threats
    if "insider" in category_lower:
        return "insider_threat"
    
    # Other mappings
    mapping = {
        "web_defacement": "other",
        "extortion": "other",
        "hacktivism": "other",
        "espionage": "other",
        "sabotage": "other",
        "fraud": "social_engineering",
        "account_takeover": "credential_theft",
        "social_engineering": "social_engineering",
    }
    
    return mapping.get(category_lower, "other")


def map_data_types(data_types: List[str]) -> Dict[str, bool]:
    """Map data types to data impact metrics (handles extensive new categories)."""
    result = {}
    
    # Extended type mapping for new comprehensive schema
    type_mapping = {
        # Student data
        "student_records": "student_data",
        "student_pii": "student_data",
        "student_ssn": "student_data",
        "student_grades": "student_data",
        "student_transcripts": "student_data",
        "student_financial_aid": "student_data",
        "student_health_records": "student_data",
        "student_immigration": "student_data",
        "student_housing": "student_data",
        "student_disciplinary": "student_data",
        # Staff/Faculty data
        "staff_data": "faculty_data",
        "employee_pii": "faculty_data",
        "employee_ssn": "faculty_data",
        "employee_payroll": "faculty_data",
        "employee_benefits": "faculty_data",
        "employee_performance": "faculty_data",
        "employee_background_checks": "faculty_data",
        # Alumni data
        "alumni_data": "alumni_data",
        "alumni_pii": "alumni_data",
        "alumni_donation_history": "alumni_data",
        # Research data
        "research_data": "research_data",
        "research_grants": "research_data",
        "research_ip": "research_data",
        "research_unpublished": "research_data",
        "research_classified": "research_data",
        "health_data": "medical_records",
        "financial_data": "financial_data",
        "credentials": "personal_information",
        "pii": "personal_information",
        "general_pii": "personal_information",
        "personal_data": "personal_information",
        "personal_information": "personal_information",
        "health_records": "medical_records",
        "health_data": "medical_records",
        "grades_transcripts": "student_data",
        "special_category_gdpr": "personal_information",
        "other": "administrative_data",
    }
    
    for dt in data_types:
        mapped = type_mapping.get(dt, "administrative_data")
        result[mapped] = True
    
    return result


def _coerce_llm_scalars(json_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalise LLM output: any field that SHOULD be a scalar but arrived as a list
    is coerced to its first element.  Known array fields (timeline, mitre_attack_techniques,
    systems_affected_codes, data_types, etc.) are left untouched.
    """
    _KNOWN_ARRAYS = {
        "timeline", "mitre_attack_techniques", "systems_affected", "systems_affected_codes",
        "data_categories", "data_categories_affected", "data_types", "operational_impacts",
        "applicable_regulations", "security_improvements", "third_parties_involved",
        "other_edu_incidents", "iocs", "target_demographics", "attack_chain",
        "vulnerabilities_exploited", "malware_families", "attacker_tools",
        "threat_actor_aliases", "institution_aliases", "law_enforcement_agencies",
        "regulators_notified", "investigating_agencies", "related_incidents",
        "key_quotes",
    }
    result = {}
    for key, value in json_data.items():
        if isinstance(value, list) and key not in _KNOWN_ARRAYS:
            result[key] = value[0] if value else None
        else:
            result[key] = value
    return result


def _build_summary(json_data: Dict[str, Any]) -> str:
    """
    Return the LLM-generated enriched_summary if it is non-empty.
    Otherwise construct a minimal factual summary from available fields so
    the dashboard never shows a blank summary for enriched incidents.
    """
    llm_summary = (json_data.get("enriched_summary") or "").strip()
    if llm_summary:
        return llm_summary

    # Fallback: build from metadata
    parts: list[str] = []
    name = json_data.get("institution_name") or json_data.get("institution_name_en")
    attack = json_data.get("attack_category") or json_data.get("attack_type_hint")
    date = json_data.get("incident_date") or json_data.get("source_published_date")
    country = json_data.get("country")
    actor = json_data.get("threat_actor_name")
    ransomware = json_data.get("ransomware_family")

    subj = name or "An educational institution"
    verb = "was targeted"
    if attack:
        verb = f"experienced a {attack.replace('_', ' ')} attack"
    parts.append(f"{subj} {verb}")
    if date:
        parts.append(f"on or around {date}")
    if country:
        parts.append(f"({country})")
    sentence = " ".join(parts).rstrip(",") + "."

    extras: list[str] = []
    if actor:
        extras.append(f"The attack was attributed to {actor}.")
    if ransomware:
        extras.append(f"Ransomware family: {ransomware}.")

    return " ".join([sentence] + extras)


def json_to_cti_enrichment(
    json_data: Dict[str, Any],
    primary_url: str,
    incident: Optional[Any] = None
) -> CTIEnrichmentResult:
    """
    Convert JSON schema extraction response to CTIEnrichmentResult.

    Args:
        json_data: JSON response from LLM extraction
        primary_url: Primary URL for the incident
        incident: Optional BaseIncident to get leak_site_url from phase1

    Returns:
        CTIEnrichmentResult object
    """
    # Normalise list-typed scalar fields from LLM (grammar-constrained generation
    # can occasionally return a single-item array for fields defined as strings).
    json_data = _coerce_llm_scalars(json_data)

    # Get threat_actor_claim_url from LLM response, or fallback to leak_site_url from phase1
    threat_actor_claim_url = json_data.get("threat_actor_claim_url")
    if not threat_actor_claim_url and incident and hasattr(incident, 'leak_site_url') and incident.leak_site_url:
        threat_actor_claim_url = incident.leak_site_url
    
    # Store claim URL in extraction notes if available
    extraction_notes_parts = []
    if threat_actor_claim_url:
        extraction_notes_parts.append(f"Threat actor claim URL: {threat_actor_claim_url}")
    if json_data.get("extraction_notes"):
        extraction_notes_parts.append(json_data.get("extraction_notes"))
    extraction_notes = "\n".join(extraction_notes_parts) if extraction_notes_parts else None
    # Education relevance - directly from LLM analysis of article content
    # Coerce is_edu_cyber_incident to bool — LLM sometimes returns "true"/"false" strings
    raw_edu = json_data.get("is_edu_cyber_incident", False)
    if isinstance(raw_edu, str):
        is_edu = raw_edu.strip().lower() in ("true", "yes", "1")
    else:
        is_edu = bool(raw_edu)
    education_relevance = EducationRelevanceCheck(
        is_education_related=is_edu,
        reasoning=json_data.get("education_relevance_reasoning", "") or "No reasoning provided by LLM",
        institution_identified=json_data.get("institution_name")
    )
    
    # Timeline - normalize event_type and date_precision values
    timeline = None
    if json_data.get("timeline"):
        timeline = []
        for event in json_data["timeline"]:
            # LLM sometimes emits a plain string instead of a dict — skip non-dicts
            if not isinstance(event, dict):
                continue
            timeline.append(TimelineEvent(
                date=event.get("date"),
                date_precision=normalize_date_precision(event.get("date_precision")),
                event_description=event.get("event_description"),
                event_type=normalize_event_type(event.get("event_type")),
                actor_attribution=event.get("actor_attribution"),
                indicators=event.get("indicators")
            ))
        if not timeline:
            timeline = None
    
    # MITRE ATT&CK techniques
    mitre_techniques = None
    raw_mitre = json_data.get("mitre_attack_techniques")
    if raw_mitre:
        parsed = []
        for tech in raw_mitre:
            if isinstance(tech, str):
                # LLM returned a bare string ID instead of an object
                parsed.append(MITREAttackTechnique(
                    technique_id=tech,
                    technique_name=None,
                    tactic=None,
                    description=None,
                    sub_techniques=None,
                ))
            elif isinstance(tech, dict):
                tid = tech.get("technique_id") or tech.get("id") or tech.get("technique")
                if not tid:
                    continue  # skip malformed entries
                sub = tech.get("sub_techniques")
                if isinstance(sub, str):
                    sub = [sub] if sub else None
                parsed.append(MITREAttackTechnique(
                    technique_id=tid,
                    technique_name=tech.get("technique_name") or tech.get("name"),
                    tactic=tech.get("tactic"),
                    description=tech.get("description"),
                    sub_techniques=sub or None,
                ))
        mitre_techniques = parsed if parsed else None
    
    # Attack dynamics - capture all attack-related fields
    attack_dynamics = None
    attack_category = json_data.get("attack_category")
    # LLM may return attack_category as a list — coerce to string (use first element)
    if isinstance(attack_category, list):
        attack_category = attack_category[0] if attack_category else None
    attack_vector = json_data.get("attack_vector")  # Direct attack_vector from schema
    # Same guard for attack_vector
    if isinstance(attack_vector, list):
        attack_vector = attack_vector[0] if attack_vector else None
    
    # Check if we have any attack-related data
    has_attack_data = (
        attack_category is not None or
        attack_vector is not None or
        json_data.get("ransomware_family_or_group") is not None or
        json_data.get("was_ransom_demanded") is not None or
        json_data.get("ransom_paid") is not None or
        json_data.get("data_breached") is not None or
        json_data.get("data_exfiltrated") is not None or
        json_data.get("data_encrypted") is not None
    )
    
    if has_attack_data:
        # Determine attack vector - prefer direct attack_vector, then derive from category
        final_attack_vector = attack_vector
        if not final_attack_vector and attack_category:
            final_attack_vector = map_attack_category_to_vector(attack_category)
        
        # NORMALIZE attack_vector to valid enum value
        final_attack_vector = normalize_attack_vector(final_attack_vector)
        
        # Determine encryption impact - normalize to valid enum
        encryption_impact = None
        raw_enc = json_data.get("encryption_impact")
        if raw_enc:
            encryption_impact = normalize_encryption_impact(raw_enc)
        elif json_data.get("data_encrypted") is True:
            encryption_impact = "full"
        elif json_data.get("data_encrypted") is False:
            encryption_impact = "none"
        
        # Determine data exfiltration
        data_exfil = json_data.get("data_exfiltrated")
        if data_exfil is None:
            data_exfil = json_data.get("data_breached")
        # Coerce to bool — LLM sometimes puts a record count (int) in this field
        if data_exfil is not None and not isinstance(data_exfil, bool):
            if isinstance(data_exfil, (int, float)):
                data_exfil = data_exfil > 0
            elif isinstance(data_exfil, str):
                data_exfil = data_exfil.strip().lower() in ("true", "yes", "1")
            else:
                data_exfil = bool(data_exfil)
        
        # Recovery timeframe — schema uses "recovery_duration_days"; legacy alias kept
        recovery_days = (
            json_data.get("recovery_duration_days")
            or json_data.get("recovery_timeframe_days")
            or (json_data.get("mttr_hours") / 24.0 if json_data.get("mttr_hours") else None)
        )
        
        # Ransom amount handling
        ransom_amt = json_data.get("ransom_amount_exact") or json_data.get("ransom_amount")
        ransom_amt_str = str(ransom_amt) if ransom_amt is not None else None
        
        # Business impact - prefer canonical field, then derive from legacy severity scale.
        business_impact = normalize_business_impact(json_data.get("business_impact"))
        if business_impact is None:
            severity_val = json_data.get("business_impact_severity")
            if isinstance(severity_val, str):
                business_impact = {
                    "catastrophic": "critical",
                    "critical": "critical",
                    "major": "severe",
                    "moderate": "moderate",
                    "minor": "limited",
                    "negligible": "minimal",
                }.get(severity_val.strip().lower())
        
        # Attack chain from extraction schema - NORMALIZE to valid enum values
        raw_attack_chain = json_data.get("attack_chain")
        attack_chain = normalize_attack_chain(raw_attack_chain)
        
        # Operational impact from extraction schema - normalize to valid enum list
        operational_impact = normalize_operational_impact(json_data.get("operational_impact"))

        _FALSE_STRINGS = {"false", "no", "0", "none", "null", "not mentioned", "unknown"}

        def _to_bool(v):
            """Coerce any LLM value to Optional[bool]. Integers/strings → bool; None → None."""
            if v is None or isinstance(v, bool):
                return v
            if isinstance(v, (int, float)):
                return v > 0
            if isinstance(v, str):
                s = v.strip().lower()
                if s in ("true", "yes", "1"):
                    return True
                if s in _FALSE_STRINGS:
                    return False
                return None  # Unrecognised string → null
            return bool(v)

        def _agency_to_bool(v):
            """For law-enforcement boolean fields: any non-empty non-false string → True.

            The LLM sometimes outputs an agency name ('fbi', 'cisa') instead of true.
            Any non-false string value is treated as True since the intent is clear.
            """
            if v is None or isinstance(v, bool):
                return v
            if isinstance(v, (int, float)):
                return v > 0
            if isinstance(v, str):
                s = v.strip().lower()
                if s in _FALSE_STRINGS or s == "":
                    return False
                return True  # non-empty, non-false → the LLM meant True
            return bool(v)

        attack_dynamics = AttackDynamics(
            attack_vector=final_attack_vector,
            attack_chain=attack_chain,
            ransomware_family=json_data.get("ransomware_family_or_group"),
            data_exfiltration=data_exfil,
            encryption_impact=encryption_impact,
            impact_scope=None,
            ransom_demanded=_to_bool(json_data.get("was_ransom_demanded")),
            ransom_amount=ransom_amt_str,
            ransom_paid=_to_bool(json_data.get("ransom_paid")),
            recovery_timeframe_days=recovery_days,
            business_impact=business_impact,
            operational_impact=operational_impact
        )
    
    # System impact (as Dict)
    system_impact = None
    # Schema field is "systems_affected"; "systems_affected_codes" is a legacy fallback
    systems_affected_codes = json_data.get("systems_affected") or json_data.get("systems_affected_codes") or []
    if systems_affected_codes:
        mapped_systems = map_systems_affected_codes(systems_affected_codes)
        # Check both raw LLM values (schema-compliant) and mapped canonical values
        # (LLM sometimes returns mapped names like "network_infrastructure" instead of "core_network")
        _sac = set(systems_affected_codes)
        _mapped_sac = set(mapped_systems) if mapped_systems else set()
        system_impact = {
            "systems_affected": mapped_systems,
            "critical_systems_affected": len(systems_affected_codes) > 0,
            "network_compromised": bool({"core_network", "wifi_network", "data_center"} & _sac)
                                   or "network_infrastructure" in _mapped_sac,
            "email_system_affected": "email_system" in _sac or "email_system" in _mapped_sac,
            "student_portal_affected": bool({"student_portal", "sis_student_information", "staff_portal"} & _sac)
                                       or "student_portal" in _mapped_sac,
            "research_systems_affected": bool({"research_computing_hpc", "research_storage", "lab_instruments", "research_databases"} & _sac)
                                         or "research_systems" in _mapped_sac,
            "hospital_systems_affected": bool({"hospital_systems", "ehr_emr", "medical_devices", "pharmacy_system"} & _sac)
                                         or "hospital_systems" in _mapped_sac,
            # Infer cloud involvement from cloud_provider field or cloud-related attack vectors
            "cloud_services_affected": bool(
                (json_data.get("cloud_provider") and str(json_data.get("cloud_provider", "")).lower() not in ("none", "unknown", ""))
                or bool({"cloud_misconfiguration", "api_key_exposure", "storage_bucket_exposure"} & set(json_data.get("attack_vector", []) if isinstance(json_data.get("attack_vector"), list) else [json_data.get("attack_vector", "")]))
                or bool({"cloud_services", "cloud_storage", "cloud_provider"} & _sac)
            ),
            "third_party_vendor_impact": json_data.get("third_parties_involved") is not None and len(json_data.get("third_parties_involved", [])) > 0,
            "vendor_name": ", ".join(json_data.get("third_parties_involved", [])) if json_data.get("third_parties_involved") else None
        }
    
    # Data impact (as Dict)
    data_impact = None
    # Schema uses "data_categories"; "data_types" is a legacy fallback
    _data_cats = json_data.get("data_categories") or json_data.get("data_types") or []
    if json_data.get("data_breached") or _data_cats:
        data_types_dict = map_data_types(_data_cats)
        data_impact = {
            "personal_information": data_types_dict.get("personal_information"),
            "student_data": data_types_dict.get("student_data"),
            "faculty_data": data_types_dict.get("faculty_data"),
            "alumni_data": data_types_dict.get("alumni_data"),
            "financial_data": data_types_dict.get("financial_data"),
            "research_data": data_types_dict.get("research_data"),
            "intellectual_property": None,
            "medical_records": data_types_dict.get("medical_records"),
            "administrative_data": data_types_dict.get("administrative_data"),
            "records_affected_min": json_data.get("records_affected_min"),
            "records_affected_max": json_data.get("records_affected_max"),
            "records_affected_exact": json_data.get("pii_records_leaked") or json_data.get("records_exfiltrated_estimate") or json_data.get("records_affected_exact"),
            "data_types_affected": _data_cats,
            "data_encrypted": json_data.get("encryption_at_rest") == "yes",
            "data_exfiltrated": json_data.get("data_breached") or json_data.get("data_exfiltrated")
        }
    
    # User impact (as Dict) - preserve actual counts, not booleans
    user_impact = None
    if (json_data.get("students_affected") is not None or 
        json_data.get("staff_affected") is not None or 
        json_data.get("faculty_affected") is not None or
        json_data.get("alumni_affected") is not None or
        json_data.get("users_affected_exact") is not None):
        
        # Calculate users_affected_exact if not provided
        users_exact = json_data.get("users_affected_exact")
        if users_exact is None:
            users_exact = json_data.get("total_individuals_affected")
        if users_exact is None:
            # Sum up known affected counts
            total = 0
            if json_data.get("students_affected"):
                total += json_data.get("students_affected", 0)
            if json_data.get("staff_affected"):
                total += json_data.get("staff_affected", 0)
            if json_data.get("faculty_affected") and isinstance(json_data.get("faculty_affected"), int):
                total += json_data.get("faculty_affected", 0)
            if total > 0:
                users_exact = total
        
        user_impact = {
            "students_affected": json_data.get("students_affected") is not None and json_data.get("students_affected") > 0 if isinstance(json_data.get("students_affected"), (int, float)) else json_data.get("students_affected"),
            "faculty_affected": json_data.get("faculty_affected"),
            "staff_affected": json_data.get("staff_affected") is not None and json_data.get("staff_affected") > 0 if isinstance(json_data.get("staff_affected"), (int, float)) else json_data.get("staff_affected"),
            "alumni_affected": json_data.get("alumni_affected"),
            "parents_affected": json_data.get("parents_affected"),
            "applicants_affected": json_data.get("applicants_affected"),
            "patients_affected": json_data.get("patients_affected"),
            "users_affected_min": json_data.get("users_affected_min"),
            "users_affected_max": json_data.get("users_affected_max"),
            "users_affected_exact": users_exact,
            # Store actual counts for CSV export
            "students_affected_count": json_data.get("students_affected"),
            "staff_affected_count": json_data.get("staff_affected"),
            "faculty_affected_count": json_data.get("faculty_affected") if isinstance(json_data.get("faculty_affected"), int) else None,
        }
    
    # Operational impact metrics (as Dict)
    # Schema field is "operational_impacts" (plural); "operational_impact" is a legacy alias.
    operational_impact_metrics = None
    op_impacts = json_data.get("operational_impacts") or json_data.get("operational_impact") or []
    if not isinstance(op_impacts, list):
        op_impacts = []
    if (json_data.get("teaching_impacted") is not None or json_data.get("research_impacted") is not None or
        json_data.get("outage_duration_hours") is not None or json_data.get("downtime_days") is not None or
        op_impacts):
        # Note: schema enum for operational_impacts has no "teaching_disrupted" value.
        # Derive it from the values that imply teaching was affected.
        _teaching_ops = {"classes_cancelled", "classes_moved_online", "semester_extended", "lms_unavailable"}
        _research_ops = {"research_halted", "research_data_lost"}
        _admissions_ops = {"admissions_suspended"}
        _payroll_ops = {"payroll_delayed"}
        _enrollment_ops = {"registration_suspended"}
        operational_impact_metrics = {
            "teaching_disrupted": (bool(json_data.get("teaching_impacted")) or bool(json_data.get("teaching_disrupted"))
                                   or bool(_teaching_ops & set(op_impacts))),
            "research_disrupted": (bool(json_data.get("research_impacted")) or bool(json_data.get("research_disrupted"))
                                   or bool(_research_ops & set(op_impacts))),
            "admissions_disrupted": (bool(json_data.get("admissions_disrupted"))
                                     or bool(_admissions_ops & set(op_impacts))),
            "payroll_disrupted": (bool(json_data.get("payroll_disrupted"))
                                  or bool(_payroll_ops & set(op_impacts))),
            "enrollment_disrupted": (bool(json_data.get("enrollment_disrupted"))
                                     or bool(_enrollment_ops & set(op_impacts))),
            "clinical_operations_disrupted": (bool(json_data.get("clinical_operations_disrupted"))
                                              or "clinical_operations_disrupted" in op_impacts),
            "online_learning_disrupted": (bool(json_data.get("online_learning_disrupted"))
                                          or "classes_moved_online" in op_impacts),
            "downtime_days": json_data.get("downtime_days") or (json_data.get("outage_duration_hours", 0) / 24.0 if json_data.get("outage_duration_hours") else None),
            "partial_service_days": json_data.get("partial_service_days"),
            "classes_cancelled": (bool(json_data.get("classes_cancelled"))
                                  or "classes_cancelled" in op_impacts or "exams_cancelled" in op_impacts),
            "exams_postponed": bool(json_data.get("exams_postponed")) or "exams_postponed" in op_impacts,
            "graduation_delayed": bool(json_data.get("graduation_delayed")) or "graduation_delayed" in op_impacts,
        }
    
    # Financial impact (as Dict) - capture all financial fields
    financial_impact = None
    has_financial_data = (
        json_data.get("was_ransom_demanded") is not None or 
        json_data.get("ransom_amount") is not None or 
        json_data.get("ransom_amount_exact") is not None or
        json_data.get("ransom_paid") is not None or
        json_data.get("currency_normalized_cost_usd") is not None or 
        json_data.get("recovery_costs_min") is not None or
        json_data.get("recovery_costs_max") is not None or
        json_data.get("legal_costs") is not None or
        json_data.get("notification_costs") is not None or
        json_data.get("insurance_claim") is not None or
        json_data.get("insurance_claim_amount") is not None
    )
    if has_financial_data:
        # Get ransom amount - could be in different fields
        ransom_amount = json_data.get("ransom_amount_exact") or json_data.get("ransom_amount")
        ransom_paid_amount = json_data.get("ransom_paid_amount")
        if ransom_paid_amount is None and json_data.get("ransom_paid") and ransom_amount:
            ransom_paid_amount = ransom_amount
        
        # Schema uses *_usd suffixed names for cost fields; legacy aliases kept
        financial_impact = {
            "ransom_demanded": json_data.get("was_ransom_demanded"),
            "ransom_amount_min": json_data.get("ransom_amount_min"),
            "ransom_amount_max": json_data.get("ransom_amount_max"),
            "ransom_amount_exact": ransom_amount,
            "ransom_currency": json_data.get("ransom_currency") or "USD",
            "ransom_paid": json_data.get("ransom_paid"),
            "ransom_paid_amount": ransom_paid_amount,
            # Schema: "recovery_cost_usd" (single); map to min as best estimate
            "recovery_costs_min": json_data.get("recovery_costs_min") or json_data.get("recovery_cost_usd"),
            "recovery_costs_max": json_data.get("recovery_costs_max"),
            # Schema: "legal_cost_usd"
            "legal_costs": json_data.get("legal_costs") or json_data.get("legal_cost_usd"),
            # Schema: "notification_cost_usd"
            "notification_costs": json_data.get("notification_costs") or json_data.get("notification_cost_usd"),
            "credit_monitoring_costs": json_data.get("credit_monitoring_costs") or json_data.get("credit_monitoring_cost_usd"),
            "insurance_claim": json_data.get("insurance_claim"),
            # Schema: "insurance_payout_usd"
            "insurance_claim_amount": json_data.get("insurance_claim_amount") or json_data.get("insurance_payout_usd"),
            "total_cost_estimate": json_data.get("currency_normalized_cost_usd") or json_data.get("estimated_total_cost_usd"),
        }
    
    # Regulatory impact (as Dict) - comprehensive regulatory data
    regulatory_impact = None
    # Schema uses "applicable_regulations" array; "regulatory_context" is a legacy alias
    applicable_regs = json_data.get("applicable_regulations") or json_data.get("regulatory_context") or []
    has_regulatory_data = (
        applicable_regs or
        json_data.get("fines_or_penalties") is not None or
        json_data.get("class_actions_or_lawsuits") is not None or
        json_data.get("gdpr_breach") is not None or
        json_data.get("hipaa_breach") is not None or
        json_data.get("ferpa_breach") is not None or
        json_data.get("breach_notification_required") is not None or
        json_data.get("fine_imposed") is not None or
        json_data.get("fine_amount_usd") is not None or
        json_data.get("lawsuits_filed") is not None or
        json_data.get("class_action_filed") is not None
    )
    if has_regulatory_data:
        # Derive breach flags from applicable_regulations array (schema) or direct booleans
        _regs_set = set(applicable_regs)
        gdpr_breach = json_data.get("gdpr_breach")
        if gdpr_breach is None:
            gdpr_breach = "GDPR" in _regs_set or None

        hipaa_breach = json_data.get("hipaa_breach")
        if hipaa_breach is None:
            hipaa_breach = "HIPAA" in _regs_set or None

        ferpa_breach = json_data.get("ferpa_breach")
        if ferpa_breach is None:
            ferpa_breach = "FERPA" in _regs_set or None

        dpa_notified = json_data.get("dpa_notified")
        if dpa_notified is None:
            dpa_notified = "UK_DPA" in _regs_set or None

        notification_dates = json_data.get("notification_dates") or {}
        # Schema field: notification_sent_date (singular); accept plural alias too
        notifications_sent_date = (json_data.get("notification_sent_date")
                                   or json_data.get("notifications_sent_date")
                                   or notification_dates.get("data_subjects_notified_date"))
        regulators_notified_date = json_data.get("regulators_notified_date") or notification_dates.get("regulator_notified_date")

        regulatory_impact = {
            "breach_notification_required": json_data.get("breach_notification_required"),
            # Schema uses "notification_sent" (singular); legacy alias "notifications_sent"
            "notifications_sent": (json_data.get("notification_sent")
                                   if json_data.get("notification_sent") is not None
                                   else json_data.get("notifications_sent")),
            "notifications_sent_date": notifications_sent_date,
            "regulators_notified": json_data.get("regulators_notified") or applicable_regs,
            "regulators_notified_date": regulators_notified_date,
            "gdpr_breach": gdpr_breach,
            "dpa_notified": dpa_notified,
            "hipaa_breach": hipaa_breach,
            "ferpa_breach": ferpa_breach,
            "investigation_opened": json_data.get("investigation_opened"),
            "fine_imposed": json_data.get("fine_imposed") if json_data.get("fine_imposed") is not None else (json_data.get("fines_or_penalties") is not None and len(str(json_data.get("fines_or_penalties", ""))) > 0),
            # Schema uses "fine_amount_usd"; legacy alias "fine_amount"
            "fine_amount": json_data.get("fine_amount_usd") or json_data.get("fine_amount"),
            "fines_or_penalties_description": json_data.get("fines_or_penalties"),
            "lawsuits_filed": json_data.get("lawsuits_filed") if json_data.get("lawsuits_filed") is not None else (json_data.get("class_actions_or_lawsuits") is not None and len(str(json_data.get("class_actions_or_lawsuits", ""))) > 0),
            "lawsuit_count": json_data.get("lawsuit_count"),
            # Schema uses "class_action_filed"; legacy alias "class_action"
            "class_action": (json_data.get("class_action_filed") if json_data.get("class_action_filed") is not None
                             else json_data.get("class_action") if json_data.get("class_action") is not None
                             else (json_data.get("class_actions_or_lawsuits") is not None and "class" in str(json_data.get("class_actions_or_lawsuits", "")).lower())),
            "class_actions_or_lawsuits_description": json_data.get("class_actions_or_lawsuits"),
            "regulatory_context": applicable_regs,
        }
    
    # Recovery metrics (as Dict) - comprehensive recovery data
    recovery_metrics = None
    has_recovery_data = (
        json_data.get("backup_status") is not None or 
        json_data.get("service_restoration_date") is not None or
        json_data.get("response_actions") is not None or
        json_data.get("recovery_started_date") is not None or
        json_data.get("recovery_completed_date") is not None or
        # Schema uses "recovery_duration_days"; legacy alias is "recovery_timeframe_days"
        json_data.get("recovery_duration_days") is not None or
        json_data.get("recovery_timeframe_days") is not None or
        json_data.get("mttr_hours") is not None or
        # Schema uses "ir_firm_engaged" / "forensics_firm_engaged"; legacy aliases kept
        json_data.get("ir_firm_engaged") is not None or
        json_data.get("incident_response_firm") is not None or
        json_data.get("forensics_firm_engaged") is not None or
        json_data.get("forensics_firm") is not None or
        json_data.get("mfa_implemented") is not None or
        json_data.get("security_improvements") is not None or
        json_data.get("response_measures") is not None or
        json_data.get("from_backup") is not None or
        json_data.get("recovery_method") is not None
    )
    if has_recovery_data:
        # Calculate recovery timeframe — schema calls it "recovery_duration_days"
        recovery_days = (
            json_data.get("recovery_duration_days")
            or json_data.get("recovery_timeframe_days")
            or (json_data.get("mttr_hours") / 24.0 if json_data.get("mttr_hours") else None)
        )

        # from_backup — schema uses "recovery_method" enum with "backup_restore"
        from_backup = json_data.get("from_backup")
        if from_backup is None:
            recovery_method = json_data.get("recovery_method", "")
            if recovery_method in ("backup_restore", "partial_backup_partial_rebuild"):
                from_backup = True
            elif json_data.get("backup_status"):
                from_backup = json_data.get("backup_status") == "available_and_used"

        recovery_metrics = {
            "recovery_started_date": json_data.get("recovery_started_date"),
            "recovery_completed_date": json_data.get("recovery_completed_date") or json_data.get("service_restoration_date"),
            "recovery_timeframe_days": recovery_days,
            "recovery_phases": json_data.get("recovery_phases"),
            "from_backup": from_backup,
            "backup_status": json_data.get("backup_status"),
            "backup_age_days": json_data.get("backup_age_days"),
            "clean_rebuild": json_data.get("clean_rebuild"),
            # Schema uses "ir_firm_engaged"; legacy alias "incident_response_firm"
            "incident_response_firm": json_data.get("ir_firm_engaged") or json_data.get("incident_response_firm"),
            # Schema uses "forensics_firm_engaged"; legacy alias "forensics_firm"
            "forensics_firm": json_data.get("forensics_firm_engaged") or json_data.get("forensics_firm"),
            "law_firm": json_data.get("law_firm") or json_data.get("legal_counsel_engaged"),
            "security_improvements": json_data.get("security_improvements") or json_data.get("response_actions"),
            # Schema: mfa_implemented is a value in security_improvements array, not a standalone field
            "mfa_implemented": json_data.get("mfa_implemented") or "mfa_implemented" in (json_data.get("security_improvements") or []) or "mfa_expanded" in (json_data.get("security_improvements") or []),
            "security_training_conducted": json_data.get("security_training_conducted"),
            "response_measures": json_data.get("response_measures"),
            "law_enforcement_involved": _agency_to_bool(json_data.get("law_enforcement_involved")),
            "law_enforcement_agency": (
                ", ".join(v for v in (json_data.get("law_enforcement_agencies") or []) if isinstance(v, str) and v.strip())
                if isinstance(json_data.get("law_enforcement_agencies"), list)
                else json_data.get("law_enforcement_agencies") or json_data.get("law_enforcement_agency")
            ),
            "mttd_hours": json_data.get("mttd_hours"),
            "mttr_hours": json_data.get("mttr_hours"),
        }
    
    # Transparency metrics (as Dict) - comprehensive disclosure data
    transparency_metrics = None
    has_transparency_data = (
        json_data.get("was_disclosed_publicly") is not None or 
        json_data.get("disclosure_source") is not None or
        json_data.get("public_disclosure") is not None or
        json_data.get("public_disclosure_date") is not None or
        json_data.get("disclosure_delay_days") is not None or
        json_data.get("transparency_level") is not None or
        json_data.get("official_statement_url") is not None or
        json_data.get("update_count") is not None
    )
    if has_transparency_data:
        transparency_metrics = {
            "disclosure_timeline": json_data.get("disclosure_timeline"),
            "public_disclosure": json_data.get("public_disclosure") if json_data.get("public_disclosure") is not None else json_data.get("was_disclosed_publicly"),
            "public_disclosure_date": json_data.get("public_disclosure_date"),
            "disclosure_delay_days": json_data.get("disclosure_delay_days"),
            "transparency_level": json_data.get("transparency_level"),
            "official_statement_url": json_data.get("official_statement_url"),
            "detailed_report_url": json_data.get("detailed_report_url"),
            "updates_provided": json_data.get("updates_provided"),
            "update_count": json_data.get("update_count"),
            "disclosure_source": json_data.get("disclosure_source"),
            "notification_dates": json_data.get("notification_dates"),
        }
    
    # Research impact (as Dict) - comprehensive research data
    research_impact = None
    has_research_data = (
        json_data.get("research_impacted") is not None or 
        json_data.get("research_projects_affected") is not None or
        json_data.get("research_data_compromised") is not None or
        json_data.get("research_impact_code") is not None or
        json_data.get("research_area") is not None or
        json_data.get("publications_delayed") is not None or
        json_data.get("grants_affected") is not None
    )
    if has_research_data:
        research_impact = {
            "research_projects_affected": json_data.get("research_projects_affected") if json_data.get("research_projects_affected") is not None else json_data.get("research_impacted"),
            "research_data_compromised": json_data.get("research_data_compromised") if json_data.get("research_data_compromised") is not None else (json_data.get("research_impact_code") in ["data_loss", "data_unavailable"] if json_data.get("research_impact_code") else None),
            "sensitive_research_impact": json_data.get("sensitive_research_impact"),
            "publications_delayed": json_data.get("publications_delayed"),
            "grants_affected": json_data.get("grants_affected"),
            "collaborations_affected": json_data.get("collaborations_affected"),
            "research_area": json_data.get("research_area"),
            "research_impact_code": json_data.get("research_impact_code"),
            "research_impact_notes_en": json_data.get("research_impact_notes_en"),
        }
    
    # Threat intelligence: vulnerabilities_exploited
    vulnerabilities_exploited = None
    raw_vulns = json_data.get("vulnerabilities_exploited")
    if isinstance(raw_vulns, list) and raw_vulns:
        cleaned_vulns = []
        for v in raw_vulns:
            if isinstance(v, dict) and any(v.get(k) for k in ("cve_id", "vulnerability_name", "affected_product")):
                cleaned_vulns.append({
                    "cve_id": v.get("cve_id"),
                    "vulnerability_name": v.get("vulnerability_name"),
                    "vulnerability_type": v.get("vulnerability_type"),
                    "affected_product": v.get("affected_product"),
                    "cvss_score": v.get("cvss_score"),
                })
        vulnerabilities_exploited = cleaned_vulns if cleaned_vulns else None

    # Threat intelligence: malware families and tools
    malware_families = json_data.get("malware_families") or None
    if isinstance(malware_families, list) and not malware_families:
        malware_families = None

    attacker_tools = json_data.get("attacker_tools") or None
    if isinstance(attacker_tools, list) and not attacker_tools:
        attacker_tools = None

    threat_actor_aliases = json_data.get("threat_actor_aliases") or None
    if isinstance(threat_actor_aliases, list) and not threat_actor_aliases:
        threat_actor_aliases = None

    return CTIEnrichmentResult(
        education_relevance=education_relevance,
        primary_url=primary_url or json_data.get("source_url"),
        initial_access_description=json_data.get("initial_access_description"),
        timeline=timeline,
        mitre_attack_techniques=mitre_techniques,
        attack_dynamics=attack_dynamics,
        data_impact=data_impact,
        system_impact=system_impact,
        user_impact=user_impact,
        operational_impact_metrics=operational_impact_metrics,
        financial_impact=financial_impact,
        regulatory_impact=regulatory_impact,
        recovery_metrics=recovery_metrics,
        transparency_metrics=transparency_metrics,
        research_impact=research_impact,
        vulnerabilities_exploited=vulnerabilities_exploited,
        malware_families=malware_families,
        attacker_tools=attacker_tools,
        dwell_time_days=json_data.get("dwell_time_days"),
        cloud_provider=json_data.get("cloud_provider"),
        infrastructure_type=json_data.get("infrastructure_type"),
        threat_actor_aliases=threat_actor_aliases,
        attack_campaign_name=json_data.get("attack_campaign_name"),
        data_volume_gb=json_data.get("data_volume_gb"),
        enriched_summary=_build_summary(json_data),
        extraction_notes=extraction_notes,
        other_edu_incidents=json_data.get("other_edu_incidents") or None,
        attack_chain=normalize_attack_chain(json_data.get("attack_chain")),
    )
