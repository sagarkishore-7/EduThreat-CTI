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
        "cloud_misconfiguration", "api_key_exposure", "storage_bucket_exposure", "phishing",
        "spear_phishing", "vulnerability_exploit", "credential_theft", "malware", "ransomware",
        "insider_threat", "supply_chain", "third_party_breach", "ddos", "dns_hijacking",
        "sim_swapping", "unknown", "other"
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


def map_attack_category_to_vector(category: str) -> Optional[str]:
    """Map attack category to attack vector (handles extensive new categories)."""
    if not category:
        return None
    
    # Normalize to lowercase for matching
    category_lower = category.lower()
    
    # Ransomware variants
    if "ransomware" in category_lower:
        return "ransomware"
    
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


def map_initial_access_vector(access_vector: str) -> Optional[str]:
    """Map initial access vector."""
    mapping = {
        "phishing": "phishing",
        "stolen_credentials": "credential_theft",
        "brute_force": "brute_force",
        "exposed_service": "vulnerability_exploit",
        "supply_chain": "supply_chain",
        "malicious_attachment": "phishing",
        "drive_by": "vulnerability_exploit",
        "vulnerability_exploit": "vulnerability_exploit",
        "unknown": None,
        "other": "other",
    }
    return mapping.get(access_vector)


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
        "grades_transcripts": "student_data",
        "special_category_gdpr": "personal_information",
        "other": "administrative_data",
    }
    
    for dt in data_types:
        mapped = type_mapping.get(dt, "administrative_data")
        result[mapped] = True
    
    return result


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
    education_relevance = EducationRelevanceCheck(
        is_education_related=json_data.get("is_edu_cyber_incident", False),
        reasoning=json_data.get("education_relevance_reasoning", "") or "No reasoning provided by LLM",
        institution_identified=json_data.get("institution_name")
    )
    
    # Timeline - normalize event_type values
    timeline = None
    if json_data.get("timeline"):
        timeline = [
            TimelineEvent(
                date=event.get("date"),
                date_precision=event.get("date_precision"),
                event_description=event.get("event_description"),
                event_type=normalize_event_type(event.get("event_type")),
                actor_attribution=event.get("actor_attribution"),
                indicators=event.get("indicators")
            )
            for event in json_data["timeline"]
        ]
    
    # MITRE ATT&CK techniques
    mitre_techniques = None
    if json_data.get("mitre_attack_techniques"):
        mitre_techniques = [
            MITREAttackTechnique(
                technique_id=tech if isinstance(tech, str) else tech.get("technique_id"),
                technique_name=tech.get("technique_name") if isinstance(tech, dict) else None,
                tactic=None,
                description=None,
                sub_techniques=None
            )
            for tech in json_data["mitre_attack_techniques"]
        ]
    
    # Attack dynamics - capture all attack-related fields
    attack_dynamics = None
    attack_category = json_data.get("attack_category")
    attack_vector = json_data.get("attack_vector")  # Direct attack_vector from schema
    
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
        
        # Recovery timeframe - check multiple sources
        recovery_days = json_data.get("recovery_timeframe_days")
        if recovery_days is None and json_data.get("mttr_hours"):
            recovery_days = json_data.get("mttr_hours") / 24.0
        
        # Ransom amount handling
        ransom_amt = json_data.get("ransom_amount_exact") or json_data.get("ransom_amount")
        ransom_amt_str = str(ransom_amt) if ransom_amt is not None else None
        
        # Business impact - normalize to valid enum
        business_impact = normalize_business_impact(json_data.get("business_impact"))
        
        # Attack chain from extraction schema - NORMALIZE to valid enum values
        raw_attack_chain = json_data.get("attack_chain")
        attack_chain = normalize_attack_chain(raw_attack_chain)
        
        # Operational impact from extraction schema - normalize to valid enum list
        operational_impact = normalize_operational_impact(json_data.get("operational_impact"))
        
        attack_dynamics = AttackDynamics(
            attack_vector=final_attack_vector,
            attack_chain=attack_chain,
            ransomware_family=json_data.get("ransomware_family_or_group"),
            data_exfiltration=data_exfil,
            encryption_impact=encryption_impact,
            impact_scope=None,
            ransom_demanded=json_data.get("was_ransom_demanded"),
            ransom_amount=ransom_amt_str,
            ransom_paid=json_data.get("ransom_paid"),
            recovery_timeframe_days=recovery_days,
            business_impact=business_impact,
            operational_impact=operational_impact
        )
    
    # System impact (as Dict)
    system_impact = None
    systems_affected_codes = json_data.get("systems_affected_codes", [])
    if systems_affected_codes:
        mapped_systems = map_systems_affected_codes(systems_affected_codes)
        system_impact = {
            "systems_affected": mapped_systems,
            "critical_systems_affected": len(systems_affected_codes) > 0,
            "network_compromised": "network" in str(systems_affected_codes).lower() or "wifi" in str(systems_affected_codes).lower() or "wired" in str(systems_affected_codes).lower(),
            "email_system_affected": "email" in systems_affected_codes,
            "student_portal_affected": "portal_student_staff" in systems_affected_codes or "sis" in systems_affected_codes,
            "research_systems_affected": "research_hpc" in systems_affected_codes or "research_lab_instruments" in systems_affected_codes,
            "hospital_systems_affected": None,
            "cloud_services_affected": "cloud_storage" in systems_affected_codes,
            "third_party_vendor_impact": json_data.get("third_parties_involved") is not None and len(json_data.get("third_parties_involved", [])) > 0,
            "vendor_name": ", ".join(json_data.get("third_parties_involved", [])) if json_data.get("third_parties_involved") else None
        }
    
    # Data impact (as Dict)
    data_impact = None
    if json_data.get("data_breached") or json_data.get("data_types"):
        data_types_dict = map_data_types(json_data.get("data_types", []))
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
            "data_types_affected": json_data.get("data_types"),
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
    operational_impact_metrics = None
    if (json_data.get("teaching_impacted") is not None or json_data.get("research_impacted") is not None or 
        json_data.get("outage_duration_hours") is not None or json_data.get("downtime_days") is not None or
        json_data.get("operational_impact")):
        # Ensure operational_impact is always a list (LLM may return None)
        operational_impact = json_data.get("operational_impact") or []
        if not isinstance(operational_impact, list):
            operational_impact = []
        operational_impact_metrics = {
            "teaching_disrupted": json_data.get("teaching_impacted", False) or json_data.get("teaching_disrupted") or "teaching_disrupted" in operational_impact,
            "research_disrupted": json_data.get("research_impacted", False) or json_data.get("research_disrupted") or "research_disrupted" in operational_impact,
            "admissions_disrupted": json_data.get("admissions_disrupted") or "admissions_disrupted" in operational_impact,
            "payroll_disrupted": json_data.get("payroll_disrupted") or "payroll_disrupted" in operational_impact,
            "enrollment_disrupted": json_data.get("enrollment_disrupted") or "enrollment_disrupted" in operational_impact,
            "clinical_operations_disrupted": json_data.get("clinical_operations_disrupted") or "clinical_operations_disrupted" in operational_impact,
            "online_learning_disrupted": json_data.get("teaching_impact_code") == "online_platform_down" or "online_learning_disrupted" in operational_impact,
            "downtime_days": json_data.get("downtime_days") or (json_data.get("outage_duration_hours", 0) / 24.0 if json_data.get("outage_duration_hours") else None),
            "partial_service_days": json_data.get("partial_service_days"),
            "classes_cancelled": json_data.get("teaching_impact_code") == "class_cancellation" or json_data.get("classes_cancelled") or "classes_cancelled" in operational_impact,
            "exams_postponed": json_data.get("teaching_impact_code") == "exam_disruption" or json_data.get("exams_postponed") or "exams_postponed" in operational_impact,
            "graduation_delayed": json_data.get("graduation_delayed") or "graduation_delayed" in operational_impact
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
        
        financial_impact = {
            "ransom_demanded": json_data.get("was_ransom_demanded"),
            "ransom_amount_min": json_data.get("ransom_amount_min"),
            "ransom_amount_max": json_data.get("ransom_amount_max"),
            "ransom_amount_exact": ransom_amount,
            "ransom_currency": json_data.get("ransom_currency") or "USD",  # Default to USD if not specified
            "ransom_paid": json_data.get("ransom_paid"),
            "ransom_paid_amount": ransom_paid_amount,
            "recovery_costs_min": json_data.get("recovery_costs_min"),
            "recovery_costs_max": json_data.get("recovery_costs_max"),
            "legal_costs": json_data.get("legal_costs"),
            "notification_costs": json_data.get("notification_costs"),
            "credit_monitoring_costs": json_data.get("credit_monitoring_costs"),
            "insurance_claim": json_data.get("insurance_claim"),
            "insurance_claim_amount": json_data.get("insurance_claim_amount"),
            "total_cost_estimate": json_data.get("currency_normalized_cost_usd"),
        }
    
    # Regulatory impact (as Dict) - comprehensive regulatory data
    regulatory_impact = None
    regulatory_context = json_data.get("regulatory_context") or []
    has_regulatory_data = (
        regulatory_context or
        json_data.get("fines_or_penalties") is not None or 
        json_data.get("class_actions_or_lawsuits") is not None or 
        json_data.get("gdpr_breach") is not None or
        json_data.get("hipaa_breach") is not None or
        json_data.get("ferpa_breach") is not None or
        json_data.get("breach_notification_required") is not None or
        json_data.get("fine_imposed") is not None or
        json_data.get("lawsuits_filed") is not None
    )
    if has_regulatory_data:
        # Determine breach types from regulatory_context if not explicitly set
        gdpr_breach = json_data.get("gdpr_breach")
        if gdpr_breach is None and regulatory_context:
            gdpr_breach = "GDPR" in regulatory_context
        
        hipaa_breach = json_data.get("hipaa_breach")
        if hipaa_breach is None and regulatory_context:
            hipaa_breach = "HIPAA" in regulatory_context
        
        ferpa_breach = json_data.get("ferpa_breach")
        if ferpa_breach is None and regulatory_context:
            ferpa_breach = "FERPA" in regulatory_context
        
        dpa_notified = json_data.get("dpa_notified")
        if dpa_notified is None and regulatory_context:
            dpa_notified = "UK_DPA" in regulatory_context
        
        # Notification dates from nested object or direct fields
        notification_dates = json_data.get("notification_dates") or {}
        notifications_sent_date = json_data.get("notifications_sent_date") or notification_dates.get("data_subjects_notified_date")
        regulators_notified_date = json_data.get("regulators_notified_date") or notification_dates.get("regulator_notified_date")
        
        regulatory_impact = {
            "breach_notification_required": json_data.get("breach_notification_required"),
            "notifications_sent": json_data.get("notifications_sent"),
            "notifications_sent_date": notifications_sent_date,
            "regulators_notified": json_data.get("regulators_notified") or regulatory_context,
            "regulators_notified_date": regulators_notified_date,
            "gdpr_breach": gdpr_breach,
            "dpa_notified": dpa_notified,
            "hipaa_breach": hipaa_breach,
            "ferc_breach": ferpa_breach,  # Note: using ferc_breach key for FERPA (matching existing schema)
            "investigation_opened": json_data.get("investigation_opened"),
            "fine_imposed": json_data.get("fine_imposed") if json_data.get("fine_imposed") is not None else (json_data.get("fines_or_penalties") is not None and len(str(json_data.get("fines_or_penalties", ""))) > 0),
            "fine_amount": json_data.get("fine_amount"),
            "fines_or_penalties_description": json_data.get("fines_or_penalties"),
            "lawsuits_filed": json_data.get("lawsuits_filed") if json_data.get("lawsuits_filed") is not None else (json_data.get("class_actions_or_lawsuits") is not None and len(str(json_data.get("class_actions_or_lawsuits", ""))) > 0),
            "lawsuit_count": json_data.get("lawsuit_count"),
            "class_action": json_data.get("class_action") if json_data.get("class_action") is not None else (json_data.get("class_actions_or_lawsuits") is not None and "class" in str(json_data.get("class_actions_or_lawsuits", "")).lower()),
            "class_actions_or_lawsuits_description": json_data.get("class_actions_or_lawsuits"),
            "regulatory_context": regulatory_context,
        }
    
    # Recovery metrics (as Dict) - comprehensive recovery data
    recovery_metrics = None
    has_recovery_data = (
        json_data.get("backup_status") is not None or 
        json_data.get("service_restoration_date") is not None or 
        json_data.get("response_actions") is not None or 
        json_data.get("recovery_started_date") is not None or 
        json_data.get("recovery_completed_date") is not None or
        json_data.get("recovery_timeframe_days") is not None or
        json_data.get("mttr_hours") is not None or
        json_data.get("incident_response_firm") is not None or
        json_data.get("forensics_firm") is not None or
        json_data.get("mfa_implemented") is not None or
        json_data.get("response_measures") is not None or
        json_data.get("from_backup") is not None
    )
    if has_recovery_data:
        # Calculate recovery timeframe
        recovery_days = json_data.get("recovery_timeframe_days")
        if recovery_days is None and json_data.get("mttr_hours"):
            recovery_days = json_data.get("mttr_hours") / 24.0
        
        # Determine from_backup
        from_backup = json_data.get("from_backup")
        if from_backup is None and json_data.get("backup_status"):
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
            "incident_response_firm": json_data.get("incident_response_firm"),
            "forensics_firm": json_data.get("forensics_firm"),
            "law_firm": json_data.get("law_firm"),
            "security_improvements": json_data.get("security_improvements") or json_data.get("response_actions"),
            "mfa_implemented": json_data.get("mfa_implemented"),
            "security_training_conducted": json_data.get("security_training_conducted"),
            "response_measures": json_data.get("response_measures"),
            "law_enforcement_involved": json_data.get("law_enforcement_involved"),
            "law_enforcement_agency": json_data.get("law_enforcement_agency"),
            "detection_source": json_data.get("detection_source"),
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
        enriched_summary=json_data.get("enriched_summary", ""),
        extraction_notes=extraction_notes
    )

