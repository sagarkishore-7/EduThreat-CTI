"""
Mapper to convert JSON schema extraction response to CTIEnrichmentResult.
"""

from typing import Dict, Any, Optional, List
from src.edu_cti.pipeline.phase2.schemas import (
    CTIEnrichmentResult,
    EducationRelevanceCheck,
    TimelineEvent,
    MITREAttackTechnique,
    AttackDynamics,
)


def map_systems_affected_codes(codes: List[str]) -> List[str]:
    """Map extraction schema system codes to CTIEnrichmentResult system codes."""
    mapping = {
        "email": "email_system",
        "website_public": "web_servers",
        "portal_student_staff": "student_portal",
        "identity_sso": "other",
        "active_directory": "other",
        "vpn_remote_access": "network_infrastructure",
        "wifi_network": "network_infrastructure",
        "wired_network_core": "network_infrastructure",
        "dns_dhcp": "network_infrastructure",
        "firewall_gateway": "network_infrastructure",
        "lms": "learning_management_system",
        "sis": "student_portal",
        "erp_finance_hr": "financial_systems",
        "hr_payroll": "payroll_system",
        "admissions_enrollment": "admissions_system",
        "exam_proctoring": "other",
        "library_systems": "other",
        "payment_billing": "financial_systems",
        "file_transfer": "file_servers",
        "cloud_storage": "cloud_services",
        "on_prem_file_share": "file_servers",
        "research_hpc": "research_systems",
        "research_lab_instruments": "research_systems",
        "phone_voip": "other",
        "printing_copy": "other",
        "backup_infrastructure": "backup_systems",
        "datacenter_facilities": "other",
        "security_tools": "other",
        "other": "other",
        "unknown": "other",
    }
    
    mapped = []
    for code in codes:
        mapped_code = mapping.get(code, "other")
        if mapped_code not in mapped:
            mapped.append(mapped_code)
    return mapped if mapped else None


def map_attack_category_to_vector(category: str) -> Optional[str]:
    """Map attack category to attack vector."""
    mapping = {
        "ransomware": "ransomware",
        "phishing": "phishing",
        "data_breach": "other",
        "ddos": "ddos",
        "malware": "malware",
        "extortion": "other",
        "supply_chain": "supply_chain",
        "web_defacement": "other",
        "unauthorized_access": "other",
        "insider_threat": "insider_threat",
        "other": "other",
    }
    return mapping.get(category, "other")


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
    """Map data types to data impact metrics."""
    result = {}
    type_mapping = {
        "student_records": "student_data",
        "staff_data": "faculty_data",
        "alumni_data": "alumni_data",
        "research_data": "research_data",
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
    # Education relevance
    education_relevance = EducationRelevanceCheck(
        is_education_related=json_data.get("is_edu_cyber_incident", False),
        reasoning=json_data.get("other_notable_details", "") or "Education relevance determined from article",
        institution_identified=json_data.get("institution_name")
    )
    
    # Timeline
    timeline = None
    if json_data.get("timeline"):
        timeline = [
            TimelineEvent(
                date=event.get("date"),
                date_precision=event.get("date_precision"),
                event_description=event.get("event_description"),
                event_type=event.get("event_type"),
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
    
    # Attack dynamics
    attack_dynamics = None
    attack_category = json_data.get("attack_category")
    if attack_category:
        attack_dynamics = AttackDynamics(
            attack_vector=map_attack_category_to_vector(attack_category),
            attack_chain=None,  # Not in extraction schema
            ransomware_family=json_data.get("ransomware_family_or_group"),
            data_exfiltration=json_data.get("data_breached"),
            encryption_impact="full" if json_data.get("encryption_at_rest") == "yes" else "partial" if json_data.get("encryption_at_rest") == "no" else None,
            impact_scope=None,
            ransom_demanded=json_data.get("was_ransom_demanded"),
            ransom_amount=str(json_data.get("ransom_amount", "")) if json_data.get("ransom_amount") is not None else None,
            ransom_paid=json_data.get("ransom_paid"),
            recovery_timeframe_days=json_data.get("mttr_hours", 0) / 24.0 if json_data.get("mttr_hours") else None,
            business_impact=None,  # Not directly mapped
            operational_impact=None  # Will be in operational_impact_metrics
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
    
    # User impact (as Dict)
    user_impact = None
    if json_data.get("students_affected") is not None or json_data.get("staff_affected") is not None or json_data.get("faculty_affected") is not None:
        user_impact = {
            "students_affected": json_data.get("students_affected", 0) > 0 if json_data.get("students_affected") is not None else None,
            "faculty_affected": json_data.get("faculty_affected"),
            "staff_affected": json_data.get("staff_affected", 0) > 0 if json_data.get("staff_affected") is not None else None,
            "alumni_affected": json_data.get("alumni_affected"),
            "parents_affected": json_data.get("parents_affected"),
            "applicants_affected": json_data.get("applicants_affected"),
            "patients_affected": json_data.get("patients_affected"),
            "users_affected_min": json_data.get("users_affected_min"),
            "users_affected_max": json_data.get("users_affected_max"),
            "users_affected_exact": json_data.get("users_affected_exact") or ((json_data.get("students_affected", 0) or 0) + (json_data.get("staff_affected", 0) or 0) if json_data.get("students_affected") is not None or json_data.get("staff_affected") is not None else None)
        }
    
    # Operational impact metrics (as Dict)
    operational_impact_metrics = None
    if (json_data.get("teaching_impacted") is not None or json_data.get("research_impacted") is not None or 
        json_data.get("outage_duration_hours") is not None or json_data.get("downtime_days") is not None or
        json_data.get("operational_impact")):
        operational_impact = json_data.get("operational_impact", [])
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
    
    # Financial impact (as Dict)
    financial_impact = None
    if (json_data.get("was_ransom_demanded") is not None or json_data.get("ransom_amount") is not None or 
        json_data.get("currency_normalized_cost_usd") is not None or json_data.get("recovery_costs_min") is not None):
        financial_impact = {
            "ransom_demanded": json_data.get("was_ransom_demanded"),
            "ransom_amount_min": json_data.get("ransom_amount_min"),
            "ransom_amount_max": json_data.get("ransom_amount_max"),
            "ransom_amount_exact": json_data.get("ransom_amount_exact") or json_data.get("ransom_amount"),
            "ransom_currency": json_data.get("ransom_currency"),
            "ransom_paid": json_data.get("ransom_paid"),
            "ransom_paid_amount": json_data.get("ransom_paid_amount") or (json_data.get("ransom_amount") if json_data.get("ransom_paid") else None),
            "recovery_costs_min": json_data.get("recovery_costs_min"),
            "recovery_costs_max": json_data.get("recovery_costs_max"),
            "legal_costs": json_data.get("legal_costs"),
            "notification_costs": json_data.get("notification_costs"),
            "credit_monitoring_costs": json_data.get("credit_monitoring_costs"),
            "insurance_claim": json_data.get("insurance_claim"),
            "insurance_claim_amount": json_data.get("insurance_claim_amount")
        }
    
    # Regulatory impact (as Dict)
    regulatory_impact = None
    if (json_data.get("regulatory_context") or json_data.get("fines_or_penalties") or 
        json_data.get("class_actions_or_lawsuits") or json_data.get("gdpr_breach") is not None):
        regulatory_impact = {
            "breach_notification_required": json_data.get("breach_notification_required"),
            "notifications_sent": json_data.get("notifications_sent"),
            "notifications_sent_date": json_data.get("notifications_sent_date") or (json_data.get("notification_dates", {}).get("data_subjects_notified_date") if json_data.get("notification_dates") else None),
            "regulators_notified": json_data.get("regulators_notified") or json_data.get("regulatory_context"),
            "regulators_notified_date": json_data.get("regulators_notified_date") or (json_data.get("notification_dates", {}).get("regulator_notified_date") if json_data.get("notification_dates") else None),
            "gdpr_breach": json_data.get("gdpr_breach") or ("GDPR" in (json_data.get("regulatory_context") or [])),
            "dpa_notified": json_data.get("dpa_notified") or ("UK_DPA" in (json_data.get("regulatory_context") or [])),
            "hipaa_breach": json_data.get("hipaa_breach") or ("HIPAA" in (json_data.get("regulatory_context") or [])),
            "ferc_breach": json_data.get("ferpa_breach") or ("FERPA" in (json_data.get("regulatory_context") or [])),
            "investigation_opened": json_data.get("investigation_opened"),
            "fine_imposed": json_data.get("fine_imposed") or (json_data.get("fines_or_penalties") is not None and len(str(json_data.get("fines_or_penalties", ""))) > 0),
            "fine_amount": json_data.get("fine_amount"),
            "lawsuits_filed": json_data.get("lawsuits_filed") or (json_data.get("class_actions_or_lawsuits") is not None and len(str(json_data.get("class_actions_or_lawsuits", ""))) > 0),
            "lawsuit_count": json_data.get("lawsuit_count"),
            "class_action": json_data.get("class_action") or (json_data.get("class_actions_or_lawsuits") is not None and "class" in str(json_data.get("class_actions_or_lawsuits", "")).lower())
        }
    
    # Recovery metrics (as Dict)
    recovery_metrics = None
    if (json_data.get("backup_status") or json_data.get("service_restoration_date") or 
        json_data.get("response_actions") or json_data.get("recovery_started_date") or 
        json_data.get("recovery_timeframe_days") is not None):
        recovery_metrics = {
            "recovery_started_date": json_data.get("recovery_started_date"),
            "recovery_completed_date": json_data.get("recovery_completed_date") or json_data.get("service_restoration_date"),
            "recovery_timeframe_days": json_data.get("recovery_timeframe_days") or (json_data.get("mttr_hours", 0) / 24.0 if json_data.get("mttr_hours") else None),
            "recovery_phases": json_data.get("recovery_phases"),
            "from_backup": json_data.get("from_backup") or (json_data.get("backup_status") == "available_and_used"),
            "backup_age_days": json_data.get("backup_age_days"),
            "clean_rebuild": json_data.get("clean_rebuild"),
            "incident_response_firm": json_data.get("incident_response_firm"),
            "forensics_firm": json_data.get("forensics_firm"),
            "law_firm": json_data.get("law_firm"),
            "security_improvements": json_data.get("security_improvements") or json_data.get("response_actions"),
            "mfa_implemented": json_data.get("mfa_implemented"),
            "security_training_conducted": json_data.get("security_training_conducted"),
            "response_measures": json_data.get("response_measures")
        }
    
    # Transparency metrics (as Dict)
    transparency_metrics = None
    if (json_data.get("was_disclosed_publicly") is not None or json_data.get("disclosure_source") or
        json_data.get("public_disclosure") is not None):
        transparency_metrics = {
            "disclosure_timeline": json_data.get("disclosure_timeline"),
            "public_disclosure": json_data.get("public_disclosure") or json_data.get("was_disclosed_publicly", False),
            "public_disclosure_date": json_data.get("public_disclosure_date"),
            "disclosure_delay_days": json_data.get("disclosure_delay_days"),
            "transparency_level": json_data.get("transparency_level"),
            "official_statement_url": json_data.get("official_statement_url"),
            "detailed_report_url": json_data.get("detailed_report_url"),
            "updates_provided": json_data.get("updates_provided"),
            "update_count": json_data.get("update_count")
        }
    
    # Research impact (as Dict)
    research_impact = None
    if json_data.get("research_impacted") is not None or json_data.get("research_projects_affected") is not None:
        research_impact = {
            "research_projects_affected": json_data.get("research_projects_affected") or json_data.get("research_impacted", False),
            "research_data_compromised": json_data.get("research_data_compromised") or (json_data.get("research_impact_code") in ["data_loss", "data_unavailable"]),
            "sensitive_research_impact": json_data.get("sensitive_research_impact"),
            "publications_delayed": json_data.get("publications_delayed"),
            "grants_affected": json_data.get("grants_affected"),
            "collaborations_affected": json_data.get("collaborations_affected"),
            "research_area": json_data.get("research_area")
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

