"""
Comprehensive JSON Schema for Cyber Threat Intelligence (CTI) extraction.

This schema integrates all fields from the Pydantic schemas to provide
extensive CTI data extraction capabilities for educational sector incidents.
"""

EXTRACTION_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Cyber Threat Intelligence - Educational Sector Incident",
    "type": "object",
    "additionalProperties": False,
    "properties": {
        # ========== EDUCATION RELEVANCE ==========
        "is_edu_cyber_incident": {
            "type": "boolean",
            "description": "Whether this incident is related to the education sector"
        },
        "education_relevance_reasoning": {
            "type": "string",
            "description": "Brief explanation (1-2 sentences) of why this is or isn't education-related"
        },
        "institution_name": {"type": "string"},
        "institution_type": {
            "type": "string",
            "enum": [
                "University", "College", "School", "Research Institute", "Research Center",
                "Vocational/Technical", "University Hospital", "Consortium", "Unknown"
            ]
        },
        "country": {"type": "string"},
        "region": {"type": "string"},
        "city": {"type": "string"},
        
        # ========== TIMELINE & DATES ==========
        "incident_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "discovery_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "publication_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "timeline": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
                    "date_precision": {
                        "type": "string",
                        "enum": ["day", "month", "year", "approximate"]
                    },
                    "event_description": {"type": "string"},
                    "event_type": {
                        "type": "string",
                        "enum": [
                            "initial_access", "discovery", "exploitation", "impact", "containment",
                            "eradication", "recovery", "disclosure", "notification", "investigation", "remediation", "other"
                        ]
                    },
                    "actor_attribution": {"type": "string"},
                    "indicators": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                }
            }
        },
        
        # ========== ATTACK MECHANICS ==========
        "attack_category": {
            "type": "string",
            "enum": [
                "ransomware", "phishing", "data_breach", "ddos", "malware", "extortion",
                "supply_chain", "web_defacement", "unauthorized_access", "insider_threat", "other"
            ]
        },
        "secondary_categories": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "ransomware", "phishing", "data_breach", "ddos", "malware", "extortion",
                    "supply_chain", "web_defacement", "unauthorized_access", "insider_threat", "other"
                ]
            }
        },
        "attack_vector": {
            "type": "string",
            "enum": [
                "phishing", "spear_phishing", "vulnerability_exploit", "credential_stuffing", "credential_theft",
                "malware", "ransomware", "insider_threat", "social_engineering", "supply_chain",
                "third_party_breach", "misconfiguration", "brute_force", "ddos", "sql_injection", "xss", "other"
            ]
        },
        "initial_access_vector": {
            "type": "string",
            "enum": [
                "phishing", "stolen_credentials", "brute_force", "exposed_service", "supply_chain",
                "malicious_attachment", "drive_by", "vulnerability_exploit", "unknown", "other"
            ]
        },
        "initial_access_description": {"type": "string"},
        "attack_chain": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "reconnaissance", "weaponization", "delivery", "exploitation", "installation",
                    "command_and_control", "actions_on_objectives", "exfiltration", "impact"
                ]
            }
        },
        "vulnerabilities": {
            "type": "array",
            "items": {"type": "string"}
        },
        
        # ========== MITRE ATT&CK ==========
        "mitre_attack_techniques": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "technique_id": {"type": "string"},
                    "technique_name": {"type": "string"},
                    "tactic": {"type": "string"},
                    "description": {"type": "string"},
                    "sub_techniques": {
            "type": "array",
            "items": {"type": "string"}
                    }
                }
            }
        },
        
        # ========== THREAT ACTOR ==========
        "threat_actor_claimed": {"type": "boolean"},
        "threat_actor_name": {"type": "string"},
        "threat_actor_claim_url": {"type": "string"},
        "ransomware_family_or_group": {"type": "string"},
        "attacker_communication_channel": {
            "type": "string",
            "enum": [
                "email", "tor_leak_site", "blog", "social_media", "dark_web_forum",
                "ransom_note", "press_contact", "unknown"
            ]
        },
        
        # ========== RANSOM ==========
        "was_ransom_demanded": {"type": "boolean"},
        "ransom_amount": {"type": "number"},
        "ransom_amount_min": {"type": "number"},
        "ransom_amount_max": {"type": "number"},
        "ransom_amount_exact": {"type": "number"},
        "ransom_currency": {"type": "string"},
        "ransom_amount_precision": {
            "type": "string",
            "enum": ["exact", "approximate", "range", "unknown"]
        },
        "ransom_paid": {"type": "boolean"},
        "ransom_paid_amount": {"type": "number"},
        "ransom_payment_details": {"type": "string"},
        
        # ========== DATA IMPACT ==========
        "data_breached": {"type": "boolean"},
        "data_exfiltrated": {"type": "boolean"},
        "data_encrypted": {"type": "boolean"},
        "data_types": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "student_records", "staff_data", "alumni_data", "research_data", "health_data",
                    "financial_data", "credentials", "pii", "grades_transcripts", "special_category_gdpr",
                    "personal_information", "intellectual_property", "medical_records", "administrative_data", "other"
                ]
            }
        },
        "pii_records_leaked": {"type": "integer"},
        "records_affected_min": {"type": "integer"},
        "records_affected_max": {"type": "integer"},
        "records_affected_exact": {"type": "integer"},
        "pii_count_precision": {
            "type": "string",
            "enum": ["exact", "estimate", "range", "unknown"]
        },
        "records_exfiltrated_estimate": {"type": "integer"},
        "data_types_affected": {
            "type": "array",
            "items": {"type": "string"}
        },
        
        # ========== SYSTEM IMPACT ==========
        "infrastructure_context": {
            "type": "string",
            "enum": ["on_prem", "cloud", "hybrid", "unknown"]
        },
        "systems_affected_codes": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "email", "website_public", "portal_student_staff", "identity_sso", "active_directory",
                    "vpn_remote_access", "wifi_network", "wired_network_core", "dns_dhcp", "firewall_gateway",
                    "lms", "sis", "erp_finance_hr", "hr_payroll", "admissions_enrollment", "exam_proctoring",
                    "library_systems", "payment_billing", "file_transfer", "cloud_storage", "on_prem_file_share",
                    "research_hpc", "research_lab_instruments", "phone_voip", "printing_copy", "backup_infrastructure",
                    "datacenter_facilities", "security_tools", "other", "unknown"
                ]
            }
        },
        "systems_affected_notes_en": {"type": "string"},
        "critical_systems_affected": {"type": "boolean"},
        "network_compromised": {"type": "boolean"},
        "email_system_affected": {"type": "boolean"},
        "student_portal_affected": {"type": "boolean"},
        "research_systems_affected": {"type": "boolean"},
        "hospital_systems_affected": {"type": "boolean"},
        "cloud_services_affected": {"type": "boolean"},
        "third_party_vendor_impact": {"type": "boolean"},
        "vendor_name": {"type": "string"},
        "encryption_impact": {
            "type": "string",
            "enum": ["full", "partial", "none"]
        },
        
        # ========== OPERATIONAL IMPACT ==========
        "service_outage_start": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "service_restoration_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "outage_duration_hours": {"type": "number", "minimum": 0},
        "outage_duration_precision": {
            "type": "string",
            "enum": ["exact", "approximate", "range", "unknown"]
        },
        "downtime_days": {"type": "number"},
        "partial_service_days": {"type": "number"},
        "teaching_impacted": {"type": "boolean"},
        "teaching_disrupted": {"type": "boolean"},
        "teaching_impact_code": {
            "type": "string",
            "enum": [
                "none", "minor_delay", "class_cancellation", "online_platform_down", "exam_disruption",
                "assignment_submission_issues", "campus_closure", "other", "unknown"
            ]
        },
        "teaching_impact_notes_en": {"type": "string"},
        "research_impacted": {"type": "boolean"},
        "research_disrupted": {"type": "boolean"},
        "research_impact_code": {
            "type": "string",
            "enum": [
                "none", "data_loss", "data_unavailable", "lab_shutdown", "compute_cluster_down", "other", "unknown"
            ]
        },
        "research_impact_notes_en": {"type": "string"},
        "research_projects_affected": {"type": "boolean"},
        "research_data_compromised": {"type": "boolean"},
        "sensitive_research_impact": {"type": "boolean"},
        "publications_delayed": {"type": "boolean"},
        "grants_affected": {"type": "boolean"},
        "collaborations_affected": {"type": "boolean"},
        "research_area": {"type": "string"},
        "operations_disruption_code": {
            "type": "string",
            "enum": [
                "none", "email_down", "website_down", "network_outage", "identity_sso_issues", "vpn_down",
                "student_portal_down", "learning_mgmt_down", "payment_system_down", "other", "unknown"
            ]
        },
        "operations_disruption_notes_en": {"type": "string"},
        "operational_impact": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "teaching_disrupted", "research_disrupted", "admissions_disrupted", "enrollment_disrupted",
                    "payroll_disrupted", "clinical_operations_disrupted", "online_learning_disrupted",
                    "classes_cancelled", "exams_postponed", "graduation_delayed", "email_system_down",
                    "student_portal_down", "network_down", "website_down", "other"
                ]
            }
        },
        "admissions_disrupted": {"type": "boolean"},
        "enrollment_disrupted": {"type": "boolean"},
        "payroll_disrupted": {"type": "boolean"},
        "clinical_operations_disrupted": {"type": "boolean"},
        "online_learning_disrupted": {"type": "boolean"},
        "classes_cancelled": {"type": "boolean"},
        "exams_postponed": {"type": "boolean"},
        "graduation_delayed": {"type": "boolean"},
        
        # ========== USER IMPACT ==========
        "students_affected": {"type": "integer"},
        "staff_affected": {"type": "integer"},
        "faculty_affected": {"type": "boolean"},
        "alumni_affected": {"type": "boolean"},
        "parents_affected": {"type": "boolean"},
        "applicants_affected": {"type": "boolean"},
        "patients_affected": {"type": "boolean"},
        "users_affected_min": {"type": "integer"},
        "users_affected_max": {"type": "integer"},
        "users_affected_exact": {"type": "integer"},
        
        # ========== FINANCIAL IMPACT ==========
        "financial_loss_reporting": {"type": "string"},
        "currency_normalized_cost_usd": {"type": "number"},
        "recovery_costs_min": {"type": "number"},
        "recovery_costs_max": {"type": "number"},
        "legal_costs": {"type": "number"},
        "notification_costs": {"type": "number"},
        "credit_monitoring_costs": {"type": "number"},
        "insurance_claim": {"type": "boolean"},
        "insurance_claim_amount": {"type": "number"},
        "business_impact": {
            "type": "string",
            "enum": ["critical", "severe", "moderate", "limited", "minimal"]
        },
        
        # ========== REGULATORY IMPACT ==========
        "regulatory_context": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": ["GDPR", "HIPAA", "FERPA", "PCI-DSS", "UK_DPA", "Other", "Unknown"]
            }
        },
        "breach_notification_required": {"type": "boolean"},
        "notifications_sent": {"type": "boolean"},
        "notifications_sent_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "regulators_notified": {
            "type": "array",
            "items": {"type": "string"}
        },
        "regulators_notified_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "gdpr_breach": {"type": "boolean"},
        "dpa_notified": {"type": "boolean"},
        "hipaa_breach": {"type": "boolean"},
        "ferpa_breach": {"type": "boolean"},
        "investigation_opened": {"type": "boolean"},
        "fine_imposed": {"type": "boolean"},
        "fine_amount": {"type": "number"},
        "lawsuits_filed": {"type": "boolean"},
        "lawsuit_count": {"type": "integer"},
        "class_action": {"type": "boolean"},
        "fines_or_penalties": {"type": "string"},
        "class_actions_or_lawsuits": {"type": "string"},
        
        # ========== RECOVERY & REMEDIATION ==========
        "recovery_started_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "recovery_completed_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "recovery_timeframe_days": {"type": "number"},
        "recovery_phases": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": ["containment", "eradication", "recovery", "lessons_learned", "post_incident_review"]
            }
        },
        "from_backup": {"type": "boolean"},
        "backup_status": {
            "type": "string",
            "enum": ["available_and_used", "available_not_used", "not_available", "unknown"]
        },
        "backup_age_days": {"type": "number"},
        "clean_rebuild": {"type": "boolean"},
        "incident_response_firm": {"type": "string"},
        "forensics_firm": {"type": "string"},
        "law_firm": {"type": "string"},
        "security_improvements": {
            "type": "array",
            "items": {"type": "string"}
        },
        "mfa_implemented": {"type": "boolean"},
        "security_training_conducted": {"type": "boolean"},
        "response_measures": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "password_reset", "account_lockout", "credential_rotation", "backup_restoration",
                    "system_rebuild", "network_isolation", "endpoint_containment", "malware_removal",
                    "patch_application", "vulnerability_remediation", "access_revocation",
                    "incident_response_team", "forensics_investigation", "law_enforcement_notification",
                    "regulatory_notification", "user_notification", "public_disclosure", "security_audit",
                    "penetration_testing", "security_training", "mfa_implementation", "network_segmentation",
                    "firewall_update", "ids_ips_deployment", "monitoring_enhancement", "other"
                ]
            }
        },
        "detection_source": {
            "type": "string",
            "enum": [
                "internal_security_team", "it_operations", "third_party_vendor", "law_enforcement",
                "student_staff_report", "unknown"
            ]
        },
        "response_actions": {
            "type": "array",
            "items": {"type": "string"}
        },
        "law_enforcement_involved": {"type": "boolean"},
        "law_enforcement_agency": {"type": "string"},
        "third_parties_involved": {
            "type": "array",
            "items": {"type": "string"}
        },
        "mttd_hours": {"type": "number"},
        "mttr_hours": {"type": "number"},
        
        # ========== TRANSPARENCY ==========
        "was_disclosed_publicly": {"type": "boolean"},
        "public_disclosure": {"type": "boolean"},
        "public_disclosure_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "disclosure_delay_days": {"type": "number"},
        "transparency_level": {
            "type": "string",
            "enum": ["high", "medium", "low", "none"]
        },
        "official_statement_url": {"type": "string"},
        "detailed_report_url": {"type": "string"},
        "updates_provided": {"type": "boolean"},
        "update_count": {"type": "integer"},
        "disclosure_source": {
            "type": "string",
            "enum": ["institution", "regulator", "attacker_site", "media", "law_enforcement", "other"]
        },
        "disclosure_timeline": {
            "type": "object",
            "properties": {
                "discovered_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
                "disclosed_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
                "notified_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"}
            }
        },
        "notification_dates": {
            "type": "object",
            "properties": {
                "regulator_notified_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
                "data_subjects_notified_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"}
            }
        },
        
        # ========== SOURCE METADATA ==========
        "source_url": {"type": "string"},
        "source_headline": {"type": "string"},
        "source_publisher": {"type": "string"},
        "source_author": {"type": "string"},
        "source_language": {"type": "string"},
        "archive_url": {"type": "string"},
        "is_paywalled": {"type": "boolean"},
        "other_sources": {
            "type": "array",
            "items": {"type": "string"}
        },
        "language_normalized": {
            "type": "string",
            "enum": ["en"]
        },
        "key_quotes": {
            "type": "array",
            "items": {"type": "string"}
        },
        "other_notable_details": {"type": "string"},
        
        # ========== SUMMARY & NOTES ==========
        "enriched_summary": {"type": "string"},
        "extraction_notes": {"type": "string"},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1.0}
    },
    "required": [
        "is_edu_cyber_incident",
        "enriched_summary"
    ]
}
