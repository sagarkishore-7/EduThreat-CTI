"""
Comprehensive JSON Schema for Cyber Threat Intelligence (CTI) extraction.

This schema provides extensive CTI data extraction capabilities for educational
sector incidents, designed for:
- Detailed threat intelligence analysis
- Cross-incident correlation and analysis  
- MITRE ATT&CK mapping
- Trend analysis across the education sector
- Threat actor tracking and attribution
- Ransomware family identification

Version: 2.0.0 (Enhanced for CTI Analysis)

Key Features:
- 50+ attack categories with granular classification
- 60+ attack vectors covering all initial access methods
- 35+ ransomware family enumeration
- 30+ data categories for comprehensive breach analysis
- 35+ system categories for impact assessment
- 25+ security improvement tracking for recovery analysis
- Cross-incident analysis fields for campaign tracking

All numeric values are standardized (e.g., "$5.2 million" → 5200000)
All dates use ISO format (YYYY-MM-DD)
All enums use exact lowercase tags
"""

EXTRACTION_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Cyber Threat Intelligence - Educational Sector Incident (Extended)",
    "description": "Comprehensive schema for extracting threat intelligence from education sector cyber incidents",
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
        "institution_name": {
            "type": "string",
            "description": "Full official name of the affected educational institution"
        },
        "institution_aliases": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Alternative names, abbreviations, or acronyms for the institution"
        },
        "institution_type": {
            "type": "string",
            "enum": [
                "university_public",
                "university_private",
                "university_research",
                "community_college",
                "technical_college",
                "vocational_school",
                "k12_public_school",
                "k12_private_school",
                "k12_charter_school",
                "school_district",
                "research_institute",
                "research_center",
                "medical_school",
                "university_hospital",
                "teaching_hospital",
                "online_university",
                "consortium",
                "education_department",
                "education_ministry",
                "student_loan_servicer",
                "education_nonprofit",
                "education_vendor",
                "unknown"
            ]
        },
        "institution_size": {
            "type": "string",
            "enum": ["small_under_5k", "medium_5k_20k", "large_20k_50k", "very_large_over_50k", "unknown"]
        },
        "country": {"type": "string"},
        "country_code": {"type": "string", "pattern": "^[A-Z]{2}$"},
        "region": {"type": "string"},
        "city": {"type": "string"},
        
        # ========== INCIDENT CLASSIFICATION ==========
        "incident_severity": {
            "type": "string",
            "enum": ["critical", "high", "medium", "low", "informational"],
            "description": "Overall severity rating of the incident"
        },
        "incident_status": {
            "type": "string",
            "enum": ["ongoing", "contained", "resolved", "unknown"]
        },
        
        # ========== TIMELINE & DATES ==========
        "incident_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "incident_date_precision": {
            "type": "string",
            "enum": ["exact", "approximate", "month_only", "year_only", "unknown"]
        },
        "discovery_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "publication_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "dwell_time_days": {
            "type": "number",
            "description": "Days between initial compromise and discovery"
        },
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
                            "initial_access",
                            "reconnaissance",
                            "lateral_movement",
                            "privilege_escalation",
                            "data_exfiltration",
                            "encryption_started",
                            "ransom_demand",
                            "discovery",
                            "containment",
                            "eradication",
                            "recovery",
                            "disclosure",
                            "notification",
                            "investigation",
                            "remediation",
                            "law_enforcement_contact",
                            "public_statement",
                            "systems_restored",
                            "other"
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
        
        # ========== ATTACK CLASSIFICATION (EXTENSIVE) ==========
        "attack_category": {
            "type": "string",
            "enum": [
                # Ransomware variants
                "ransomware_encryption",
                "ransomware_double_extortion",
                "ransomware_triple_extortion",
                "ransomware_data_leak_only",
                # Phishing variants
                "phishing_credential_harvest",
                "phishing_malware_delivery",
                "spear_phishing",
                "whaling",
                "business_email_compromise",
                "smishing",
                "vishing",
                # Data breach types
                "data_breach_external",
                "data_breach_internal",
                "data_exposure_misconfiguration",
                "data_leak_accidental",
                # Network attacks
                "ddos_volumetric",
                "ddos_application",
                "ddos_protocol",
                # Malware types
                "malware_trojan",
                "malware_worm",
                "malware_backdoor",
                "malware_rootkit",
                "malware_cryptominer",
                "malware_infostealer",
                "malware_rat",
                "malware_botnet",
                # Access attacks
                "unauthorized_access",
                "privilege_escalation",
                "credential_stuffing",
                "brute_force",
                "password_spraying",
                # Web attacks
                "web_defacement",
                "sql_injection",
                "xss_attack",
                "api_abuse",
                # Insider threats
                "insider_malicious",
                "insider_negligent",
                "insider_compromised",
                # Supply chain
                "supply_chain_software",
                "supply_chain_hardware",
                "supply_chain_service_provider",
                "third_party_compromise",
                # Other
                "social_engineering",
                "physical_breach",
                "account_takeover",
                "extortion_no_ransomware",
                "hacktivism",
                "espionage",
                "sabotage",
                "fraud",
                "unknown",
                "other"
            ]
        },
        "secondary_attack_categories": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Additional attack types observed in the incident"
        },
        
        # ========== ATTACK VECTOR (EXTENSIVE) ==========
        "attack_vector": {
            "type": "string",
            "enum": [
                # Email-based
                "phishing_email",
                "spear_phishing_email",
                "malicious_attachment",
                "malicious_link",
                "business_email_compromise",
                # Credential-based
                "stolen_credentials",
                "credential_stuffing",
                "brute_force",
                "password_spraying",
                "credential_phishing",
                "session_hijacking",
                # Vulnerability exploitation
                "vulnerability_exploit_known",
                "vulnerability_exploit_zero_day",
                "unpatched_system",
                "misconfiguration",
                "default_credentials",
                # Web-based
                "drive_by_download",
                "watering_hole",
                "malvertising",
                "sql_injection",
                "xss",
                "csrf",
                "ssrf",
                "path_traversal",
                # Network-based
                "exposed_service",
                "exposed_rdp",
                "exposed_vpn",
                "exposed_ssh",
                "exposed_database",
                "exposed_api",
                "man_in_the_middle",
                # Supply chain
                "supply_chain_compromise",
                "third_party_vendor",
                "software_update_compromise",
                "trusted_relationship",
                # Physical/Social
                "social_engineering",
                "pretexting",
                "baiting",
                "tailgating",
                "usb_drop",
                # Insider
                "insider_access",
                "former_employee",
                # Cloud-specific
                "cloud_misconfiguration",
                "api_key_exposure",
                "storage_bucket_exposure",
                # Other
                "dns_hijacking",
                "bgp_hijacking",
                "sim_swapping",
                "unknown",
                "other"
            ]
        },
        "initial_access_description": {"type": "string"},
        
        # ========== KILL CHAIN / ATTACK CHAIN ==========
        "attack_chain": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "reconnaissance",
                    "resource_development",
                    "initial_access",
                    "execution",
                    "persistence",
                    "privilege_escalation",
                    "defense_evasion",
                    "credential_access",
                    "discovery",
                    "lateral_movement",
                    "collection",
                    "command_and_control",
                    "exfiltration",
                    "impact"
                ]
            }
        },
        
        # ========== VULNERABILITIES ==========
        "vulnerabilities_exploited": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "cve_id": {"type": "string", "pattern": "^CVE-\\d{4}-\\d+$"},
                    "vulnerability_name": {"type": "string"},
                    "vulnerability_type": {
                        "type": "string",
                        "enum": [
                            "remote_code_execution",
                            "privilege_escalation",
                            "authentication_bypass",
                            "sql_injection",
                            "xss",
                            "ssrf",
                            "deserialization",
                            "path_traversal",
                            "buffer_overflow",
                            "memory_corruption",
                            "information_disclosure",
                            "denial_of_service",
                            "zero_day",
                            "other"
                        ]
                    },
                    "affected_product": {"type": "string"},
                    "cvss_score": {"type": "number", "minimum": 0, "maximum": 10}
                }
            }
        },
        
        # ========== MITRE ATT&CK (EXTENSIVE) ==========
        "mitre_attack_techniques": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "technique_id": {"type": "string", "pattern": "^T\\d{4}(\\.\\d{3})?$"},
                    "technique_name": {"type": "string"},
                    "tactic": {
                        "type": "string",
                        "enum": [
                            "reconnaissance",
                            "resource_development",
                            "initial_access",
                            "execution",
                            "persistence",
                            "privilege_escalation",
                            "defense_evasion",
                            "credential_access",
                            "discovery",
                            "lateral_movement",
                            "collection",
                            "command_and_control",
                            "exfiltration",
                            "impact"
                        ]
                    },
                    "description": {"type": "string"},
                    "sub_techniques": {
            "type": "array",
            "items": {"type": "string"}
                    }
                }
            }
        },
        
        # ========== THREAT ACTOR (EXTENSIVE) ==========
        "threat_actor_claimed": {"type": "boolean"},
        "threat_actor_name": {"type": "string"},
        "threat_actor_aliases": {
            "type": "array",
            "items": {"type": "string"}
        },
        "threat_actor_category": {
            "type": "string",
            "enum": [
                "apt_nation_state",
                "apt_state_sponsored",
                "cybercriminal_organized",
                "cybercriminal_individual",
                "ransomware_gang",
                "ransomware_affiliate",
                "hacktivist",
                "insider_threat",
                "script_kiddie",
                "competitor",
                "unknown",
                "other"
            ]
        },
        "threat_actor_motivation": {
            "type": "string",
            "enum": [
                "financial_gain",
                "espionage",
                "hacktivism",
                "sabotage",
                "personal_grievance",
                "notoriety",
                "research_theft",
                "competitive_advantage",
                "unknown"
            ]
        },
        "threat_actor_origin_country": {"type": "string"},
        "threat_actor_claim_url": {"type": "string"},
        
        # ========== RANSOMWARE/MALWARE (EXTENSIVE) ==========
        "ransomware_family": {
            "type": "string",
            "enum": [
                # Major ransomware families
                "lockbit",
                "lockbit_2",
                "lockbit_3",
                "blackcat_alphv",
                "cl0p_clop",
                "akira",
                "play",
                "8base",
                "bianlian",
                "royal",
                "black_basta",
                "medusa",
                "rhysida",
                "hunters_international",
                "inc_ransom",
                "vice_society",
                "hive",
                "conti",
                "ryuk",
                "revil_sodinokibi",
                "darkside",
                "blackmatter",
                "maze",
                "netwalker",
                "ragnar_locker",
                "avaddon",
                "cuba",
                "pysa_mespinoza",
                "babuk",
                "grief",
                "snatch",
                "quantum",
                "karakurt",
                "lorenz",
                "noescape",
                "cactus",
                "trigona",
                "money_message",
                "nokoyawa",
                "ransomhouse",
                "daixin",
                "unknown",
                "other"
            ]
        },
        "malware_families": {
            "type": "array",
            "items": {
                "type": "string"
            },
            "description": "List of malware families involved in the attack"
        },
        "attacker_tools": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "cobalt_strike",
                    "metasploit",
                    "mimikatz",
                    "psexec",
                    "bloodhound",
                    "sharphound",
                    "powershell_empire",
                    "covenant",
                    "sliver",
                    "brute_ratel",
                    "impacket",
                    "rubeus",
                    "kerbrute",
                    "hashcat",
                    "john_the_ripper",
                    "nmap",
                    "masscan",
                    "shodan",
                    "rclone",
                    "mega_sync",
                    "winscp",
                    "filezilla",
                    "anydesk",
                    "teamviewer",
                    "atera",
                    "splashtop",
                    "ngrok",
                    "ligolo",
                    "chisel",
                    "plink",
                    "other"
                ]
            }
        },
        "attacker_communication_channel": {
            "type": "string",
            "enum": [
                "email",
                "tor_leak_site",
                "tor_negotiation_site",
                "telegram",
                "tox",
                "session",
                "dark_web_forum",
                "ransom_note",
                "press_contact",
                "twitter_x",
                "unknown"
            ]
        },
        
        # ========== RANSOM DETAILS ==========
        "was_ransom_demanded": {"type": "boolean"},
        "ransom_amount": {"type": "number", "description": "Standardized to USD"},
        "ransom_amount_min": {"type": "number"},
        "ransom_amount_max": {"type": "number"},
        "ransom_amount_exact": {"type": "number"},
        "ransom_currency": {"type": "string"},
        "ransom_cryptocurrency": {
            "type": "string",
            "enum": ["bitcoin", "monero", "ethereum", "other", "unknown"]
        },
        "ransom_paid": {"type": "boolean"},
        "ransom_paid_amount": {"type": "number"},
        "ransom_negotiated": {"type": "boolean"},
        "ransom_deadline_given": {"type": "boolean"},
        "ransom_deadline_days": {"type": "number"},
        "decryptor_received": {"type": "boolean"},
        "decryptor_worked": {"type": "boolean"},
        
        # ========== INDICATORS OF COMPROMISE ==========
        "iocs": {
            "type": "object",
            "properties": {
                "ip_addresses": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "domains": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "urls": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "file_hashes": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "hash_type": {
                                "type": "string",
                                "enum": ["md5", "sha1", "sha256", "sha512"]
                            },
                            "hash_value": {"type": "string"}
                        }
                    }
                },
                "email_addresses": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "cryptocurrency_wallets": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "file_names": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "registry_keys": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            }
        },
        
        # ========== DATA IMPACT (EXTENSIVE) ==========
        "data_breached": {"type": "boolean"},
        "data_exfiltrated": {"type": "boolean"},
        "data_encrypted": {"type": "boolean"},
        "data_destroyed": {"type": "boolean"},
        "data_published": {"type": "boolean"},
        "data_sold": {"type": "boolean"},
        "data_categories": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    # Student data
                    "student_pii",
                    "student_ssn",
                    "student_grades",
                    "student_transcripts",
                    "student_financial_aid",
                    "student_disciplinary",
                    "student_health_records",
                    "student_immigration",
                    "student_housing",
                    # Staff/Faculty data
                    "employee_pii",
                    "employee_ssn",
                    "employee_payroll",
                    "employee_benefits",
                    "employee_performance",
                    "employee_background_checks",
                    # Alumni data
                    "alumni_pii",
                    "alumni_donation_history",
                    # Research data
                    "research_data",
                    "research_grants",
                    "research_ip",
                    "research_unpublished",
                    "research_classified",
                    # Financial data
                    "financial_records",
                    "bank_accounts",
                    "credit_cards",
                    "tax_records",
                    "donor_information",
                    # Medical data
                    "medical_records",
                    "health_insurance",
                    "mental_health",
                    "disability_records",
                    # Credentials
                    "usernames_passwords",
                    "api_keys",
                    "certificates",
                    # Other
                    "intellectual_property",
                    "legal_documents",
                    "contracts",
                    "internal_communications",
                    "security_configurations",
                    "network_diagrams",
                    "other"
                ]
            }
        },
        "records_affected_min": {"type": "integer"},
        "records_affected_max": {"type": "integer"},
        "records_affected_exact": {"type": "integer"},
        "data_volume_gb": {"type": "number"},
        
        # ========== SYSTEM IMPACT (EXTENSIVE) ==========
        "infrastructure_type": {
            "type": "string",
            "enum": ["on_premises", "cloud_only", "hybrid", "multi_cloud", "unknown"]
        },
        "cloud_provider": {
            "type": "string",
            "enum": ["aws", "azure", "gcp", "oracle", "other", "none", "unknown"]
        },
        "systems_affected": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    # Core IT
                    "email_system",
                    "active_directory",
                    "identity_management",
                    "vpn",
                    "firewall",
                    "dns",
                    "dhcp",
                    "file_servers",
                    "backup_systems",
                    "virtualization",
                    # Network
                    "core_network",
                    "wifi_network",
                    "voip_phone",
                    "data_center",
                    # Web/Public
                    "public_website",
                    "student_portal",
                    "staff_portal",
                    "alumni_portal",
                    "applicant_portal",
                    # Academic
                    "lms_learning_management",
                    "sis_student_information",
                    "registration_system",
                    "grade_system",
                    "library_system",
                    "exam_proctoring",
                    # Administrative
                    "erp_system",
                    "hr_system",
                    "payroll_system",
                    "financial_system",
                    "procurement",
                    "admissions_system",
                    "financial_aid_system",
                    # Research
                    "research_computing_hpc",
                    "research_storage",
                    "lab_instruments",
                    "research_databases",
                    # Healthcare (for teaching hospitals)
                    "ehr_emr",
                    "hospital_systems",
                    "medical_devices",
                    "pharmacy_system",
                    # Other
                    "printing_system",
                    "parking_system",
                    "physical_access",
                    "cctv_security",
                    "other"
                ]
            }
        },
        "critical_systems_affected": {"type": "boolean"},
        "network_compromised": {"type": "boolean"},
        "domain_admin_compromised": {"type": "boolean"},
        "backup_compromised": {"type": "boolean"},
        "encryption_extent": {
            "type": "string",
            "enum": ["full_encryption", "partial_encryption", "no_encryption", "unknown"]
        },
        "systems_encrypted_count": {"type": "integer"},
        "servers_affected_count": {"type": "integer"},
        "endpoints_affected_count": {"type": "integer"},
        
        # ========== OPERATIONAL IMPACT ==========
        "outage_start_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "outage_end_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "outage_duration_hours": {"type": "number"},
        "downtime_days": {"type": "number"},
        "partial_service_days": {"type": "number"},
        "operational_impacts": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "classes_cancelled",
                    "classes_moved_online",
                    "exams_postponed",
                    "exams_cancelled",
                    "graduation_delayed",
                    "semester_extended",
                    "campus_closed",
                    "research_halted",
                    "research_data_lost",
                    "payroll_delayed",
                    "financial_aid_delayed",
                    "admissions_suspended",
                    "registration_suspended",
                    "email_unavailable",
                    "website_down",
                    "student_portal_down",
                    "lms_unavailable",
                    "network_offline",
                    "vpn_unavailable",
                    "library_closed",
                    "it_helpdesk_overwhelmed",
                    "manual_processes_required",
                    "clinical_operations_disrupted",
                    "patient_care_affected",
                    "other"
                ]
            }
        },
        
        # ========== USER IMPACT ==========
        "students_affected": {"type": "integer"},
        "staff_affected": {"type": "integer"},
        "faculty_affected": {"type": "integer"},
        "alumni_affected": {"type": "integer"},
        "applicants_affected": {"type": "integer"},
        "patients_affected": {"type": "integer"},
        "donors_affected": {"type": "integer"},
        "total_individuals_affected": {"type": "integer"},
        
        # ========== FINANCIAL IMPACT ==========
        "estimated_total_cost_usd": {"type": "number"},
        "ransom_cost_usd": {"type": "number"},
        "recovery_cost_usd": {"type": "number"},
        "legal_cost_usd": {"type": "number"},
        "notification_cost_usd": {"type": "number"},
        "credit_monitoring_cost_usd": {"type": "number"},
        "lost_revenue_usd": {"type": "number"},
        "insurance_claim": {"type": "boolean"},
        "insurance_payout_usd": {"type": "number"},
        "business_impact_severity": {
            "type": "string",
            "enum": ["catastrophic", "critical", "major", "moderate", "minor", "negligible"]
        },
        
        # ========== REGULATORY IMPACT ==========
        "applicable_regulations": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "FERPA",
                    "HIPAA",
                    "GDPR",
                    "CCPA_CPRA",
                    "PCI_DSS",
                    "GLBA",
                    "SOX",
                    "UK_DPA",
                    "Australia_Privacy_Act",
                    "Canada_PIPEDA",
                    "state_breach_notification",
                    "other"
                ]
            }
        },
        "breach_notification_required": {"type": "boolean"},
        "notification_sent": {"type": "boolean"},
        "notification_sent_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "regulators_notified": {
            "type": "array",
            "items": {"type": "string"}
        },
        "investigation_opened": {"type": "boolean"},
        "investigating_agencies": {
            "type": "array",
            "items": {"type": "string"}
        },
        "fine_imposed": {"type": "boolean"},
        "fine_amount_usd": {"type": "number"},
        "lawsuits_filed": {"type": "boolean"},
        "lawsuit_count": {"type": "integer"},
        "class_action_filed": {"type": "boolean"},
        "settlement_amount_usd": {"type": "number"},
        
        # ========== RESPONSE & RECOVERY ==========
        "incident_response_activated": {"type": "boolean"},
        "ir_firm_engaged": {"type": "string"},
        "forensics_firm_engaged": {"type": "string"},
        "legal_counsel_engaged": {"type": "string"},
        "pr_firm_engaged": {"type": "string"},
        "law_enforcement_involved": {"type": "boolean"},
        "law_enforcement_agencies": {
            "type": "array",
            "items": {"type": "string"}
        },
        "fbi_involved": {"type": "boolean"},
        "cisa_involved": {"type": "boolean"},
        "recovery_method": {
            "type": "string",
            "enum": [
                "backup_restore",
                "decryptor_used",
                "ransom_paid_decryption",
                "clean_rebuild",
                "partial_backup_partial_rebuild",
                "ongoing",
                "unknown"
            ]
        },
        "recovery_started_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "recovery_completed_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "recovery_duration_days": {"type": "number"},
        "mttd_hours": {"type": "number", "description": "Mean Time To Detect"},
        "mttr_hours": {"type": "number", "description": "Mean Time To Recover"},
        "security_improvements": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "mfa_implemented",
                    "mfa_expanded",
                    "password_policy_strengthened",
                    "network_segmentation",
                    "endpoint_detection_response",
                    "siem_implemented",
                    "soc_established",
                    "backup_strategy_improved",
                    "air_gapped_backups",
                    "immutable_backups",
                    "security_awareness_training",
                    "phishing_simulation",
                    "vulnerability_management",
                    "penetration_testing",
                    "security_audit",
                    "zero_trust_initiative",
                    "privileged_access_management",
                    "email_security_enhanced",
                    "web_filtering",
                    "dns_filtering",
                    "encryption_at_rest",
                    "encryption_in_transit",
                    "incident_response_plan_updated",
                    "tabletop_exercises",
                    "cyber_insurance_obtained",
                    "vendor_security_review",
                    "other"
                ]
            }
        },
        
        # ========== TRANSPARENCY & DISCLOSURE ==========
        "public_disclosure": {"type": "boolean"},
        "public_disclosure_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "disclosure_delay_days": {"type": "number"},
        "disclosure_source": {
            "type": "string",
            "enum": [
                "institution_statement",
                "media_report",
                "attacker_leak_site",
                "regulatory_filing",
                "law_enforcement",
                "social_media",
                "security_researcher",
                "other"
            ]
        },
        "transparency_level": {
            "type": "string",
            "enum": ["excellent", "good", "adequate", "poor", "none"]
        },
        "official_statement_url": {"type": "string"},
        "incident_report_url": {"type": "string"},
        "updates_provided_count": {"type": "integer"},
        
        # ========== CROSS-INCIDENT ANALYSIS FIELDS ==========
        "attack_campaign_name": {
            "type": "string",
            "description": "If part of a larger campaign (e.g., MOVEit exploitation wave)"
        },
        "related_incidents": {
            "type": "array",
            "items": {"type": "string"},
            "description": "IDs or references to related incidents"
        },
        "common_vulnerability_exploited": {"type": "string"},
        "sector_targeting_pattern": {
            "type": "string",
            "enum": ["targeted_education_only", "opportunistic_multi_sector", "unknown"]
        },
        
        # ========== SOURCE METADATA ==========
        "source_url": {"type": "string"},
        "source_headline": {"type": "string"},
        "source_publisher": {"type": "string"},
        "source_language": {"type": "string"},
        "key_quotes": {
            "type": "array",
            "items": {"type": "string"}
        },
        
        # ========== SUMMARY & NOTES ==========
        "enriched_summary": {
            "type": "string",
            "description": "Comprehensive 2-3 paragraph summary of the incident for threat intelligence"
        },
        "extraction_notes": {
            "type": "string",
            "description": "Notes about data quality, missing information, or extraction challenges"
        },
        "confidence_score": {
            "type": "number",
            "minimum": 0,
            "maximum": 1.0,
            "description": "Confidence in the accuracy of extracted information"
        }
    },
    "required": [
        "is_edu_cyber_incident",
        "enriched_summary"
    ]
}

# Mapping of ransomware families to their known aliases
RANSOMWARE_ALIASES = {
    "lockbit": ["lockbit", "lockbit 2.0", "lockbit 3.0", "lockbit black", "lockbit green"],
    "blackcat_alphv": ["blackcat", "alphv", "alpha", "noberus"],
    "cl0p_clop": ["cl0p", "clop", "ta505"],
    "revil_sodinokibi": ["revil", "sodinokibi", "sodin"],
    "conti": ["conti", "ryuk successor"],
    "vice_society": ["vice society", "vs"],
    "royal": ["royal", "blacksuit"],
    "black_basta": ["black basta", "blackbasta"],
}

# Attack category mapping for normalization
ATTACK_CATEGORY_MAPPING = {
    "ransomware": "ransomware_encryption",
    "phishing": "phishing_credential_harvest",
    "data_breach": "data_breach_external",
    "ddos": "ddos_volumetric",
    "malware": "malware_trojan",
}
