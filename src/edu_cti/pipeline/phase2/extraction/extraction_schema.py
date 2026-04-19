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
            "description": (
                "True ONLY if the article reports a specific, discrete cyber incident affecting one or more "
                "identified education institutions (school, university, district, edtech vendor, etc.). "
                "Set to FALSE for: annual/periodic threat reports (e.g. Malwarebytes report, Verizon DBIR), "
                "aggregate statistics articles ('ransomware attacks rose X%'), trend analysis pieces, "
                "best-practice or advice articles, or any article that discusses many incidents in aggregate "
                "without reporting a single specific event against a named victim."
            )
        },
        "education_relevance_reasoning": {
            "type": "string",
            "description": (
                "Brief explanation (1-2 sentences) of why this is or isn't a specific education sector incident. "
                "If false, state whether it is a report, trend article, or other non-incident content."
            )
        },
        "institution_name": {
            "type": "string",
            "description": (
                "Full official name of the affected educational institution, extracted from the article body only. "
                "Do NOT use the article headline, URL, or subtitle as the institution name. "
                "Set to null if the victim institution is not explicitly named in the article text."
            )
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
        "country_code": {"type": "string", "description": "ISO 3166-1 alpha-2 country code, e.g. US, GB, DE"},
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
        "incident_date": {"type": "string", "description": "ISO 8601 date of the incident, e.g. 2024-03-15"},
        "incident_date_precision": {
            "type": "string",
            "enum": ["exact", "approximate", "month_only", "year_only", "unknown"]
        },
        "discovery_date": {"type": "string", "description": "ISO 8601 date when the incident was discovered, e.g. 2024-03-20"},
        "publication_date": {"type": "string", "description": "ISO 8601 date the article/report was published, e.g. 2024-03-22"},
        "dwell_time_days": {
            "type": "number",
            "description": (
                "Days between initial compromise and discovery. "
                "Only populate if BOTH the compromise date and discovery date are explicitly stated in the article. "
                "Do NOT estimate or calculate from approximate dates."
            )
        },
        "timeline": {
            "type": "array",
            "description": (
                "Chronological list of ONLY the events explicitly stated or directly implied by the article. "
                "Do NOT infer, fabricate, or interpolate dates that are not specifically mentioned. "
                "If the article says 'three years ago' without a specific date, use date_precision='approximate' and "
                "estimate the year only — do not add monthly checkpoints. "
                "Omit any date you cannot cite a direct quote or clear implication from the article for."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "ISO 8601 date of this event, e.g. 2024-03-15"},
                    "date_precision": {
                        "type": "string",
                        "enum": ["day", "month", "year", "approximate"]
                    },
                    "event_description": {
                        "type": "string",
                        "description": (
                            "One sentence from or based on the article that explains WHY this date is classified "
                            "as this event type. Quote or paraphrase the specific evidence. "
                            "Examples: 'Administrators became aware of unauthorized grade changes on this date.' "
                            "/ 'Superintendent sent letter to parents notifying them of the breach.' "
                            "/ 'School district publicly disclosed the incident via Facebook post.' "
                            "Keep it under 25 words."
                        )
                    },
                    "event_type": {
                        "type": "string",
                        "enum": [
                            "initial_access",
                            "reconnaissance",
                            "lateral_movement",
                            "privilege_escalation",
                            "exploitation",
                            "data_exfiltration",
                            "encryption_started",
                            "ransom_demand",
                            "impact",
                            "operational_impact",
                            "discovery",
                            "containment",
                            "eradication",
                            "recovery",
                            "disclosure",
                            "notification",
                            "investigation",
                            "remediation",
                            "response_action",
                            "security_improvement",
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
                    "cve_id": {"type": "string", "description": "CVE identifier, e.g. CVE-2024-12345"},
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
                    "cvss_score": {"type": "number", "minimum": 0, "maximum": 10, "description": "CVSS score — only if explicitly stated in the article. Do NOT use your knowledge of the CVE's published score."}
                }
            }
        },
        
        # ========== MITRE ATT&CK (EXTENSIVE) ==========
        "mitre_attack_techniques": {
            "type": "array",
            "description": (
                "MITRE ATT&CK techniques directly evidenced by the article's description of the attack. "
                "Each technique must map to something explicitly stated in the article — do NOT add techniques "
                "based on what is typical for this attack type. "
                "If the article provides no technical detail about how the attack occurred, set to null. "
                "Use standard technique IDs (e.g. T1566 for phishing, T1486 for encryption, T1078 for valid accounts)."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "technique_id": {
                        "type": "string",
                        "description": "MITRE ATT&CK technique ID, e.g. T1566 or T1566.001"
                    },
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
                },
                "required": ["technique_id", "technique_name", "tactic"]
            }
        },
        
        # ========== THREAT ACTOR (EXTENSIVE) ==========
        "threat_actor_claimed": {"type": "boolean"},
        "threat_actor_name": {
            "type": "string",
            "description": "Name of the threat actor or group as explicitly stated in the article. Set to null if not named — do NOT infer from ransomware family or attack type."
        },
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
        "threat_actor_origin_country": {
            "type": "string",
            "description": "Country of origin as explicitly stated in the article. Do NOT infer from the actor's known attribution — set to null unless the article says so."
        },
        "threat_actor_claim_url": {"type": "string"},
        
        # ========== RANSOMWARE/MALWARE (EXTENSIVE) ==========
        "ransomware_family": {
            "type": "string",
            "description": "Ransomware family as explicitly named in the article. Set to null if not named — do NOT infer from ransom note style or attack pattern.",
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
        "ransom_amount": {"type": "number", "description": "Ransom amount in USD — only from an explicitly stated figure. Do NOT estimate. Null if not stated."},
        "ransom_amount_min": {"type": "number", "description": "Minimum ransom amount in USD — only from an explicitly stated figure. Null if not stated."},
        "ransom_amount_max": {"type": "number", "description": "Maximum ransom amount in USD — only from an explicitly stated figure. Null if not stated."},
        "ransom_amount_exact": {"type": "number", "description": "Exact ransom amount in USD — only from an explicitly stated figure. Null if not stated."},
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
            "description": (
                "Categories of data exposed or stolen. Map article text to enum values using this guide: "
                "student_pii — student names, IDs, contact info, dates of birth, addresses; "
                "student_ssn — student social security numbers; "
                "student_grades/student_transcripts — academic records, GPA, course grades, transcripts; "
                "student_financial_aid — FAFSA, student loans, scholarships, financial aid; "
                "student_health_records — student medical or health data; "
                "student_disciplinary — disciplinary records; "
                "student_immigration — visa or immigration documents; "
                "student_housing — housing or dormitory records; "
                "employee_pii — staff or faculty names, dates of birth, phone numbers, home addresses, job titles, employment dates; "
                "employee_ssn — staff or faculty social security numbers; "
                "employee_payroll — salary, payroll, compensation, direct deposit info; "
                "employee_benefits — benefits enrollment, insurance elections; "
                "employee_background_checks — background check results; "
                "alumni_pii — alumni names, contact information; "
                "alumni_donation_history — donor or giving history; "
                "research_data — general research data or datasets; "
                "research_grants — grant applications or award data; "
                "research_ip — intellectual property from research; "
                "research_unpublished — unpublished manuscripts or findings; "
                "financial_records — billing records, invoices, financial statements, health billing; "
                "bank_accounts — bank account numbers or ACH data; "
                "credit_cards — payment card data, PAN, CVV; "
                "tax_records — W-2, tax filings, IRS data; "
                "medical_records — patient health records, EHR, clinical data; "
                "health_insurance — health insurance policy or claims data; "
                "usernames_passwords — login credentials, password hashes; "
                "other — any other sensitive data mentioned that does not fit above. "
                "Set to [] only if the article contains no mention of specific data types."
            ),
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
        "records_affected_min": {"type": "integer", "description": "Minimum records affected — only from an explicitly stated number in the article. Null if not stated."},
        "records_affected_max": {"type": "integer", "description": "Maximum records affected — only from an explicitly stated number in the article. Null if not stated."},
        "records_affected_exact": {"type": "integer", "description": "Exact records affected — only from an explicitly stated number in the article. Null if not stated."},
        "data_volume_gb": {"type": "number", "description": "Data volume in GB — only if explicitly stated in the article. Null if not stated."},
        
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
        "outage_start_date": {"type": "string", "description": "ISO 8601 date the outage began"},
        "outage_end_date": {"type": "string", "description": "ISO 8601 date the outage ended"},
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
        "notification_sent_date": {"type": "string", "description": "ISO 8601 date the breach notification was sent"},
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
        "recovery_started_date": {"type": "string", "description": "ISO 8601 date recovery operations began"},
        "recovery_completed_date": {"type": "string", "description": "ISO 8601 date recovery was completed"},
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
        "public_disclosure_date": {"type": "string", "description": "ISO 8601 date the incident was publicly disclosed"},
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
        
        # ========== NOTES ==========
        # enriched_summary has been moved to a dedicated second LLM call so the full
        # token budget here is spent exclusively on structured intelligence fields.
        "extraction_notes": {
            "type": "string",
            "description": "Notes about data quality, missing information, or extraction challenges"
        },
        "confidence_score": {
            "type": "number",
            "minimum": 0,
            "maximum": 1.0,
            "description": "Confidence in the accuracy of extracted information"
        },
        "other_edu_incidents": {
            "type": "array",
            "description": (
                "OTHER education sector cyber incidents briefly mentioned in this article "
                "that are NOT the primary incident being extracted. "
                "Only populate when this is a roundup/digest/weekly-breach article "
                "covering multiple separate victims. "
                "Each entry becomes a separate incident record in the database."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "victim_name": {
                        "type": "string",
                        "description": "Full name of the educational institution"
                    },
                    "incident_date": {
                        "type": "string",
                        "description": "ISO date YYYY-MM-DD or null if not stated"
                    },
                    "attack_type": {
                        "type": "string",
                        "description": "e.g. ransomware, data_breach, phishing"
                    },
                    "country": {
                        "type": "string",
                        "description": "Country of the institution"
                    },
                    "brief_description": {
                        "type": "string",
                        "description": "1-2 sentence summary of what happened"
                    }
                },
                "required": ["victim_name"]
            }
        }
    },
    "required": [
        "is_edu_cyber_incident",
    ]
}

# ── Second LLM call: summary-only schema ──────────────────────────────────────
SUMMARY_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "enriched_summary": {
            "type": "string",
            "description": (
                "2-3 sentence plain-English summary of this specific cyber incident. "
                "State: who was attacked, what type of attack, what was impacted, "
                "and any known outcome or response. Be factual and concise."
            )
        }
    },
    "required": ["enriched_summary"]
}

SUMMARY_PROMPT = (
    "Write a 2-3 sentence plain-English summary of this specific cyber incident. "
    "State who was attacked, what type of attack occurred, what data or systems were "
    "impacted, and any known response or outcome. Be factual and concise.\n\n"
    "Institution: {institution}\n"
    "Attack type: {attack_category}\n\n"
    "Article:\n{text}"
)
