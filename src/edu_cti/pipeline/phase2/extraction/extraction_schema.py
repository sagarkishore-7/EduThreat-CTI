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
                "library",
                "tribal_college",
                "military_academy",
                "edtech_platform",
                "tutoring_service",
                "consortium",
                "education_department",
                "education_ministry",
                "student_loan_servicer",
                "education_nonprofit",
                "education_vendor",
                "unknown"
            ],
            "description": (
                "Canonical institution type. Choose the most specific match. "
                "university_public — state/publicly funded universities (look for 'state university', 'public university', funded by state legislature). "
                "university_private — private non-research colleges/universities: named after a founder/person, religious affiliation (Catholic, Baptist, Jesuit, Methodist, etc.), or when the article does not mention state funding. "
                "university_research — R1/R2 research-intensive universities: look for 'research university', large NSF/NIH grants, doctoral programs emphasized, or the word 'research' is central to its identity. "
                "school_district — when victim is named as a 'school district', 'unified school district', 'ISD', 'USD', 'CUSD', 'PUSD', 'CISD', 'HISD', 'county schools', 'public schools' (city/county-level). "
                "k12_public_school — single public elementary, middle, or high school. "
                "k12_private_school — single private K-12 school (non-charter). "
                "k12_charter_school — charter school. "
                "community_college — two-year community or junior college. "
                "technical_college — technical/vocational two-year college. "
                "edtech_platform — software/SaaS vendors serving education (e.g. PowerSchool, Illuminate, Blackbaud). "
                "tutoring_service — online tutoring/test-prep companies (e.g. Chegg). "
                "library — public or academic libraries. "
                "tribal_college — tribal/first-nations colleges. "
                "military_academy — military service academies. "
                "Use unknown ONLY when no classification is possible from the article text."
            )
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
                            "REQUIRED — do NOT leave null or empty. "
                            "One sentence drawn directly from the article that explains what happened on this date. "
                            "If you cannot write a concrete sentence from the article, omit this timeline entry entirely. "
                            "Examples: 'Administrators became aware of unauthorized grade changes.' "
                            "/ 'District sent notification letters to affected families.' "
                            "/ 'Ransomware encrypted servers causing campus-wide network outage.' "
                            "Keep under 25 words."
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
            "description": (
                "MITRE Unified Kill Chain phases present in this attack. "
                "ALWAYS populate when attack_category is known — do NOT leave null for ransomware, "
                "data breach, or phishing incidents. "
                "Required minimums: ransomware_* → ['initial_access','execution','impact'] at minimum; "
                "data_breach_external/data_exposure → ['initial_access','exfiltration']; "
                "phishing_* → ['initial_access']; ddos_* → ['impact']; "
                "supply_chain_* → ['initial_access','execution']. "
                "Only include phases directly evidenced by the article text."
            ),
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
                "COMPLETENESS REQUIREMENT: populate technique_id, technique_name, tactic, AND description "
                "for every entry — do NOT add an entry with only technique_id. Either fill all four or omit. "
                "Use standard technique IDs (e.g. T1566 for phishing, T1486 for encryption, T1078 for valid accounts)."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "technique_id": {
                        "type": "string",
                        "description": "MITRE ATT&CK technique ID, e.g. T1566 or T1566.001"
                    },
                    "technique_name": {
                        "type": "string",
                        "description": "Official MITRE technique name, e.g. 'Phishing' or 'Data Encrypted for Impact'"
                    },
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
                    "description": {
                        "type": "string",
                        "description": (
                            "REQUIRED — one sentence from the article explaining how this technique was used. "
                            "Do NOT leave null. Example: 'Ransomware encrypted files across administrative servers.' "
                            "/ 'Attacker used phishing email to steal VPN credentials.' Keep under 20 words."
                        )
                    },
                    "sub_techniques": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                },
                "required": ["technique_id", "technique_name", "tactic", "description"]
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
                "ransomhub",
                "interlock",
                "fog",
                "meow",
                "phobos",
                "avoslocker",
                "blacksuit",
                "doppelpaymer",
                "qilin",
                "pysa",
                "prometheus",
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
                "ALL categories of data exposed or stolen — enumerate every type that applies, do NOT stop at the first match. "
                "Example: 'names, SSNs, and health insurance of employees' → ['employee_pii', 'employee_ssn', 'health_insurance']. "
                "Map article text to enum values using this guide: "
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
            "description": (
                "IT systems impacted by the attack. Map article text to enum values using this guide: "
                "email_system — email, Microsoft Exchange, Outlook, Gmail, mail servers; "
                "active_directory — Active Directory, AD, domain controller, LDAP; "
                "identity_management — SSO, identity provider, IAM, MFA systems; "
                "vpn — VPN, remote access, Cisco AnyConnect; "
                "firewall — firewall, perimeter security, gateway; "
                "file_servers — file servers, network shares, NAS, shared drives, storage servers; "
                "backup_systems — backups, backup servers, DR systems, Veeam, Acronis; "
                "virtualization — virtual machines, VMware, Hyper-V, hypervisor; "
                "core_network — network, LAN, campus network, switches, routers, internet access; "
                "wifi_network — WiFi, wireless network, wireless access points; "
                "data_center — data center, server room, on-premises infrastructure; "
                "public_website — website, web server, public-facing site, homepage; "
                "student_portal — student portal, student information system (SIS), student records; "
                "staff_portal — staff portal, employee self-service; "
                "lms_learning_management — LMS, Canvas, Blackboard, Moodle, online learning platform; "
                "sis_student_information — student information system, Banner, PeopleSoft, enrollment system; "
                "registration_system — registration, course enrollment, admissions portal; "
                "grade_system — grading system, gradebook, academic records system; "
                "library_system — library system, catalog, OPAC; "
                "erp_system — ERP, enterprise resource planning, SAP, Oracle; "
                "hr_system — HR system, human resources, HRIS, PeopleSoft HR; "
                "payroll_system — payroll system, payroll processing; "
                "financial_system — financial system, accounting, finance, billing; "
                "research_computing_hpc — HPC, research computing, supercomputer, high-performance compute; "
                "ehr_emr — EHR, EMR, electronic health records, patient records, clinical systems; "
                "hospital_systems — hospital systems, clinical operations, health system; "
                "medical_devices — medical devices, IoT medical, connected devices; "
                "printing_system — print services, printers; "
                "physical_access — badge access, door locks, building security; "
                "cctv_security — CCTV, cameras, surveillance. "
                "Set to [] if the article does not specify which systems were affected."
            ),
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
        "network_compromised": {
            "type": "boolean",
            "description": (
                "True if the attacker gained network-level access beyond a single endpoint — i.e., lateral movement, "
                "domain controller compromise, or multi-system impact is described. "
                "Set to True for ALL ransomware attacks that encrypted multiple systems (ransomware by definition requires network access). "
                "Set to False ONLY for isolated single-endpoint incidents or phishing with no follow-on network access confirmed. "
                "Use null only when the article gives no information about network impact."
            )
        },
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

# ── Split extraction schemas (experimental 3-call approach) ───────────────────
# Call 1: Core identification & classification (~45 fields, ~2.5K token schema).
#   attack_chain placed immediately after attack_vector so it gets focused attention.
# Call 2: Deep intelligence (~35 fields, ~2.5K token schema) — the chronically null fields.
#   timeline (event_description), mitre_attack_techniques (all 4 fields), regulatory,
#   financial breakdown, recovery, disclosure.

EXTRACTION_SCHEMA_PART1 = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "CTI Extraction Part 1 — Core Identification & Classification",
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "is_edu_cyber_incident": {"type": "boolean"},
        "education_relevance_reasoning": {"type": "string"},
        "institution_name": {"type": "string"},
        "institution_aliases": {"type": "array", "items": {"type": "string"}},
        "institution_type": {
            "type": "string",
            "enum": [
                "university_public", "university_private", "university_research",
                "community_college", "technical_college", "vocational_school",
                "k12_public_school", "k12_private_school", "k12_charter_school",
                "school_district", "research_institute", "research_center",
                "medical_school", "university_hospital", "teaching_hospital",
                "online_university", "library", "tribal_college", "military_academy",
                "edtech_platform", "tutoring_service", "consortium",
                "education_department", "education_ministry", "student_loan_servicer",
                "education_nonprofit", "education_vendor", "unknown",
            ],
        },
        "institution_size": {
            "type": "string",
            "enum": ["small_under_5k", "medium_5k_20k", "large_20k_50k", "very_large_over_50k", "unknown"],
        },
        "country": {"type": "string"},
        "country_code": {"type": "string"},
        "region": {"type": "string"},
        "city": {"type": "string"},
        "incident_severity": {
            "type": "string",
            "enum": ["critical", "high", "medium", "low", "informational"],
        },
        "incident_status": {
            "type": "string",
            "enum": ["ongoing", "contained", "resolved", "unknown"],
        },
        "incident_date": {"type": "string"},
        "incident_date_precision": {
            "type": "string",
            "enum": ["exact", "approximate", "month_only", "year_only", "unknown"],
        },
        "discovery_date": {"type": "string"},
        "publication_date": {"type": "string"},
        "attack_category": {
            "type": "string",
            "enum": [
                "ransomware_encryption", "ransomware_double_extortion",
                "ransomware_triple_extortion", "ransomware_data_leak_only",
                "phishing_credential_harvest", "phishing_malware_delivery",
                "spear_phishing", "whaling", "business_email_compromise",
                "smishing", "vishing",
                "data_breach_external", "data_breach_internal",
                "data_exposure_misconfiguration", "data_leak_accidental",
                "ddos_volumetric", "ddos_application", "ddos_protocol",
                "malware_trojan", "malware_worm", "malware_backdoor",
                "malware_rootkit", "malware_cryptominer", "malware_infostealer",
                "malware_rat", "malware_botnet",
                "unauthorized_access", "privilege_escalation",
                "credential_stuffing", "brute_force", "password_spraying",
                "web_defacement", "sql_injection", "xss_attack", "api_abuse",
                "insider_malicious", "insider_negligent", "insider_compromised",
                "supply_chain_software", "supply_chain_hardware",
                "supply_chain_service_provider", "third_party_compromise",
                "social_engineering", "physical_breach", "account_takeover",
                "extortion_no_ransomware", "hacktivism", "espionage",
                "sabotage", "fraud", "unknown", "other",
            ],
        },
        "secondary_attack_categories": {"type": "array", "items": {"type": "string"}},
        "attack_vector": {
            "type": "string",
            "enum": [
                "phishing_email", "spear_phishing_email", "malicious_attachment",
                "malicious_link", "business_email_compromise",
                "stolen_credentials", "credential_stuffing", "brute_force",
                "password_spraying", "credential_phishing", "session_hijacking",
                "vulnerability_exploit_known", "vulnerability_exploit_zero_day",
                "unpatched_system", "misconfiguration", "default_credentials",
                "drive_by_download", "watering_hole", "malvertising",
                "sql_injection", "xss", "csrf", "ssrf", "path_traversal",
                "exposed_service", "exposed_rdp", "exposed_vpn", "exposed_ssh",
                "exposed_database", "exposed_api", "man_in_the_middle",
                "supply_chain_compromise", "third_party_vendor",
                "software_update_compromise", "trusted_relationship",
                "social_engineering", "pretexting", "baiting", "tailgating", "usb_drop",
                "insider_access", "former_employee",
                "cloud_misconfiguration", "api_key_exposure", "storage_bucket_exposure",
                "dns_hijacking", "bgp_hijacking", "sim_swapping", "unknown", "other",
            ],
        },
        # attack_chain immediately after attack_vector — LLM sees kill-chain prompt
        # while still in the attack-classification context, reducing null rate.
        "attack_chain": {
            "type": "array",
            "description": (
                "MITRE Unified Kill Chain phases present. ALWAYS populate when attack_category is known. "
                "ransomware_* → ['initial_access','execution','impact'] minimum. "
                "data_breach_external → ['initial_access','exfiltration']. "
                "ddos_* → ['impact']. Only phases evidenced by the article."
            ),
            "items": {
                "type": "string",
                "enum": [
                    "reconnaissance", "resource_development", "initial_access", "execution",
                    "persistence", "privilege_escalation", "defense_evasion", "credential_access",
                    "discovery", "lateral_movement", "collection", "command_and_control",
                    "exfiltration", "impact",
                ],
            },
        },
        "initial_access_description": {"type": "string"},
        "threat_actor_claimed": {"type": "boolean"},
        "threat_actor_name": {"type": "string"},
        "threat_actor_aliases": {"type": "array", "items": {"type": "string"}},
        "threat_actor_category": {
            "type": "string",
            "enum": [
                "apt_nation_state", "apt_state_sponsored", "cybercriminal_organized",
                "cybercriminal_individual", "ransomware_gang", "ransomware_affiliate",
                "hacktivist", "insider_threat", "script_kiddie", "competitor", "unknown", "other",
            ],
        },
        "threat_actor_motivation": {
            "type": "string",
            "enum": [
                "financial_gain", "espionage", "hacktivism", "sabotage",
                "personal_grievance", "notoriety", "research_theft",
                "competitive_advantage", "unknown",
            ],
        },
        "threat_actor_origin_country": {"type": "string"},
        "ransomware_family": {
            "type": "string",
            "enum": [
                "lockbit", "lockbit_2", "lockbit_3", "blackcat_alphv", "cl0p_clop",
                "akira", "play", "8base", "bianlian", "royal", "black_basta", "medusa",
                "rhysida", "hunters_international", "inc_ransom", "vice_society", "hive",
                "conti", "ryuk", "revil_sodinokibi", "darkside", "blackmatter", "maze",
                "netwalker", "ragnar_locker", "avaddon", "cuba", "pysa_mespinoza", "babuk",
                "grief", "snatch", "quantum", "karakurt", "lorenz", "noescape", "cactus",
                "trigona", "money_message", "nokoyawa", "ransomhouse", "daixin", "ransomhub",
                "interlock", "fog", "meow", "phobos", "avoslocker", "blacksuit",
                "doppelpaymer", "qilin", "pysa", "prometheus", "unknown", "other",
            ],
        },
        "malware_families": {"type": "array", "items": {"type": "string"}},
        "attacker_tools": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "cobalt_strike", "metasploit", "mimikatz", "psexec", "bloodhound",
                    "sharphound", "powershell_empire", "covenant", "sliver", "brute_ratel",
                    "impacket", "rubeus", "kerbrute", "hashcat", "john_the_ripper",
                    "nmap", "masscan", "shodan", "rclone", "mega_sync", "winscp",
                    "filezilla", "anydesk", "teamviewer", "atera", "splashtop",
                    "ngrok", "ligolo", "chisel", "plink", "other",
                ],
            },
        },
        "was_ransom_demanded": {"type": "boolean"},
        "ransom_amount_exact": {"type": "number"},
        "ransom_paid": {"type": "boolean"},
        "data_breached": {"type": "boolean"},
        "data_exfiltrated": {"type": "boolean"},
        "data_encrypted": {"type": "boolean"},
        "data_destroyed": {"type": "boolean"},
        "data_published": {"type": "boolean"},
        "data_sold": {"type": "boolean"},
        "data_categories": {
            "type": "array",
            "description": "ALL data types exposed — enumerate every category that applies.",
            "items": {
                "type": "string",
                "enum": [
                    "student_pii", "student_ssn", "student_grades", "student_transcripts",
                    "student_financial_aid", "student_disciplinary", "student_health_records",
                    "student_immigration", "student_housing",
                    "employee_pii", "employee_ssn", "employee_payroll", "employee_benefits",
                    "employee_performance", "employee_background_checks",
                    "alumni_pii", "alumni_donation_history",
                    "research_data", "research_grants", "research_ip", "research_unpublished",
                    "research_classified",
                    "financial_records", "bank_accounts", "credit_cards", "tax_records",
                    "donor_information",
                    "medical_records", "health_insurance", "mental_health", "disability_records",
                    "usernames_passwords", "api_keys", "certificates",
                    "intellectual_property", "legal_documents", "contracts",
                    "internal_communications", "security_configurations",
                    "network_diagrams", "general_pii", "health_records", "other",
                ],
            },
        },
        "records_affected_min": {"type": "integer"},
        "records_affected_max": {"type": "integer"},
        "records_affected_exact": {"type": "integer"},
        "data_volume_gb": {"type": "number"},
        "systems_affected": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "email_system", "active_directory", "identity_management", "vpn", "firewall",
                    "dns", "dhcp", "file_servers", "backup_systems", "virtualization",
                    "core_network", "wifi_network", "voip_phone", "data_center",
                    "public_website", "student_portal", "staff_portal", "alumni_portal",
                    "applicant_portal", "lms_learning_management", "sis_student_information",
                    "registration_system", "grade_system", "library_system", "exam_proctoring",
                    "erp_system", "hr_system", "payroll_system", "financial_system",
                    "procurement", "admissions_system", "financial_aid_system",
                    "research_computing_hpc", "research_storage", "lab_instruments",
                    "research_databases", "ehr_emr", "hospital_systems", "medical_devices",
                    "pharmacy_system", "printing_system", "parking_system",
                    "physical_access", "cctv_security", "other",
                ],
            },
        },
        "critical_systems_affected": {"type": "boolean"},
        "network_compromised": {"type": "boolean"},
        "domain_admin_compromised": {"type": "boolean"},
        "backup_compromised": {"type": "boolean"},
        "encryption_extent": {
            "type": "string",
            "enum": ["full_encryption", "partial_encryption", "no_encryption", "unknown"],
        },
        "outage_duration_hours": {"type": "number"},
        "downtime_days": {"type": "number"},
        "operational_impacts": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "classes_cancelled", "classes_moved_online", "exams_postponed",
                    "exams_cancelled", "graduation_delayed", "semester_extended",
                    "campus_closed", "research_halted", "research_data_lost",
                    "payroll_delayed", "financial_aid_delayed", "admissions_suspended",
                    "registration_suspended", "email_unavailable", "website_down",
                    "student_portal_down", "lms_unavailable", "network_offline",
                    "vpn_unavailable", "library_closed", "it_helpdesk_overwhelmed",
                    "manual_processes_required", "clinical_operations_disrupted",
                    "patient_care_affected", "other",
                ],
            },
        },
        "students_affected": {"type": "integer"},
        "staff_affected": {"type": "integer"},
        "total_individuals_affected": {"type": "integer"},
        "confidence_score": {"type": "number", "minimum": 0, "maximum": 1.0},
        "extraction_notes": {"type": "string"},
        "other_edu_incidents": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "victim_name": {"type": "string"},
                    "incident_date": {"type": "string"},
                    "attack_type": {"type": "string"},
                    "country": {"type": "string"},
                    "brief_description": {"type": "string"},
                },
                "required": ["victim_name"],
            },
        },
    },
    "required": ["is_edu_cyber_incident"],
}


EXTRACTION_SCHEMA_PART2 = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "CTI Extraction Part 2 — Deep Intelligence",
    "description": (
        "Second extraction pass. Fill ONLY these fields from the article. "
        "Core facts (institution, attack type, data) were already extracted in Part 1 — do not repeat them here."
    ),
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "timeline": {
            "type": "array",
            "description": (
                "Chronological events EXPLICITLY stated in the article. "
                "event_description is REQUIRED for every entry — if you cannot write a concrete "
                "one-sentence description from the article text, OMIT that entry entirely. "
                "Never leave event_description null."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "date": {"type": "string"},
                    "date_precision": {
                        "type": "string",
                        "enum": ["day", "month", "year", "approximate"],
                    },
                    "event_description": {
                        "type": "string",
                        "description": (
                            "REQUIRED. One sentence from the article: what specifically happened on this date. "
                            "Example: 'District sent notification letters to affected families.' "
                            "Keep under 25 words. If you cannot write this, omit the whole entry."
                        ),
                    },
                    "event_type": {
                        "type": "string",
                        "enum": [
                            "initial_access", "reconnaissance", "lateral_movement",
                            "privilege_escalation", "exploitation", "data_exfiltration",
                            "encryption_started", "ransom_demand", "impact",
                            "operational_impact", "discovery", "containment", "eradication",
                            "recovery", "disclosure", "notification", "investigation",
                            "remediation", "response_action", "security_improvement",
                            "law_enforcement_contact", "public_statement",
                            "systems_restored", "other",
                        ],
                    },
                },
                "required": ["event_description"],
            },
        },
        "mitre_attack_techniques": {
            "type": "array",
            "description": (
                "MITRE ATT&CK techniques directly evidenced by article text. "
                "ALL FOUR fields are REQUIRED for every entry: technique_id, technique_name, tactic, description. "
                "If you cannot fill all four from the article, omit that technique entirely. "
                "Do NOT add techniques based on what is typical — only what the article explicitly describes."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "technique_id": {"type": "string", "description": "e.g. T1566 or T1566.001"},
                    "technique_name": {"type": "string", "description": "Official MITRE name, e.g. 'Phishing'"},
                    "tactic": {
                        "type": "string",
                        "enum": [
                            "reconnaissance", "resource_development", "initial_access", "execution",
                            "persistence", "privilege_escalation", "defense_evasion",
                            "credential_access", "discovery", "lateral_movement", "collection",
                            "command_and_control", "exfiltration", "impact",
                        ],
                    },
                    "description": {
                        "type": "string",
                        "description": (
                            "REQUIRED. One sentence from the article how this technique was used. "
                            "Example: 'Ransomware encrypted files across administrative servers.' Under 20 words."
                        ),
                    },
                },
                "required": ["technique_id", "technique_name", "tactic", "description"],
            },
        },
        "vulnerabilities_exploited": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "cve_id": {"type": "string"},
                    "vulnerability_name": {"type": "string"},
                    "vulnerability_type": {
                        "type": "string",
                        "enum": [
                            "remote_code_execution", "privilege_escalation",
                            "authentication_bypass", "sql_injection", "xss", "ssrf",
                            "deserialization", "path_traversal", "buffer_overflow",
                            "memory_corruption", "information_disclosure",
                            "denial_of_service", "zero_day", "other",
                        ],
                    },
                    "affected_product": {"type": "string"},
                    "cvss_score": {"type": "number", "minimum": 0, "maximum": 10},
                },
            },
        },
        "iocs": {
            "type": "object",
            "properties": {
                "ip_addresses": {"type": "array", "items": {"type": "string"}},
                "domains": {"type": "array", "items": {"type": "string"}},
                "urls": {"type": "array", "items": {"type": "string"}},
                "file_hashes": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "hash_type": {"type": "string", "enum": ["md5", "sha1", "sha256", "sha512"]},
                            "hash_value": {"type": "string"},
                        },
                    },
                },
                "email_addresses": {"type": "array", "items": {"type": "string"}},
                "file_names": {"type": "array", "items": {"type": "string"}},
            },
        },
        "applicable_regulations": {
            "type": "array",
            "description": (
                "Regulations applicable to this incident. Apply inference rules: "
                "FERPA → any US education institution with student data breached. "
                "HIPAA → any health/medical records involved. "
                "GDPR → any EU institution or EU resident data. "
                "UK_DPA → UK institution. state_breach_notification → any US institution."
            ),
            "items": {
                "type": "string",
                "enum": [
                    "FERPA", "HIPAA", "GDPR", "CCPA_CPRA", "PCI_DSS", "GLBA", "SOX",
                    "UK_DPA", "Australia_Privacy_Act", "Canada_PIPEDA",
                    "state_breach_notification", "other",
                ],
            },
        },
        "breach_notification_required": {"type": "boolean"},
        "notification_sent": {"type": "boolean"},
        "notification_sent_date": {"type": "string"},
        "regulators_notified": {"type": "array", "items": {"type": "string"}},
        "investigation_opened": {"type": "boolean"},
        "investigating_agencies": {"type": "array", "items": {"type": "string"}},
        "fine_imposed": {"type": "boolean"},
        "fine_amount_usd": {"type": "number"},
        "lawsuits_filed": {"type": "boolean"},
        "lawsuit_count": {"type": "integer"},
        "class_action_filed": {"type": "boolean"},
        "settlement_amount_usd": {"type": "number"},
        "ransom_amount": {"type": "number"},
        "ransom_amount_min": {"type": "number"},
        "ransom_amount_max": {"type": "number"},
        "ransom_currency": {"type": "string"},
        "ransom_cryptocurrency": {
            "type": "string",
            "enum": ["bitcoin", "monero", "ethereum", "other", "unknown"],
        },
        "ransom_negotiated": {"type": "boolean"},
        "decryptor_received": {"type": "boolean"},
        "decryptor_worked": {"type": "boolean"},
        "estimated_total_cost_usd": {"type": "number"},
        "recovery_cost_usd": {"type": "number"},
        "legal_cost_usd": {"type": "number"},
        "insurance_claim": {"type": "boolean"},
        "insurance_payout_usd": {"type": "number"},
        "dwell_time_days": {"type": "number"},
        "incident_response_activated": {"type": "boolean"},
        "ir_firm_engaged": {"type": "string"},
        "forensics_firm_engaged": {"type": "string"},
        "law_enforcement_involved": {"type": "boolean"},
        "law_enforcement_agencies": {"type": "array", "items": {"type": "string"}},
        "fbi_involved": {"type": "boolean"},
        "cisa_involved": {"type": "boolean"},
        "recovery_method": {
            "type": "string",
            "enum": [
                "backup_restore", "decryptor_used", "ransom_paid_decryption",
                "clean_rebuild", "partial_backup_partial_rebuild", "ongoing", "unknown",
            ],
        },
        "recovery_duration_days": {"type": "number"},
        "security_improvements": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "mfa_implemented", "mfa_expanded", "password_policy_strengthened",
                    "network_segmentation", "endpoint_detection_response",
                    "siem_implemented", "soc_established", "backup_strategy_improved",
                    "air_gapped_backups", "immutable_backups", "security_awareness_training",
                    "phishing_simulation", "vulnerability_management", "penetration_testing",
                    "security_audit", "zero_trust_initiative", "privileged_access_management",
                    "email_security_enhanced", "web_filtering", "dns_filtering",
                    "encryption_at_rest", "encryption_in_transit",
                    "incident_response_plan_updated", "tabletop_exercises",
                    "cyber_insurance_obtained", "vendor_security_review", "other",
                ],
            },
        },
        "public_disclosure": {"type": "boolean"},
        "public_disclosure_date": {"type": "string"},
        "disclosure_delay_days": {"type": "number"},
        "disclosure_source": {
            "type": "string",
            "enum": [
                "institution_statement", "media_report", "attacker_leak_site",
                "regulatory_filing", "law_enforcement", "social_media",
                "security_researcher", "other",
            ],
        },
        "transparency_level": {
            "type": "string",
            "enum": ["excellent", "good", "adequate", "poor", "none"],
        },
        "attack_campaign_name": {"type": "string"},
        "sector_targeting_pattern": {
            "type": "string",
            "enum": ["targeted_education_only", "opportunistic_multi_sector", "unknown"],
        },
        "key_quotes": {"type": "array", "items": {"type": "string"}},
    },
    "required": [],
}

PART2_PROMPT_TEMPLATE = """You are a Senior Cyber Threat Intelligence Analyst. A previous analysis pass already extracted the core facts below. Your job is to extract the DEEP INTELLIGENCE fields from the same article — focus entirely on timeline, MITRE ATT&CK, regulatory implications, financial details, and recovery.

CORE FACTS FROM PART 1 (do not re-extract these):
- Institution: {institution_name} ({institution_type}, {country})
- Attack type: {attack_category}
- Attack vector: {attack_vector}
- Ransomware: {ransomware_family}
- Data breached: {data_categories}
- Records: {records_affected_exact}
- Published: {publication_date}

YOUR TASK — extract ONLY the following fields from the article:

1. TIMELINE — event_description is REQUIRED for every entry. If you cannot write a concrete one-sentence description drawn directly from the article for a date, OMIT that entry entirely. Never leave event_description null or empty. Good: "District sent breach notification letters to 47,000 affected families." Bad: leaving event_description as null.

2. MITRE ATT&CK — all four fields (technique_id, technique_name, tactic, description) are REQUIRED for every technique. If you cannot fill all four from the article, omit that technique entirely. Only include techniques directly evidenced by the article — not what is typical for this attack type.

3. REGULATORY — apply these rules:
   - FERPA applies to any US education institution where student data was breached
   - HIPAA applies if medical or health records were involved
   - GDPR applies to EU institutions or EU resident data
   - UK_DPA applies to UK institutions
   - state_breach_notification applies to any US institution

4. FINANCIAL — only from explicitly stated figures in the article. Null if not stated.

5. RECOVERY — IR firms, law enforcement, recovery method, security improvements — only from the article.

6. DISCLOSURE — how and when was this disclosed? Transparency level?

NULL RULES: If information is not in the article, set to null. Do not guess. Do not fabricate.

ARTICLE:
URL: {url}
Title: {title}
Published: {publication_date}

{text}

---
Output ONLY the JSON object for Part 2 fields. No prose."""


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
    "CONSISTENCY RULES — your summary MUST match these structured fields:\n"
    "- If attack_category starts with 'ransomware_', use the word 'ransomware' in your summary.\n"
    "- If attack_category is 'supply_chain_software' or 'third_party_compromise', name the vendor product.\n"
    "- If attack_category is 'phishing_*' or 'business_email_compromise', mention phishing or email fraud.\n"
    "- Do NOT contradict the attack_category — the summary and the category must tell the same story.\n\n"
    "Institution: {institution}\n"
    "Attack type: {attack_category}\n\n"
    "Article:\n{text}"
)
