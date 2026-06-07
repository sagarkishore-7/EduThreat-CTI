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
                "Normalized English name of the affected educational institution, extracted from the article body only. "
                "Use the best English victim label for cross-article matching (translate or romanise when needed). "
                "Do NOT use the article headline, URL, or subtitle as the institution name. "
                "Do NOT include threat actor names, attack verbs, or generic wrappers. "
                "Set to null if the victim institution is not explicitly named in the article text."
            )
        },
        "institution_aliases": {
            "type": "array",
            "items": {"type": "string"},
            "description": (
                "Alternative names for the institution explicitly mentioned in the article, including native-language "
                "forms, acronyms, abbreviations, legal names, and campus-specific variants."
            )
        },
        "institution_type": {
            "type": "string",
            "enum": [
                "university",
                "community_college",
                "technical_college",
                "vocational_school",
                "k12_school",
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
                "university — any degree-granting higher education institution (state universities, Ivy League, liberal arts colleges, polytechnics). "
                "community_college — two-year community or junior college. "
                "technical_college — technical/vocational two-year college. "
                "k12_school — single K-12 school (elementary, middle, or high school). "
                "school_district — district-level organization ('school district', 'unified school district', 'ISD', 'USD', 'county schools', 'public schools' at city/county level). "
                "research_institute / research_center — standalone research organisation, not degree-granting. "
                "edtech_platform — software/SaaS vendors serving education (e.g. PowerSchool, Illuminate, Blackbaud). "
                "tutoring_service — online tutoring/test-prep companies (e.g. Chegg). "
                "library — libraries (school, academic, or public). "
                "tribal_college — tribal/first-nations colleges. "
                "military_academy — military service academies. "
                "Use unknown ONLY when no classification is possible from the article text."
            )
        },
        "institution_size": {
            "type": "string",
            "enum": ["small_under_5k", "medium_5k_20k", "large_20k_50k", "very_large_over_50k", "unknown"],
            "description": "Enrollment size: small_under_5k (<5,000 students); medium_5k_20k (5–20K); large_20k_50k (20–50K); very_large_over_50k (>50K). Use unknown if not stated."
        },
        "country": {"type": "string", "description": "Standard English country name of the victim institution. Translate and romanise non-Latin names. E.g. 'United States', 'United Kingdom', 'Japan'."},
        "country_code": {"type": "string", "description": "ISO 3166-1 alpha-2 country code, e.g. US, GB, DE"},
        "region": {"type": "string", "description": "State, province, or region of the victim institution. E.g. 'California', 'Ontario', 'Bavaria'. Null if not stated."},
        "city": {"type": "string", "description": "City where the victim institution is located. Null if not stated."},
        
        # ========== INCIDENT CLASSIFICATION ==========
        "incident_severity": {
            "type": "string",
            "enum": ["critical", "high", "medium", "low", "informational"],
            "description": "Overall severity rating of the incident"
        },
        "incident_status": {
            "type": "string",
            "enum": ["ongoing", "contained", "resolved", "unknown"],
            "description": "Current status at time of article: ongoing — attack still active or systems still down; contained — attack stopped, recovery in progress; resolved — fully recovered and back to normal; unknown — not stated in article."
        },
        
        # ========== TIMELINE & DATES ==========
        "incident_date": {"type": "string", "description": "ISO 8601 date of the incident, e.g. 2024-03-15"},
        "incident_date_precision": {
            "type": "string",
            "enum": ["exact", "approximate", "month_only", "year_only", "unknown"],
            "description": "Precision of incident_date: exact — specific date stated; approximate — resolved from relative expression ('last week', 'two months ago'); month_only — only month/year known; year_only — only year known; unknown — no date determinable."
        },
        "discovery_date": {"type": "string", "description": "ISO 8601 date when the incident was discovered, e.g. 2024-03-20"},
        "publication_date": {"type": "string", "description": "ISO 8601 date the article/report was published, e.g. 2024-03-22"},
        "academic_period_affected": {
            "type": "string",
            "enum": [
                "start_of_semester", "mid_semester", "finals_week", "enrollment_period",
                "admissions_period", "graduation_period", "summer_break",
                "winter_break", "spring_break", "unknown"
            ],
            "description": (
                "Part of the academic calendar during which the incident occurred or was discovered. "
                "Infer from incident_date and any article context ('during finals', 'at the start of term'). "
                "Null if not determinable."
            )
        },
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
            "description": (
                "Primary classification of the attack. Choose the single best match. "
                "ransomware_encryption — files encrypted, ransom demanded for decryption key. "
                "ransomware_double_extortion — files encrypted AND data exfiltrated/threatened. "
                "ransomware_data_leak_only — data stolen and published/threatened, but NO encryption. "
                "data_breach_external — external attacker accessed and stole data without ransomware. "
                "data_exposure_misconfiguration — data exposed publicly due to misconfigured storage/server. "
                "phishing_credential_harvest — phishing used to steal login credentials. "
                "business_email_compromise — fraudulent email used to divert payments or deceive staff. "
                "supply_chain_software — victim compromised via a software vendor (e.g. MOVEit, SolarWinds). "
                "third_party_compromise — victim affected because a third-party service provider was breached. "
                "ddos_* — distributed denial of service attacks. "
                "unauthorized_access — attacker gained access to systems without explicit ransom or data theft. "
                "Use unknown only when insufficient information is in the article to classify."
            ),
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
            "description": (
                "How the attacker initially gained access. Choose the single best match. "
                "phishing_email — generic phishing to many targets. "
                "spear_phishing_email — targeted phishing at specific individuals. "
                "stolen_credentials — credentials obtained elsewhere and reused. "
                "vulnerability_exploit_known — known CVE exploited (patch was available). "
                "vulnerability_exploit_zero_day — zero-day exploit, no patch existed. "
                "exposed_rdp — Remote Desktop Protocol exposed to internet. "
                "exposed_vpn — VPN appliance exploited (e.g. Pulse Secure, Fortinet). "
                "third_party_vendor — access gained through a vendor's compromised system. "
                "supply_chain_compromise — via a software update or supplier. "
                "misconfiguration — improperly secured system exposed (open S3 bucket, unsecured database). "
                "Use unknown when initial access method is not stated in the article."
            ),
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
        "initial_access_description": {
            "type": "string",
            "description": "One sentence describing how the attacker gained initial access, drawn directly from the article. E.g. 'Attacker exploited unpatched MOVEit vulnerability to access file transfer server.' Null if initial access method is not described."
        },

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
                    "cwe_id": {"type": "string", "description": "CWE weakness identifier if stated or clearly implied, e.g. CWE-89 for SQL injection. Null if not determinable."},
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
        "threat_actor_claimed": {
            "type": "boolean",
            "description": "Set to true if the threat actor publicly claimed responsibility (on a leak site, Telegram, dark web forum, or similar). Set to false if claims were explicitly denied. Null if not mentioned."
        },
        "threat_actor_name": {
            "type": "string",
            "description": "Name of the threat actor or group as explicitly stated in the article. Set to null if not named — do NOT infer from ransomware family or attack type."
        },
        "threat_actor_aliases": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Alternative names or aliases for the threat actor group as mentioned in the article."
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
            ],
            "description": "Category of threat actor: ransomware_gang — the group operating a RaaS platform; ransomware_affiliate — a group using a RaaS platform; apt_nation_state — confirmed government-sponsored; apt_state_sponsored — state-affiliated but not confirmed direct; cybercriminal_organized — organised crime not specifically ransomware; hacktivist — politically motivated; insider_threat — current or former employee. Use unknown when actor is named but category is unclear."
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
            ],
            "description": "Primary motivation as stated or implied by the article: financial_gain — ransom or data monetisation; espionage — intelligence collection; hacktivism — political/ideological; research_theft — targeting research IP; unknown — not stated."
        },
        "threat_actor_origin_country": {
            "type": "string",
            "description": "Country of origin as explicitly stated in the article. Do NOT infer from the actor's known attribution — set to null unless the article says so."
        },
        "threat_actor_claim_url": {"type": "string", "description": "URL of the threat actor's post claiming responsibility (leak site, Telegram, forum). Null if not stated."},
        "attribution_confidence": {
            "type": "string",
            "enum": ["confirmed", "high", "moderate", "low", "speculative", "unknown"],
            "description": "Confidence in the threat-actor attribution as supported by the article: confirmed — actor claimed responsibility or was confirmed by law enforcement/forensics; high — attributed by a named investigator or the victim; moderate — attributed by a single source with reasoning; low — attributed only by inference; speculative — named only as a suspicion; unknown — no actor named. Set to unknown when threat_actor_name is null."
        },
        "source_reliability": {
            "type": "string",
            "enum": ["A", "B", "C", "D", "E", "F"],
            "description": "Admiralty-scale reliability of the reporting source for this incident: A — completely reliable (official statement, regulator, court filing); B — usually reliable (established security outlet, named investigator); C — fairly reliable (general news with attribution); D — not usually reliable (single unverified report); E — unreliable; F — cannot be judged. Grade the article reporting the incident, not the threat actor."
        },

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
        # ========== RANSOM DETAILS ==========
        "was_ransom_demanded": {
            "type": "boolean",
            "description": (
                "Set to true if the article states a ransom was demanded — e.g. 'demanded payment', "
                "'ransom note left', 'attackers demanded X', 'asked for payment in exchange for decryption key', "
                "'threatened to publish data unless paid'. For attack_category ransomware_encryption or "
                "ransomware_double_extortion this is almost always true. "
                "Set to false only if the article explicitly confirms no ransom was demanded. Null if not mentioned."
            )
        },
        "ransom_amount": {"type": "number", "description": "Ransom amount in USD — only from an explicitly stated figure. Do NOT estimate. Null if not stated."},
        "ransom_amount_min": {"type": "number", "description": "Minimum ransom amount in USD — only from an explicitly stated figure. Null if not stated."},
        "ransom_amount_max": {"type": "number", "description": "Maximum ransom amount in USD — only from an explicitly stated figure. Null if not stated."},
        "ransom_amount_exact": {"type": "number", "description": "Exact ransom amount in USD — only from an explicitly stated figure. Null if not stated."},
        "ransom_currency": {"type": "string", "description": "Currency of the ransom demand, e.g. 'USD', 'EUR'. Null if not stated."},
        "ransom_cryptocurrency": {
            "type": "string",
            "description": "Cryptocurrency demanded for ransom payment. Null if not stated.",
            "enum": ["bitcoin", "monero", "ethereum", "other", "unknown"]
        },
        "ransom_paid": {
            "type": "boolean",
            "description": "Set to true if the article explicitly states ransom was paid. Set to false if the institution explicitly refused to pay or the article confirms no payment. Null if not mentioned."
        },
        "ransom_paid_amount": {"type": "number", "description": "Amount paid in USD. Only from explicitly stated figure. Null if not stated."},
        "ransom_negotiated": {
            "type": "boolean",
            "description": "Set to true if ransom negotiations between victim and attacker are described. Null if not mentioned."
        },
        "ransom_deadline_days": {"type": "number", "description": "Number of days given to pay the ransom. Null if not stated."},
        "decryptor_received": {
            "type": "boolean",
            "description": "Set to true if the victim received a working decryption key or tool after the attack. Null if not mentioned."
        },
        "decryptor_worked": {
            "type": "boolean",
            "description": "Set to true if the decryption tool successfully restored encrypted files. Null if not mentioned."
        },
        
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
        "data_breached": {
            "type": "boolean",
            "description": "Set to true if personal or sensitive data was confirmed to have been accessed, stolen, or exposed. Set to false ONLY if the article explicitly confirms no data was accessed. Null if not mentioned. For ransomware_double_extortion this is almost always true."
        },
        "data_exfiltrated": {
            "type": "boolean",
            "description": "Set to true if data was confirmed to have been copied and taken by the attacker. True for ransomware_double_extortion by definition. Null if not confirmed in the article."
        },
        "data_encrypted": {
            "type": "boolean",
            "description": "Set to true if files or systems were encrypted by the attacker. True for ransomware_encryption and ransomware_double_extortion. Set to false if article confirms no encryption occurred. Null if not mentioned."
        },
        "data_destroyed": {
            "type": "boolean",
            "description": "Set to true if data was permanently deleted or corrupted. Null if not mentioned."
        },
        "data_published": {
            "type": "boolean",
            "description": "Set to true if stolen data was published on a leak site, dark web, or made publicly available. Null if not mentioned."
        },
        "data_sold": {
            "type": "boolean",
            "description": "Set to true if stolen data was reportedly offered for sale. Null if not mentioned."
        },
        "dark_web_posting_confirmed": {
            "type": "boolean",
            "description": (
                "True if the article explicitly confirms stolen data was posted on a dark web leak site or forum by the attacker — "
                "e.g. 'data appeared on ALPHV's leak site', 'LockBit published files'. "
                "Stronger than data_published: requires confirmed posting, not just threat. Null if not mentioned."
            )
        },
        "prior_breach_same_institution": {
            "type": "boolean",
            "description": (
                "True if the article mentions this institution was previously breached or cyberattacked before this incident — "
                "e.g. 'hit for the second time', 'suffered a similar attack in 2021'. "
                "Signals repeat targeting. Null if prior history not mentioned."
            )
        },
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
        "pii_records_leaked": {"type": "integer", "description": "Number of PII records specifically confirmed as leaked, published, or exfiltrated externally (e.g. posted on a leak site, sold, or confirmed stolen). Distinct from records_affected_exact which counts all exposed records. Only from explicitly stated numbers. Null if not stated."},
        "data_volume_gb": {"type": "number", "description": "Data volume in GB — only if explicitly stated in the article. Null if not stated."},
        
        # ========== SYSTEM IMPACT (EXTENSIVE) ==========
        "cloud_provider": {
            "type": "string",
            "description": "Cloud provider involved if cloud infrastructure was affected. Use none if purely on-premises. Null if not mentioned.",
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
        "critical_systems_affected": {
            "type": "boolean",
            "description": "Set to true if safety-critical or operationally essential systems were impacted (patient care, core network, financial systems, student records, research computing). Null if not described."
        },
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
        "domain_admin_compromised": {"type": "boolean", "description": "Set to true if the attacker gained domain administrator or Active Directory admin access. Trigger phrases: 'domain controller compromised', 'admin credentials stolen', 'AD taken over'. Null if not mentioned."},
        "backup_compromised": {"type": "boolean", "description": "Set to true if backup systems were deleted, encrypted, or corrupted by the attacker. Trigger phrases: 'backups were deleted', 'encrypted backups', 'backup systems were also affected'. Null if not mentioned."},
        "encryption_extent": {
            "type": "string",
            "enum": ["full_encryption", "partial_encryption", "no_encryption", "unknown"],
            "description": "Scope of encryption: full_encryption — all/most systems; partial_encryption — some systems; no_encryption — data theft without encryption; unknown — ransomware confirmed but extent not stated."
        },
        "servers_affected_count": {"type": "integer", "description": "Number of servers affected. Null if not stated."},

        # ========== OPERATIONAL IMPACT ==========
        "outage_start_date": {"type": "string", "description": "ISO 8601 date the outage began"},
        "outage_end_date": {"type": "string", "description": "ISO 8601 date the outage ended"},
        "outage_duration_hours": {"type": "number", "description": "Total outage duration in hours. Only from explicitly stated figures. Null if not stated."},
        "downtime_days": {"type": "number", "description": "Days the institution was fully down or systems were completely unavailable. Only from explicitly stated figures. Null if not stated."},
        "teaching_impacted": {
            "type": "boolean",
            "description": (
                "Set to true if teaching, classes, or academic instruction was disrupted by the incident — "
                "e.g. classes cancelled, moved online, exams postponed, semester extended. "
                "Set to false only if the article explicitly states teaching continued normally. "
                "Null if not mentioned."
            )
        },
        "research_impacted": {
            "type": "boolean",
            "description": (
                "Set to true if research operations were disrupted — e.g. research computing unavailable, "
                "HPC clusters down, research data inaccessible, lab instruments offline, grants delayed. "
                "Set to false only if the article explicitly states research was unaffected. "
                "Null if not mentioned."
            )
        },
        "operational_impacts": {
            "description": "Operational disruptions caused by the attack. Select all that apply based on article text.",
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
        "students_affected": {"type": "integer", "description": "Number of students whose data was affected. Only from explicitly stated numbers. Null if not stated."},
        "staff_affected": {"type": "integer", "description": "Number of staff/employees whose data was affected. Null if not stated."},
        "faculty_affected": {"type": "integer", "description": "Number of faculty members whose data was affected. Null if not stated."},
        "alumni_affected": {"type": "integer", "description": "Number of alumni whose data was affected. Null if not stated."},
        "parents_affected": {"type": "integer", "description": "Number of parents or guardians whose data was affected (common in K-12 breaches where parent contact info is stored). Null if not stated."},
        "applicants_affected": {"type": "integer", "description": "Number of applicants or prospective students affected. Null if not stated."},
        "patients_affected": {"type": "integer", "description": "Number of patients affected (for university hospitals and medical schools). Null if not stated."},
        "total_individuals_affected": {"type": "integer", "description": "Total number of individuals affected across all categories. Null if not stated."},
        "users_affected_min": {"type": "integer", "description": "Minimum total users affected across all groups. Only from explicitly stated figures. Null if not stated."},
        "users_affected_max": {"type": "integer", "description": "Maximum total users affected across all groups. Only from explicitly stated figures. Null if not stated."},
        "users_affected_exact": {"type": "integer", "description": "Exact total users affected across all groups. Only from explicitly stated figures. Null if not stated."},

        # ========== FINANCIAL IMPACT ==========
        "estimated_total_cost_usd": {"type": "number", "description": "Total estimated cost in USD — only from explicitly stated figures. Null if not stated."},
        "recovery_cost_usd": {"type": "number", "description": "Recovery and remediation cost in USD — only from explicitly stated figures. Null if not stated."},
        "legal_cost_usd": {"type": "number", "description": "Legal cost in USD — only from explicitly stated figures. Null if not stated."},
        "insurance_claim": {
            "type": "boolean",
            "description": "Set to true if the institution filed or was expected to file a cyber insurance claim. Null if not mentioned."
        },
        "insurance_payout_usd": {"type": "number", "description": "Insurance payout amount in USD. Null if not stated."},
        "business_impact_severity": {
            "type": "string",
            "enum": ["catastrophic", "critical", "major", "moderate", "minor", "negligible"],
            "description": "How severely operations were disrupted: catastrophic — complete operational shutdown, major data loss; critical — major systems down for weeks; major — significant multi-day disruption; moderate — notable but limited scope; minor — quickly resolved; negligible — near-zero impact."
        },
        "business_impact": {
            "type": "string",
            "enum": ["critical", "severe", "moderate", "limited", "minimal"],
            "description": "Canonical business impact label used by the dataset and dashboard. critical — existential or campus-wide shutdown; severe — major multi-day disruption; moderate — notable but contained impact; limited — short-lived or localized impact; minimal — very small business impact. Null if not stated."
        },
        
        # ========== REGULATORY IMPACT ==========
        "applicable_regulations": {
            "type": "array",
            "description": (
                "Legal/regulatory frameworks that govern this breach — infer from country and data categories if not explicitly stated. "
                "FERPA — US institution with student education records; "
                "HIPAA — US institution with patient health data; "
                "GDPR — EU/EEA/UK institution with any personal data of EU residents; "
                "UK_DPA — UK institution (post-Brexit, applies alongside GDPR); "
                "CCPA_CPRA — California institution or incident involving California residents; "
                "Canada_PIPEDA — Canadian institution; "
                "Australia_Privacy_Act — Australian institution; "
                "state_breach_notification — US state breach notification law applies (applies in most US states when PII is breached); "
                "PCI_DSS — payment card data was involved. "
                "Set to null if none clearly apply."
            ),
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
        "breach_notification_required": {
            "type": "boolean",
            "description": "Set to true if the breach involves PII in a jurisdiction requiring notification (FERPA, GDPR, HIPAA, US state laws), or if the article mentions notification obligations. Null if not determinable from the article."
        },
        "notification_sent": {
            "type": "boolean",
            "description": "Set to true if the institution sent breach notifications to affected individuals or regulators. Look for: 'notified affected individuals', 'sent letters to', 'offering credit monitoring', 'emailed affected'. Null if not mentioned."
        },
        "notification_sent_date": {"type": "string", "description": "ISO 8601 date the breach notification was sent to affected individuals. Null if not stated."},
        "notification_delay_days": {
            "type": "integer",
            "description": (
                "Days from breach discovery to victim notification. "
                "Only from explicitly stated figures in the article. "
                "Distinct from disclosure_delay_days which measures time to PUBLIC disclosure. "
                "Null if not stated."
            )
        },
        "dpa_notified": {
            "type": "boolean",
            "description": "Set to true if a Data Protection Authority or equivalent privacy regulator was explicitly notified (for example ICO, CNIL, or another national/state privacy regulator). Null if not mentioned."
        },
        "regulators_notified": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Regulatory bodies notified. E.g. ['Maine Attorney General', 'HHS', 'ICO', 'CNIL']. Null if not mentioned."
        },
        "investigation_opened": {
            "type": "boolean",
            "description": "Set to true if a formal investigation was opened by law enforcement or a regulatory body. Null if not mentioned."
        },
        "investigating_agencies": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Agencies conducting formal investigations. E.g. ['FBI', 'ICO', 'Maine AG']. Null if none mentioned."
        },
        "fine_imposed": {
            "type": "boolean",
            "description": "Set to true if a regulatory fine or penalty was imposed on the institution. Null if not mentioned."
        },
        "fine_amount_usd": {"type": "number", "description": "Fine amount in USD. Only from explicitly stated figure. Null if not stated."},
        "lawsuits_filed": {
            "type": "boolean",
            "description": "Set to true if a lawsuit was filed against the institution related to this incident. Null if not mentioned."
        },
        "lawsuit_count": {"type": "integer", "description": "Number of lawsuits filed. Null if not stated."},
        "class_action_filed": {
            "type": "boolean",
            "description": "Set to true if a class action lawsuit was filed against the institution. Null if not mentioned."
        },
        "settlement_amount_usd": {"type": "number", "description": "Settlement amount in USD. Only from explicitly stated figure. Null if not stated."},
        
        # ========== RESPONSE & RECOVERY ==========
        "incident_response_activated": {"type": "boolean", "description": "Set to true if a formal incident response was initiated — e.g. 'activated incident response plan', 'IR team deployed', 'emergency response team assembled'. Null if not mentioned."},
        "ir_firm_engaged": {"type": "string", "description": "Name of the external incident response firm engaged (e.g. 'Mandiant', 'CrowdStrike', 'Palo Alto Unit 42'). Null if not stated."},
        "forensics_firm_engaged": {"type": "string", "description": "Name of the digital forensics firm engaged. May be the same as ir_firm_engaged. Null if not stated."},
        "law_enforcement_involved": {
            "type": "boolean",
            "description": (
                "REQUIRED when any law enforcement body is mentioned in relation to this incident. "
                "Set to true for ANY of the following (worldwide): "
                "FBI, CISA, local/national police, Interpol, Europol, NCA, NCSC, Secret Service, "
                "BKA, Gendarmerie, ANSSI, NCSC-UK, AFP, RCMP, or any national cybercrime unit. "
                "Trigger phrases (any language): 'working with the FBI', 'police were notified', "
                "'authorities are investigating', 'referred to law enforcement', 'cooperating with police', "
                "'federal agents', 'national cyber agency', 'handed over to police', 'criminal investigation opened'. "
                "Set to false ONLY if the article explicitly states law enforcement was NOT contacted. "
                "Set to null if law enforcement is not mentioned at all in the article. "
                "Always pair with law_enforcement_agencies listing the specific bodies named."
            )
        },
        "law_enforcement_agencies": {
            "type": "array",
            "items": {"type": "string"},
            "description": (
                "List every law enforcement or government agency named in the article as involved. "
                "Use the agency's official short name as written in the article. "
                "Examples: ['FBI', 'CISA', 'Metropolitan Police', 'Europol', 'NCSC', 'AFP', 'BKA']. "
                "null if no specific agency is named."
            )
        },
        "recovery_method": {
            "type": "string",
            "description": "Primary method used to recover systems. backup_restore — recovered from backups; decryptor_used — decryption key obtained without paying; ransom_paid_decryption — paid ransom and received decryptor; clean_rebuild — rebuilt systems from scratch; partial_backup_partial_rebuild — mixed approach; ongoing — recovery still in progress at time of article; unknown — recovery mentioned but method not stated.",
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
        "backup_status": {
            "type": "string",
            "enum": ["available_and_used", "available_not_used", "unavailable", "compromised", "unknown"],
            "description": "Status of the victim's backups during recovery. available_and_used — backups existed and were used; available_not_used — backups existed but another recovery method was used; unavailable — no usable backups existed; compromised — backups existed but were encrypted, deleted, or otherwise unusable due to the attack; unknown — backup status not stated."
        },
        "backup_age_days": {
            "type": "number",
            "description": "Age in days of the backup used for restoration. Only from explicitly stated figures. Null if not stated."
        },
        "recovery_started_date": {"type": "string", "description": "ISO 8601 date recovery operations began"},
        "recovery_completed_date": {"type": "string", "description": "ISO 8601 date recovery was completed"},
        "recovery_duration_days": {"type": "number", "description": "Total days from incident to full recovery. Only from explicitly stated figures. Null if not stated."},
        "mttd_hours": {"type": "number", "description": "Mean Time To Detect"},
        "mttr_hours": {"type": "number", "description": "Mean Time To Recover"},
        "security_improvements": {
            "type": "array",
            "description": "Security measures implemented AFTER the incident as part of remediation or improvements. Only include measures explicitly stated in the article as being adopted post-incident.",
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
        "public_disclosure": {"type": "boolean", "description": "Set to true if the institution publicly disclosed the incident (issued a statement, notified media, posted on website, or filed a regulatory notice). Null if not mentioned."},
        "public_disclosure_date": {"type": "string", "description": "ISO 8601 date the incident was publicly disclosed"},
        "disclosure_delay_days": {"type": "number", "description": "Days between incident discovery and public disclosure. Only from explicitly stated dates in the article. Null if not calculable."},
        "disclosure_source": {
            "type": "string",
            "description": "How the incident became publicly known. institution_statement — the victim institution issued a press release or statement; media_report — first reported by journalists; attacker_leak_site — disclosed by the attacker publishing data; regulatory_filing — via HHS, AG, SEC, or other regulatory filing; security_researcher — discovered and reported by a security researcher.",
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
            "description": "How forthcoming the institution was in disclosing incident details. excellent — detailed statement with specifics, timeline, and remediation; good — clear statement with most key facts; adequate — basic acknowledgment with limited detail; poor — minimal or evasive disclosure; none — no public disclosure made.",
            "enum": ["excellent", "good", "adequate", "poor", "none"]
        },
        "official_statement_url": {"type": "string", "description": "URL of the institution's official public statement about the incident. Null if not linked in the article."},

        # ========== RESEARCH IMPACT ==========
        "research_projects_affected": {
            "type": "integer",
            "description": "Number of research projects directly impacted. Only from explicitly stated figures. Null if not stated."
        },
        "research_data_compromised": {
            "type": "boolean",
            "description": "Set to true if research datasets, unpublished results, intellectual property, or lab data were stolen, exposed, encrypted, or made unavailable. Null if not mentioned."
        },
        "publications_delayed": {
            "type": "boolean",
            "description": "Set to true if papers, dissertations, or other academic publications were delayed because of the incident. Null if not mentioned."
        },
        "grants_affected": {
            "type": "boolean",
            "description": "Set to true if funded research grants, grant reporting, or sponsored projects were delayed or disrupted. Null if not mentioned."
        },
        "research_area": {
            "type": "string",
            "description": "Research domain affected, such as biomedical, genomics, AI, defense, chemistry, or climate science. Null if not stated."
        },
        
        # ========== CROSS-INCIDENT ANALYSIS FIELDS ==========
        "attack_campaign_name": {
            "type": "string",
            "description": "If part of a larger campaign (e.g., MOVEit exploitation wave)"
        },
        "sector_targeting_pattern": {
            "type": "string",
            "description": "Whether this attack appeared to specifically target education or was opportunistic. targeted_education_only — attacker chose this victim because it was in education; opportunistic_multi_sector — attack affected multiple sectors, education was incidental; unknown — cannot be determined.",
            "enum": ["targeted_education_only", "opportunistic_multi_sector", "unknown"]
        },

        # ========== SOURCE METADATA ==========
        "source_url": {"type": "string", "description": "URL of the article being analysed. Copy verbatim from the article metadata provided."},
        "source_headline": {"type": "string", "description": "Headline or title of the article. Copy verbatim from the article."},
        "source_publisher": {"type": "string", "description": "Name of the news outlet or publisher (e.g. 'BleepingComputer', 'The Register', 'EdScoop'). Null if not identifiable."},
        "source_language": {"type": "string", "description": "ISO 639-1 language code of the article (e.g. 'en', 'de', 'nl', 'fr'). Defaults to 'en' for English."},
        "key_quotes": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Direct quotes from the article that are particularly informative — e.g. official statements, spokesperson comments, or notable admissions. Only exact quotes — do not paraphrase."
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
                "OTHER education-sector institutions affected, that are NOT the primary "
                "victim being extracted. Populate in TWO cases: "
                "(1) ROUNDUP/digest articles covering multiple separate victims; and "
                "(2) VENDOR / SUPPLY-CHAIN breaches where the article names the vendor "
                "(e.g. Instructure/Canvas, PowerSchool, MOVEit) as the primary subject "
                "AND names the individual universities, colleges, or school districts "
                "impacted through that vendor — list each NAMED institution here. "
                "Each entry becomes a separate victim incident record. "
                "ONLY list explicitly NAMED institutions — never vague counts like "
                "'hundreds of schools' or 'thousands of students'. "
                "For vendor breaches, set is_via_vendor=true and DO NOT copy the "
                "vendor's aggregate record count onto these per-institution entries."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "victim_name": {
                        "type": "string",
                        "description": "Full name of the explicitly named educational institution"
                    },
                    "incident_date": {
                        "type": "string",
                        "description": "ISO date YYYY-MM-DD or null if not stated"
                    },
                    "attack_type": {
                        "type": "string",
                        "description": "e.g. ransomware, data_breach, phishing, supply_chain_compromise"
                    },
                    "country": {
                        "type": "string",
                        "description": "Country of the institution"
                    },
                    "brief_description": {
                        "type": "string",
                        "description": "1-2 sentence summary of what happened"
                    },
                    "is_via_vendor": {
                        "type": "boolean",
                        "description": (
                            "True if this institution was affected THROUGH a compromised "
                            "vendor/supplier named in the article (supply-chain), rather "
                            "than as a directly, independently targeted victim."
                        )
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
                "university", "community_college", "technical_college", "vocational_school",
                "k12_school", "school_district", "research_institute", "research_center",
                "medical_school", "university_hospital", "teaching_hospital",
                "online_university", "library", "tribal_college", "military_academy",
                "edtech_platform", "tutoring_service", "consortium",
                "education_department", "education_ministry", "student_loan_servicer",
                "education_nonprofit", "education_vendor", "unknown",
            ],
            "description": (
                "university — any degree-granting higher education institution. "
                "k12_school — single K-12 school (elementary, middle, or high school). "
                "school_district — district-level organization ('school district', 'ISD', 'USD', 'county schools'). "
                "community_college — two-year community/junior college. technical_college — technical/vocational two-year college."
            ),
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
        "academic_period_affected": {
            "type": "string",
            "enum": [
                "start_of_semester", "mid_semester", "finals_week", "enrollment_period",
                "admissions_period", "graduation_period", "summer_break",
                "winter_break", "spring_break", "unknown"
            ],
            "description": "Academic calendar period. Infer from incident_date and article context. Null if not determinable.",
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
        "was_ransom_demanded": {
            "type": "boolean",
            "description": (
                "Set to true if the article states a ransom was demanded — e.g. 'demanded payment', "
                "'ransom note left', 'attackers demanded X', 'asked for payment in exchange for decryption key', "
                "'threatened to publish data unless paid'. For ransomware_encryption or ransomware_double_extortion "
                "attacks this is almost always true. "
                "Set to false only if the article explicitly confirms no ransom was demanded. Null if not mentioned."
            )
        },
        "ransom_amount_exact": {"type": "number"},
        "ransom_paid": {"type": "boolean"},
        "data_breached": {"type": "boolean"},
        "data_exfiltrated": {"type": "boolean"},
        "data_encrypted": {"type": "boolean"},
        "data_destroyed": {"type": "boolean"},
        "data_published": {"type": "boolean"},
        "data_sold": {"type": "boolean"},
        "dark_web_posting_confirmed": {"type": "boolean", "description": "True if article confirms data was posted on a dark web leak site. Null if not mentioned."},
        "prior_breach_same_institution": {"type": "boolean", "description": "True if article mentions institution was previously breached. Null if not mentioned."},
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
        "teaching_impacted": {"type": "boolean", "description": "True if teaching/classes were disrupted. Null if not mentioned."},
        "research_impacted": {"type": "boolean", "description": "True if research operations were disrupted. Null if not mentioned."},
        "students_affected": {"type": "integer"},
        "staff_affected": {"type": "integer"},
        "faculty_affected": {"type": "integer"},
        "alumni_affected": {"type": "integer"},
        "parents_affected": {"type": "integer", "description": "Number of parents/guardians affected (common in K-12 breaches). Null if not stated."},
        "applicants_affected": {"type": "integer"},
        "patients_affected": {"type": "integer", "description": "Number of patients affected (university hospitals). Null if not stated."},
        "pii_records_leaked": {"type": "integer", "description": "PII records confirmed leaked/published externally. Null if not stated."},
        "total_individuals_affected": {"type": "integer"},
        "users_affected_min": {"type": "integer"},
        "users_affected_max": {"type": "integer"},
        "users_affected_exact": {"type": "integer"},
        "confidence_score": {"type": "number", "minimum": 0, "maximum": 1.0},
        "extraction_notes": {"type": "string"},
        "other_edu_incidents": {
            "type": "array",
            "description": (
                "Other NAMED education institutions affected — either separate victims in a "
                "roundup article, OR the individual universities/colleges/school districts "
                "named as impacted through a compromised vendor (Instructure/Canvas, "
                "PowerSchool, MOVEit, etc.). Each becomes its own victim incident. Only list "
                "explicitly named institutions, never vague counts. For vendor breaches set "
                "is_via_vendor=true and do NOT copy the vendor's aggregate record count here."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "victim_name": {"type": "string"},
                    "incident_date": {"type": "string"},
                    "attack_type": {"type": "string"},
                    "country": {"type": "string"},
                    "brief_description": {"type": "string"},
                    "is_via_vendor": {"type": "boolean"},
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
        "ransom_paid_amount": {"type": "number", "description": "Amount actually paid in USD. Only from explicitly stated figure. Null if not stated."},
        "ransom_deadline_days": {"type": "number", "description": "Number of days given to pay the ransom before data would be published/deleted. Null if not stated."},
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
        "business_impact": {
            "type": "string",
            "enum": ["critical", "severe", "moderate", "limited", "minimal"],
        },
        "dwell_time_days": {"type": "number"},
        "mttd_hours": {"type": "number", "description": "Mean Time To Detect — hours from initial compromise to detection. Only when both dates explicitly stated. Null otherwise."},
        "mttr_hours": {"type": "number", "description": "Mean Time To Recover — hours from detection to full restoration. Only when explicitly stated. Null otherwise."},
        "incident_response_activated": {"type": "boolean"},
        "ir_firm_engaged": {"type": "string"},
        "forensics_firm_engaged": {"type": "string"},
        "dpa_notified": {"type": "boolean"},
        "law_enforcement_involved": {
            "type": "boolean",
            "description": (
                "REQUIRED when any law enforcement body is mentioned. "
                "true for ANY agency worldwide: FBI, CISA, police, Interpol, Europol, NCA, NCSC, BKA, AFP, RCMP, etc. "
                "Triggers: 'working with the FBI', 'police were notified', 'authorities are investigating', "
                "'referred to law enforcement', 'federal agents', 'criminal investigation opened'. "
                "false ONLY if article explicitly states law enforcement was NOT contacted. null if not mentioned at all."
            )
        },
        "law_enforcement_agencies": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Every agency named in the article, e.g. ['FBI', 'Europol', 'Metropolitan Police']. null if none named."
        },
        "recovery_method": {
            "type": "string",
            "enum": [
                "backup_restore", "decryptor_used", "ransom_paid_decryption",
                "clean_rebuild", "partial_backup_partial_rebuild", "ongoing", "unknown",
            ],
        },
        "backup_status": {
            "type": "string",
            "enum": ["available_and_used", "available_not_used", "unavailable", "compromised", "unknown"],
        },
        "backup_age_days": {"type": "number"},
        "recovery_duration_days": {"type": "number"},
        "recovery_started_date": {"type": "string", "description": "ISO 8601 date recovery operations began. Null if not stated."},
        "recovery_completed_date": {"type": "string", "description": "ISO 8601 date full recovery was completed. Null if not stated."},
        "outage_start_date": {"type": "string", "description": "ISO 8601 date the outage began. Null if not stated."},
        "outage_end_date": {"type": "string", "description": "ISO 8601 date the outage ended. Null if not stated."},
        "servers_affected_count": {"type": "integer", "description": "Number of servers affected. Only from explicitly stated figures. Null if not stated."},
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
        "official_statement_url": {"type": "string", "description": "URL of the institution's official public statement. Null if not linked in the article."},
        "threat_actor_claim_url": {"type": "string", "description": "URL of the attacker's post claiming responsibility (leak site, Telegram, forum). Null if not stated."},
        "cloud_provider": {
            "type": "string",
            "enum": ["aws", "azure", "google_cloud", "oracle_cloud", "ibm_cloud", "other_cloud", "none", "unknown"],
            "description": "Cloud provider involved if cloud infrastructure was affected. Use none if on-premises only.",
        },
        "notification_delay_days": {
            "type": "integer",
            "description": "Days from breach discovery to victim notification. Only from explicitly stated figures. Null if not stated.",
        },
        "research_projects_affected": {"type": "integer"},
        "research_data_compromised": {"type": "boolean"},
        "publications_delayed": {"type": "boolean"},
        "grants_affected": {"type": "boolean"},
        "research_area": {"type": "string"},
        "attack_campaign_name": {"type": "string"},
        "sector_targeting_pattern": {
            "type": "string",
            "enum": ["targeted_education_only", "opportunistic_multi_sector", "unknown"],
        },
        "key_quotes": {"type": "array", "items": {"type": "string"}},
    },
    "required": [],
}

# Fields intentionally excluded from the LLM prompt contract.
# These are either:
# 1. deterministically derived from stronger extracted facts,
# 2. article metadata we already have from fetch/storage, or
# 3. low-signal range/self-scored values that invite hallucination.
DIRECT_EXTRACTION_FIELDS = (
    "is_edu_cyber_incident",
    "education_relevance_reasoning",
    "institution_name",
    "institution_aliases",
    "institution_type",
    "country",
    "region",
    "city",
    "incident_status",
    "incident_date",
    "incident_date_precision",
    "academic_period_affected",
    "discovery_date",
    "timeline",
    "attack_category",
    "secondary_attack_categories",
    "attack_vector",
    "initial_access_description",
    "attack_chain",
    "vulnerabilities_exploited",
    "mitre_attack_techniques",
    "threat_actor_claimed",
    "threat_actor_name",
    "threat_actor_aliases",
    "threat_actor_category",
    "threat_actor_motivation",
    "threat_actor_origin_country",
    "threat_actor_claim_url",
    "ransomware_family",
    "malware_families",
    "attacker_tools",
    "was_ransom_demanded",
    "ransom_amount",
    "ransom_amount_exact",
    "ransom_currency",
    "ransom_cryptocurrency",
    "ransom_paid",
    "ransom_paid_amount",
    "ransom_negotiated",
    "ransom_deadline_days",
    "decryptor_received",
    "decryptor_worked",
    "iocs",
    "data_breached",
    "data_exfiltrated",
    "data_encrypted",
    "data_destroyed",
    "data_published",
    "data_sold",
    "dark_web_posting_confirmed",
    "prior_breach_same_institution",
    "data_categories",
    "records_affected_exact",
    "pii_records_leaked",
    "data_volume_gb",
    "cloud_provider",
    "systems_affected",
    "critical_systems_affected",
    "network_compromised",
    "domain_admin_compromised",
    "backup_compromised",
    "encryption_extent",
    "servers_affected_count",
    "outage_start_date",
    "outage_end_date",
    "outage_duration_hours",
    "downtime_days",
    "teaching_impacted",
    "research_impacted",
    "operational_impacts",
    "students_affected",
    "staff_affected",
    "faculty_affected",
    "alumni_affected",
    "parents_affected",
    "applicants_affected",
    "patients_affected",
    "total_individuals_affected",
    "users_affected_exact",
    "estimated_total_cost_usd",
    "recovery_cost_usd",
    "legal_cost_usd",
    "insurance_claim",
    "insurance_payout_usd",
    "business_impact",
    "breach_notification_required",
    "notification_sent",
    "notification_sent_date",
    "notification_delay_days",
    "dpa_notified",
    "regulators_notified",
    "investigation_opened",
    "investigating_agencies",
    "fine_imposed",
    "fine_amount_usd",
    "lawsuits_filed",
    "lawsuit_count",
    "class_action_filed",
    "settlement_amount_usd",
    "incident_response_activated",
    "ir_firm_engaged",
    "forensics_firm_engaged",
    "law_enforcement_involved",
    "law_enforcement_agencies",
    "recovery_method",
    "backup_status",
    "backup_age_days",
    "recovery_started_date",
    "recovery_completed_date",
    "recovery_duration_days",
    "mttd_hours",
    "mttr_hours",
    "security_improvements",
    "public_disclosure",
    "public_disclosure_date",
    "disclosure_delay_days",
    "disclosure_source",
    "transparency_level",
    "official_statement_url",
    "research_projects_affected",
    "research_data_compromised",
    "publications_delayed",
    "grants_affected",
    "research_area",
    "attack_campaign_name",
    "sector_targeting_pattern",
    "extraction_notes",
    "other_edu_incidents",
)

DERIVED_OR_METADATA_FIELDS = (
    "country_code",
    "publication_date",
    "institution_size",
    "incident_severity",
    "dwell_time_days",
    "users_affected_min",
    "users_affected_max",
    "ransom_amount_min",
    "ransom_amount_max",
    "business_impact_severity",
    "applicable_regulations",
    "source_url",
    "source_headline",
    "source_publisher",
    "source_language",
    "key_quotes",
    "confidence_score",
)

_PROMPT_EXCLUDED_FIELDS = set(DERIVED_OR_METADATA_FIELDS)


def _drop_prompt_fields(schema: dict, excluded_fields: set[str]) -> None:
    properties = schema.get("properties", {})
    for field in excluded_fields:
        properties.pop(field, None)


for _schema in (EXTRACTION_SCHEMA, EXTRACTION_SCHEMA_PART1, EXTRACTION_SCHEMA_PART2):
    _drop_prompt_fields(_schema, _PROMPT_EXCLUDED_FIELDS)

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

3. REGULATORY / DISCLOSURE FACTS — capture only article-grounded evidence:
   - Whether notifications were sent
   - When notifications were sent
   - Which regulators or agencies were notified
   - Whether an investigation was opened
   - Any fines, lawsuits, or class actions explicitly mentioned
   - The pipeline derives higher-level regulation labels later from these facts

4. FINANCIAL — only from explicitly stated figures in the article. Null if not stated.

5. RECOVERY — IR firms, law enforcement, recovery method, security improvements — only from the article.

6. DISCLOSURE — how and when was this disclosed? Capture concrete evidence, not subjective confidence.

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
    "VICTIM ANCHOR RULES:\n"
    "- The summary must stay focused on this victim: {institution}\n"
    "- If the article mentions multiple institutions, vendors, or a more prominent unrelated organization,\n"
    "  summarize ONLY the incident as it affected {institution}.\n"
    "- Do NOT switch the summary focus to another organization.\n\n"
    "CONSISTENCY RULES — your summary MUST match these structured fields:\n"
    "- If attack_category starts with 'ransomware_', use the word 'ransomware' in your summary.\n"
    "- If attack_category is 'supply_chain_software' or 'third_party_compromise', name the vendor product.\n"
    "- If attack_category is 'phishing_*' or 'business_email_compromise', mention phishing or email fraud.\n"
    "- Do NOT contradict the attack_category — the summary and the category must tell the same story.\n\n"
    "Institution: {institution}\n"
    "Attack type: {attack_category}\n\n"
    "Article:\n{text}"
)
