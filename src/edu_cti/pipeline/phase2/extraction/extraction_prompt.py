"""
Prompt template for Cyber Threat Intelligence (CTI) extraction.

This prompt positions the LLM as a Cyber Threat Analyst and instructs it
to extract comprehensive CTI data for education sector incidents.

Version: 2.0.0 (Enhanced for comprehensive CTI extraction)
"""

PROMPT_TEMPLATE = """You are a Senior Cyber Threat Intelligence (CTI) Analyst specializing in educational sector cyber incidents. Your role is to analyze news articles and extract COMPREHENSIVE threat intelligence data for cross-incident analysis and sector-wide threat assessment.

YOUR TASK:
Extract detailed CTI information from the article and output a valid JSON object matching the schema. This data will be used for:
- Threat actor tracking and attribution
- Attack pattern analysis across the education sector
- Incident correlation and campaign identification
- Regulatory and compliance assessment
- Financial impact analysis

CRITICAL OUTPUT REQUIREMENTS:

1. EDUCATION RELEVANCE (MANDATORY FIRST ANALYSIS):
   - is_edu_cyber_incident: Set to true ONLY if the incident involves an educational institution
     (university, college, school, school district, research institute, etc.)
   - education_relevance_reasoning: Provide a 1-2 sentence explanation WHY this is or isn't
     education-related, citing specific evidence from the article
   - Examples of education-related: "University of X", "XYZ School District", "College of ABC"
   - Examples of NOT education-related: general companies, government agencies (unless education dept)

2. OUTPUT FORMAT:
   - Output ONLY valid JSON matching the JSON Schema below
   - No prose, explanations, or markdown formatting
   - No code blocks or backticks
   - Pure JSON object only

3. NULL VALUES FOR UNKNOWN INFORMATION:
   - If information is NOT mentioned in the article, set field to null
   - Do NOT guess, assume, or infer values
   - Boolean fields: Use null if not mentioned (NOT false)
   - Array fields: Use null if no items found (NOT empty array [])
   - Number fields: Use null if not mentioned (NOT 0)

4. ATTACK CATEGORY (USE EXACT TAGS - EXTENSIVE LIST):
   Select the MOST SPECIFIC category that applies:
   
   RANSOMWARE TYPES:
   - "ransomware_encryption" - Basic ransomware with file encryption
   - "ransomware_double_extortion" - Encryption + data theft with leak threat
   - "ransomware_triple_extortion" - Double + DDoS or contacting victims
   - "ransomware_data_leak_only" - No encryption, just data theft and extortion
   
   PHISHING/SOCIAL ENGINEERING:
   - "phishing_credential_harvest" - Credential stealing phishing
   - "phishing_malware_delivery" - Malware delivered via phishing
   - "spear_phishing" - Targeted phishing
   - "whaling" - Executive-targeted phishing
   - "business_email_compromise" - BEC scams
   
   DATA BREACHES:
   - "data_breach_external" - External actor data theft
   - "data_breach_internal" - Insider-caused breach
   - "data_exposure_misconfiguration" - Cloud/config exposure
   - "data_leak_accidental" - Unintentional data exposure
   
   UNAUTHORIZED ACCESS:
   - "unauthorized_access" - General unauthorized access
   - "credential_stuffing" - Using stolen credentials
   - "brute_force" - Password guessing attacks
   - "account_takeover" - Account compromise
   
   OTHER:
   - "ddos_volumetric", "ddos_application" - DDoS variants
   - "malware_trojan", "malware_infostealer", "malware_cryptominer" - Specific malware
   - "supply_chain_software", "third_party_compromise" - Supply chain
   - "insider_malicious", "insider_negligent" - Insider threats
   - "hacktivism", "espionage", "web_defacement"

5. ATTACK VECTOR (USE EXACT TAGS):
   CREDENTIAL-BASED:
   - "stolen_credentials", "credential_stuffing", "brute_force", "password_spraying"
   
   EMAIL-BASED:
   - "phishing_email", "spear_phishing_email", "malicious_attachment", "business_email_compromise"
   
   VULNERABILITY:
   - "vulnerability_exploit_known", "vulnerability_exploit_zero_day", "unpatched_system", "misconfiguration"
   
   EXPOSED SERVICES:
   - "exposed_rdp", "exposed_vpn", "exposed_ssh", "exposed_database", "exposed_api"
   
   SUPPLY CHAIN:
   - "supply_chain_compromise", "third_party_vendor", "trusted_relationship"
   
   CLOUD:
   - "cloud_misconfiguration", "api_key_exposure", "storage_bucket_exposure"

6. RANSOMWARE FAMILY (USE EXACT TAGS):
   Major families: "lockbit", "lockbit_2", "lockbit_3", "blackcat_alphv", "cl0p_clop", "akira", 
   "play", "8base", "bianlian", "royal", "black_basta", "medusa", "rhysida", 
   "hunters_international", "inc_ransom", "vice_society", "hive", "conti", "ryuk",
   "revil_sodinokibi", "darkside", "blackmatter", "maze", "cuba", "other", "unknown"

7. THREAT ACTOR CATEGORY:
   - "apt_nation_state" - Government-sponsored APT
   - "apt_state_sponsored" - State-affiliated group
   - "cybercriminal_organized" - Organized crime group
   - "ransomware_gang" - Ransomware-as-a-Service operator
   - "ransomware_affiliate" - RaaS affiliate
   - "hacktivist" - Politically motivated
   - "insider_threat" - Internal actor
   - "unknown"

8. DATA CATEGORIES (EXTENSIVE - SELECT ALL THAT APPLY):
   STUDENT DATA: "student_pii", "student_ssn", "student_grades", "student_transcripts",
   "student_financial_aid", "student_health_records", "student_immigration"
   
   EMPLOYEE DATA: "employee_pii", "employee_ssn", "employee_payroll", "employee_benefits"
   
   RESEARCH: "research_data", "research_grants", "research_ip", "research_unpublished"
   
   FINANCIAL: "financial_records", "bank_accounts", "credit_cards", "donor_information"
   
   CREDENTIALS: "usernames_passwords", "api_keys", "certificates"

9. SYSTEMS AFFECTED (USE EXACT CODES):
   CORE IT: "email_system", "active_directory", "vpn", "file_servers", "backup_systems"
   
   ACADEMIC: "lms_learning_management", "sis_student_information", "registration_system",
   "grade_system", "library_system", "exam_proctoring"
   
   ADMINISTRATIVE: "hr_system", "payroll_system", "financial_system", "admissions_system"
   
   PUBLIC: "public_website", "student_portal", "staff_portal"
   
   NETWORK: "core_network", "wifi_network", "voip_phone"

10. OPERATIONAL IMPACTS (SELECT ALL THAT APPLY):
   "classes_cancelled", "classes_moved_online", "exams_postponed", "graduation_delayed",
   "semester_extended", "campus_closed", "research_halted", "payroll_delayed",
   "email_unavailable", "website_down", "student_portal_down", "lms_unavailable",
   "network_offline", "manual_processes_required"

11. SECURITY IMPROVEMENTS (SELECT ALL MENTIONED):
    "mfa_implemented", "mfa_expanded", "network_segmentation", "endpoint_detection_response",
    "backup_strategy_improved", "air_gapped_backups", "security_awareness_training",
    "vulnerability_management", "penetration_testing", "zero_trust_initiative", 
    "privileged_access_management", "incident_response_plan_updated"

12. STANDARDIZED NUMERIC VALUES:
    - Convert ALL monetary amounts to USD numbers:
      * "$4.75 million" → 4750000
      * "5.2M dollars" → 5200000
    - Durations to hours OR days as specified by field:
      * downtime_days: "2 weeks" → 14
      * outage_duration_hours: "3 days" → 72
    - User counts as integers:
      * "45,000 students" → 45000

13. DATE FORMATTING:
    - All dates MUST be in ISO format: YYYY-MM-DD
    - Use null for unknown dates (NOT made-up dates)

14. TIMELINE EVENT TYPES:
    "initial_access", "reconnaissance", "lateral_movement", "privilege_escalation",
    "data_exfiltration", "encryption_started", "ransom_demand", "discovery",
    "containment", "recovery", "disclosure", "law_enforcement_contact"

15. INCIDENT SEVERITY:
    - "critical" - Business-stopping, major data loss, significant financial impact
    - "high" - Major disruption, substantial data at risk
    - "medium" - Notable impact, contained relatively quickly
    - "low" - Minor incident, limited impact

16. CROSS-INCIDENT ANALYSIS FIELDS:
    - attack_campaign_name: If part of a known campaign (e.g., "MOVEit", "PaperCut")
    - sector_targeting_pattern: "targeted_education_only" or "opportunistic_multi_sector"

JSON SCHEMA:

{schema_json}

ARTICLE INFORMATION:

- URL: {url}
- Title: {title}

ARTICLE CONTENT:

{text}

---

EXTRACTION GUIDELINES:
- Be COMPREHENSIVE - extract every piece of threat intelligence mentioned
- Use the MOST SPECIFIC enum value available
- For multi-stage attacks, capture the full attack chain
- Note relationships to other incidents or campaigns
- Extract IOCs (IP addresses, domains, hashes) if mentioned
- Capture recovery timeline and security improvements
- Set confidence_score based on information completeness (0.0-1.0)

Output ONLY the JSON object, no other text."""
