"""
Prompt template for Cyber Threat Intelligence (CTI) extraction.

The JSON schema is passed separately as the Ollama format= parameter (grammar-constrained
generation), so it is NOT included in this prompt. This removes ~8K tokens per call while
keeping all semantic guidance the model needs to make the right choices within the schema.

Version: 3.0.0 (Token-optimised — schema moved to format= parameter)
"""

PROMPT_TEMPLATE = """You are a Senior Cyber Threat Intelligence (CTI) Analyst specialising in education sector cyber incidents. Analyse the article below and extract comprehensive threat intelligence, outputting a valid JSON object matching the schema.

YOUR TASK:
Extract detailed CTI information for cross-incident analysis, threat actor tracking, attack pattern analysis, regulatory assessment, and financial impact analysis.

CRITICAL OUTPUT REQUIREMENTS:

1. EDUCATION RELEVANCE (MANDATORY FIRST ANALYSIS):
   - is_edu_cyber_incident: Set to true if the incident involves an educational institution
     (university, college, school, school district, research institute, theological seminary, etc.)
   - CRITICAL: Pay special attention to:
     * Data breach notifications and formal regulatory filings (e.g., Maine Attorney General filings)
     * Organizations explicitly marked as "Type of Organization: Education" in breach notifications
     * Theological seminaries, religious educational institutions, Bible colleges
     * Any institution with "seminary", "academy", "institute", "college", "university", "school" in the name
     * Educational service providers, student information systems, learning management systems
   - education_relevance_reasoning: Provide a 1-2 sentence explanation WHY this is or isn't
     education-related, citing specific evidence from the article
   - Examples of education-related:
     * "University of X", "XYZ School District", "College of ABC"
     * "Asbury Theological Seminary" (theological seminary = educational institution)
     * "Type of Organization: Education" in data breach notifications
   - Examples of NOT education-related: general companies, government agencies (unless education dept)

2. NORMALISATION RULES (MANDATORY — apply to ALL text fields):
   - INSTITUTION NAMES: Always translate and romanise to standard English.
     * Japanese: "国立大学法人 新潟大学" → "Niigata University"
     * Chinese: "北京大学" → "Peking University"
     * Arabic, Korean, Cyrillic, or any other non-Latin script → translate to English
     * Use the internationally recognised English name where one exists
     * If no English name exists, romanise (transliterate) to Latin characters
     * institution_name must be ONLY the victim institution label, not a headline.
       Remove threat actor names, attack verbs, and wrappers like
       "Qilin Ransomware Targets ..." or "... suffers cyberattack"
   - ALL TEXT FIELDS (institution_name, city, region, threat_actor_name, etc.):
     Output in English only. No non-Latin characters anywhere in the JSON.
   - CURRENCY: Convert ALL monetary values to USD integers before storing.
     * British pounds, Euros, Bitcoin, etc. must be converted to USD equivalent
     * Use approximate exchange rate if exact rate not stated in article
     * "$4.75 million" → 4750000; "£2M" → ~2540000; "€5M" → ~5400000
   - COUNTRY / COUNTRY CODE: Use the standard English country name and ISO 3166-1 alpha-2 code.

3. NULL VALUES FOR UNKNOWN INFORMATION:
   - If information is NOT mentioned in the article, set field to null
   - Do NOT guess, assume, or infer values not explicitly stated
   - Boolean fields: Use null if not mentioned (NOT false)
   - Array fields: Use null if no items found (NOT empty array [])
   - Number fields: Use null if not mentioned (NOT 0)

4. ATTACK CATEGORY — choose the most specific tag that applies:
   RANSOMWARE:
   - "ransomware_encryption" — file encryption only, no confirmed exfiltration
   - "ransomware_double_extortion" — encryption + data theft with leak threat
   - "ransomware_triple_extortion" — double extortion + DDoS or contacting victims/partners
   - "ransomware_data_leak_only" — data stolen and threatened for release, no encryption

   PHISHING / SOCIAL ENGINEERING:
   - "phishing_credential_harvest" — phishing aimed at stealing login credentials
   - "phishing_malware_delivery" — phishing email used to deliver malware payload
   - "spear_phishing" — targeted phishing at specific individuals
   - "whaling" — executive-targeted phishing (CEO, CFO, etc.)
   - "business_email_compromise" — fraudulent email impersonating trusted party for wire transfer/data

   DATA BREACHES:
   - "data_breach_external" — external actor exfiltrated data
   - "data_breach_internal" — insider-caused data theft or exposure
   - "data_exposure_misconfiguration" — data exposed due to cloud/server misconfiguration
   - "data_leak_accidental" — unintentional data exposure (no malicious actor)

   NETWORK / MALWARE / OTHER:
   - "ddos_volumetric", "ddos_application", "ddos_protocol" — DDoS by type
   - "malware_trojan", "malware_worm", "malware_backdoor", "malware_rootkit",
     "malware_cryptominer", "malware_infostealer", "malware_rat", "malware_botnet"
   - "unauthorized_access" — confirmed intrusion with no further classification
   - "credential_stuffing" — automated login attempts with previously leaked credentials
   - "brute_force" — systematic password guessing
   - "supply_chain_software", "third_party_compromise" — compromise via vendor/software
   - "insider_malicious", "insider_negligent" — internal actor incidents
   - "hacktivism" — politically/ideologically motivated attack
   - "espionage" — nation-state intelligence collection
   - "web_defacement" — website content replaced by attacker

5. ATTACK VECTOR — primary method used for initial access:
   CREDENTIAL-BASED: "stolen_credentials", "credential_stuffing", "brute_force",
     "password_spraying", "credential_phishing", "session_hijacking"
   EMAIL-BASED: "phishing_email", "spear_phishing_email", "malicious_attachment",
     "malicious_link", "business_email_compromise"
   VULNERABILITY: "vulnerability_exploit_known" (CVE exists), "vulnerability_exploit_zero_day"
     (no patch at time), "unpatched_system", "misconfiguration", "default_credentials"
   EXPOSED SERVICES: "exposed_rdp", "exposed_vpn", "exposed_ssh", "exposed_database",
     "exposed_api", "exposed_service" (generic)
   SUPPLY CHAIN: "supply_chain_compromise", "third_party_vendor", "software_update_compromise",
     "trusted_relationship"
   WEB: "drive_by_download", "watering_hole", "sql_injection", "xss", "ssrf", "path_traversal"
   CLOUD: "cloud_misconfiguration", "api_key_exposure", "storage_bucket_exposure"
   PHYSICAL/SOCIAL: "social_engineering", "usb_drop", "tailgating"
   INSIDER: "insider_access", "former_employee"

6. RANSOMWARE FAMILY:
   Use the exact known family name in lowercase (e.g. "lockbit", "blackcat_alphv", "cl0p_clop",
   "akira", "play", "black_basta", "rhysida", "medusa", "conti", "ryuk", "revil_sodinokibi").
   Use "unknown" if the family is not identified. Use "other" for confirmed but unlisted families.

7. THREAT ACTOR CATEGORY:
   - "apt_nation_state" — confirmed government-sponsored APT group
   - "apt_state_sponsored" — state-affiliated but not confirmed government-direct
   - "cybercriminal_organized" — organised crime group (not ransomware-specific)
   - "ransomware_gang" — ransomware-as-a-Service operator (the group running the platform)
   - "ransomware_affiliate" — RaaS affiliate (uses the platform, not the operator)
   - "hacktivist" — politically or ideologically motivated attacker
   - "insider_threat" — current or former employee, contractor
   - "unknown"

8. DATA CATEGORIES, SYSTEMS AFFECTED, OPERATIONAL IMPACTS, SECURITY IMPROVEMENTS:
   Select all tags that apply from the schema enum. Tag names are self-describing
   (e.g. "student_pii", "employee_ssn", "email_system", "classes_cancelled",
   "mfa_implemented"). Extract only values explicitly mentioned in the article.

9. STANDARDIZED NUMERIC VALUES:
   - Convert ALL monetary amounts to USD integers:
     * "$4.75 million" → 4750000
     * "5.2M dollars" → 5200000
   - Durations as specified by field:
     * downtime_days: "2 weeks" → 14
     * outage_duration_hours: "3 days" → 72
   - User/record counts as integers: "45,000 students" → 45000

10. DATE FORMATTING AND RELATIVE DATE RESOLUTION:
   - All dates MUST be in ISO format: YYYY-MM-DD
   - incident_date = date the attack/breach OCCURRED (not when it was reported)
   - publication_date = date this article was published (use the URL or article metadata)
   - RELATIVE DATES: Resolve all relative time expressions using publication_date as the anchor.
     Examples (assuming article published 2024-03-15):
     * "last week" → 2024-03-08 (subtract 7 days), set date_precision to "approximate"
     * "last month" → 2024-02-15, set date_precision to "month_only"
     * "yesterday" → 2024-03-14, set date_precision to "day"
     * "two weeks ago" → 2024-03-01, set date_precision to "approximate"
     * "earlier this year" → 2024-01-01 (use year only), set date_precision to "year_only"
     * "last year" → 2023-01-01, set date_precision to "year_only"
     * "recently" → use publication_date minus 14 days as estimate, set date_precision to "approximate"
   - If you cannot determine publication_date from the article, use null for relative dates
     rather than guessing — do NOT fabricate absolute dates from unresolvable relative expressions
   - Always set incident_date_precision to reflect how confident the date is:
     "exact" (specific date stated), "approximate" (relative expression resolved),
     "month_only" (only month known), "year_only" (only year known), "unknown"

11. INCIDENT SEVERITY:
    - "critical" — business-stopping, major confirmed data loss, significant financial impact
    - "high" — major operational disruption, substantial data at risk
    - "medium" — notable impact, contained relatively quickly, limited data exposure
    - "low" — minor incident, limited impact, quickly resolved

12. CROSS-INCIDENT ANALYSIS:
    - attack_campaign_name: Only if the article explicitly links this to a named campaign
      (e.g., "MOVEit", "PaperCut", "Cl0p campaign") — do NOT infer campaign names
    - sector_targeting_pattern: "targeted_education_only" or "opportunistic_multi_sector"

13. ROUNDUP / MULTI-INCIDENT ARTICLES:
    If this article covers MULTIPLE separate education sector incidents (digest, weekly roundup,
    breach summary, "Week in Breach", "ransomware attacks in 2023", etc.):
    - Extract the PRIMARY/most-detailed incident in all fields above as normal
    - List every OTHER education institution mentioned as a victim in `other_edu_incidents`
    - Each entry needs at minimum: victim_name, plus any date/attack_type/country mentioned
    - Do NOT duplicate the primary victim in `other_edu_incidents`
    - If the article covers a SINGLE incident, leave `other_edu_incidents` as null

ARTICLE INFORMATION:

- URL: {url}
- Title: {title}{target_institution_line}

ARTICLE CONTENT:

{text}

---

EXTRACTION GUIDELINES:
- Be COMPREHENSIVE — extract every piece of threat intelligence mentioned in the article
- For multi-stage attacks, capture the full attack chain in the timeline
- Extract IOCs (IP addresses, domains, file hashes) if mentioned
- Capture recovery timeline and any security improvements implemented post-incident
- Use the MOST SPECIFIC enum value available for classification fields
- Set confidence_score based on information completeness (0.0 = almost no detail, 1.0 = very detailed)

Output ONLY the JSON object, no other text."""
