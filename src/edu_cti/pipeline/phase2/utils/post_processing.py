"""
Post-processing rules applied after LLM enrichment to improve field quality.

These are deterministic rules — no LLM re-calls required.  They only FILL null
fields; they never overwrite a value that the LLM already provided.  Safe to run
retroactively against existing enrichments.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional

# ── Ransomware family keyword scan ───────────────────────────────────────────────
# Ordered longest-first so more-specific terms win over substrings.
_RANSOMWARE_KEYWORDS: list[tuple[str, str]] = [
    ("black basta", "black_basta"),
    ("blackbasta", "black_basta"),
    ("blackcat", "blackcat_alphv"),
    ("alphv", "blackcat_alphv"),
    ("lockbit 3", "lockbit"),
    ("lockbit 2", "lockbit"),
    ("lockbit", "lockbit"),
    ("cl0p", "cl0p_clop"),
    ("clop", "cl0p_clop"),
    ("revil", "revil_sodinokibi"),
    ("sodinokibi", "revil_sodinokibi"),
    ("vice society", "vice_society"),
    ("royal ransomware", "royal"),
    ("play ransomware", "play"),
    ("hive ransomware", "hive"),
    ("hunters international", "hunters_international"),
    ("inc ransom", "inc_ransom"),
    ("ransomhub ransomware", "ransomhub"),
    ("ransomhub", "ransomhub"),
    ("blacksuit ransomware", "blacksuit"),
    ("blacksuit", "blacksuit"),
    ("phobos ransomware", "phobos"),
    ("phobos", "phobos"),
    ("avoslocker", "avoslocker"),
    ("fog ransomware group", "fog"),
    ("doppelpaymer", "doppelpaymer"),
    ("qilin ransomware", "qilin"),
    ("qilin", "qilin"),
    ("doppelpaymer", "doppelpaymer"),
    ("blackmatter", "blackmatter"),
    ("blacksuit", "blacksuit"),
    ("medusa ransomware", "medusa"),
    ("conti ransomware", "conti"),
    ("conti group", "conti"),
    ("conti", "conti"),
    ("ryuk", "ryuk"),
    ("akira", "akira"),
    ("rhysida", "rhysida"),
    ("avoslocker", "avoslocker"),
    ("darkside", "darkside"),
    ("netwalker", "netwalker"),
    ("maze ransomware", "maze"),
    ("phobos", "phobos"),
    ("pysa", "pysa"),
    ("lorenz", "lorenz"),
    ("8base", "8base"),
    ("fog ransomware", "fog"),
    ("interlock ransomware", "interlock"),
    ("interlock group", "interlock"),
    ("interlock", "interlock"),
    ("meow ransomware", "meow"),
    ("grief ransomware", "grief"),
    ("noescape", "noescape"),
    ("no escape ransomware", "noescape"),
    ("ta505", "ta505"),
    ("quantum ransomware", "quantum"),
    ("cuba ransomware", "cuba"),
    ("snatch ransomware", "snatch"),
    ("prometheus ransomware", "prometheus"),
]

# ── US state lookup by city (unambiguous only) ───────────────────────────────────
_US_CITY_TO_STATE: dict[str, str] = {
    "boston": "Massachusetts", "cambridge": "Massachusetts", "worcester": "Massachusetts",
    "lowell": "Massachusetts", "springfield": "Massachusetts",
    "new haven": "Connecticut", "hartford": "Connecticut", "bridgeport": "Connecticut",
    "storrs": "Connecticut",
    "new york city": "New York", "new york": "New York", "buffalo": "New York",
    "ithaca": "New York", "albany": "New York", "rochester": "New York",
    "binghamton": "New York", "stony brook": "New York", "purchase": "New York",
    "philadelphia": "Pennsylvania", "pittsburgh": "Pennsylvania", "state college": "Pennsylvania",
    "harrisburg": "Pennsylvania", "scranton": "Pennsylvania", "allentown": "Pennsylvania",
    "university park": "Pennsylvania",
    "baltimore": "Maryland", "rockville": "Maryland", "annapolis": "Maryland",
    "college park": "Maryland", "towson": "Maryland",
    "richmond": "Virginia", "norfolk": "Virginia", "virginia beach": "Virginia",
    "blacksburg": "Virginia", "charlottesville": "Virginia", "harrisonburg": "Virginia",
    "fairfax": "Virginia", "roanoke": "Virginia",
    "raleigh": "North Carolina", "chapel hill": "North Carolina", "durham": "North Carolina",
    "charlotte": "North Carolina", "greensboro": "North Carolina",
    "winston-salem": "North Carolina", "fayetteville": "North Carolina",
    "columbia": "South Carolina", "charleston": "South Carolina", "clemson": "South Carolina",
    "atlanta": "Georgia", "athens": "Georgia", "savannah": "Georgia",
    "tallahassee": "Florida", "miami": "Florida", "orlando": "Florida",
    "tampa": "Florida", "gainesville": "Florida", "jacksonville": "Florida",
    "fort lauderdale": "Florida", "boca raton": "Florida",
    "baton rouge": "Louisiana", "new orleans": "Louisiana", "shreveport": "Louisiana",
    "nashville": "Tennessee", "memphis": "Tennessee", "knoxville": "Tennessee",
    "chattanooga": "Tennessee",
    "birmingham": "Alabama", "tuscaloosa": "Alabama", "auburn": "Alabama",
    "huntsville": "Alabama", "mobile": "Alabama",
    "jackson": "Mississippi", "hattiesburg": "Mississippi",
    "little rock": "Arkansas", "fayetteville": "Arkansas",
    "oklahoma city": "Oklahoma", "tulsa": "Oklahoma", "norman": "Oklahoma",
    "stillwater": "Oklahoma",
    "dallas": "Texas", "houston": "Texas", "san antonio": "Texas", "austin": "Texas",
    "fort worth": "Texas", "lubbock": "Texas", "waco": "Texas",
    "corpus christi": "Texas", "plano": "Texas", "el paso": "Texas",
    "garland": "Texas", "amarillo": "Texas", "laredo": "Texas",
    "midland": "Texas", "odessa": "Texas", "college station": "Texas",
    "denton": "Texas", "killeen": "Texas", "wichita falls": "Texas",
    "albuquerque": "New Mexico", "santa fe": "New Mexico", "las cruces": "New Mexico",
    "phoenix": "Arizona", "tucson": "Arizona", "tempe": "Arizona",
    "mesa": "Arizona", "scottsdale": "Arizona",
    "denver": "Colorado", "boulder": "Colorado", "colorado springs": "Colorado",
    "fort collins": "Colorado", "aurora": "Colorado", "greeley": "Colorado",
    "pueblo": "Colorado",
    "salt lake city": "Utah", "provo": "Utah", "ogden": "Utah",
    "las vegas": "Nevada", "reno": "Nevada",
    "portland": "Oregon", "eugene": "Oregon", "corvallis": "Oregon",
    "salem": "Oregon", "bend": "Oregon",
    "pullman": "Washington", "seattle": "Washington", "spokane": "Washington",
    "tacoma": "Washington", "bellingham": "Washington", "olympia": "Washington",
    "boise": "Idaho", "pocatello": "Idaho", "moscow": "Idaho",
    "missoula": "Montana", "billings": "Montana", "bozeman": "Montana",
    "cheyenne": "Wyoming", "laramie": "Wyoming",
    "fargo": "North Dakota", "grand forks": "North Dakota", "bismarck": "North Dakota",
    "sioux falls": "South Dakota", "rapid city": "South Dakota", "vermillion": "South Dakota",
    "omaha": "Nebraska", "lincoln": "Nebraska",
    "des moines": "Iowa", "ames": "Iowa", "iowa city": "Iowa", "cedar rapids": "Iowa",
    "wichita": "Kansas", "manhattan": "Kansas", "lawrence": "Kansas",
    "minneapolis": "Minnesota", "st. paul": "Minnesota", "saint paul": "Minnesota",
    "duluth": "Minnesota", "mankato": "Minnesota", "moorhead": "Minnesota",
    "milwaukee": "Wisconsin", "madison": "Wisconsin", "green bay": "Wisconsin",
    "eau claire": "Wisconsin", "oshkosh": "Wisconsin", "la crosse": "Wisconsin",
    "chicago": "Illinois", "champaign": "Illinois", "urbana": "Illinois",
    "peoria": "Illinois", "rockford": "Illinois", "evanston": "Illinois",
    "indianapolis": "Indiana", "bloomington": "Indiana", "south bend": "Indiana",
    "west lafayette": "Indiana", "terre haute": "Indiana", "muncie": "Indiana",
    "columbus": "Ohio", "cleveland": "Ohio", "cincinnati": "Ohio",
    "toledo": "Ohio", "akron": "Ohio", "dayton": "Ohio",
    "ann arbor": "Michigan", "detroit": "Michigan", "east lansing": "Michigan",
    "grand rapids": "Michigan", "kalamazoo": "Michigan", "flint": "Michigan",
    "mount pleasant": "Michigan",
    "lexington": "Kentucky", "louisville": "Kentucky", "bowling green": "Kentucky",
    "los angeles": "California", "san francisco": "California", "san jose": "California",
    "san diego": "California", "sacramento": "California", "berkeley": "California",
    "long beach": "California", "riverside": "California", "fresno": "California",
    "bakersfield": "California", "anaheim": "California", "santa barbara": "California",
    "irvine": "California", "santa cruz": "California", "davis": "California",
    "pasadena": "California", "san luis obispo": "California", "santa ana": "California",
    "stockton": "California", "modesto": "California", "chula vista": "California",
    "moreno valley": "California", "oxnard": "California", "fontana": "California",
    "fullerton": "California", "pomona": "California", "hayward": "California",
    "anchorage": "Alaska", "fairbanks": "Alaska",
    "honolulu": "Hawaii", "hilo": "Hawaii",
    "newark": "New Jersey", "jersey city": "New Jersey", "princeton": "New Jersey",
    "trenton": "New Jersey", "camden": "New Jersey", "new brunswick": "New Jersey",
    "montclair": "New Jersey",
    "providence": "Rhode Island", "kingston": "Rhode Island",
    "burlington": "Vermont", "montpelier": "Vermont",
    "concord": "New Hampshire", "manchester": "New Hampshire", "durham": "New Hampshire",
    "portland": "Maine", "bangor": "Maine", "orono": "Maine",
    "dover": "Delaware", "newark": "Delaware",
    "columbia": "Missouri", "st. louis": "Missouri", "saint louis": "Missouri",
    "kansas city": "Missouri", "springfield": "Missouri", "rolla": "Missouri",
    "morgantown": "West Virginia", "huntington": "West Virginia",
    "charleston": "West Virginia",
}

# ── Status confirmation keywords ─────────────────────────────────────────────────
_CONFIRMATION_RE = re.compile(
    r"\b(?:"
    r"apologize[sd]?|apologise[sd]?|"
    r"confirmed? (?:the |a )?(?:breach|incident|attack|hack)|"
    r"officially (?:confirmed|acknowledged|disclosed)|"
    r"notif(?:y|ied) affected|is offering credit monitoring|"
    r"offered credit monitoring|credit monitoring (?:services?|offer)|"
    r"breach notification|data breach notification|"
    r"disclosed? the (?:breach|incident)|publicly disclos(?:ed?|ing)|"
    r"announced? (?:a |the )?(?:breach|incident|data breach)|"
    r"admitted (?:the |a )?(?:breach|attack|hack)|"
    r"acknowledged (?:the |a )?(?:breach|attack|incident)|"
    r"paid (?:the )?ransom|ransom (?:was )?paid|"
    r"has (?:sent|issued) notifications?|notifying (?:affected|impacted)"
    r")\b",
    re.IGNORECASE,
)

# ── K-12 institution name patterns ───────────────────────────────────────────────
_K12_DISTRICT_RE = re.compile(
    r"\b(?:"
    r"independent school district|unified school district|school district|"
    r"public schools|city schools|county schools|parish schools|"
    r"board of education|board of ed|city school district"
    r")\b",
    re.IGNORECASE,
)
_K12_DISTRICT_ABBR_RE = re.compile(r"\b(?:ISD|USD|CUSD|CISD|HISD|LAUSD|VUSD|RUSD|TUSD)\b")
_K12_SCHOOL_RE = re.compile(
    r"\b(?:"
    r"elementary school|middle school|junior high school|high school|"
    r"k-12|k12 (?!solutions)|charter school|"
    r"preparatory school|prep school|primary school|secondary school"
    r")\b",
    re.IGNORECASE,
)


# ── Headline detection ───────────────────────────────────────────────────────────
_HEADLINE_SOURCE_RE = re.compile(
    # " - Source Name" suffix (1-3 words after the dash)
    r"\s+[-–—]\s+\w[\w\s]{0,30}$"
)


def is_headline_format(name: str, title: Optional[str] = None) -> bool:
    """
    Return True if *name* looks like a news headline rather than an institution name.

    Signals:
    - Very long (>70 chars): institution names rarely exceed that.
    - Ends with " - PublicationName" (common news title suffix).
    - Matches the incident title exactly (LLM copied the title verbatim).
    """
    if not name:
        return False
    if len(name) > 70:
        return True
    if _HEADLINE_SOURCE_RE.search(name):
        return True
    if title and name.lower().strip() == title.lower().strip():
        return True
    return False


# ── Public API ───────────────────────────────────────────────────────────────────

def extract_ransomware_family(summary: Optional[str], title: Optional[str]) -> Optional[str]:
    """Scan summary and title for known ransomware family names."""
    text = " ".join(filter(None, [summary, title])).lower()
    if not text:
        return None
    for keyword, canonical in _RANSOMWARE_KEYWORDS:
        if keyword in text:
            return canonical
    return None


def infer_institution_type(name: Optional[str], existing_type: Optional[str]) -> Optional[str]:
    """
    Infer institution_type from institution name patterns when the existing
    value is null or 'unknown'.  Never demotes a known type.
    """
    if not name or existing_type not in (None, "unknown"):
        return existing_type
    if _K12_DISTRICT_RE.search(name) or _K12_DISTRICT_ABBR_RE.search(name):
        return "school_district"
    if _K12_SCHOOL_RE.search(name):
        return "k12_public_school"
    return existing_type


def infer_us_region(city: Optional[str], country_code: Optional[str]) -> Optional[str]:
    """Return US state for the given city when country_code is 'US'."""
    if not city or country_code != "US":
        return None
    return _US_CITY_TO_STATE.get(city.lower().strip())


def infer_regulatory_impact(flat_data: Dict[str, Any]) -> None:
    """
    Apply deterministic regulatory rules.  Only fills null fields.

    Rules:
    - FERPA  → US edu institution + data_breached + student/FERPA in data_categories
    - GDPR   → EU/EEA/UK country + data_breached
    - HIPAA  → US + data_breached + health/medical/PHI in data_categories or summary
    - breach_notification_required → US + data_breached + PII categories
    - notifications_sent → credit-monitoring / apology language in summary
    """
    if not flat_data.get("is_education_related"):
        return

    institution_type = (flat_data.get("institution_type") or "").lower()
    country_code = (flat_data.get("country_code") or "").upper()
    data_breached = flat_data.get("data_breached")
    summary = (flat_data.get("enriched_summary") or "").lower()

    # data_categories may be stored as a JSON string or a list
    _raw_cats = flat_data.get("data_categories") or ""
    if isinstance(_raw_cats, list):
        data_cats_text = " ".join(str(c) for c in _raw_cats).lower()
    else:
        try:
            parsed = json.loads(_raw_cats)
            data_cats_text = " ".join(str(c) for c in parsed).lower() if isinstance(parsed, list) else str(parsed).lower()
        except (json.JSONDecodeError, TypeError):
            data_cats_text = str(_raw_cats).lower()

    _is_edu_inst = any(
        t in institution_type for t in (
            "university", "college", "school", "k12", "vocational",
            "library", "education", "research", "military_academy",
        )
    )

    _EU_CODES = {
        "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR",
        "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL",
        "PT", "RO", "SE", "SI", "SK",
        "GB",        # UK GDPR
        "IS", "NO", "LI",  # EEA
    }

    # FERPA
    if flat_data.get("ferpa_breach") is None and data_breached and country_code == "US" and _is_edu_inst:
        if any(kw in data_cats_text for kw in ("student", "grade", "transcript", "ferpa", "educational record")):
            flat_data["ferpa_breach"] = True

    # GDPR
    if flat_data.get("gdpr_breach") is None and data_breached and country_code in _EU_CODES:
        flat_data["gdpr_breach"] = True

    # HIPAA
    if flat_data.get("hipaa_breach") is None and data_breached and country_code == "US":
        if any(kw in data_cats_text or kw in summary for kw in (
            "health", "medical", "hipaa", "phi", "patient", "ehr", "emr",
        )):
            flat_data["hipaa_breach"] = True

    # breach_notification_required
    if flat_data.get("breach_notification_required") is None and data_breached and country_code == "US":
        _pii_cats = ("pii", "ssn", "social_security", "employee", "student", "health",
                     "financial", "personal_information", "medical", "address", "date_of_birth")
        if any(kw in data_cats_text for kw in _pii_cats):
            flat_data["breach_notification_required"] = True

    # notifications_sent — inferred from credit-monitoring / apology language
    if flat_data.get("notifications_sent") is None:
        _notify_kws = (
            "credit monitoring", "identity protection", "identity theft protection",
            "notified affected", "sent notification", "apologizes", "apologised",
            "breach notification letter", "data breach notification",
            "has notified", "is notifying", "began notifying",
        )
        if any(kw in summary for kw in _notify_kws):
            flat_data["notifications_sent"] = True


def infer_confirmed_status(enriched_summary: Optional[str], title: Optional[str]) -> bool:
    """Return True if the summary or title contains language that confirms the incident."""
    text = " ".join(filter(None, [enriched_summary, title]))
    return bool(_CONFIRMATION_RE.search(text)) if text else False


_DATA_CAT_KEYWORDS: list[tuple[str, str]] = [
    # Student data
    ("student record", "student_pii"),
    ("student data", "student_pii"),
    ("student information", "student_pii"),
    ("student personal", "student_pii"),
    ("student name", "student_pii"),
    ("student ssn", "student_ssn"),
    ("student social security", "student_ssn"),
    ("student address", "student_pii"),
    ("student grade", "student_grades"),
    ("student transcript", "student_transcripts"),
    ("student financial aid", "student_financial_aid"),
    ("student health", "student_health_records"),
    # Employee data
    ("employee record", "employee_pii"),
    ("employee data", "employee_pii"),
    ("employee information", "employee_pii"),
    ("staff record", "employee_pii"),
    ("staff data", "employee_pii"),
    ("faculty record", "employee_pii"),
    ("faculty data", "employee_pii"),
    ("employee ssn", "employee_ssn"),
    ("staff ssn", "employee_ssn"),
    ("employee social security", "employee_ssn"),
    ("employee tax", "employee_tax_info"),
    ("w-2", "employee_tax_info"),
    ("payroll", "employee_tax_info"),
    ("employee salary", "employee_salary"),
    # Financial
    ("financial record", "financial_records"),
    ("financial data", "financial_records"),
    ("financial information", "financial_records"),
    ("bank account", "financial_records"),
    ("credit card", "payment_card_data"),
    ("payment card", "payment_card_data"),
    # Health
    ("health record", "health_records"),
    ("medical record", "health_records"),
    ("health information", "health_records"),
    ("patient data", "health_records"),
    ("protected health information", "phi"),
    (" phi ", "phi"),
    # SSNs (generic)
    ("social security number", "ssn_general"),
    ("ssn", "ssn_general"),
    # Credentials
    ("password", "credentials"),
    ("credential", "credentials"),
    ("login information", "credentials"),
]


# ── MITRE ATT&CK technique lookup (technique_id → (name, tactic)) ────────────────
# Covers the techniques most commonly seen in education-sector incident reporting.
_MITRE_TECHNIQUE_INFO: dict[str, tuple[str, str]] = {
    # Reconnaissance
    "T1595": ("Active Scanning", "Reconnaissance"),
    "T1595.001": ("Scanning IP Blocks", "Reconnaissance"),
    "T1595.002": ("Vulnerability Scanning", "Reconnaissance"),
    "T1592": ("Gather Victim Host Information", "Reconnaissance"),
    "T1589": ("Gather Victim Identity Information", "Reconnaissance"),
    "T1590": ("Gather Victim Network Information", "Reconnaissance"),
    "T1591": ("Gather Victim Org Information", "Reconnaissance"),
    "T1598": ("Phishing for Information", "Reconnaissance"),
    # Resource Development
    "T1583": ("Acquire Infrastructure", "Resource Development"),
    "T1584": ("Compromise Infrastructure", "Resource Development"),
    "T1585": ("Establish Accounts", "Resource Development"),
    "T1587": ("Develop Capabilities", "Resource Development"),
    "T1588": ("Obtain Capabilities", "Resource Development"),
    # Initial Access
    "T1133": ("External Remote Services", "Initial Access"),
    "T1190": ("Exploit Public-Facing Application", "Initial Access"),
    "T1195": ("Supply Chain Compromise", "Initial Access"),
    "T1199": ("Trusted Relationship", "Initial Access"),
    "T1200": ("Hardware Additions", "Initial Access"),
    "T1078": ("Valid Accounts", "Initial Access"),
    "T1566": ("Phishing", "Initial Access"),
    "T1566.001": ("Spearphishing Attachment", "Initial Access"),
    "T1566.002": ("Spearphishing Link", "Initial Access"),
    "T1091": ("Replication Through Removable Media", "Initial Access"),
    # Execution
    "T1059": ("Command and Scripting Interpreter", "Execution"),
    "T1059.001": ("PowerShell", "Execution"),
    "T1059.003": ("Windows Command Shell", "Execution"),
    "T1059.006": ("Python", "Execution"),
    "T1203": ("Exploitation for Client Execution", "Execution"),
    "T1204": ("User Execution", "Execution"),
    "T1204.001": ("Malicious Link", "Execution"),
    "T1204.002": ("Malicious File", "Execution"),
    "T1047": ("Windows Management Instrumentation", "Execution"),
    "T1053": ("Scheduled Task/Job", "Execution"),
    "T1053.005": ("Scheduled Task", "Execution"),
    "T1569": ("System Services", "Execution"),
    "T1569.002": ("Service Execution", "Execution"),
    # Persistence
    "T1098": ("Account Manipulation", "Persistence"),
    "T1136": ("Create Account", "Persistence"),
    "T1136.001": ("Local Account", "Persistence"),
    "T1543": ("Create or Modify System Process", "Persistence"),
    "T1547": ("Boot or Logon Autostart Execution", "Persistence"),
    "T1574": ("Hijack Execution Flow", "Persistence"),
    "T1078.004": ("Cloud Accounts", "Persistence"),
    "T1505": ("Server Software Component", "Persistence"),
    "T1505.003": ("Web Shell", "Persistence"),
    # Privilege Escalation
    "T1055": ("Process Injection", "Privilege Escalation"),
    "T1055.001": ("Dynamic-link Library Injection", "Privilege Escalation"),
    "T1068": ("Exploitation for Privilege Escalation", "Privilege Escalation"),
    "T1548": ("Abuse Elevation Control Mechanism", "Privilege Escalation"),
    "T1134": ("Access Token Manipulation", "Privilege Escalation"),
    # Defense Evasion
    "T1027": ("Obfuscated Files or Information", "Defense Evasion"),
    "T1036": ("Masquerading", "Defense Evasion"),
    "T1070": ("Indicator Removal", "Defense Evasion"),
    "T1070.001": ("Clear Windows Event Logs", "Defense Evasion"),
    "T1070.004": ("File Deletion", "Defense Evasion"),
    "T1112": ("Modify Registry", "Defense Evasion"),
    "T1140": ("Deobfuscate/Decode Files or Information", "Defense Evasion"),
    "T1202": ("Indirect Command Execution", "Defense Evasion"),
    "T1497": ("Virtualization/Sandbox Evasion", "Defense Evasion"),
    "T1562": ("Impair Defenses", "Defense Evasion"),
    "T1562.001": ("Disable or Modify Tools", "Defense Evasion"),
    "T1564": ("Hide Artifacts", "Defense Evasion"),
    # Credential Access
    "T1003": ("OS Credential Dumping", "Credential Access"),
    "T1003.001": ("LSASS Memory", "Credential Access"),
    "T1056": ("Input Capture", "Credential Access"),
    "T1110": ("Brute Force", "Credential Access"),
    "T1110.001": ("Password Guessing", "Credential Access"),
    "T1110.003": ("Password Spraying", "Credential Access"),
    "T1110.004": ("Credential Stuffing", "Credential Access"),
    "T1187": ("Forced Authentication", "Credential Access"),
    "T1212": ("Exploitation for Credential Access", "Credential Access"),
    "T1539": ("Steal Web Session Cookie", "Credential Access"),
    "T1552": ("Unsecured Credentials", "Credential Access"),
    "T1555": ("Credentials from Password Stores", "Credential Access"),
    "T1558": ("Steal or Forge Kerberos Tickets", "Credential Access"),
    # Discovery
    "T1016": ("System Network Configuration Discovery", "Discovery"),
    "T1018": ("Remote System Discovery", "Discovery"),
    "T1046": ("Network Service Discovery", "Discovery"),
    "T1057": ("Process Discovery", "Discovery"),
    "T1082": ("System Information Discovery", "Discovery"),
    "T1083": ("File and Directory Discovery", "Discovery"),
    "T1087": ("Account Discovery", "Discovery"),
    "T1135": ("Network Share Discovery", "Discovery"),
    "T1201": ("Password Policy Discovery", "Discovery"),
    "T1482": ("Domain Trust Discovery", "Discovery"),
    # Lateral Movement
    "T1021": ("Remote Services", "Lateral Movement"),
    "T1021.001": ("Remote Desktop Protocol", "Lateral Movement"),
    "T1021.002": ("SMB/Windows Admin Shares", "Lateral Movement"),
    "T1021.004": ("SSH", "Lateral Movement"),
    "T1021.006": ("Windows Remote Management", "Lateral Movement"),
    "T1080": ("Taint Shared Content", "Lateral Movement"),
    "T1210": ("Exploitation of Remote Services", "Lateral Movement"),
    "T1534": ("Internal Spearphishing", "Lateral Movement"),
    "T1570": ("Lateral Tool Transfer", "Lateral Movement"),
    # Collection
    "T1005": ("Data from Local System", "Collection"),
    "T1039": ("Data from Network Shared Drive", "Collection"),
    "T1041": ("Exfiltration Over C2 Channel", "Exfiltration"),
    "T1056.001": ("Keylogging", "Collection"),
    "T1074": ("Data Staged", "Collection"),
    "T1113": ("Screen Capture", "Collection"),
    "T1119": ("Automated Collection", "Collection"),
    "T1560": ("Archive Collected Data", "Collection"),
    "T1560.001": ("Archive via Utility", "Collection"),
    # Command and Control
    "T1071": ("Application Layer Protocol", "Command and Control"),
    "T1071.001": ("Web Protocols", "Command and Control"),
    "T1071.004": ("DNS", "Command and Control"),
    "T1090": ("Proxy", "Command and Control"),
    "T1095": ("Non-Application Layer Protocol", "Command and Control"),
    "T1102": ("Web Service", "Command and Control"),
    "T1105": ("Ingress Tool Transfer", "Command and Control"),
    "T1132": ("Data Encoding", "Command and Control"),
    "T1573": ("Encrypted Channel", "Command and Control"),
    # Exfiltration
    "T1020": ("Automated Exfiltration", "Exfiltration"),
    "T1030": ("Data Transfer Size Limits", "Exfiltration"),
    "T1048": ("Exfiltration Over Alternative Protocol", "Exfiltration"),
    "T1048.003": ("Exfiltration Over Unencrypted Non-C2 Protocol", "Exfiltration"),
    "T1537": ("Transfer Data to Cloud Account", "Exfiltration"),
    "T1567": ("Exfiltration Over Web Service", "Exfiltration"),
    # Impact
    "T1485": ("Data Destruction", "Impact"),
    "T1486": ("Data Encrypted for Impact", "Impact"),
    "T1489": ("Service Stop", "Impact"),
    "T1490": ("Inhibit System Recovery", "Impact"),
    "T1491": ("Defacement", "Impact"),
    "T1495": ("Firmware Corruption", "Impact"),
    "T1499": ("Endpoint Denial of Service", "Impact"),
    "T1529": ("System Shutdown/Reboot", "Impact"),
    "T1531": ("Account Access Removal", "Impact"),
    "T1657": ("Financial Theft", "Impact"),
}


def _fill_mitre_technique_names(flat_data: Dict[str, Any]) -> None:
    """Fill null technique_name and tactic from static lookup when technique_id is known."""
    raw = flat_data.get("mitre_techniques_json")
    if not raw:
        return
    try:
        techniques = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return
    if not isinstance(techniques, list):
        return

    changed = False
    for tech in techniques:
        if not isinstance(tech, dict):
            continue
        tid = (tech.get("technique_id") or "").strip()
        if not tid:
            continue
        info = _MITRE_TECHNIQUE_INFO.get(tid)
        if not info:
            continue
        name, tactic = info
        if not tech.get("technique_name"):
            tech["technique_name"] = name
            changed = True
        if not tech.get("tactic"):
            tech["tactic"] = tactic
            changed = True

    if changed:
        flat_data["mitre_techniques_json"] = json.dumps(techniques)


def _infer_data_categories(flat_data: Dict[str, Any], summary: Optional[str]) -> None:
    """Add data_categories inferred from enriched_summary keywords (never removes existing)."""
    if not summary:
        return
    text = summary.lower()

    # Parse existing categories
    raw = flat_data.get("data_categories")
    if isinstance(raw, list):
        existing: list = list(raw)
    elif isinstance(raw, str) and raw:
        try:
            existing = json.loads(raw)
            if not isinstance(existing, list):
                existing = []
        except (json.JSONDecodeError, TypeError):
            existing = []
    else:
        existing = []

    added = False
    for keyword, category in _DATA_CAT_KEYWORDS:
        if keyword in text and category not in existing:
            existing.append(category)
            added = True

    if added:
        flat_data["data_categories"] = json.dumps(existing)


def apply_post_processing(
    flat_data: Dict[str, Any],
    incident_row: Optional[Any],
    *,
    summary: Optional[str] = None,
) -> None:
    """
    Orchestrator: apply all post-processing rules to *flat_data* in-place.

    Args:
        flat_data:    The dict built by _flatten_enrichment_for_db(), modified in-place.
        incident_row: The sqlite3.Row from the incidents table (may be None).
        summary:      Pre-resolved enriched_summary (avoids re-reading from flat_data).
    """
    _title = incident_row["title"] if incident_row else None
    _summary = summary or flat_data.get("enriched_summary")

    # 1. Ransomware family from summary / title keyword scan.
    # Override "unknown" too — keyword scan gives a definitive name where GBNF forced "unknown".
    current_family = flat_data.get("ransomware_family")
    if not current_family or current_family == "unknown":
        family = extract_ransomware_family(_summary, _title)
        if family:
            flat_data["ransomware_family"] = family

    # 2. institution_type from institution name patterns
    current_type = flat_data.get("institution_type")
    if not current_type or current_type == "unknown":
        inferred = infer_institution_type(flat_data.get("institution_name"), current_type)
        if inferred and inferred != current_type:
            flat_data["institution_type"] = inferred

    # 3. US state (region) from city lookup
    if not flat_data.get("region") and flat_data.get("city"):
        state = infer_us_region(flat_data.get("city"), flat_data.get("country_code"))
        if state:
            flat_data["region"] = state

    # 4. Regulatory impact rules
    infer_regulatory_impact(flat_data)

    # 5. network_compromised: ransomware by definition encrypts/disrupts network-connected systems
    attack_cat = (flat_data.get("attack_category") or "").lower()
    if attack_cat.startswith("ransomware_") and not flat_data.get("network_compromised"):
        flat_data["network_compromised"] = True

    # 6. data_categories: keyword scan of enriched_summary to catch fields LLM missed
    _infer_data_categories(flat_data, _summary)

    # 7. MITRE technique name/tactic from static lookup when technique_id is known
    _fill_mitre_technique_names(flat_data)

    # 8. data_breached: infer from exfiltration or ransom payment signals in flat_data
    if flat_data.get("data_breached") is None:
        _attack_cat = (flat_data.get("attack_category") or "").lower()
        if flat_data.get("ransom_paid") or flat_data.get("data_exfiltrated"):
            flat_data["data_breached"] = True
        elif any(kw in _attack_cat for kw in ("exfiltration", "data_leak", "data_breach", "data_exposure")):
            flat_data["data_breached"] = True
        elif flat_data.get("data_categories") or flat_data.get("records_affected_exact") or flat_data.get("records_affected_min"):
            flat_data["data_breached"] = True
