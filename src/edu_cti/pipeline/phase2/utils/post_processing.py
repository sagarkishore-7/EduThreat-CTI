"""
Post-processing rules applied after LLM enrichment to improve field quality.

These are deterministic rules — no LLM re-calls required.  They only FILL null
fields; they never overwrite a value that the LLM already provided.  Safe to run
retroactively against existing enrichments.
"""

from __future__ import annotations

import json
import re
from datetime import date as _date, datetime, timedelta
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

_WEEKDAY_TO_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}

_RELATIVE_NUMBER_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
}

_RELATIVE_WEEKDAY_RE = re.compile(
    r"\b(?:last|past)\s+"
    r"(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b",
    re.IGNORECASE,
)
_RELATIVE_COUNT_RE = re.compile(
    r"\b(?:(\d+)|("
    + "|".join(_RELATIVE_NUMBER_WORDS.keys())
    + r"))\s+(day|week|month|year)s?\s+ago\b",
    re.IGNORECASE,
)
_YESTERDAY_RE = re.compile(r"\byesterday\b", re.IGNORECASE)
_LAST_WEEK_RE = re.compile(r"\blast\s+week\b", re.IGNORECASE)
_LAST_MONTH_RE = re.compile(r"\blast\s+month\b", re.IGNORECASE)
_LAST_YEAR_RE = re.compile(r"\blast\s+year\b", re.IGNORECASE)
_EARLIER_THIS_MONTH_RE = re.compile(r"\bearlier\s+this\s+month\b", re.IGNORECASE)
_EARLIER_THIS_YEAR_RE = re.compile(r"\bearlier\s+this\s+year\b", re.IGNORECASE)

_OCCURRENCE_EVENT_TYPES = {
    "initial_access", "reconnaissance", "exploitation",
    "lateral_movement", "privilege_escalation",
    "data_exfiltration", "encryption_started",
    "ransom_demand", "impact", "operational_impact",
}

_RESPONSE_EVENT_TYPES = {
    "discovery", "containment", "eradication", "recovery",
    "notification", "disclosure", "public_statement",
    "investigation", "remediation", "law_enforcement_contact",
    "systems_restored", "response_action", "security_improvement",
}

# ── Canadian city → province (unambiguous only) ─────────────────────────────────
_CA_CITY_TO_PROVINCE: dict[str, str] = {
    # Ontario
    "toronto": "Ontario", "north york": "Ontario", "scarborough": "Ontario",
    "etobicoke": "Ontario", "mississauga": "Ontario", "brampton": "Ontario",
    "hamilton": "Ontario", "london": "Ontario", "windsor": "Ontario",
    "kitchener": "Ontario", "waterloo": "Ontario", "guelph": "Ontario",
    "barrie": "Ontario", "kingston": "Ontario", "sudbury": "Ontario",
    "thunder bay": "Ontario", "oshawa": "Ontario", "st. catharines": "Ontario",
    "ottawa": "Ontario", "kanata": "Ontario",
    # Quebec
    "montreal": "Quebec", "québec city": "Quebec", "laval": "Quebec",
    "gatineau": "Quebec", "longueuil": "Quebec", "sherbrooke": "Quebec",
    "trois-rivières": "Quebec", "saguenay": "Quebec",
    # British Columbia
    "vancouver": "British Columbia", "burnaby": "British Columbia",
    "surrey": "British Columbia", "richmond": "British Columbia",
    "kelowna": "British Columbia", "abbotsford": "British Columbia",
    "victoria": "British Columbia", "nanaimo": "British Columbia",
    "kamloops": "British Columbia", "prince george": "British Columbia",
    # Alberta
    "calgary": "Alberta", "edmonton": "Alberta", "red deer": "Alberta",
    "lethbridge": "Alberta", "medicine hat": "Alberta", "grande prairie": "Alberta",
    # Manitoba
    "winnipeg": "Manitoba", "brandon": "Manitoba", "steinbach": "Manitoba",
    # Saskatchewan
    "regina": "Saskatchewan", "saskatoon": "Saskatchewan",
    # Nova Scotia
    "halifax": "Nova Scotia", "dartmouth": "Nova Scotia", "sydney": "Nova Scotia",
    "truro": "Nova Scotia",
    # New Brunswick
    "fredericton": "New Brunswick", "moncton": "New Brunswick",
    "saint john": "New Brunswick",
    # Newfoundland and Labrador
    "st. john's": "Newfoundland and Labrador", "corner brook": "Newfoundland and Labrador",
    # PEI
    "charlottetown": "Prince Edward Island",
    # Nova Scotia
    "cape breton": "Nova Scotia",
}

# ── UK city → nation ─────────────────────────────────────────────────────────────
_UK_CITY_TO_NATION: dict[str, str] = {
    # England
    "london": "England", "birmingham": "England", "manchester": "England",
    "leeds": "England", "liverpool": "England", "sheffield": "England",
    "bristol": "England", "newcastle": "England", "nottingham": "England",
    "leicester": "England", "coventry": "England", "bradford": "England",
    "exeter": "England", "cambridge": "England", "oxford": "England",
    "brighton": "England", "southampton": "England", "portsmouth": "England",
    "reading": "England", "wolverhampton": "England", "sunderland": "England",
    "middlesbrough": "England", "hull": "England", "kingston upon hull": "England",
    "stoke-on-trent": "England", "derby": "England", "plymouth": "England",
    "norwich": "England", "bath": "England", "york": "England",
    "huddersfield": "England", "lincoln": "England", "peterborough": "England",
    "gloucester": "England", "shrewsbury": "England", "worcester": "England",
    "kent": "England", "essex": "England", "hertford": "England",
    "hatfield": "England", "colchester": "England",
    # Scotland
    "edinburgh": "Scotland", "glasgow": "Scotland", "aberdeen": "Scotland",
    "dundee": "Scotland", "inverness": "Scotland", "stirling": "Scotland",
    "perth": "Scotland", "paisley": "Scotland", "livingston": "Scotland",
    # Wales
    "cardiff": "Wales", "swansea": "Wales", "newport": "Wales",
    "wrexham": "Wales", "bangor": "Wales",
    # Northern Ireland
    "belfast": "Northern Ireland", "derry": "Northern Ireland",
    "londonderry": "Northern Ireland", "lisburn": "Northern Ireland",
}

# ── Australian city → state ──────────────────────────────────────────────────────
_AU_CITY_TO_STATE: dict[str, str] = {
    # New South Wales
    "sydney": "New South Wales", "wollongong": "New South Wales",
    "newcastle": "New South Wales", "canberra": "New South Wales",
    "bathurst": "New South Wales", "albury": "New South Wales",
    "tamworth": "New South Wales", "wagga wagga": "New South Wales",
    # Victoria
    "melbourne": "Victoria", "geelong": "Victoria", "ballarat": "Victoria",
    "bendigo": "Victoria", "shepparton": "Victoria", "mildura": "Victoria",
    # Queensland
    "brisbane": "Queensland", "gold coast": "Queensland", "cairns": "Queensland",
    "townsville": "Queensland", "toowoomba": "Queensland", "sunshine coast": "Queensland",
    "rockhampton": "Queensland", "mackay": "Queensland",
    # Western Australia
    "perth": "Western Australia", "fremantle": "Western Australia",
    "bunbury": "Western Australia", "geraldton": "Western Australia",
    # South Australia
    "adelaide": "South Australia", "mount gambier": "South Australia",
    # ACT
    "act": "Australian Capital Territory",
    # Tasmania
    "hobart": "Tasmania", "launceston": "Tasmania",
    # Northern Territory
    "darwin": "Northern Territory", "alice springs": "Northern Territory",
}

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
# ── Defunct ransomware families (disbanded/rebranded) ────────────────────────────
# Maps canonical family name → approximate date it ceased operations.
# Attacks dated AFTER the cutoff should not be attributed to these families.
_DEFUNCT_AFTER: dict[str, str] = {
    "conti":             "2022-06-01",
    "revil_sodinokibi":  "2022-02-01",
    "darkside":          "2021-05-15",
    "maze":              "2020-11-01",
    "doppelpaymer":      "2022-03-01",
    "avoslocker":        "2023-05-01",
    "hive":              "2023-01-27",
    "cl0p_clop":         "2024-06-01",
}

# ── Attack vector keyword scan ────────────────────────────────────────────────────
# (keyword, canonical_vector) ordered most-specific first
_ATTACK_VECTOR_KEYWORDS: list[tuple[str, str]] = [
    ("spear.?phishing",        "spear_phishing_email"),
    ("spearphishing",          "spear_phishing_email"),
    ("phishing email",         "phishing_email"),
    ("phishing link",          "malicious_link"),
    ("phishing attach",        "malicious_attachment"),
    ("malicious attach",       "malicious_attachment"),
    ("malicious link",         "malicious_link"),
    ("business email compromise", "business_email_compromise"),
    (r"\bbec\b",               "business_email_compromise"),
    ("credential stuff",       "credential_stuffing"),
    ("password spray",         "password_spraying"),
    ("brute.?forc",            "brute_force"),
    ("stolen credential",      "stolen_credentials"),
    ("compromised credential", "stolen_credentials"),
    ("valid account",          "stolen_credentials"),
    (r"\brdp\b",               "exposed_rdp"),
    ("remote desktop",         "exposed_rdp"),
    (r"\bvpn\b",               "exposed_vpn"),
    ("exposed.*ssh",           "exposed_ssh"),
    ("exposed.*database",      "exposed_database"),
    ("exposed.*api",           "exposed_api"),
    ("zero.?day",              "vulnerability_exploit_zero_day"),
    (r"\bcve-\d{4}",           "vulnerability_exploit_known"),
    ("unpatched",              "unpatched_system"),
    ("misconfigur",            "misconfiguration"),
    ("default credential",     "default_credentials"),
    ("default password",       "default_credentials"),
    ("sql injection",          "sql_injection"),
    (r"\bssrf\b",              "ssrf"),
    (r"\bxss\b",               "xss"),
    ("supply chain",           "supply_chain_compromise"),
    ("third.?party vendor",    "third_party_vendor"),
    ("watering hole",          "watering_hole"),
    ("drive.?by",              "drive_by_download"),
    ("usb",                    "usb_drop"),
    ("insider",                "insider_access"),
    ("social engineer",        "social_engineering"),
    ("cloud misconfigur",      "cloud_misconfiguration"),
    ("storage bucket",         "storage_bucket_exposure"),
    ("api key",                "api_key_exposure"),
    ("phishing",               "phishing_email"),  # generic fallback
]


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
    r"has (?:sent|issued) notifications?|notifying (?:affected|impacted)|"
    # Dark web publication = confirmed exfiltration
    r"(?:published|posted|uploaded|leaked?) (?:the data )?(?:on|to) (?:the )?dark web|"
    r"dark web (?:forum|leak|listing|post)|"
    # Payment disclosures (e.g. "paid $457,000")
    r"paid \$[\d,]+|ransom (?:payment|amount) of|settlement (?:of|worth) \$|"
    # Regulatory filings / attorney general notifications
    r"notified the (?:attorney general|state attorney)|"
    r"regulatory (?:filing|notice|notification)|"
    r"attorney general (?:notification|filing|notice)|"
    # Lawsuits confirm the incident is real
    r"class[- ]action (?:lawsuit|suit|filing?)|"
    r"lawsuit (?:has been )?filed"
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
# International K-12 school name patterns
_K12_SCHOOL_INTL_RE = re.compile(
    r"\b(?:"
    r"primaria|secundaria|preparatoria|bachillerato|"           # Spanish
    r"école (?:primaire|élémentaire|maternelle|secondaire)|"   # French compound
    r"collège|lycée|"                                           # French schools
    r"basisschool|middelbare school|voortgezet onderwijs|"     # Dutch
    r"scuola (?:elementare|media|secondaria|superiore)|"       # Italian
    r"istituto (?:comprensivo|superiore)"                       # Italian
    r")\b",
    re.IGNORECASE,
)
# International university / higher-ed name patterns
_UNIVERSITY_INTL_RE = re.compile(
    r"\b(?:"
    r"universität|università|université|universidad|universidade|"
    r"fachhochschule|hochschule|"
    r"polytechnique|politecnico|politécnica|politécnico"
    r")\b",
    re.IGNORECASE,
)


# ── Headline detection ───────────────────────────────────────────────────────────
_HEADLINE_SOURCE_RE = re.compile(
    # " - Source Name" suffix — allow dots for domains like WTVR.com, BBC.co.uk
    r"\s+[-–—]\s+\w[\w\s\.]{0,30}$"
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
    if _K12_SCHOOL_RE.search(name) or _K12_SCHOOL_INTL_RE.search(name):
        return "k12_school"
    if _UNIVERSITY_INTL_RE.search(name):
        return "university"
    return existing_type


def infer_us_region(city: Optional[str], country_code: Optional[str]) -> Optional[str]:
    """Return US state for the given city when country_code is 'US'."""
    if not city or country_code != "US":
        return None
    return _US_CITY_TO_STATE.get(city.lower().strip())


def infer_region_from_city(city: Optional[str], country_code: Optional[str]) -> Optional[str]:
    """Return region/state/province/nation for the given city across US, CA, GB, AU."""
    if not city or not country_code:
        return None
    key = city.lower().strip()
    cc = country_code.upper()
    if cc == "US":
        return _US_CITY_TO_STATE.get(key)
    if cc == "CA":
        return _CA_CITY_TO_PROVINCE.get(key)
    if cc == "GB":
        return _UK_CITY_TO_NATION.get(key)
    if cc == "AU":
        return _AU_CITY_TO_STATE.get(key)
    return None


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
    if flat_data.get("is_education_related") is False:
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

    # FERPA — US education institution + data breach → FERPA applies.
    # Student education records are covered by FERPA; generic "personal data"
    # at a university almost certainly includes student records.
    if flat_data.get("ferpa_breach") is None and data_breached and country_code == "US" and _is_edu_inst:
        if any(kw in data_cats_text for kw in ("student", "grade", "transcript", "ferpa", "educational record")):
            flat_data["ferpa_breach"] = True
        elif any(kw in data_cats_text for kw in ("personal_data", "general_pii", "personal_information")):
            # Generic "personal data" at a US edu institution → assume student records included
            flat_data["ferpa_breach"] = True
        elif any(kw in summary for kw in ("student data", "student records", "student information")):
            flat_data["ferpa_breach"] = True

    # GDPR — deterministic: EU/EEA data breach → GDPR applies (only when LLM left it null)
    if flat_data.get("gdpr_breach") is None and data_breached and country_code in _EU_CODES:
        flat_data["gdpr_breach"] = True

    # HIPAA — deterministic: US health data breach → HIPAA applies (only when LLM left it null)
    if flat_data.get("hipaa_breach") is None and data_breached and country_code == "US":
        if any(kw in data_cats_text or kw in summary for kw in (
            "health", "medical", "hipaa", "phi", "patient", "ehr", "emr",
        )):
            flat_data["hipaa_breach"] = True

    # breach_notification_required — all 50 US states have breach notification laws
    # when personal identifiable information is involved.
    if flat_data.get("breach_notification_required") is None and data_breached and country_code == "US":
        _pii_cats = ("pii", "ssn", "social_security", "employee", "student", "health",
                     "financial", "personal_information", "medical", "address", "date_of_birth",
                     "personal_data", "personal_information", "general_pii")
        if any(kw in data_cats_text for kw in _pii_cats) or \
           any(kw in summary for kw in ("personal data", "personal information", "pii", "ssn",
                                        "student data", "employee data", "health records")):
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

    existing_regs: list[str] = []
    _existing_context = flat_data.get("regulatory_context")
    if isinstance(_existing_context, list):
        existing_regs = [str(v) for v in _existing_context if str(v).strip()]
    elif isinstance(_existing_context, str) and _existing_context.strip():
        try:
            parsed = json.loads(_existing_context)
            if isinstance(parsed, list):
                existing_regs = [str(v) for v in parsed if str(v).strip()]
        except (json.JSONDecodeError, TypeError):
            existing_regs = []

    def _append_reg(value: str) -> None:
        if value not in existing_regs:
            existing_regs.append(value)

    if flat_data.get("ferpa_breach"):
        _append_reg("FERPA")
    if flat_data.get("hipaa_breach"):
        _append_reg("HIPAA")
    if flat_data.get("gdpr_breach"):
        _append_reg("GDPR")
    if country_code == "GB" and data_breached:
        _append_reg("UK_DPA")
    if flat_data.get("dpa_notified") and country_code in _EU_CODES:
        _append_reg("GDPR")
        if country_code == "GB":
            _append_reg("UK_DPA")
    if flat_data.get("breach_notification_required") and country_code == "US":
        _append_reg("state_breach_notification")

    if existing_regs:
        flat_data["regulatory_context"] = json.dumps(existing_regs)


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
    # Looser health variants (catches "health data", "temas de salud", etc.)
    ("health data", "health_records"),
    ("health information", "health_records"),
    ("temas de salud", "health_records"),         # Spanish: "health topics/data"
    ("datos de salud", "health_records"),         # Spanish: "health data"
    # General PII — broad personal data references
    ("personal data", "general_pii"),
    ("personal information", "general_pii"),
    ("datos personales", "general_pii"),          # Spanish
    ("données personnelles", "general_pii"),       # French
    ("persoonsgegevens", "general_pii"),           # Dutch
    ("dati personali", "general_pii"),             # Italian
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
    """
    Fill null technique_name, tactic, and description from the MITRE ATT&CK STIX
    bundle (697 active techniques, cached locally). Falls back to the static
    hand-curated table (_MITRE_TECHNIQUE_INFO) for any technique not found in STIX.
    """
    raw = flat_data.get("mitre_techniques_json")
    if not raw:
        return
    try:
        techniques = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return
    if not isinstance(techniques, list):
        return

    # Try STIX-based lookup first
    try:
        from src.edu_cti.pipeline.phase2.extraction.mitre_stix import hydrate_mitre_techniques
        changed = hydrate_mitre_techniques(techniques)
    except Exception as exc:
        logger.debug("MITRE STIX hydration skipped: %s", exc)
        changed = False

    # Static fallback for any technique still missing name/tactic after STIX
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


def infer_attack_vector(summary: Optional[str], title: Optional[str]) -> Optional[str]:
    """Scan summary+title for attack vector signals. Returns canonical vector or None."""
    text = " ".join(filter(None, [summary, title])).lower()
    if not text:
        return None
    for pattern, vector in _ATTACK_VECTOR_KEYWORDS:
        if re.search(pattern, text, re.IGNORECASE):
            return vector
    return None


def infer_data_volume_gb(summary: Optional[str]) -> Optional[float]:
    """Extract data volume in GB from summary text (e.g. '430 GB', '1.2 terabytes')."""
    if not summary:
        return None
    # GB / gigabyte(s)
    m = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:GB|gigabyte)', summary, re.IGNORECASE)
    if m:
        return float(m.group(1).replace(",", "."))
    # TB / terabyte(s) → convert to GB
    m = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:TB|terabyte)', summary, re.IGNORECASE)
    if m:
        return float(m.group(1).replace(",", ".")) * 1024
    # MB — only capture if clearly large (>= 100 MB)
    m = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:MB|megabyte)', summary, re.IGNORECASE)
    if m:
        mb = float(m.group(1).replace(",", "."))
        return mb / 1024 if mb >= 100 else None
    return None


def infer_region_from_institution_name(name: Optional[str], country_code: Optional[str]) -> Optional[str]:
    """
    Extract region from 'University of X at City' or 'City University' patterns.
    Covers US, CA, GB, and AU via the respective city→region lookups.
    """
    if not name or not country_code:
        return None
    cc = country_code.upper()

    def _lookup(city_str: str) -> Optional[str]:
        return infer_region_from_city(city_str, cc)

    # "University of X at City" / "X at City"
    m = re.search(r'\bat\s+([A-Z][a-z]+(?:[\s-][A-Z][a-z]+)?)', name)
    if m:
        region = _lookup(m.group(1).strip())
        if region:
            return region

    # "City University" / "City College" — city is first word(s)
    m = re.search(r'^([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s+(?:University|College|School|Institute)', name)
    if m:
        region = _lookup(m.group(1).strip())
        if region:
            return region

    # "University of Guelph", "University of Manitoba" etc. — extract city after "of"
    m = re.search(r'\bof\s+([A-Z][a-z]+(?:[\s-][A-Z][a-z]+)?)', name)
    if m:
        region = _lookup(m.group(1).strip())
        if region:
            return region

    return None


def _sanitize_defunct_ransomware(flat_data: Dict[str, Any]) -> None:
    """
    Clear ransomware_family when the family is known to have disbanded before the incident date.
    LLMs commonly hallucinate Conti/REvil for recent attacks.
    """
    family = flat_data.get("ransomware_family")
    if not family or family not in _DEFUNCT_AFTER:
        return
    incident_date = flat_data.get("incident_date") or flat_data.get("source_published_date")
    if not incident_date:
        return
    try:
        cutoff = _DEFUNCT_AFTER[family]
        if str(incident_date)[:10] > cutoff:
            flat_data["ransomware_family"] = None
    except Exception:
        pass


def _coerce_iso_date(value: Any) -> Optional[str]:
    """Normalize a date-like value to YYYY-MM-DD when possible."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    if len(text) >= 10 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", text[:10]):
        return text[:10]

    cleaned = re.sub(r"(\d{1,2})(st|nd|rd|th)\b", r"\1", text, flags=re.IGNORECASE)
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"

    try:
        from dateutil import parser as date_parser

        return date_parser.parse(cleaned, fuzzy=True).date().isoformat()
    except Exception:
        pass

    try:
        return datetime.fromisoformat(cleaned).date().isoformat()
    except Exception:
        return None


def _subtract_months(anchor: _date, months: int) -> _date:
    total_months = (anchor.year * 12 + anchor.month - 1) - months
    year = total_months // 12
    month = total_months % 12 + 1
    day = min(
        anchor.day,
        (
            _date(year + (month // 12), (month % 12) + 1, 1) - timedelta(days=1)
            if month < 12
            else _date(year + 1, 1, 1) - timedelta(days=1)
        ).day,
    )
    return _date(year, month, day)


def _derive_relative_incident_date(
    article_text: Optional[str],
    publication_date: Optional[str],
) -> Optional[tuple[str, str]]:
    """Resolve conservative relative-date phrases against article publication date."""
    publication_iso = _coerce_iso_date(publication_date)
    if not publication_iso or not article_text:
        return None

    try:
        anchor = _date.fromisoformat(publication_iso)
    except ValueError:
        return None

    text = re.sub(r"\s+", " ", article_text).strip().lower()
    if not text:
        return None

    weekday_match = _RELATIVE_WEEKDAY_RE.search(text)
    if weekday_match:
        target_idx = _WEEKDAY_TO_INDEX[weekday_match.group(1).lower()]
        delta_days = (anchor.weekday() - target_idx) % 7
        if delta_days == 0:
            delta_days = 7
        return ((anchor - timedelta(days=delta_days)).isoformat(), "approximate")

    if _YESTERDAY_RE.search(text):
        return ((anchor - timedelta(days=1)).isoformat(), "day")

    count_match = _RELATIVE_COUNT_RE.search(text)
    if count_match:
        raw_count = count_match.group(1) or count_match.group(2)
        unit = count_match.group(3).lower()
        count = int(raw_count) if raw_count.isdigit() else _RELATIVE_NUMBER_WORDS.get(raw_count.lower(), 0)
        if count > 0:
            if unit == "day":
                precision = "day" if count == 1 else "approximate"
                return ((anchor - timedelta(days=count)).isoformat(), precision)
            if unit == "week":
                return ((anchor - timedelta(days=count * 7)).isoformat(), "approximate")
            if unit == "month":
                return (_subtract_months(anchor, count).isoformat(), "month_only")
            if unit == "year":
                return (f"{anchor.year - count}-01-01", "year_only")

    if _LAST_WEEK_RE.search(text):
        return ((anchor - timedelta(days=7)).isoformat(), "approximate")

    if _LAST_MONTH_RE.search(text):
        return (_subtract_months(anchor, 1).isoformat(), "month_only")

    if _LAST_YEAR_RE.search(text):
        return (f"{anchor.year - 1}-01-01", "year_only")

    if _EARLIER_THIS_MONTH_RE.search(text):
        return (_date(anchor.year, anchor.month, 1).isoformat(), "month_only")

    if _EARLIER_THIS_YEAR_RE.search(text):
        return (_date(anchor.year, 1, 1).isoformat(), "year_only")

    return None


def _fill_timeline_list_dates(payload: Dict[str, Any]) -> None:
    timeline = payload.get("timeline")
    if not isinstance(timeline, list):
        return

    incident_date = _coerce_iso_date(payload.get("incident_date"))
    published_date = _coerce_iso_date(payload.get("source_published_date")) or _coerce_iso_date(
        payload.get("publication_date")
    )
    if not incident_date and not published_date:
        return

    changed = False
    for event in timeline:
        if not isinstance(event, dict) or event.get("date"):
            continue
        event_type = str(event.get("event_type") or "").strip().lower() or None
        if event_type in _OCCURRENCE_EVENT_TYPES and incident_date:
            event["date"] = incident_date
            event["date_precision"] = event.get("date_precision") or "approximate"
            changed = True
        elif event_type in _RESPONSE_EVENT_TYPES:
            anchor = published_date or incident_date
            if anchor:
                event["date"] = anchor
                event["date_precision"] = event.get("date_precision") or "approximate"
                changed = True
        elif event_type in (None, "", "other") and incident_date:
            event["date"] = incident_date
            event["date_precision"] = event.get("date_precision") or "approximate"
            changed = True

    if changed:
        payload["timeline"] = timeline


def apply_extraction_date_fallbacks(
    payload: Dict[str, Any],
    *,
    article_text: Optional[str],
    article_publish_date: Optional[str],
    source_published_date: Optional[str],
) -> None:
    """
    Deterministically repair date fields after LLM extraction.

    Rules:
    - Backfill publication_date from article/source metadata when missing.
    - Preserve incident_date=null unless we can justify it from relative wording.
    - Never silently copy publication_date into incident_date.
    """
    existing_publication_date = _coerce_iso_date(payload.get("publication_date"))
    article_publication_date = _coerce_iso_date(article_publish_date)
    source_publication_date = _coerce_iso_date(source_published_date)

    publication_date = existing_publication_date or article_publication_date or source_publication_date
    if publication_date and not existing_publication_date:
        payload["publication_date"] = publication_date
        if not payload.get("publication_date_basis"):
            if article_publication_date and publication_date == article_publication_date:
                payload["publication_date_basis"] = "article_metadata_fallback"
            elif source_publication_date and publication_date == source_publication_date:
                payload["publication_date_basis"] = "source_metadata_fallback"
    elif existing_publication_date and not payload.get("publication_date_basis"):
        payload["publication_date_basis"] = "llm_extracted"

    source_date = _coerce_iso_date(payload.get("source_published_date")) or source_publication_date
    if not source_date:
        source_date = publication_date
    if source_date and not _coerce_iso_date(payload.get("source_published_date")):
        payload["source_published_date"] = source_date

    existing_incident_date = _coerce_iso_date(payload.get("incident_date"))
    if existing_incident_date and not payload.get("incident_date_basis"):
        precision = str(payload.get("incident_date_precision") or "").strip().lower()
        if precision in {"approximate", "month_only", "year_only"}:
            payload["incident_date_basis"] = "llm_relative_or_partial"
        else:
            payload["incident_date_basis"] = "llm_extracted"

    if not existing_incident_date:
        derived = _derive_relative_incident_date(article_text, publication_date or source_date)
        if derived is not None:
            incident_date, precision = derived
            payload["incident_date"] = incident_date
            payload["incident_date_basis"] = "deterministic_relative_to_publication_date"
            current_precision = str(payload.get("incident_date_precision") or "").strip().lower()
            if current_precision in {"", "unknown", "null", "none"}:
                payload["incident_date_precision"] = precision

    _fill_timeline_list_dates(payload)


def _guard_timeline_dates(flat_data: Dict[str, Any], incident_row: Optional[Any]) -> None:
    """
    Null out timeline event dates that are >90 days after source_published_date.

    LLMs sometimes write the current processing date into timeline events when
    the article content couldn't be fetched (only title/subtitle available).
    The incident_date field has its own guard in db.py; this extends it to
    the per-event dates stored inside timeline_json.
    """
    timeline_raw = flat_data.get("timeline_json")
    if not timeline_raw:
        return

    src_str = (
        (incident_row["source_published_date"] if incident_row else None)
        or flat_data.get("source_published_date")
        or flat_data.get("incident_date")
    )
    if not src_str:
        return

    try:
        from datetime import date as _date
        src_dt = _date.fromisoformat(str(src_str)[:10])
    except (ValueError, TypeError):
        return

    try:
        events = json.loads(timeline_raw) if isinstance(timeline_raw, str) else list(timeline_raw)
    except (json.JSONDecodeError, TypeError):
        return

    if not isinstance(events, list):
        return

    changed = False
    for event in events:
        if not isinstance(event, dict):
            continue
        ev_date = event.get("date")
        if not ev_date:
            continue
        try:
            ev_dt = _date.fromisoformat(str(ev_date)[:10])
            if (ev_dt - src_dt).days > 90:
                event["date"] = None
                event["date_precision"] = "approximate"
                changed = True
        except (ValueError, TypeError):
            continue

    if changed:
        flat_data["timeline_json"] = json.dumps(events)


def _fill_timeline_dates(flat_data: Dict[str, Any]) -> None:
    """
    Fill null timeline event dates using known incident anchors.

    Two reliable anchors exist even when articles don't give per-event timestamps:
    - incident_date  → when the attack / breach actually occurred
    - source_published_date → when the article / notification was published
                              (proxy for disclosure / notification events)

    Rules:
    - Occurrence events (initial_access, exploitation, data_exfiltration,
      encryption_started, ransom_demand, impact) → incident_date
    - Response / disclosure events (notification, disclosure, public_statement,
      containment, recovery, remediation, law_enforcement_contact,
      systems_restored, investigation) → source_published_date
    - Events with null event_type → incident_date as approximate fallback
    Never overwrites a date the LLM already populated.
    """
    timeline_raw = flat_data.get("timeline_json")
    if not timeline_raw:
        return

    incident_date = flat_data.get("incident_date")
    published_date = flat_data.get("source_published_date")

    if not incident_date and not published_date:
        return

    try:
        events = json.loads(timeline_raw) if isinstance(timeline_raw, str) else list(timeline_raw)
    except (json.JSONDecodeError, TypeError):
        return
    if not isinstance(events, list):
        return

    _OCCURRENCE_TYPES = {
        "initial_access", "reconnaissance", "exploitation",
        "lateral_movement", "privilege_escalation",
        "data_exfiltration", "encryption_started",
        "ransom_demand", "impact", "operational_impact",
    }
    _RESPONSE_TYPES = {
        "discovery", "containment", "eradication", "recovery",
        "notification", "disclosure", "public_statement",
        "investigation", "remediation", "law_enforcement_contact",
        "systems_restored", "response_action", "security_improvement",
    }

    changed = False
    for event in events:
        if not isinstance(event, dict) or event.get("date"):
            continue  # already has a date — never overwrite
        etype = event.get("event_type")
        if etype in _OCCURRENCE_TYPES and incident_date:
            event["date"] = str(incident_date)[:10]
            event["date_precision"] = "approximate"
            changed = True
        elif etype in _RESPONSE_TYPES:
            anchor = published_date or incident_date
            if anchor:
                event["date"] = str(anchor)[:10]
                event["date_precision"] = "approximate"
                changed = True
        elif etype is None or etype == "other":
            # Unknown type — use incident_date as best-effort fallback
            if incident_date:
                event["date"] = str(incident_date)[:10]
                event["date_precision"] = "approximate"
                changed = True

    if changed:
        flat_data["timeline_json"] = json.dumps(events)


def _fill_transparency_from_timeline(flat_data: Dict[str, Any]) -> None:
    """
    Derive transparency fields from timeline disclosure/notification events.

    If public_disclosure_date is not already set by the LLM, scan timeline events
    for disclosure/notification/public_statement events with dates and use the
    earliest as the disclosure date. Then calculate disclosure_delay_days and
    transparency_level from the gap to incident_date.

    Thresholds:
      0–3 days  → 'full'    (GDPR-grade rapid disclosure)
      4–30 days → 'partial' (within standard notification window)
      31–90 days→ 'minimal' (delayed but eventual)
      >90 days  → 'none'    (very delayed)
    """
    if flat_data.get("public_disclosure_date"):
        return  # LLM already filled — never overwrite

    timeline_raw = flat_data.get("timeline_json")
    if not timeline_raw:
        return

    try:
        events = json.loads(timeline_raw) if isinstance(timeline_raw, str) else list(timeline_raw)
    except (json.JSONDecodeError, TypeError):
        return
    if not isinstance(events, list):
        return

    _DISCLOSURE_TYPES = {"disclosure", "public_statement", "notification"}

    earliest_dt = None
    for event in events:
        if not isinstance(event, dict):
            continue
        if event.get("event_type") not in _DISCLOSURE_TYPES:
            continue
        date_str = event.get("date")
        if not date_str:
            continue
        try:
            event_dt = datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
            if earliest_dt is None or event_dt < earliest_dt:
                earliest_dt = event_dt
        except (ValueError, TypeError):
            continue

    if earliest_dt is None:
        return

    flat_data["public_disclosure_date"] = earliest_dt.strftime("%Y-%m-%d")

    if flat_data.get("public_disclosure") is None:
        flat_data["public_disclosure"] = True

    incident_date_str = flat_data.get("incident_date")
    if incident_date_str and flat_data.get("disclosure_delay_days") is None:
        try:
            incident_dt = datetime.strptime(str(incident_date_str)[:10], "%Y-%m-%d")
            delay_days = max(0, (earliest_dt - incident_dt).days)
            if delay_days < 3650:  # sanity: ignore implausible gaps > 10 years
                flat_data["disclosure_delay_days"] = float(delay_days)
                if not flat_data.get("transparency_level"):
                    if delay_days <= 3:
                        flat_data["transparency_level"] = "full"
                    elif delay_days <= 30:
                        flat_data["transparency_level"] = "partial"
                    elif delay_days <= 90:
                        flat_data["transparency_level"] = "minimal"
                    else:
                        flat_data["transparency_level"] = "none"
        except (ValueError, TypeError):
            pass


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

    # 1b. Sanity-check: clear family if gang was defunct before the incident date.
    _sanitize_defunct_ransomware(flat_data)

    # 2. institution_type from institution name patterns
    current_type = flat_data.get("institution_type")
    if not current_type or current_type == "unknown":
        inferred = infer_institution_type(flat_data.get("institution_name"), current_type)
        if inferred and inferred != current_type:
            flat_data["institution_type"] = inferred

    # 3. Region from city lookup — covers US, CA, GB, AU
    if not flat_data.get("region") and flat_data.get("city"):
        region = infer_region_from_city(flat_data.get("city"), flat_data.get("country_code"))
        if region:
            flat_data["region"] = region

    # 3b. Region from institution name patterns (city embedded in name, all countries)
    if not flat_data.get("region"):
        region = infer_region_from_institution_name(
            flat_data.get("institution_name"), flat_data.get("country_code")
        )
        if region:
            flat_data["region"] = region

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

    # 8. data_exfiltrated: infer from attack_category or summary keywords when LLM left it null.
    if flat_data.get("data_exfiltrated") is None:
        _attack_cat = (flat_data.get("attack_category") or "").lower()
        _exfil_kws = ("exfiltrat", "data_leak", "data_exposure", "data_theft", "stolen data",
                      "data stolen", "data leaked", "data published", "data posted",
                      "published on dark web", "leaked online", "ransomware")
        if any(kw in _attack_cat for kw in ("exfiltration", "data_leak", "data_exposure")) or \
           any(kw in (_summary or "").lower() for kw in _exfil_kws):
            flat_data["data_exfiltrated"] = True

    # 8b. was_ransom_demanded: infer when ransom_amount is set or summary mentions demand.
    if flat_data.get("was_ransom_demanded") is None:
        _ransom_amount = flat_data.get("ransom_amount") or flat_data.get("ransom_amount_usd")
        _ransom_kws = ("ransom demand", "demanded ransom", "ransom of $", "paid ransom",
                       "ransom payment", "ransom note", "ransom was demanded", "demanded a ransom",
                       "ransomware group demanded", "demanded $", "threatened to publish")
        if _ransom_amount or any(kw in (_summary or "").lower() for kw in _ransom_kws):
            flat_data["was_ransom_demanded"] = True

    # 8c. data_breached: infer from exfiltration or ransom payment signals in flat_data
    if flat_data.get("data_breached") is None:
        _attack_cat = (flat_data.get("attack_category") or "").lower()
        if flat_data.get("ransom_paid") or flat_data.get("data_exfiltrated"):
            flat_data["data_breached"] = True
        elif any(kw in _attack_cat for kw in ("exfiltration", "data_leak", "data_breach", "data_exposure")):
            flat_data["data_breached"] = True
        elif flat_data.get("data_categories") or flat_data.get("records_affected_exact") or flat_data.get("records_affected_min"):
            flat_data["data_breached"] = True

    # 9. attack_vector from summary/title keyword scan when LLM left it null/unknown
    if not flat_data.get("attack_vector") or flat_data.get("attack_vector") == "unknown":
        vector = infer_attack_vector(_summary, _title)
        if vector:
            flat_data["attack_vector"] = vector

    # 10. data_volume_gb from summary text (e.g. "430 GB", "1.2 TB")
    if flat_data.get("data_volume_gb") is None:
        vol = infer_data_volume_gb(_summary)
        if vol is not None:
            flat_data["data_volume_gb"] = vol

    # 11. Timeline date guard: null out timeline event dates that are >90 days
    # after source_published_date (LLM confuses current processing date with
    # article date when the article content couldn't be fetched).
    _guard_timeline_dates(flat_data, incident_row)

    # 11b. Fill null timeline dates using incident_date / source_published_date as anchors.
    # Runs after the guard so we never re-fill a date that was just nulled out.
    _fill_timeline_dates(flat_data)

    # 11c. Derive transparency_metrics fields from dated disclosure/notification timeline events.
    # Runs after 11b so timeline events have their best-available dates before we scan them.
    _fill_transparency_from_timeline(flat_data)

    # 12b. law_enforcement_involved: infer from agencies array or enriched_summary
    # when LLM left the boolean null despite mentioning an agency.
    if flat_data.get("law_enforcement_involved") is None:
        _agencies = flat_data.get("law_enforcement_agencies") or []
        if isinstance(_agencies, str):
            try:
                import json as _json
                _agencies = _json.loads(_agencies)
            except Exception:
                _agencies = [_agencies]
        _le_keywords = {"fbi", "cisa", "police", "interpol", "nca", "europol", "ncsc",
                        "secret service", "law enforcement", "federal authorities",
                        "bka", "afp", "rcmp", "anssi", "investigators", "authorities",
                        "gendarmerie", "carabinieri", "guardia civil"}
        _agencies_lower = " ".join(str(a).lower() for a in _agencies)
        _summary_lower = (_summary or "").lower()
        if (_agencies_lower and any(kw in _agencies_lower for kw in _le_keywords)) or \
           any(kw in _summary_lower for kw in _le_keywords):
            flat_data["law_enforcement_involved"] = True

    # 12. Propagate records_affected_exact → students_affected when data_categories
    # confirms student data was involved and user_impact is otherwise null.
    if flat_data.get("students_affected") is None:
        _cats = flat_data.get("data_categories") or ""
        _cats_str = _cats if isinstance(_cats, str) else json.dumps(_cats)
        if any(kw in _cats_str.lower() for kw in ("student", "pupil", "ferpa", "transcript", "grade")):
            _n = flat_data.get("records_affected_exact")
            if _n:
                flat_data["students_affected"] = _n
