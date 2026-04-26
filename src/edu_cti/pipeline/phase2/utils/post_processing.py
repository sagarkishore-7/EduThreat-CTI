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
    ("ransomhub", "ransomhub"),
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

    # 1. Ransomware family from summary / title keyword scan
    if not flat_data.get("ransomware_family"):
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
