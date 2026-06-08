"""
Country code to name mapping and normalization utilities.

Provides ISO 3166-1 alpha-2 to full country name mapping,
and utilities to normalize country data in the database.
"""

from typing import Optional

# ISO 3166-1 alpha-2 to full country name mapping
COUNTRY_CODE_TO_NAME = {
    "US": "United States",
    "GB": "United Kingdom",
    "CA": "Canada",
    "AU": "Australia",
    "DE": "Germany",
    "FR": "France",
    "IT": "Italy",
    "ES": "Spain",
    "NL": "Netherlands",
    "BE": "Belgium",
    "CH": "Switzerland",
    "AT": "Austria",
    "SE": "Sweden",
    "NO": "Norway",
    "DK": "Denmark",
    "FI": "Finland",
    "PL": "Poland",
    "CZ": "Czech Republic",
    "IE": "Ireland",
    "PT": "Portugal",
    "GR": "Greece",
    "HU": "Hungary",
    "RO": "Romania",
    "BG": "Bulgaria",
    "HR": "Croatia",
    "SK": "Slovakia",
    "SI": "Slovenia",
    "LT": "Lithuania",
    "LV": "Latvia",
    "EE": "Estonia",
    "LU": "Luxembourg",
    "LI": "Liechtenstein",
    "MT": "Malta",
    "CY": "Cyprus",
    "IS": "Iceland",
    "JP": "Japan",
    "CN": "China",
    "IN": "India",
    "KR": "South Korea",
    "SG": "Singapore",
    "MY": "Malaysia",
    "TH": "Thailand",
    "PH": "Philippines",
    "ID": "Indonesia",
    "VN": "Vietnam",
    "NZ": "New Zealand",
    "BR": "Brazil",
    "MX": "Mexico",
    "AR": "Argentina",
    "BO": "Bolivia",
    "CL": "Chile",
    "CO": "Colombia",
    "CR": "Costa Rica",
    "EC": "Ecuador",
    "PE": "Peru",
    "UY": "Uruguay",
    "SV": "El Salvador",
    "JM": "Jamaica",
    "PR": "Puerto Rico",
    "BM": "Bermuda",
    "BA": "Bosnia and Herzegovina",
    "DZ": "Algeria",
    "LY": "Libya",
    "MA": "Morocco",
    "TN": "Tunisia",
    "ZA": "South Africa",
    "EG": "Egypt",
    "ET": "Ethiopia",
    "GH": "Ghana",
    "NG": "Nigeria",
    "KE": "Kenya",
    "TZ": "Tanzania",
    "UG": "Uganda",
    "ZW": "Zimbabwe",
    "IL": "Israel",
    "AE": "United Arab Emirates",
    "BH": "Bahrain",
    "JO": "Jordan",
    "KW": "Kuwait",
    "LB": "Lebanon",
    "OM": "Oman",
    "PS": "Palestine",
    "QA": "Qatar",
    "SA": "Saudi Arabia",
    "LK": "Sri Lanka",
    "TR": "Turkey",
    "RU": "Russia",
    "UA": "Ukraine",
    "PK": "Pakistan",
    "BD": "Bangladesh",
    "TW": "Taiwan",
    "HK": "Hong Kong",
    "KZ": "Kazakhstan",
    "IR": "Iran",
    "CU": "Cuba",
    "SY": "Syria",
    "SD": "Sudan",
    "PY": "Paraguay",
    "KG": "Kyrgyzstan",
    "GG": "Guernsey",
    "DO": "Dominican Republic",
    "BY": "Belarus",
}

# Reverse mapping: full name to code (for flag lookup)
COUNTRY_NAME_TO_CODE = {v: k for k, v in COUNTRY_CODE_TO_NAME.items()}

# Common variations and aliases
COUNTRY_ALIASES = {
    "United States of America": "United States",
    "USA": "United States",
    "U.S.A.": "United States",
    "U.S.": "United States",
    "U.S.A": "United States",
    "US of America": "United States",
    "America": "United States",
    "The United States": "United States",
    "United States Of America": "United States",
    "UK": "United Kingdom",
    "U.K.": "United Kingdom",
    "Great Britain": "United Kingdom",
    "Britain": "United Kingdom",
    "England": "United Kingdom",
    "Scotland": "United Kingdom",
    "Wales": "United Kingdom",
    "Northern Ireland": "United Kingdom",
    "Bosnia": "Bosnia and Herzegovina",
    "Bosnia & Herzegovina": "Bosnia and Herzegovina",
}


def normalize_country(country: Optional[str]) -> Optional[str]:
    """
    Normalize country code or name to full country name.
    
    Args:
        country: Country code (e.g., "US"), full name (e.g., "United States"), 
                or alias (e.g., "USA")
    
    Returns:
        Full country name (e.g., "United States") or None if not found
    """
    if not country:
        return None
    
    country = country.strip()
    if not country:
        return None
    
    # Check if it's already a full name
    if country in COUNTRY_NAME_TO_CODE:
        return country
    
    # Check aliases first
    if country in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[country]
    
    # Check if it's a country code (case-insensitive)
    country_upper = country.upper()
    if country_upper in COUNTRY_CODE_TO_NAME:
        return COUNTRY_CODE_TO_NAME[country_upper]
    
    # Try case-insensitive match for full names
    for code, name in COUNTRY_CODE_TO_NAME.items():
        if name.lower() == country.lower():
            return name
    
    # If not found, return as-is (might be a valid name we don't have in mapping)
    # This allows for countries not in our mapping to pass through
    return country


def get_country_code(country_name: str) -> Optional[str]:
    """
    Get ISO 3166-1 alpha-2 country code from country name.
    
    Args:
        country_name: Full country name (e.g., "United States")
    
    Returns:
        Country code (e.g., "US") or None if not found
    """
    if not country_name:
        return None
    
    country_name = country_name.strip()
    
    # Normalize first
    normalized = normalize_country(country_name)
    if not normalized:
        return None
    
    return COUNTRY_NAME_TO_CODE.get(normalized)


def get_flag_emoji(country_name: str) -> str:
    """
    Get flag emoji for a country name.
    
    Uses country code to generate flag emoji.
    Returns globe emoji if country not found.
    
    Args:
        country_name: Full country name or country code
    
    Returns:
        Flag emoji string or "🌍" if not found
    """
    if not country_name:
        return "🌍"
    
    # Get country code
    code = get_country_code(country_name)
    if not code:
        # Try direct lookup if it's already a code
        code = country_name.upper() if len(country_name) == 2 else None
        if code not in COUNTRY_CODE_TO_NAME:
            return "🌍"
    
    # Convert country code to flag emoji
    # Flag emojis are constructed from regional indicator symbols
    # Each letter is offset by 0x1F1E6 (A) to get the flag symbol
    try:
        code_point_a = 0x1F1E6  # Regional Indicator Symbol Letter A
        flag_emoji = "".join(
            chr(code_point_a + ord(char) - ord("A"))
            for char in code
        )
        return flag_emoji
    except Exception:
        return "🌍"


def normalize_countries_in_database(conn) -> int:
    """
    Normalize all country codes to full names in the database.
    
    Updates both incidents and incident_enrichments_flat tables.
    Also sets country_code (ISO 3166-1 alpha-2) for CTI reports.
    
    Args:
        conn: Database connection
    
    Returns:
        Number of rows updated
    """
    updated_count = 0

    incident_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(incidents)").fetchall()
    }
    flat_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(incident_enrichments_flat)").fetchall()
    }
    incidents_has_country_code = "country_code" in incident_columns
    flat_has_country_code = "country_code" in flat_columns
    
    # Update incidents table
    cur = conn.execute("SELECT DISTINCT country FROM incidents WHERE country IS NOT NULL")
    countries = [row[0] for row in cur.fetchall()]
    
    for country in countries:
        normalized = normalize_country(country)
        country_code = get_country_code(normalized) if normalized else None
        
        if normalized and normalized != country:
            if country_code and incidents_has_country_code:
                cur = conn.execute(
                    "UPDATE incidents SET country = ?, country_code = ? WHERE country = ?",
                    (normalized, country_code, country)
                )
            else:
                cur = conn.execute(
                    "UPDATE incidents SET country = ? WHERE country = ?",
                    (normalized, country)
                )
            updated_count += cur.rowcount
        elif normalized and country_code and incidents_has_country_code:
            # Country name is already normalized, but country_code might be missing
            cur = conn.execute(
                "UPDATE incidents SET country_code = ? WHERE country = ? AND (country_code IS NULL OR country_code = '')",
                (country_code, normalized)
            )
            updated_count += cur.rowcount
    
    # Update incident_enrichments_flat table
    cur = conn.execute(
        "SELECT DISTINCT country FROM incident_enrichments_flat WHERE country IS NOT NULL"
    )
    countries = [row[0] for row in cur.fetchall()]
    
    for country in countries:
        normalized = normalize_country(country)
        country_code = get_country_code(normalized) if normalized else None
        
        if normalized and normalized != country:
            if country_code and flat_has_country_code:
                cur = conn.execute(
                    "UPDATE incident_enrichments_flat SET country = ?, country_code = ? WHERE country = ?",
                    (normalized, country_code, country)
                )
            else:
                cur = conn.execute(
                    "UPDATE incident_enrichments_flat SET country = ? WHERE country = ?",
                    (normalized, country)
                )
            updated_count += cur.rowcount
        elif normalized and country_code and flat_has_country_code:
            # Country name is already normalized, but country_code might be missing
            cur = conn.execute(
                "UPDATE incident_enrichments_flat SET country_code = ? WHERE country = ? AND (country_code IS NULL OR country_code = '')",
                (country_code, normalized)
            )
            updated_count += cur.rowcount
    
    conn.commit()
    return updated_count


# ─────────────────────────────────────────────────────────────────────────────
# Region + flag derivation (keyed by ISO alpha-2 code — the reliable, normalized
# value we store). The frontend should consume these directly rather than guess
# from free-text country names. Keep this in sync with COUNTRY_CODE_TO_NAME so a
# new country never falls into an "Other" bucket or shows a missing flag.
# ─────────────────────────────────────────────────────────────────────────────

COUNTRY_CODE_TO_REGION = {
    # North America
    "US": "North America", "CA": "North America", "MX": "North America",
    "BM": "North America", "PR": "North America",
    # Latin America & Caribbean
    "BR": "Latin America", "AR": "Latin America", "BO": "Latin America",
    "CL": "Latin America", "CO": "Latin America", "EC": "Latin America",
    "PE": "Latin America", "UY": "Latin America", "PY": "Latin America",
    "CR": "Latin America", "SV": "Latin America", "JM": "Latin America",
    "CU": "Latin America", "DO": "Latin America",
    # Europe
    "GB": "Europe", "DE": "Europe", "FR": "Europe", "IT": "Europe",
    "ES": "Europe", "NL": "Europe", "BE": "Europe", "CH": "Europe",
    "AT": "Europe", "SE": "Europe", "NO": "Europe", "DK": "Europe",
    "FI": "Europe", "PL": "Europe", "CZ": "Europe", "IE": "Europe",
    "PT": "Europe", "GR": "Europe", "HU": "Europe", "RO": "Europe",
    "BG": "Europe", "HR": "Europe", "SK": "Europe", "SI": "Europe",
    "LT": "Europe", "LV": "Europe", "EE": "Europe", "LU": "Europe",
    "LI": "Europe", "MT": "Europe", "CY": "Europe", "IS": "Europe",
    "BA": "Europe", "GG": "Europe", "BY": "Europe", "RU": "Europe",
    "UA": "Europe",
    # Asia Pacific
    "AU": "Asia Pacific", "NZ": "Asia Pacific", "JP": "Asia Pacific",
    "CN": "Asia Pacific", "KR": "Asia Pacific", "SG": "Asia Pacific",
    "HK": "Asia Pacific", "TW": "Asia Pacific", "IN": "Asia Pacific",
    "PH": "Asia Pacific", "MY": "Asia Pacific", "TH": "Asia Pacific",
    "ID": "Asia Pacific", "VN": "Asia Pacific", "BD": "Asia Pacific",
    "PK": "Asia Pacific", "LK": "Asia Pacific", "KZ": "Asia Pacific",
    "KG": "Asia Pacific",
    # Middle East
    "IL": "Middle East", "AE": "Middle East", "SA": "Middle East",
    "BH": "Middle East", "JO": "Middle East", "KW": "Middle East",
    "LB": "Middle East", "OM": "Middle East", "PS": "Middle East",
    "QA": "Middle East", "TR": "Middle East", "IR": "Middle East",
    "SY": "Middle East",
    # Africa
    "ZA": "Africa", "EG": "Africa", "NG": "Africa", "KE": "Africa",
    "GH": "Africa", "ET": "Africa", "TZ": "Africa", "UG": "Africa",
    "ZW": "Africa", "DZ": "Africa", "LY": "Africa", "MA": "Africa",
    "TN": "Africa", "SD": "Africa",
}


def get_region_for_code(country_code: Optional[str]) -> Optional[str]:
    """Return the macro-region for an ISO alpha-2 country code, or None."""
    if not country_code:
        return None
    return COUNTRY_CODE_TO_REGION.get(country_code.strip().upper())


def get_region(country: Optional[str]) -> Optional[str]:
    """Return the macro-region for a country name or code (best effort)."""
    if not country:
        return None
    code = country.strip().upper()
    if code in COUNTRY_CODE_TO_REGION:
        return COUNTRY_CODE_TO_REGION[code]
    derived_code = get_country_code(country)
    return get_region_for_code(derived_code)


def get_flag_emoji_for_code(country_code: Optional[str]) -> Optional[str]:
    """Return the Unicode regional-indicator flag for an ISO alpha-2 code.

    Pure function of the code (each letter -> regional indicator symbol), so it
    works for *every* valid country code with no maintenance. Distinct from
    :func:`get_flag_emoji`, which accepts a country *name*; the usual cause of a
    missing flag is passing a name instead of the stored ``country_code``.
    """
    if not country_code:
        return None
    code = country_code.strip().upper()
    if len(code) != 2 or not code.isalpha():
        return None
    return "".join(chr(0x1F1E6 + (ord(ch) - ord("A"))) for ch in code)


# ─────────────────────────────────────────────────────────────────────────────
# Complete ISO 3166-1 alpha-2 reference: every code -> (display name, region).
# Merged into the maps above so a NEW country never appears without a name,
# falls into an "Other" region, or shows a missing flag. Keep this comprehensive
# rather than only the countries seen so far in the corpus.
# ─────────────────────────────────────────────────────────────────────────────

_ISO_COUNTRY_REFERENCE = {
    # code: (name, region)
    "AD": ("Andorra", "Europe"), "AE": ("United Arab Emirates", "Middle East"),
    "AF": ("Afghanistan", "Asia Pacific"), "AG": ("Antigua and Barbuda", "Latin America"),
    "AI": ("Anguilla", "Latin America"), "AL": ("Albania", "Europe"),
    "AM": ("Armenia", "Asia Pacific"), "AO": ("Angola", "Africa"),
    "AR": ("Argentina", "Latin America"), "AT": ("Austria", "Europe"),
    "AU": ("Australia", "Asia Pacific"), "AW": ("Aruba", "Latin America"),
    "AZ": ("Azerbaijan", "Asia Pacific"), "BA": ("Bosnia and Herzegovina", "Europe"),
    "BB": ("Barbados", "Latin America"), "BD": ("Bangladesh", "Asia Pacific"),
    "BE": ("Belgium", "Europe"), "BF": ("Burkina Faso", "Africa"),
    "BG": ("Bulgaria", "Europe"), "BH": ("Bahrain", "Middle East"),
    "BI": ("Burundi", "Africa"), "BJ": ("Benin", "Africa"),
    "BM": ("Bermuda", "North America"), "BN": ("Brunei", "Asia Pacific"),
    "BO": ("Bolivia", "Latin America"), "BR": ("Brazil", "Latin America"),
    "BS": ("Bahamas", "Latin America"), "BT": ("Bhutan", "Asia Pacific"),
    "BW": ("Botswana", "Africa"), "BY": ("Belarus", "Europe"),
    "BZ": ("Belize", "Latin America"), "CA": ("Canada", "North America"),
    "CD": ("DR Congo", "Africa"), "CF": ("Central African Republic", "Africa"),
    "CG": ("Republic of the Congo", "Africa"), "CH": ("Switzerland", "Europe"),
    "CI": ("Ivory Coast", "Africa"), "CL": ("Chile", "Latin America"),
    "CM": ("Cameroon", "Africa"), "CN": ("China", "Asia Pacific"),
    "CO": ("Colombia", "Latin America"), "CR": ("Costa Rica", "Latin America"),
    "CU": ("Cuba", "Latin America"), "CV": ("Cape Verde", "Africa"),
    "CY": ("Cyprus", "Europe"), "CZ": ("Czech Republic", "Europe"),
    "DE": ("Germany", "Europe"), "DJ": ("Djibouti", "Africa"),
    "DK": ("Denmark", "Europe"), "DM": ("Dominica", "Latin America"),
    "DO": ("Dominican Republic", "Latin America"), "DZ": ("Algeria", "Africa"),
    "EC": ("Ecuador", "Latin America"), "EE": ("Estonia", "Europe"),
    "EG": ("Egypt", "Africa"), "ER": ("Eritrea", "Africa"),
    "ES": ("Spain", "Europe"), "ET": ("Ethiopia", "Africa"),
    "FI": ("Finland", "Europe"), "FJ": ("Fiji", "Asia Pacific"),
    "FM": ("Micronesia", "Asia Pacific"), "FO": ("Faroe Islands", "Europe"),
    "FR": ("France", "Europe"), "GA": ("Gabon", "Africa"),
    "GB": ("United Kingdom", "Europe"), "GD": ("Grenada", "Latin America"),
    "GE": ("Georgia", "Asia Pacific"), "GF": ("French Guiana", "Latin America"),
    "GG": ("Guernsey", "Europe"), "GH": ("Ghana", "Africa"),
    "GI": ("Gibraltar", "Europe"), "GL": ("Greenland", "North America"),
    "GM": ("Gambia", "Africa"), "GN": ("Guinea", "Africa"),
    "GP": ("Guadeloupe", "Latin America"), "GQ": ("Equatorial Guinea", "Africa"),
    "GR": ("Greece", "Europe"), "GT": ("Guatemala", "Latin America"),
    "GU": ("Guam", "Asia Pacific"), "GW": ("Guinea-Bissau", "Africa"),
    "GY": ("Guyana", "Latin America"), "HK": ("Hong Kong", "Asia Pacific"),
    "HN": ("Honduras", "Latin America"), "HR": ("Croatia", "Europe"),
    "HT": ("Haiti", "Latin America"), "HU": ("Hungary", "Europe"),
    "ID": ("Indonesia", "Asia Pacific"), "IE": ("Ireland", "Europe"),
    "IL": ("Israel", "Middle East"), "IM": ("Isle of Man", "Europe"),
    "IN": ("India", "Asia Pacific"), "IQ": ("Iraq", "Middle East"),
    "IR": ("Iran", "Middle East"), "IS": ("Iceland", "Europe"),
    "IT": ("Italy", "Europe"), "JE": ("Jersey", "Europe"),
    "JM": ("Jamaica", "Latin America"), "JO": ("Jordan", "Middle East"),
    "JP": ("Japan", "Asia Pacific"), "KE": ("Kenya", "Africa"),
    "KG": ("Kyrgyzstan", "Asia Pacific"), "KH": ("Cambodia", "Asia Pacific"),
    "KI": ("Kiribati", "Asia Pacific"), "KM": ("Comoros", "Africa"),
    "KN": ("Saint Kitts and Nevis", "Latin America"), "KP": ("North Korea", "Asia Pacific"),
    "KR": ("South Korea", "Asia Pacific"), "KW": ("Kuwait", "Middle East"),
    "KY": ("Cayman Islands", "Latin America"), "KZ": ("Kazakhstan", "Asia Pacific"),
    "LA": ("Laos", "Asia Pacific"), "LB": ("Lebanon", "Middle East"),
    "LC": ("Saint Lucia", "Latin America"), "LI": ("Liechtenstein", "Europe"),
    "LK": ("Sri Lanka", "Asia Pacific"), "LR": ("Liberia", "Africa"),
    "LS": ("Lesotho", "Africa"), "LT": ("Lithuania", "Europe"),
    "LU": ("Luxembourg", "Europe"), "LV": ("Latvia", "Europe"),
    "LY": ("Libya", "Africa"), "MA": ("Morocco", "Africa"),
    "MC": ("Monaco", "Europe"), "MD": ("Moldova", "Europe"),
    "ME": ("Montenegro", "Europe"), "MG": ("Madagascar", "Africa"),
    "MH": ("Marshall Islands", "Asia Pacific"), "MK": ("North Macedonia", "Europe"),
    "ML": ("Mali", "Africa"), "MM": ("Myanmar", "Asia Pacific"),
    "MN": ("Mongolia", "Asia Pacific"), "MO": ("Macau", "Asia Pacific"),
    "MQ": ("Martinique", "Latin America"), "MR": ("Mauritania", "Africa"),
    "MT": ("Malta", "Europe"), "MU": ("Mauritius", "Africa"),
    "MV": ("Maldives", "Asia Pacific"), "MW": ("Malawi", "Africa"),
    "MX": ("Mexico", "North America"), "MY": ("Malaysia", "Asia Pacific"),
    "MZ": ("Mozambique", "Africa"), "NA": ("Namibia", "Africa"),
    "NC": ("New Caledonia", "Asia Pacific"), "NE": ("Niger", "Africa"),
    "NG": ("Nigeria", "Africa"), "NI": ("Nicaragua", "Latin America"),
    "NL": ("Netherlands", "Europe"), "NO": ("Norway", "Europe"),
    "NP": ("Nepal", "Asia Pacific"), "NZ": ("New Zealand", "Asia Pacific"),
    "OM": ("Oman", "Middle East"), "PA": ("Panama", "Latin America"),
    "PE": ("Peru", "Latin America"), "PF": ("French Polynesia", "Asia Pacific"),
    "PG": ("Papua New Guinea", "Asia Pacific"), "PH": ("Philippines", "Asia Pacific"),
    "PK": ("Pakistan", "Asia Pacific"), "PL": ("Poland", "Europe"),
    "PR": ("Puerto Rico", "North America"), "PS": ("Palestine", "Middle East"),
    "PT": ("Portugal", "Europe"), "PW": ("Palau", "Asia Pacific"),
    "PY": ("Paraguay", "Latin America"), "QA": ("Qatar", "Middle East"),
    "RE": ("Reunion", "Africa"), "RO": ("Romania", "Europe"),
    "RS": ("Serbia", "Europe"), "RU": ("Russia", "Europe"),
    "RW": ("Rwanda", "Africa"), "SA": ("Saudi Arabia", "Middle East"),
    "SB": ("Solomon Islands", "Asia Pacific"), "SC": ("Seychelles", "Africa"),
    "SD": ("Sudan", "Africa"), "SE": ("Sweden", "Europe"),
    "SG": ("Singapore", "Asia Pacific"), "SI": ("Slovenia", "Europe"),
    "SK": ("Slovakia", "Europe"), "SL": ("Sierra Leone", "Africa"),
    "SM": ("San Marino", "Europe"), "SN": ("Senegal", "Africa"),
    "SO": ("Somalia", "Africa"), "SR": ("Suriname", "Latin America"),
    "SS": ("South Sudan", "Africa"), "SV": ("El Salvador", "Latin America"),
    "SY": ("Syria", "Middle East"), "SZ": ("Eswatini", "Africa"),
    "TC": ("Turks and Caicos Islands", "Latin America"), "TD": ("Chad", "Africa"),
    "TG": ("Togo", "Africa"), "TH": ("Thailand", "Asia Pacific"),
    "TJ": ("Tajikistan", "Asia Pacific"), "TL": ("Timor-Leste", "Asia Pacific"),
    "TM": ("Turkmenistan", "Asia Pacific"), "TN": ("Tunisia", "Africa"),
    "TO": ("Tonga", "Asia Pacific"), "TR": ("Turkey", "Middle East"),
    "TT": ("Trinidad and Tobago", "Latin America"), "TV": ("Tuvalu", "Asia Pacific"),
    "TW": ("Taiwan", "Asia Pacific"), "TZ": ("Tanzania", "Africa"),
    "UA": ("Ukraine", "Europe"), "UG": ("Uganda", "Africa"),
    "US": ("United States", "North America"), "UY": ("Uruguay", "Latin America"),
    "UZ": ("Uzbekistan", "Asia Pacific"), "VC": ("Saint Vincent and the Grenadines", "Latin America"),
    "VE": ("Venezuela", "Latin America"), "VG": ("British Virgin Islands", "Latin America"),
    "VI": ("U.S. Virgin Islands", "Latin America"), "VN": ("Vietnam", "Asia Pacific"),
    "VU": ("Vanuatu", "Asia Pacific"), "WS": ("Samoa", "Asia Pacific"),
    "XK": ("Kosovo", "Europe"), "YE": ("Yemen", "Middle East"),
    "ZA": ("South Africa", "Africa"), "ZM": ("Zambia", "Africa"),
    "ZW": ("Zimbabwe", "Africa"),
}

# Backfill the primary maps so every ISO code resolves to a name + region + flag.
for _code, (_name, _region) in _ISO_COUNTRY_REFERENCE.items():
    COUNTRY_CODE_TO_NAME.setdefault(_code, _name)
    COUNTRY_CODE_TO_REGION.setdefault(_code, _region)
# Keep the reverse name->code map in sync with the expanded set.
COUNTRY_NAME_TO_CODE = {v: k for k, v in COUNTRY_CODE_TO_NAME.items()}
