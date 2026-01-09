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
    "CL": "Chile",
    "CO": "Colombia",
    "PE": "Peru",
    "ZA": "South Africa",
    "EG": "Egypt",
    "NG": "Nigeria",
    "KE": "Kenya",
    "IL": "Israel",
    "AE": "United Arab Emirates",
    "SA": "Saudi Arabia",
    "TR": "Turkey",
    "RU": "Russia",
    "UA": "Ukraine",
    "PK": "Pakistan",
    "BD": "Bangladesh",
    "TW": "Taiwan",
    "HK": "Hong Kong",
}

# Reverse mapping: full name to code (for flag lookup)
COUNTRY_NAME_TO_CODE = {v: k for k, v in COUNTRY_CODE_TO_NAME.items()}

# Common variations and aliases
COUNTRY_ALIASES = {
    "United States of America": "United States",
    "USA": "United States",
    "U.S.A.": "United States",
    "U.S.": "United States",
    "UK": "United Kingdom",
    "U.K.": "United Kingdom",
    "Great Britain": "United Kingdom",
    "Britain": "United Kingdom",
    "England": "United Kingdom",
    "Scotland": "United Kingdom",
    "Wales": "United Kingdom",
    "Northern Ireland": "United Kingdom",
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
    
    # Update incidents table
    cur = conn.execute("SELECT DISTINCT country FROM incidents WHERE country IS NOT NULL")
    countries = [row[0] for row in cur.fetchall()]
    
    for country in countries:
        normalized = normalize_country(country)
        country_code = get_country_code(normalized) if normalized else None
        
        if normalized and normalized != country:
            if country_code:
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
        elif normalized and country_code:
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
            if country_code:
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
        elif normalized and country_code:
            # Country name is already normalized, but country_code might be missing
            cur = conn.execute(
                "UPDATE incident_enrichments_flat SET country_code = ? WHERE country = ? AND (country_code IS NULL OR country_code = '')",
                (country_code, normalized)
            )
            updated_count += cur.rowcount
    
    conn.commit()
    return updated_count
