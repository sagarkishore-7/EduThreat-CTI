"""
Comparitech Ransomware Attack Map — Education sector curated source.

Data source: Comparitech maintains a comprehensive ransomware tracker
covering US organizations across all industries. The underlying data is
served as a Datawrapper CSV.

We filter for Industry="Education" and ingest each row as a BaseIncident
with structured ransomware metadata (strain, ransom amount, records
affected).  For article discovery we construct Google News RSS search
URLs so Phase 2 can fetch and enrich with full article text.

Reference: https://www.comparitech.com/ransomware-attack-map/
"""

import csv
import io
import logging
from typing import Callable, List, Optional

from src.edu_cti.core.http import HttpClient, build_http_client
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.utils import now_utc_iso, parse_date_with_precision

logger = logging.getLogger(__name__)

SOURCE_NAME = "comparitech"

# Datawrapper chart backing the Comparitech ransomware map table
DATASET_URL = "https://datawrapper.dwcdn.net/PljNz/723/dataset.csv"

# US state → abbreviation for compact display / dedup
US_STATES = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ",
    "New Mexico": "NM", "New York": "NY", "North Carolina": "NC",
    "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR",
    "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
    "District of Columbia": "DC",
}

MONTH_MAP = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


def _parse_month_year(raw: str) -> tuple:
    """Parse 'March 2020' → ('2020-03', 'month') or '2020' → ('2020', 'year')."""
    raw = raw.strip()
    parts = raw.split()
    if len(parts) == 2:
        month_name, year = parts
        mm = MONTH_MAP.get(month_name.lower())
        if mm and year.isdigit():
            return f"{year}-{mm}", "month"
    # Fallback: year only
    if raw.isdigit() and len(raw) == 4:
        return raw, "year"
    return None, "unknown"


def _guess_institution_type(name: str) -> Optional[str]:
    """Rough institution type guess from name."""
    lower = name.lower()
    if any(k in lower for k in ("university", "college", "community college")):
        return "University"
    if any(k in lower for k in (
        "school district", "school", "isd", "independent school",
        "public schools", "unified school", "k-12", "k-8",
        "high school", "middle school", "elementary",
        "academy", "charter",
    )):
        return "School"
    if any(k in lower for k in ("institute", "research", "laboratory")):
        return "Research Institute"
    return "Unknown"


def _parse_ransom_amount(raw: str) -> Optional[str]:
    """Parse ransom amount string, return cleaned value or None."""
    raw = raw.strip()
    if not raw or raw.lower() in ("unknown", "n/a", ""):
        return None
    # Remove commas, dollar signs
    cleaned = raw.replace(",", "").replace("$", "").strip()
    return cleaned if cleaned else None


def build_comparitech_incidents(
    client: Optional[HttpClient] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
) -> List[BaseIncident]:
    """
    Fetch Comparitech ransomware map CSV and extract education incidents.

    For each education incident, constructs Google News search URLs as
    all_urls so Phase 2 can fetch articles and do LLM enrichment.

    Args:
        client: Optional HTTP client
        save_callback: Optional callback to save incidents incrementally

    Returns:
        List of BaseIncident objects for education sector ransomware attacks
    """
    http_client = client or build_http_client()
    logger.info(f"Fetching Comparitech dataset from {DATASET_URL}")

    try:
        resp = http_client.get(DATASET_URL, to_soup=False)
        if resp is None or resp.status_code != 200:
            logger.error(f"Failed to fetch Comparitech CSV (status={getattr(resp, 'status_code', None)})")
            return []
        csv_text = resp.text
    except Exception as e:
        logger.error(f"Error fetching Comparitech CSV: {e}")
        return []

    reader = csv.DictReader(io.StringIO(csv_text))
    incidents: List[BaseIncident] = []
    seen_keys = set()
    now = now_utc_iso()

    # First pass: collect all education rows (fast, no network)
    edu_rows = []
    for row in reader:
        industry = (row.get("Industry") or "").strip()
        if industry != "Education":
            continue
        name = (row.get("Company Affected") or "").strip()
        if not name:
            continue
        edu_rows.append(row)

    logger.info(f"Comparitech: {len(edu_rows)} education rows found, saving immediately (article discovery via Oxylabs SERP in Phase 2)")

    for idx, row in enumerate(edu_rows, 1):
        name = (row.get("Company Affected") or "").strip()
        if not name:
            continue

        month_year_raw = (row.get("Month, Year") or "").strip()
        year_raw = (row.get("Year") or "").strip()
        city = (row.get("City/County") or "").strip()
        state = (row.get("State") or "").strip()
        records_affected = (row.get("# Records Affected") or "").strip()
        ransom_paid = (row.get("Ransom Paid") or "").strip()
        ransom_amount_raw = (row.get("Ransom Amount") or "").strip()
        ransomware_strain = (row.get("Ransomware Strain") or "").strip()

        # Parse date
        incident_date, date_precision = _parse_month_year(month_year_raw)
        if not incident_date and year_raw:
            incident_date = year_raw
            date_precision = "year"

        # Dedup key: name + date + city
        dedup_key = f"{name.lower()}|{incident_date or ''}|{city.lower()}"
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)

        # Build unique ID
        incident_id = make_incident_id(SOURCE_NAME, dedup_key)

        # Build notes with structured ransomware metadata
        notes_parts = []
        if ransomware_strain and ransomware_strain.lower() != "unknown":
            notes_parts.append(f"Ransomware: {ransomware_strain}")
        if ransom_paid and ransom_paid.lower() not in ("unknown", ""):
            notes_parts.append(f"Ransom paid: {ransom_paid}")
        ransom_amount = _parse_ransom_amount(ransom_amount_raw)
        if ransom_amount:
            notes_parts.append(f"Ransom amount: ${ransom_amount}")
        if records_affected and records_affected.lower() not in ("unknown", "0", ""):
            notes_parts.append(f"Records affected: {records_affected}")
        notes = " | ".join(notes_parts) if notes_parts else None

        # Title
        year_str = year_raw or (incident_date[:4] if incident_date else "")
        title = f"Ransomware attack on {name}"
        if year_str:
            title += f" ({year_str})"

        # Region (US state)
        region = state if state else None

        incident = BaseIncident(
            incident_id=incident_id,
            source=SOURCE_NAME,
            source_event_id=dedup_key,
            institution_name=name,
            victim_raw_name=name,
            institution_type=_guess_institution_type(name),
            country="US",
            region=region,
            city=city or None,
            incident_date=incident_date,
            date_precision=date_precision,
            source_published_date=None,
            ingested_at=now,
            title=title,
            subtitle=None,
            primary_url=None,
            all_urls=[],  # Phase 2 discovers articles via Oxylabs SERP
            leak_site_url=None,
            source_detail_url=None,  # Reference page, not an article — would cause all incidents to dedup into one
            screenshot_url=None,
            attack_type_hint="ransomware",
            status="confirmed",
            source_confidence="high",
            notes=notes,
        )
        incidents.append(incident)

        # Progress logging
        if idx % 50 == 0 or idx == len(edu_rows):
            logger.info(f"Comparitech: [{idx}/{len(edu_rows)}] {len(incidents)} incidents saved")

        # Batch save every 100 incidents
        if save_callback and len(incidents) >= 100 and len(incidents) % 100 == 0:
            save_callback(incidents[-100:])

    logger.info(f"Comparitech: found {len(incidents)} education ransomware incidents")

    # Final save of any remaining incidents
    if save_callback and incidents:
        # Save all (the pipeline deduplicates, so re-saving is safe)
        save_callback(incidents)

    return incidents
