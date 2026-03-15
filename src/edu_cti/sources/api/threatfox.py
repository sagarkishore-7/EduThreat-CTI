"""
Abuse.ch ThreatFox source for EduThreat-CTI.

ThreatFox is a free IOC (Indicator of Compromise) sharing platform run by
abuse.ch. It provides community-sourced malware IOCs with threat context.

We use the public JSON export endpoint (no API key required) to fetch recent
IOCs and correlate malware families seen targeting education institutions.

Export endpoint: https://threatfox.abuse.ch/export/json/recent/
Cost: FREE (no API key needed)
Coverage: Community-contributed IOCs (domains, IPs, URLs, hashes)
"""

import logging
from datetime import datetime
from typing import Callable, Dict, List, Optional

import requests

from src.edu_cti.core.models import BaseIncident, make_incident_id

logger = logging.getLogger(__name__)

SOURCE_NAME = "threatfox"
EXPORT_RECENT_URL = "https://threatfox.abuse.ch/export/json/recent/"
EXPORT_FULL_URL = "https://threatfox.abuse.ch/export/json/full/"

# Malware families commonly seen targeting education
EDU_MALWARE_FAMILIES = [
    "lockbit", "clop", "blackcat", "alphv", "royal", "vice society",
    "hive", "conti", "ryuk", "maze", "pysa", "mespinoza", "medusa",
    "akira", "rhysida", "play", "bianlian", "blackbasta", "black basta",
    "emotet", "trickbot", "qakbot", "icedid", "cobalt strike",
]

# Keywords that indicate education sector targeting
EDU_KEYWORDS = [
    "university", "college", "school", "education", "academic",
    "campus", ".edu", "student", "faculty",
]


def _fetch_recent_export() -> Optional[Dict]:
    """Fetch recent IOCs from ThreatFox JSON export."""
    try:
        resp = requests.get(EXPORT_RECENT_URL, timeout=60, headers={
            "User-Agent": "EduThreat-CTI/2.0 (education-sector-research)",
        })
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            return data
        logger.warning("ThreatFox recent export returned unexpected format")
        return None
    except requests.RequestException as e:
        logger.error(f"Failed to fetch ThreatFox recent export: {e}")
        return None
    except ValueError as e:
        logger.error(f"Failed to parse ThreatFox JSON: {e}")
        return None


def _fetch_full_export() -> Optional[Dict]:
    """Fetch full IOC export from ThreatFox (ZIP containing JSON)."""
    import io
    import zipfile

    try:
        resp = requests.get(EXPORT_FULL_URL, timeout=180, headers={
            "User-Agent": "EduThreat-CTI/2.0 (education-sector-research)",
        })
        resp.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            # Find the JSON file inside the ZIP
            json_files = [f for f in zf.namelist() if f.endswith(".json")]
            if not json_files:
                logger.error("ThreatFox full export ZIP contains no JSON files")
                return None

            import json
            with zf.open(json_files[0]) as jf:
                data = json.load(jf)
                if isinstance(data, dict):
                    return data
                logger.warning("ThreatFox full export JSON has unexpected format")
                return None

    except requests.RequestException as e:
        logger.error(f"Failed to fetch ThreatFox full export: {e}")
        return None
    except (zipfile.BadZipFile, ValueError) as e:
        logger.error(f"Failed to parse ThreatFox full export: {e}")
        return None


def build_threatfox_incidents(
    *,
    max_pages: Optional[int] = None,
    client=None,
    save_callback: Optional[Callable] = None,
    incremental: bool = True,
    start_year: int = 2019,
) -> List[BaseIncident]:
    """
    Fetch recent IOCs from ThreatFox and extract education-relevant entries.

    Uses the public JSON export (no auth needed). Filters for:
    1. IOCs with education-related tags or references
    2. Malware families known to target education sector

    Args:
        max_pages: Not used (single export)
        client: Not used
        save_callback: Callback for incremental saving
        incremental: Not used (always fetches recent)
        start_year: Earliest year to include

    Returns:
        List of BaseIncident objects
    """
    # Use full export for historical, recent for incremental
    if not incremental:
        logger.info("Fetching full IOC export from ThreatFox (historical)...")
        data = _fetch_full_export()
    else:
        logger.info("Fetching recent IOCs from ThreatFox...")
        data = _fetch_recent_export()
    if not data:
        return []

    incidents: List[BaseIncident] = []
    seen_ids: set = set()

    # data is a dict of {ioc_id: [ioc_entries]}
    for ioc_id, entries in data.items():
        if not isinstance(entries, list):
            continue

        for entry in entries:
            malware = (entry.get("malware") or "").lower()
            malware_printable = entry.get("malware_printable") or ""
            tags = (entry.get("tags") or "").lower()
            reference = entry.get("reference") or ""
            ioc_value = entry.get("ioc_value") or ""
            ioc_type = entry.get("ioc_type") or ""
            first_seen = entry.get("first_seen_utc") or ""
            confidence = entry.get("confidence_level", 0)

            # Filter: education-related IOCs or known edu-targeting malware
            combined = f"{malware} {tags} {reference} {ioc_value}".lower()
            is_edu_relevant = any(kw in combined for kw in EDU_KEYWORDS)
            is_edu_malware = any(mf in malware for mf in EDU_MALWARE_FAMILIES)

            if not is_edu_relevant and not is_edu_malware:
                continue

            # Date filter
            pub_date = first_seen[:10] if first_seen and len(first_seen) >= 10 else None
            if pub_date:
                try:
                    year = int(pub_date[:4])
                    if year < start_year:
                        continue
                except (ValueError, IndexError):
                    pass

            # Dedup within this fetch
            dedup_key = f"{ioc_type}:{ioc_value}"
            if dedup_key in seen_ids:
                continue
            seen_ids.add(dedup_key)

            source_event_id = str(ioc_id)
            incident_id = make_incident_id(SOURCE_NAME, source_event_id)

            title = f"ThreatFox IOC: {malware_printable} ({ioc_type})"
            subtitle = f"IOC: {ioc_value[:100]}"

            all_urls = []
            if reference and reference.startswith("http"):
                all_urls.append(reference)
            all_urls.append(f"https://threatfox.abuse.ch/ioc/{ioc_id}/")

            # Map confidence
            if confidence >= 75:
                src_confidence = "high"
            elif confidence >= 50:
                src_confidence = "medium"
            else:
                src_confidence = "low"

            incident = BaseIncident(
                incident_id=incident_id,
                source=SOURCE_NAME,
                source_event_id=source_event_id,
                university_name=title[:200],
                victim_raw_name=None,
                institution_type=None,
                country=None,
                region=None,
                city=None,
                incident_date=pub_date,
                date_precision="day" if pub_date else "unknown",
                source_published_date=pub_date,
                ingested_at=datetime.utcnow().isoformat(),
                title=title[:200],
                subtitle=subtitle[:200],
                primary_url=None,
                all_urls=all_urls[:10],
                attack_type_hint=_classify_threat_type(entry),
                status="suspected",
                source_confidence=src_confidence,
                notes=f"malware={malware_printable};ioc_type={ioc_type};tags={tags[:100]}",
            )

            incidents.append(incident)

            if save_callback:
                save_callback([incident])

    logger.info(f"ThreatFox: Found {len(incidents)} education-relevant IOCs from {len(data)} total")
    return incidents


def _classify_threat_type(entry: Dict) -> Optional[str]:
    """Classify threat type from ThreatFox entry."""
    threat_type = (entry.get("threat_type") or "").lower()
    malware = (entry.get("malware") or "").lower()

    if "ransomware" in malware or "ransom" in malware:
        return "ransomware"
    if "botnet" in threat_type:
        return "botnet"
    if "c2" in threat_type or "command" in threat_type:
        return "command_and_control"
    if "payload" in threat_type:
        return "malware"
    if "stealer" in malware or "infostealer" in malware:
        return "data_theft"
    return "malware"
