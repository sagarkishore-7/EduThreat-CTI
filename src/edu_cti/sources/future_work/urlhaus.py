"""
Abuse.ch URLhaus source for EduThreat-CTI.

URLhaus is a project from abuse.ch that collects and shares malicious URLs
used for malware distribution. It tracks URLs hosting malware payloads,
phishing kits, and exploit kits.

We use the public JSON download endpoint (no API key required) to fetch
recent malicious URLs and filter for education-sector targets.

Download endpoint: https://urlhaus.abuse.ch/downloads/json_recent/
Cost: FREE (no API key needed)
Coverage: Community-reported malicious URLs
"""

import logging
from datetime import datetime
from typing import Callable, Dict, List, Optional

import requests

from src.edu_cti.core.models import BaseIncident, make_incident_id

logger = logging.getLogger(__name__)

SOURCE_NAME = "urlhaus"
DOWNLOAD_URL = "https://urlhaus.abuse.ch/downloads/json_recent/"

# Education-related patterns in URLs or tags
EDU_URL_PATTERNS = [
    ".edu", ".edu.", ".ac.uk", ".ac.jp", ".ac.in", ".ac.kr",
    "university", "college", "school", "campus", "student",
    "academic", "faculty", "library",
]

# Threat types commonly targeting education
EDU_THREAT_TAGS = [
    "emotet", "trickbot", "qakbot", "icedid", "cobalt",
    "ransomware", "lockbit", "phishing", "credential",
]


def build_urlhaus_incidents(
    *,
    max_pages: Optional[int] = None,
    client=None,
    save_callback: Optional[Callable] = None,
    incremental: bool = True,
    start_year: int = 2019,
) -> List[BaseIncident]:
    """
    Fetch recent malicious URLs from URLhaus and filter for education targets.

    Uses the public JSON download (no auth needed). Filters for:
    1. URLs targeting .edu domains or education institutions
    2. Malware families known to target education

    Args:
        max_pages: Not used
        client: Not used
        save_callback: Callback for incremental saving
        incremental: Not used (always fetches recent)
        start_year: Earliest year to include

    Returns:
        List of BaseIncident objects
    """
    logger.info("Fetching recent malicious URLs from URLhaus...")

    try:
        resp = requests.get(DOWNLOAD_URL, timeout=60, headers={
            "User-Agent": "EduThreat-CTI/2.0 (education-sector-research)",
        })
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch URLhaus export: {e}")
        return []
    except ValueError as e:
        logger.error(f"Failed to parse URLhaus JSON: {e}")
        return []

    if not isinstance(data, dict):
        logger.warning("URLhaus export returned unexpected format")
        return []

    incidents: List[BaseIncident] = []
    seen_urls: set = set()

    # data is a dict of {url_id: [url_entries]}
    for url_id, entries in data.items():
        if not isinstance(entries, list):
            continue

        for entry in entries:
            url = entry.get("url") or ""
            url_status = entry.get("url_status") or ""
            threat = entry.get("threat") or ""
            dateadded = entry.get("dateadded") or ""
            tags = entry.get("tags") or []
            urlhaus_link = entry.get("urlhaus_link") or ""
            reporter = entry.get("reporter") or ""

            if not url:
                continue

            # Convert tags to string for matching
            tags_str = " ".join(tags) if isinstance(tags, list) else str(tags)
            combined = f"{url} {tags_str} {threat}".lower()

            # Filter: education-related URLs or education-targeting malware
            is_edu_url = any(pat in url.lower() for pat in EDU_URL_PATTERNS)
            is_edu_threat = any(tag in combined for tag in EDU_THREAT_TAGS)

            if not is_edu_url and not is_edu_threat:
                continue

            # Date filter
            pub_date = dateadded[:10] if dateadded and len(dateadded) >= 10 else None
            if pub_date:
                try:
                    year = int(pub_date[:4])
                    if year < start_year:
                        continue
                except (ValueError, IndexError):
                    pass

            # Dedup
            if url in seen_urls:
                continue
            seen_urls.add(url)

            source_event_id = str(url_id)
            incident_id = make_incident_id(SOURCE_NAME, source_event_id)

            title = f"URLhaus: Malicious URL ({threat})"
            if is_edu_url:
                title = f"URLhaus: Education-targeted malicious URL ({threat})"

            subtitle = f"URL: {url[:150]}"

            all_urls = []
            if urlhaus_link:
                all_urls.append(urlhaus_link)

            incident = BaseIncident(
                incident_id=incident_id,
                source=SOURCE_NAME,
                source_event_id=source_event_id,
                institution_name=title[:200],
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
                attack_type_hint=_classify_url_threat(threat, tags_str),
                status="suspected",
                source_confidence="medium" if url_status == "online" else "low",
                notes=f"threat={threat};tags={tags_str[:100]};status={url_status}",
            )

            incidents.append(incident)

            if save_callback:
                save_callback([incident])

    logger.info(f"URLhaus: Found {len(incidents)} education-relevant URLs from {len(data)} total")
    return incidents


def _classify_url_threat(threat: str, tags: str) -> Optional[str]:
    """Classify threat type from URLhaus entry."""
    combined = f"{threat} {tags}".lower()

    if "ransomware" in combined or "ransom" in combined:
        return "ransomware"
    if "phishing" in combined or "credential" in combined:
        return "phishing"
    if "malware_download" in combined or "payload" in combined:
        return "malware"
    if "botnet" in combined:
        return "botnet"
    if "miner" in combined or "crypto" in combined:
        return "cryptomining"
    return "malware"
