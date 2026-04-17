"""
AlienVault OTX (Open Threat Exchange) source for EduThreat-CTI.

OTX provides free threat intelligence "pulses" that contain IOCs,
descriptions, and references. We search for education-related pulses
and extract IOCs for correlation with our incident data.

API: https://otx.alienvault.com/api/v1/
Cost: FREE (API key required, free registration)
Coverage: Community-contributed threat intelligence
"""

import logging
import os
import time
from datetime import datetime
from typing import Callable, Dict, List, Optional

import requests

from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.config import EDUCATION_KEYWORDS

logger = logging.getLogger(__name__)

OTX_BASE_URL = "https://otx.alienvault.com/api/v1"
SOURCE_NAME = "otx_alienvault"

# OTX API key (free registration at https://otx.alienvault.com)
OTX_API_KEY = os.getenv("OTX_API_KEY", "")


def _get_headers() -> Dict[str, str]:
    """Get API headers with optional API key."""
    headers = {"Accept": "application/json"}
    if OTX_API_KEY:
        headers["X-OTX-API-KEY"] = OTX_API_KEY
    return headers


def search_pulses(
    query: str,
    limit: int = 50,
    page: int = 1,
) -> List[Dict]:
    """
    Search OTX pulses by keyword.

    Args:
        query: Search query
        limit: Max results per page
        page: Page number

    Returns:
        List of pulse dicts
    """
    try:
        resp = requests.get(
            f"{OTX_BASE_URL}/search/pulses",
            params={"q": query, "limit": limit, "page": page},
            headers=_get_headers(),
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])
    except Exception as e:
        logger.error(f"OTX search failed for '{query}': {e}")
        return []


def get_pulse_indicators(pulse_id: str) -> List[Dict]:
    """
    Get IOC indicators from a specific pulse.

    Args:
        pulse_id: OTX pulse ID

    Returns:
        List of indicator dicts with type and value
    """
    try:
        resp = requests.get(
            f"{OTX_BASE_URL}/pulses/{pulse_id}/indicators",
            headers=_get_headers(),
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])
    except Exception as e:
        logger.debug(f"Failed to fetch indicators for pulse {pulse_id}: {e}")
        return []


def build_otx_incidents(
    *,
    max_pages: Optional[int] = 3,
    client=None,
    save_callback: Optional[Callable] = None,
    incremental: bool = True,
    start_year: int = 2019,
) -> List[BaseIncident]:
    """
    Search OTX for education-related threat intelligence pulses.

    Args:
        max_pages: Max pages to search per keyword (default: 3)
        client: Not used
        save_callback: Callback for incremental saving
        incremental: If True, limit to recent pulses
        start_year: Earliest year to include

    Returns:
        List of BaseIncident objects from OTX pulses
    """
    if not OTX_API_KEY:
        logger.warning(
            "OTX_API_KEY not set — skipping OTX source (API key now required). "
            "Get a free key at https://otx.alienvault.com"
        )
        return []

    logger.info("Searching OTX for education-sector threat intelligence...")

    # Education-specific search queries
    search_queries = [
        "university ransomware",
        "education sector cyberattack",
        "school district breach",
        "college data breach",
        "academic institution hack",
        "education ransomware",
    ]

    incidents: List[BaseIncident] = []
    seen_pulse_ids: set = set()

    for query in search_queries:
        pages_to_fetch = max_pages or 3

        for page in range(1, pages_to_fetch + 1):
            pulses = search_pulses(query, limit=20, page=page)

            if not pulses:
                break

            for pulse in pulses:
                pulse_id = pulse.get("id", "")
                if not pulse_id or pulse_id in seen_pulse_ids:
                    continue
                seen_pulse_ids.add(pulse_id)

                name = pulse.get("name", "")
                description = pulse.get("description", "")
                created = pulse.get("created", "")
                modified = pulse.get("modified", "")
                references = pulse.get("references", []) or []
                tags = pulse.get("tags", []) or []
                adversary = pulse.get("adversary", "")
                indicator_count = pulse.get("indicator_count", 0)

                # Date filter
                pub_date = created[:10] if created and len(created) >= 10 else None
                if pub_date:
                    try:
                        year = int(pub_date[:4])
                        if year < start_year:
                            continue
                    except (ValueError, IndexError):
                        pass

                # Build incident
                source_event_id = pulse_id
                incident_id = make_incident_id(SOURCE_NAME, source_event_id)

                # Build URLs from references
                all_urls = [ref for ref in references if ref and ref.startswith("http")]
                otx_url = f"https://otx.alienvault.com/pulse/{pulse_id}"
                all_urls.append(otx_url)

                incident = BaseIncident(
                    incident_id=incident_id,
                    source=SOURCE_NAME,
                    source_event_id=source_event_id,
                    institution_name=name[:200] if name else "Unknown",
                    victim_raw_name=None,
                    institution_type=None,
                    country=None,  # OTX pulses are global
                    region=None,
                    city=None,
                    incident_date=pub_date,
                    date_precision="day" if pub_date else "unknown",
                    source_published_date=pub_date,
                    ingested_at=datetime.utcnow().isoformat(),
                    title=name[:200] if name else "OTX Pulse",
                    subtitle=description[:200] if description else None,
                    primary_url=None,
                    all_urls=all_urls[:10],  # Cap URLs
                    attack_type_hint=None,
                    status="suspected",
                    source_confidence="medium",
                    notes=f"tags={','.join(tags[:10])};indicators={indicator_count};adversary={adversary}" if tags or adversary else None,
                )

                incidents.append(incident)

                if save_callback:
                    save_callback([incident])

            # Rate limiting
            time.sleep(1.0)

    logger.info(f"OTX: Found {len(incidents)} education-related pulses")
    return incidents


def enrich_ioc_from_otx(ioc_type: str, ioc_value: str) -> Optional[Dict]:
    """
    Look up a single IOC in OTX for reputation/context.

    Useful for enriching IOCs extracted from incidents.

    Args:
        ioc_type: One of 'IPv4', 'domain', 'hostname', 'url', 'FileHash-MD5',
                  'FileHash-SHA1', 'FileHash-SHA256', 'CVE'
        ioc_value: The IOC value

    Returns:
        Dict with OTX enrichment data, or None
    """
    type_map = {
        "ipv4": "IPv4",
        "domain": "domain",
        "url": "url",
        "md5": "file",
        "sha1": "file",
        "sha256": "file",
        "cve": "cve",
    }

    otx_type = type_map.get(ioc_type.lower())
    if not otx_type:
        return None

    # Build the appropriate endpoint
    if otx_type == "file":
        endpoint = f"{OTX_BASE_URL}/indicators/file/{ioc_value}/general"
    elif otx_type == "cve":
        endpoint = f"{OTX_BASE_URL}/indicators/cve/{ioc_value}/general"
    else:
        endpoint = f"{OTX_BASE_URL}/indicators/{otx_type}/{ioc_value}/general"

    try:
        resp = requests.get(endpoint, headers=_get_headers(), timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            pulse_count = len(data.get("pulse_info", {}).get("pulses", []))
            return {
                "source": "otx",
                "pulse_count": pulse_count,
                "reputation": data.get("reputation", 0),
                "tags": data.get("pulse_info", {}).get("related_tags", []),
                "country": data.get("country_name"),
            }
    except Exception as e:
        logger.debug(f"OTX IOC lookup failed for {ioc_type}:{ioc_value}: {e}")

    return None
