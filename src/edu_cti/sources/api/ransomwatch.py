"""
RansomLook API source for EduThreat-CTI.

RansomLook (https://www.ransomlook.io) is the active successor to the archived
ransomware.live project (archived March 2026). It monitors ransomware group
leak sites and provides a free JSON API.

API: https://www.ransomlook.io/api/victims
Cost: FREE (no API key needed)
Coverage: Real-time ransomware victim tracking from 100+ leak sites
Historical: Data available from ~2020 onwards
"""

import logging
import time
from datetime import datetime
from typing import Callable, List, Optional

import requests

from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.config import EDUCATION_KEYWORDS
from src.edu_cti.core.countries import normalize_country, get_country_code

logger = logging.getLogger(__name__)

RANSOMLOOK_API_URLS = [
    "https://www.ransomlook.io/api/recent",
    "https://raw.githubusercontent.com/joshhighet/ransomwatch/refs/heads/main/posts.json",  # Static archive mirror
]
SOURCE_NAME = "ransomlook"

# Education-related terms for filtering victims
EDU_FILTER_TERMS = [
    "university", "college", "school", "academy", "education",
    "student", "campus", "institute", "polytechnic", "seminary",
    "lycee", "gymnasium", "faculty", "academic", "k-12", "k12",
    "unified school", "school district", "school board",
    "community college", "state university",
    # International terms
    "universit",  # catches université, università, universität, universidad
    "escuela", "colegio", "faculdade", "hochschule",
    "universidade", "università", "université",
    "大学", "学院", "学校",  # Chinese
    "대학교", "대학",  # Korean
]


def _is_education_victim(victim_name: str, description: str = "") -> bool:
    """Check if a ransomware victim appears to be an education institution."""
    combined = f"{victim_name} {description}".lower()
    return any(term in combined for term in EDU_FILTER_TERMS)


def build_ransomlook_incidents(
    *,
    max_pages: Optional[int] = None,
    client=None,
    save_callback: Optional[Callable] = None,
    incremental: bool = True,
    start_year: int = 2019,
) -> List[BaseIncident]:
    """
    Fetch education-sector ransomware victims from RansomLook API.

    Args:
        max_pages: Not used (single API call)
        client: Not used (uses requests directly)
        save_callback: Callback for incremental saving
        incremental: If True, only fetch recent victims
        start_year: Earliest year to include (default: 2019)

    Returns:
        List of BaseIncident objects for education-sector victims
    """
    logger.info("Fetching ransomware victims from RansomLook API...")

    incidents: List[BaseIncident] = []

    victims = None
    for api_url in RANSOMLOOK_API_URLS:
        try:
            resp = requests.get(
                api_url,
                timeout=60,
                headers={"Accept": "application/json"},
            )
            resp.raise_for_status()
            victims = resp.json()
            logger.info(f"RansomLook: fetched from {api_url}")
            break
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch RansomLook from {api_url}: {e}")
            continue
        except ValueError as e:
            logger.warning(f"Failed to parse RansomLook JSON from {api_url}: {e}")
            continue

    if victims is None:
        logger.error("Failed to fetch RansomLook from all endpoints")
        return incidents

    logger.info(f"RansomWatch returned {len(victims)} total victims, filtering for education...")

    edu_count = 0
    for victim in victims:
        victim_name = victim.get("post_title", "") or victim.get("victim", "") or victim.get("name", "")
        description = victim.get("description", "") or ""
        group_name = victim.get("group_name", "") or victim.get("group", "")
        discovered = victim.get("discovered", "") or victim.get("date", "")
        country = victim.get("country", "")
        website = victim.get("website", "") or victim.get("link", "")
        post_url = victim.get("post_url", "") or victim.get("url", "")

        if not victim_name:
            continue

        # Filter for education sector
        if not _is_education_victim(victim_name, description):
            continue

        # Date filtering
        if discovered:
            try:
                year = int(discovered[:4])
                if year < start_year:
                    continue
            except (ValueError, IndexError):
                pass

        edu_count += 1

        # Normalize country
        country_normalized = normalize_country(country) if country else None

        # Build URLs list
        all_urls = []
        if post_url:
            all_urls.append(post_url)
        if website and website.startswith("http"):
            all_urls.append(website)

        # Create incident
        source_event_id = f"{group_name}_{victim_name}_{discovered}".replace(" ", "_").lower()
        incident_id = make_incident_id(SOURCE_NAME, source_event_id)

        incident = BaseIncident(
            incident_id=incident_id,
            source=SOURCE_NAME,
            source_event_id=source_event_id,
            institution_name=victim_name,
            victim_raw_name=victim_name,
            institution_type=None,  # Will be classified by LLM
            country=country_normalized,
            region=None,
            city=None,
            incident_date=discovered[:10] if discovered and len(discovered) >= 10 else None,
            date_precision="day" if discovered and len(discovered) >= 10 else "unknown",
            source_published_date=discovered[:10] if discovered else None,
            ingested_at=datetime.utcnow().isoformat(),
            title=f"{victim_name} - {group_name} ransomware",
            subtitle=description[:200] if description else None,
            primary_url=None,
            all_urls=all_urls,
            leak_site_url=post_url if post_url else None,
            attack_type_hint="ransomware",
            status="confirmed",
            source_confidence="high",
            notes=f"group={group_name}" if group_name else None,
        )

        incidents.append(incident)

        if save_callback:
            save_callback([incident])

    logger.info(f"RansomWatch: Found {edu_count} education-sector victims out of {len(victims)} total")
    return incidents
