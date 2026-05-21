"""
RansomLook API source for EduThreat-CTI.

RansomLook (https://www.ransomlook.io) is the active successor to the archived
ransomware.live project (archived March 2026). It monitors ransomware group
leak sites and provides a free JSON API.

API: https://www.ransomlook.io/api/recent
Historical mirror: https://raw.githubusercontent.com/joshhighet/ransomwatch/refs/heads/main/posts.json
Cost: FREE (no API key needed)
Coverage: Real-time ransomware victim tracking from 100+ leak sites
Historical: Data available from ~2020 onwards
"""

import logging
from datetime import datetime, timezone
from typing import Callable, List, Optional
from urllib.parse import urljoin

import requests

from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.countries import normalize_country

logger = logging.getLogger(__name__)

RANSOMLOOK_BASE_URL = "https://www.ransomlook.io"
RANSOMLOOK_RECENT_API_URL = f"{RANSOMLOOK_BASE_URL}/api/recent"
RANSOMLOOK_ARCHIVE_API_URL = "https://raw.githubusercontent.com/joshhighet/ransomwatch/refs/heads/main/posts.json"
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


def _absolute_ransomlook_url(value: str) -> Optional[str]:
    """Return an absolute RansomLook URL for relative detail/screenshot paths."""
    value = (value or "").strip()
    if not value:
        return None
    if value.startswith(("http://", "https://")):
        return value
    return urljoin(RANSOMLOOK_BASE_URL + "/", value.lstrip("/"))


def _fetch_records(url: str) -> list[dict]:
    resp = requests.get(
        url,
        timeout=60,
        headers={"Accept": "application/json"},
    )
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, list):
        raise ValueError(f"Unexpected RansomLook payload type: {type(payload).__name__}")
    return [row for row in payload if isinstance(row, dict)]


def _fetch_ransomlook_records(*, incremental: bool) -> list[dict]:
    """Fetch RansomLook rows, using the archive for historical runs."""
    # /api/recent is only the latest 100 rows. Historical runs must use the
    # static archive or they silently miss old education victims.
    urls = [RANSOMLOOK_RECENT_API_URL] if incremental else [RANSOMLOOK_ARCHIVE_API_URL, RANSOMLOOK_RECENT_API_URL]
    fallback_urls = [RANSOMLOOK_ARCHIVE_API_URL] if incremental else []

    merged: dict[tuple[str, str, str], dict] = {}
    fetched_any = False
    for api_url in urls:
        try:
            records = _fetch_records(api_url)
            fetched_any = True
            logger.info("RansomLook: fetched %d records from %s", len(records), api_url)
        except requests.RequestException as e:
            logger.warning("Failed to fetch RansomLook from %s: %s", api_url, e)
            continue
        except ValueError as e:
            logger.warning("Failed to parse RansomLook JSON from %s: %s", api_url, e)
            continue

        for row in records:
            key = (
                (row.get("group_name") or row.get("group") or "").strip().lower(),
                (row.get("post_title") or row.get("victim") or row.get("name") or "").strip().lower(),
                (row.get("discovered") or row.get("date") or "").strip()[:19],
            )
            if not any(key):
                continue
            existing = merged.get(key)
            if existing is None:
                merged[key] = dict(row)
            else:
                # Prefer richer recent API fields while keeping archive-only keys.
                merged[key] = {**existing, **{k: v for k, v in row.items() if v not in (None, "")}}

    if not merged:
        for api_url in fallback_urls:
            try:
                records = _fetch_records(api_url)
                fetched_any = True
                logger.info("RansomLook: fetched %d fallback records from %s", len(records), api_url)
            except requests.RequestException as e:
                logger.warning("Failed to fetch RansomLook fallback from %s: %s", api_url, e)
                continue
            except ValueError as e:
                logger.warning("Failed to parse RansomLook fallback JSON from %s: %s", api_url, e)
                continue
            for row in records:
                key = (
                    (row.get("group_name") or row.get("group") or "").strip().lower(),
                    (row.get("post_title") or row.get("victim") or row.get("name") or "").strip().lower(),
                    (row.get("discovered") or row.get("date") or "").strip()[:19],
                )
                if any(key):
                    merged[key] = dict(row)

    if not fetched_any:
        logger.error("Failed to fetch RansomLook from all endpoints")
    return list(merged.values())


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

    victims = _fetch_ransomlook_records(incremental=incremental)
    if not victims:
        return incidents

    logger.info(f"RansomWatch returned {len(victims)} total victims, filtering for education...")

    edu_count = 0
    for victim in victims:
        victim_name = victim.get("post_title", "") or victim.get("victim", "") or victim.get("name", "")
        description = victim.get("description", "") or ""
        group_name = victim.get("group_name", "") or victim.get("group", "")
        discovered = victim.get("discovered", "") or victim.get("date", "")
        country = victim.get("country", "")
        website = victim.get("website", "")
        raw_url = victim.get("url", "") or ""
        detail_url = _absolute_ransomlook_url(victim.get("link", "") or raw_url)
        post_url = victim.get("post_url", "") or (raw_url if ".onion" in raw_url else "")
        screenshot_url = _absolute_ransomlook_url(victim.get("screen", "") or victim.get("screenshot", ""))
        magnet = victim.get("magnet")

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

        note_parts = []
        if group_name:
            note_parts.append(f"group={group_name}")
        if website:
            note_parts.append(f"victim_website={website}")
        if magnet:
            note_parts.append("magnet_available=true")

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
            discovery_date=discovered[:10] if discovered and len(discovered) >= 10 else None,
            ingested_at=datetime.now(timezone.utc).isoformat(),
            title=f"{victim_name} - {group_name} ransomware",
            subtitle=description[:200] if description else None,
            primary_url=None,
            all_urls=[],
            leak_site_url=post_url if post_url else None,
            source_detail_url=detail_url,
            screenshot_url=screenshot_url,
            attack_type_hint="ransomware",
            threat_actor=group_name or None,
            status="confirmed",
            source_confidence="high",
            notes="; ".join(note_parts) if note_parts else None,
            raw_source_payload=dict(victim),
        )

        incidents.append(incident)

        if save_callback:
            save_callback([incident])

    logger.info(f"RansomWatch: Found {edu_count} education-sector victims out of {len(victims)} total")
    return incidents
