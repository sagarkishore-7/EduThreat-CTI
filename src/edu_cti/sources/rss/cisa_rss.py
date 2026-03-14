"""
CISA Cybersecurity Alerts & Advisories RSS source for EduThreat-CTI.

Monitors CISA's RSS feeds for cybersecurity alerts that may affect
education institutions. Filters by education keywords.

Feed: https://www.cisa.gov/cybersecurity-advisories/all.xml
Cost: FREE (US government)
Coverage: US-focused cybersecurity advisories
"""

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Callable, List, Optional

import requests

from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.config import EDUCATION_KEYWORDS
from src.edu_cti.sources.rss.common import parse_rss_date

logger = logging.getLogger(__name__)

SOURCE_NAME = "cisa_alerts"

CISA_FEEDS = [
    "https://www.cisa.gov/cybersecurity-advisories/all.xml",
]

# Additional keywords specific to CISA alerts relevant to education
CISA_EDU_KEYWORDS = EDUCATION_KEYWORDS + [
    "ransomware", "phishing", "k-12", "higher education",
    "campus", "academic", "school", "college", "university",
    "moveit", "powerschool", "ellucian", "blackboard", "canvas",
]


def _matches_education(text: str) -> bool:
    """Check if text contains education-related keywords."""
    if not text:
        return False
    lowered = text.lower()
    return any(kw in lowered for kw in CISA_EDU_KEYWORDS)


def build_cisa_rss_incidents(
    *,
    max_pages: Optional[int] = None,
    client=None,
    save_callback: Optional[Callable] = None,
    incremental: bool = True,
    max_age_days: int = 365 * 7,  # 7 years to cover back to 2019
) -> List[BaseIncident]:
    """
    Fetch education-relevant CISA alerts from RSS feeds.

    Args:
        max_pages: Not used (RSS)
        client: Not used
        save_callback: Callback for incremental saving
        incremental: If True, limit by max_age_days
        max_age_days: Max age of items to include

    Returns:
        List of BaseIncident objects
    """
    logger.info("Fetching CISA cybersecurity advisories RSS...")

    cutoff = datetime.utcnow() - timedelta(days=max_age_days)
    incidents: List[BaseIncident] = []

    for feed_url in CISA_FEEDS:
        try:
            resp = requests.get(feed_url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch CISA RSS {feed_url}: {e}")
            continue

        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as e:
            logger.error(f"Failed to parse CISA RSS XML: {e}")
            continue

        # Handle both RSS 2.0 and Atom feeds
        items = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry")

        for item in items:
            # RSS 2.0 format
            title = _get_text(item, "title") or _get_text(item, "{http://www.w3.org/2005/Atom}title")
            link = _get_text(item, "link") or _get_attr(item, "{http://www.w3.org/2005/Atom}link", "href")
            description = _get_text(item, "description") or _get_text(item, "{http://www.w3.org/2005/Atom}summary")
            pub_date_str = _get_text(item, "pubDate") or _get_text(item, "{http://www.w3.org/2005/Atom}published")

            if not title or not link:
                continue

            # Filter for education relevance
            combined = f"{title} {description or ''}"
            if not _matches_education(combined):
                continue

            # Parse date
            pub_date = None
            date_precision = "unknown"
            if pub_date_str:
                pub_date = parse_rss_date(pub_date_str)
                if pub_date:
                    date_precision = "day"
                    # Check cutoff
                    try:
                        dt = datetime.fromisoformat(pub_date.replace("Z", "+00:00").split("+")[0])
                        if dt < cutoff:
                            continue
                    except (ValueError, IndexError):
                        pass

            source_event_id = link
            incident_id = make_incident_id(SOURCE_NAME, source_event_id)

            incident = BaseIncident(
                incident_id=incident_id,
                source=SOURCE_NAME,
                source_event_id=source_event_id,
                university_name=title[:200],
                victim_raw_name=None,
                institution_type=None,
                country="United States",
                region=None,
                city=None,
                incident_date=pub_date,
                date_precision=date_precision,
                source_published_date=pub_date,
                ingested_at=datetime.utcnow().isoformat(),
                title=title[:200],
                subtitle=description[:200] if description else None,
                primary_url=None,
                all_urls=[link],
                attack_type_hint=None,
                status="suspected",
                source_confidence="high",
                notes="source=cisa_advisory",
            )

            incidents.append(incident)

            if save_callback:
                save_callback([incident])

    logger.info(f"CISA RSS: Found {len(incidents)} education-relevant advisories")
    return incidents


def _get_text(element, tag: str) -> Optional[str]:
    """Get text content of a child element."""
    child = element.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return None


def _get_attr(element, tag: str, attr: str) -> Optional[str]:
    """Get attribute of a child element."""
    child = element.find(tag)
    if child is not None:
        return child.get(attr)
    return None
