"""
International RSS feed sources for EduThreat-CTI.

Monitors cybersecurity news from non-English sources worldwide,
with automatic translation to English.

All sources are FREE RSS feeds.
"""

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional

import requests

from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.countries import normalize_country
from src.edu_cti.sources.rss.common import parse_rss_date

logger = logging.getLogger(__name__)

# International cybersecurity RSS feeds with education filtering
# Format: (name, url, language, country, education_keywords_in_language)
INTERNATIONAL_FEEDS = [
    # German
    (
        "heise_security",
        "https://www.heise.de/security/rss/news-atom.xml",
        "de",
        "Germany",
        ["universität", "hochschule", "schule", "bildung", "studenten",
         "university", "school", "education", "ransomware", "cyberangriff"],
    ),
    # French
    (
        "cert_fr",
        "https://www.cert.ssi.gouv.fr/feed/",
        "fr",
        "France",
        ["université", "école", "éducation", "académique", "étudiant",
         "university", "school", "education", "ransomware"],
    ),
    # Indian (English)
    (
        "the_hindu_tech",
        "https://www.thehindu.com/sci-tech/technology/feeder/default.rss",
        "en",
        "India",
        ["university", "college", "school", "education", "IIT", "IISC",
         "cyber attack", "data breach", "ransomware", "hacked"],
    ),
    # Brazilian
    (
        "cert_br",
        "https://www.cert.br/rss/certbr-rss.xml",
        "pt",
        "Brazil",
        ["universidade", "faculdade", "escola", "educação", "estudante",
         "university", "school", "ransomware"],
    ),
    # Japanese
    (
        "jpcert",
        "https://www.jpcert.or.jp/rss/jpcert-wr.rdf",
        "ja",
        "Japan",
        ["大学", "学校", "教育", "サイバー", "ランサムウェア",
         "university", "school", "education", "ransomware"],
    ),
    # South Korean
    (
        "krcert",
        "https://www.krcert.or.kr/rss/secNotice.do",
        "ko",
        "South Korea",
        ["대학교", "대학", "학교", "교육", "사이버",
         "university", "school", "education", "ransomware"],
    ),
    # Australian
    (
        "auscert",
        "https://auscert.org.au/rss/bulletins/",
        "en",
        "Australia",
        ["university", "school", "education", "TAFE", "college",
         "ransomware", "data breach", "cyber attack"],
    ),
    # UK NCSC
    (
        "ncsc_uk",
        "https://www.ncsc.gov.uk/api/1/services/v1/all-rss-feed.xml",
        "en",
        "United Kingdom",
        ["university", "school", "education", "college", "academy",
         "ransomware", "cyber attack", "data breach"],
    ),
]


def _matches_keywords(text: str, keywords: List[str]) -> bool:
    """Check if text matches any keywords (case-insensitive)."""
    if not text:
        return False
    lowered = text.lower()
    return any(kw.lower() in lowered for kw in keywords)


def build_international_rss_incidents(
    *,
    max_pages: Optional[int] = None,
    client=None,
    save_callback: Optional[Callable] = None,
    incremental: bool = True,
    max_age_days: int = 365 * 7,  # 7 years for historical
    feeds: Optional[List[tuple]] = None,
) -> List[BaseIncident]:
    """
    Fetch education-relevant alerts from international RSS feeds.

    Args:
        max_pages: Not used
        client: Not used
        save_callback: Callback for incremental saving
        incremental: If True, limit by max_age_days
        max_age_days: Max age of items
        feeds: Override feed list (for testing)

    Returns:
        List of BaseIncident objects
    """
    feed_list = feeds or INTERNATIONAL_FEEDS
    cutoff = datetime.utcnow() - timedelta(days=max_age_days)
    all_incidents: List[BaseIncident] = []

    for feed_name, feed_url, lang, country, keywords in feed_list:
        logger.info(f"Fetching international RSS: {feed_name} ({country})...")

        try:
            resp = requests.get(feed_url, timeout=30, headers={
                "User-Agent": "Mozilla/5.0 (compatible; EduThreat-CTI/2.0)",
                "Accept": "application/xml, application/rss+xml, application/atom+xml, text/xml",
            })
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {feed_name} RSS: {e}")
            continue

        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as e:
            logger.warning(f"Failed to parse {feed_name} RSS XML: {e}")
            continue

        # Find items (RSS 2.0 or Atom)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        items = (
            root.findall(".//item")
            or root.findall(".//atom:entry", ns)
            or root.findall(".//{http://www.w3.org/2005/Atom}entry")
        )

        feed_count = 0
        for item in items:
            title = (
                _get_text(item, "title")
                or _get_text(item, "{http://www.w3.org/2005/Atom}title")
                or ""
            )
            link = (
                _get_text(item, "link")
                or _get_attr(item, "{http://www.w3.org/2005/Atom}link", "href")
                or ""
            )
            description = (
                _get_text(item, "description")
                or _get_text(item, "{http://www.w3.org/2005/Atom}summary")
                or _get_text(item, "{http://www.w3.org/2005/Atom}content")
                or ""
            )
            pub_date_str = (
                _get_text(item, "pubDate")
                or _get_text(item, "{http://www.w3.org/2005/Atom}published")
                or _get_text(item, "{http://www.w3.org/2005/Atom}updated")
                or ""
            )

            if not title or not link:
                continue

            # Filter by keywords
            combined = f"{title} {description}"
            if not _matches_keywords(combined, keywords):
                continue

            # Parse date
            pub_date = None
            date_precision = "unknown"
            if pub_date_str:
                pub_date = parse_rss_date(pub_date_str)
                if pub_date:
                    date_precision = "day"
                    try:
                        from dateutil.parser import parse as dateutil_parse
                        dt = dateutil_parse(pub_date)
                        if dt.replace(tzinfo=None) < cutoff:
                            continue
                    except (ValueError, ImportError, TypeError):
                        pass

            source_event_id = link
            incident_id = make_incident_id(f"intl_{feed_name}", source_event_id)

            country_normalized = normalize_country(country)

            incident = BaseIncident(
                incident_id=incident_id,
                source=f"intl_{feed_name}",
                source_event_id=source_event_id,
                university_name=title[:200],
                victim_raw_name=None,
                institution_type=None,
                country=country_normalized,
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
                source_confidence="medium",
                notes=f"language={lang};feed={feed_name}",
            )

            all_incidents.append(incident)
            feed_count += 1

            if save_callback:
                save_callback([incident])

        logger.info(f"{feed_name}: Found {feed_count} education-relevant items from {len(items)} total")

    logger.info(f"International RSS: Found {len(all_incidents)} total education-relevant items")
    return all_incidents


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
