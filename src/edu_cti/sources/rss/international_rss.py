"""
International RSS feed sources for EduThreat-CTI.

Monitors cybersecurity news from non-English sources worldwide,
with automatic translation to English.

All sources are FREE RSS feeds.
"""

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional

import requests

from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.countries import normalize_country
from src.edu_cti.core.date_parsing import parse_datetime_with_known_timezones
from src.edu_cti.sources.rss.common import parse_rss_date

logger = logging.getLogger(__name__)

# International cybersecurity RSS feeds with education filtering
# Format: (name, url, language, country, edu_keywords, cyber_keywords)
# Both edu AND cyber keywords must match for an article to be included.
INTERNATIONAL_FEEDS = [
    # ---- Europe ----
    # German
    (
        "heise_security",
        "https://www.heise.de/security/rss/news-atom.xml",
        "de",
        "Germany",
        ["universität", "hochschule", "schule", "bildung", "studenten", "university", "school"],
        ["cyberangriff", "ransomware", "hackerangriff", "datenleck", "sicherheitslücke",
         "cyberattack", "data breach", "hacked", "malware", "phishing"],
    ),
    # French CERT
    (
        "cert_fr",
        "https://www.cert.ssi.gouv.fr/feed/",
        "fr",
        "France",
        ["université", "école", "éducation", "académique", "étudiant", "university", "school"],
        ["cyberattaque", "ransomware", "piratage", "fuite de données", "vulnérabilité",
         "cyberattack", "data breach", "hacked", "malware"],
    ),
    # Spain — INCIBE (Spanish CERT)
    (
        "incibe_es",
        "https://www.incibe.es/incibe/sala-de-prensa/rss",
        "es",
        "Spain",
        ["universidad", "escuela", "colegio", "educación", "instituto", "university", "school"],
        ["ciberataque", "ransomware", "hackeo", "brecha de datos", "vulnerabilidad",
         "cyberattack", "data breach", "malware"],
    ),
    # Italy — CSIRT Italia
    (
        "csirt_italia",
        "https://www.csirt.gov.it/feed/rss",
        "it",
        "Italy",
        ["università", "scuola", "istituto", "educazione", "university", "school"],
        ["attacco informatico", "ransomware", "violazione dati", "vulnerabilità",
         "cyberattack", "data breach", "malware"],
    ),
    # Netherlands — NCSC-NL
    (
        "ncsc_nl",
        "https://advisories.ncsc.nl/rss/advisories",
        "nl",
        "Netherlands",
        ["universiteit", "school", "hogeschool", "onderwijs", "university"],
        ["cyberaanval", "ransomware", "datalek", "kwetsbaarheid",
         "cyberattack", "data breach", "malware"],
    ),
    # UK — NCSC
    (
        "ncsc_uk",
        "https://www.ncsc.gov.uk/api/1/services/v1/all-rss-feed.xml",
        "en",
        "United Kingdom",
        ["university", "school", "education", "college", "academy"],
        ["ransomware", "cyber attack", "data breach", "hacked", "malware", "phishing"],
    ),
    # Finland — NCSC-FI
    (
        "ncsc_fi",
        "https://www.kyberturvallisuuskeskus.fi/feed/rss/en",
        "en",
        "Finland",
        ["university", "school", "education", "campus"],
        ["ransomware", "cyber attack", "data breach", "vulnerability", "malware"],
    ),
    # Sweden — CERT-SE
    (
        "cert_se",
        "https://www.cert.se/feed.rss",
        "sv",
        "Sweden",
        ["universitet", "skola", "utbildning", "högskola", "university", "school"],
        ["cyberattack", "ransomware", "dataintrång", "sårbarhet",
         "cyber attack", "data breach", "malware"],
    ),
    # Poland — CERT Polska
    (
        "cert_pl",
        "https://cert.pl/en/rss.xml",
        "en",
        "Poland",
        ["university", "school", "education", "uniwersytet", "szkoła"],
        ["ransomware", "cyber attack", "data breach", "malware", "phishing"],
    ),
    # ---- Asia-Pacific ----
    # India — The Hindu Tech
    (
        "the_hindu_tech",
        "https://www.thehindu.com/sci-tech/technology/feeder/default.rss",
        "en",
        "India",
        ["university", "college", "school", "education", "IIT", "IISC"],
        ["cyber attack", "data breach", "ransomware", "hacked", "malware", "phishing"],
    ),
    # Australia — ACSC
    (
        "acsc_au",
        "https://www.cyber.gov.au/about-us/view-all-content/rss.xml",
        "en",
        "Australia",
        ["university", "school", "education", "TAFE", "campus"],
        ["ransomware", "cyber attack", "data breach", "malware", "vulnerability"],
    ),
    # Singapore — SingCERT
    (
        "singcert_sg",
        "https://www.csa.gov.sg/api/rss/alerts-and-advisories",
        "en",
        "Singapore",
        ["university", "school", "education", "NUS", "NTU", "polytechnic"],
        ["ransomware", "cyber attack", "data breach", "vulnerability", "malware"],
    ),
    # ---- Americas ----
    # Brazil — CERT.br
    (
        "cert_br",
        "https://www.cert.br/rss/certbr-rss.xml",
        "pt",
        "Brazil",
        ["universidade", "faculdade", "escola", "educação", "estudante", "university"],
        ["ataque cibernético", "ransomware", "invasão", "vazamento de dados",
         "cyberattack", "data breach", "malware"],
    ),
    # Canada — Cyber Centre
    (
        "cccs_ca",
        "https://www.cyber.gc.ca/api/cccs/atom/v1/get?feed=alerts_advisories&lang=en",
        "en",
        "Canada",
        ["university", "school", "college", "education", "campus"],
        ["ransomware", "cyber attack", "data breach", "malware", "vulnerability"],
    ),
    # ---- Middle East ----
    # Saudi Arabia — Saudi CERT (English)
    (
        "cert_sa",
        "https://cert.gov.sa/en/rss/",
        "en",
        "Saudi Arabia",
        ["university", "school", "education", "campus"],
        ["ransomware", "cyber attack", "data breach", "vulnerability", "malware"],
    ),
]

_INVALID_XML_CHARS_RE = re.compile(
    r"[\x00-\x08\x0b\x0c\x0e-\x1f]"
)


def _parse_feed_xml(content: bytes, text: str, encoding: Optional[str], feed_name: str):
    """Parse RSS/Atom XML with conservative fallbacks for imperfect feeds."""
    try:
        return ET.fromstring(content)
    except ET.ParseError as original_error:
        cleaned_text = _INVALID_XML_CHARS_RE.sub("", text)
        try:
            return ET.fromstring(cleaned_text.encode(encoding or "utf-8"))
        except ET.ParseError:
            pass

        try:
            from lxml import etree

            parser = etree.XMLParser(recover=True)
            root = etree.fromstring(content, parser=parser)
            if root is not None:
                logger.warning(
                    f"{feed_name} RSS XML was malformed; parsed with lxml recovery"
                )
                return root
        except Exception:
            pass

        raise original_error


def _matches_edu_and_cyber(text: str, edu_keywords: List[str], cyber_keywords: List[str]) -> bool:
    """Check if text matches BOTH an education keyword AND a cyber keyword."""
    if not text:
        return False
    lowered = text.lower()
    has_edu = any(kw.lower() in lowered for kw in edu_keywords)
    if not has_edu:
        return False
    return any(kw.lower() in lowered for kw in cyber_keywords)


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

    for feed_entry in feed_list:
        # Support both old 5-tuple and new 6-tuple format
        if len(feed_entry) == 6:
            feed_name, feed_url, lang, country, edu_kw, cyber_kw = feed_entry
        else:
            feed_name, feed_url, lang, country, edu_kw = feed_entry
            # Fallback: use generic cyber keywords
            cyber_kw = ["ransomware", "cyberattack", "cyber attack", "data breach",
                        "hacked", "malware", "phishing", "vulnerability"]
        logger.info(f"Fetching international RSS: {feed_name} ({country})...")

        try:
            resp = requests.get(feed_url, timeout=45, headers={
                "User-Agent": "Mozilla/5.0 (compatible; EduThreat-CTI/2.0)",
                "Accept": "application/xml, application/rss+xml, application/atom+xml, text/xml",
            })
            if resp.status_code == 404:
                logger.warning(f"{feed_name} RSS feed returned 404 — URL may have changed: {feed_url}")
                continue
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {feed_name} RSS: {e}")
            continue

        try:
            root = _parse_feed_xml(resp.content, resp.text, resp.encoding, feed_name)
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

            # Filter by BOTH education AND cyber keywords
            combined = f"{title} {description}"
            if not _matches_edu_and_cyber(combined, edu_kw, cyber_kw):
                continue

            # Parse date
            pub_date = None
            date_precision = "unknown"
            if pub_date_str:
                pub_date = parse_rss_date(pub_date_str)
                if pub_date:
                    date_precision = "day"
                    try:
                        dt = parse_datetime_with_known_timezones(pub_date)
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
                institution_name=title[:200],
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
