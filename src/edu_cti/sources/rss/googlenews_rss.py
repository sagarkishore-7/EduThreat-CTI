"""
Google News RSS source for EduThreat-CTI.

Uses Google News RSS feeds to collect education-sector cyber incidents worldwide.
Supports 15+ languages with targeted cyber+education queries.

Two modes:
- Incremental (daily cron): Fetches current feed (~last 30 days), no date params
- Historical: Walks 6-month date windows from HISTORICAL_START_YEAR to present,
  clamped to GOOGLE_NEWS_RSS_EFFECTIVE_START_YEAR (2019) since Google News has
  no meaningful coverage before that date.

URL pattern:
  https://news.google.com/rss/search?q={query}+after:{start}+before:{end}&hl={lang}&gl={country}&ceid={country}:{lang}
"""

import html
import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Callable, List, Optional, Tuple
from urllib.parse import quote

import requests

from src.edu_cti.core.config import (
    HISTORICAL_START_YEAR,
    GOOGLE_NEWS_RSS_EFFECTIVE_START_YEAR,
    GOOGLE_NEWS_RSS_QUERIES,
)
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.sources.rss.common import parse_rss_date

logger = logging.getLogger(__name__)

# Queries are defined centrally in:
#   src/edu_cti/core/config.py → GOOGLE_NEWS_RSS_QUERIES
# Each entry is a (query, language_code, country_code) tuple.
# Edit config.py to add/modify queries — changes apply here automatically.
GOOGLE_NEWS_QUERIES = GOOGLE_NEWS_RSS_QUERIES

# Delay between requests to be respectful
REQUEST_DELAY = 2.0


def _build_google_news_url(
    query: str,
    lang: str,
    country: str,
    after_date: Optional[str] = None,
    before_date: Optional[str] = None,
) -> str:
    """Build Google News RSS search URL with optional date range."""
    encoded_query = quote(query)
    date_part = ""
    if after_date:
        date_part += f"+after:{after_date}"
    if before_date:
        date_part += f"+before:{before_date}"

    return (
        f"https://news.google.com/rss/search?"
        f"q={encoded_query}{date_part}"
        f"&hl={lang}&gl={country}&ceid={country}:{lang}"
    )


def _generate_date_windows(start_year: int) -> List[tuple]:
    """Generate 6-month date windows from start_year to present."""
    windows = []
    current = datetime(start_year, 1, 1)
    now = datetime.utcnow()

    while current < now:
        end = current + timedelta(days=182)  # ~6 months
        if end > now:
            end = now
        windows.append((
            current.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
        ))
        current = end

    return windows


def _clean_google_news_description(description: str) -> str:
    """Convert Google RSS description HTML into plain text suitable for UI/search."""
    if not description:
        return ""

    text = html.unescape(description)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _fetch_google_news_rss(url: str) -> List[dict]:
    """Fetch and parse a Google News RSS feed. Returns list of item dicts."""
    items = []
    try:
        resp = requests.get(url, timeout=30, headers={
            "User-Agent": "Mozilla/5.0 (compatible; EduThreat-CTI/2.0)",
            "Accept": "application/xml, application/rss+xml, text/xml",
        })
        if resp.status_code == 429:
            logger.warning("Google News rate limited — backing off 30s")
            time.sleep(30)
            return items
        if resp.status_code != 200:
            logger.debug(f"Google News returned {resp.status_code} for {url[:120]}")
            return items
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"Failed to fetch Google News RSS: {e}")
        return items

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        logger.warning(f"Failed to parse Google News RSS XML: {e}")
        return items

    for item in root.findall(".//item"):
        title_el = item.find("title")
        link_el = item.find("link")
        pub_date_el = item.find("pubDate")
        desc_el = item.find("description")
        source_el = item.find("source")

        title = title_el.text.strip() if title_el is not None and title_el.text else ""
        link = link_el.text.strip() if link_el is not None and link_el.text else ""
        pub_date = pub_date_el.text.strip() if pub_date_el is not None and pub_date_el.text else ""
        description = desc_el.text.strip() if desc_el is not None and desc_el.text else ""
        description = _clean_google_news_description(description)
        source_name = source_el.text.strip() if source_el is not None and source_el.text else ""

        if title and link:
            items.append({
                "title": title,
                "link": link,
                "pub_date": pub_date,
                "description": description,
                "source_name": source_name,
            })

    return items


def _resolve_google_news_link(url: str) -> Optional[str]:
    """Decode a Google News redirect URL to the actual article URL."""
    if "news.google.com" not in url:
        return url
    try:
        from googlenewsdecoder import new_decoderv1
        result = new_decoderv1(url)
        if result and result.get("status") and result.get("decoded_url"):
            return result["decoded_url"]
    except ImportError:
        logger.debug("googlenewsdecoder not installed — returning None")
    except Exception as e:
        logger.debug(f"Failed to decode Google News URL: {e}")
    return None


def build_googlenews_rss_incidents(
    *,
    max_pages: Optional[int] = None,
    client=None,
    save_callback: Optional[Callable] = None,
    incremental: bool = True,
    max_age_days: int = 30,
) -> List[BaseIncident]:
    """
    Fetch education-sector cyber incidents from Google News RSS worldwide.

    Args:
        max_pages: Not used (kept for interface compatibility)
        client: Not used
        save_callback: Callback for incremental saving
        incremental: If True, fetch recent only. If False, walk 6-month
                     date windows from HISTORICAL_START_YEAR to present.
        max_age_days: Max age for incremental mode (default 30 days)

    Returns:
        List of BaseIncident objects
    """
    all_incidents: List[BaseIncident] = []
    seen_urls: set = set()
    total_fetched = 0
    total_matched = 0

    if incremental:
        # Daily mode: no date windows, fetch current feed
        date_windows = [(None, None)]
        logger.info(f"Google News RSS: Incremental mode (last ~{max_age_days} days)")
    else:
        # Historical mode: walk 6-month windows from start year.
        # Google News has no coverage before ~2019, so cap the effective start year
        # to avoid hundreds of empty API calls for years 2000-2018.
        effective_start = max(HISTORICAL_START_YEAR, GOOGLE_NEWS_RSS_EFFECTIVE_START_YEAR)
        if HISTORICAL_START_YEAR < GOOGLE_NEWS_RSS_EFFECTIVE_START_YEAR:
            logger.info(
                f"Google News RSS: HISTORICAL_START_YEAR={HISTORICAL_START_YEAR} is before "
                f"Google News coverage — clamping to {GOOGLE_NEWS_RSS_EFFECTIVE_START_YEAR}"
            )
        date_windows = _generate_date_windows(effective_start)
        logger.info(
            f"Google News RSS: Historical mode — {len(date_windows)} windows "
            f"from {effective_start} to present"
        )

    cutoff = datetime.utcnow() - timedelta(days=max_age_days) if incremental else None

    for window_idx, (after_date, before_date) in enumerate(date_windows):
        window_label = f"{after_date} → {before_date}" if after_date else "current"
        if not incremental:
            logger.info(f"Window [{window_idx + 1}/{len(date_windows)}]: {window_label}")

        for query, lang, country in GOOGLE_NEWS_QUERIES:
            url = _build_google_news_url(query, lang, country, after_date, before_date)
            items = _fetch_google_news_rss(url)
            total_fetched += len(items)

            for item in items:
                raw_link = item["link"]

                # Resolve Google News redirect URL to actual article URL
                link = _resolve_google_news_link(raw_link)
                if not link:
                    continue

                # Dedup by resolved URL
                if link in seen_urls:
                    continue
                seen_urls.add(link)

                title = item["title"]
                description = item.get("description", "")

                total_matched += 1

                # Parse date
                pub_date = None
                date_precision = "unknown"
                if item["pub_date"]:
                    parsed_pub_date = parse_rss_date(item["pub_date"])
                    if parsed_pub_date:
                        date_precision = "day"
                        # In incremental mode, skip items older than cutoff
                        if cutoff:
                            if parsed_pub_date.replace(tzinfo=None) < cutoff:
                                continue
                        pub_date = parsed_pub_date.date().isoformat()

                source_event_id = link
                incident_id = make_incident_id("googlenews_rss", source_event_id)

                incident = BaseIncident(
                    incident_id=incident_id,
                    source="googlenews_rss",
                    source_event_id=source_event_id,
                    institution_name=title[:200],
                    victim_raw_name=None,
                    institution_type=None,
                    country=None,  # Will be resolved during enrichment
                    region=None,
                    city=None,
                    incident_date=pub_date,
                    date_precision=date_precision,
                    source_published_date=pub_date,
                    ingested_at=datetime.utcnow().isoformat(),
                    title=title[:200],
                    subtitle=description[:300] if description else None,
                    primary_url=None,
                    all_urls=[link],
                    attack_type_hint=None,
                    status="suspected",
                    source_confidence="medium",
                    notes=f"lang={lang};country={country};query={query[:50]}",
                )

                all_incidents.append(incident)

                if save_callback:
                    save_callback([incident])

            # Rate limit between queries
            time.sleep(REQUEST_DELAY)

        if not incremental and (window_idx + 1) % 3 == 0:
            logger.info(
                f"Progress: {window_idx + 1}/{len(date_windows)} windows, "
                f"{total_fetched} fetched, {total_matched} matched, "
                f"{len(all_incidents)} unique incidents"
            )

    logger.info(
        f"Google News RSS complete: {total_fetched} items fetched, "
        f"{total_matched} cyber-relevant, {len(all_incidents)} unique incidents"
    )
    return all_incidents
