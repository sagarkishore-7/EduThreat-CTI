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
import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Callable, List, Optional, Tuple
from urllib.parse import quote, urlparse

import requests
from bs4 import BeautifulSoup

from src.edu_cti.core.config import (
    HISTORICAL_START_YEAR,
    GOOGLE_NEWS_RSS_EFFECTIVE_START_YEAR,
    GOOGLE_NEWS_RSS_HISTORICAL_WINDOW_DAYS,
    GOOGLE_NEWS_RSS_QUERIES,
    GOOGLE_NEWS_RSS_REQUEST_DELAY_SECONDS,
)
from src.edu_cti.core.discovery_policy import (
    QUERY_SCOPED_HIGH_RECALL,
    discovery_policy_for_source,
    record_source_discovery_metrics,
    semantic_prefilter_allowed,
)
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.sources.rss.common import parse_rss_date

logger = logging.getLogger(__name__)
SOURCE_NAME = "googlenews_rss"

# Queries are defined centrally in:
#   src/edu_cti/core/config.py → GOOGLE_NEWS_RSS_QUERIES
# Each entry is a (query, language_code, country_code) tuple.


def _google_news_decode_timeout() -> float:
    try:
        return max(1.0, float(os.environ.get("EDU_CTI_GOOGLE_NEWS_DECODE_TIMEOUT_SECONDS", "4")))
    except (TypeError, ValueError):
        return 4.0


def _extract_google_news_base64(link: str) -> Optional[str]:
    try:
        parsed = urlparse(link)
    except Exception:
        return None
    path = parsed.path.split("/")
    if parsed.hostname == "news.google.com" and len(path) > 1 and path[-2] in {"articles", "read"}:
        return path[-1]
    return None


def _resolve_google_news_article_url_with_timeouts(link: str) -> Optional[str]:
    """Resolve newer Google News wrappers with bounded network calls."""
    base64_str = _extract_google_news_base64(link)
    if not base64_str:
        return None

    timeout = _google_news_decode_timeout()
    params = None
    for path_prefix in ("articles", "rss/articles"):
        try:
            response = requests.get(
                f"https://news.google.com/{path_prefix}/{base64_str}",
                timeout=timeout,
            )
            response.raise_for_status()
        except requests.RequestException:
            continue

        element = BeautifulSoup(response.text, "html.parser").select_one("c-wiz > div[jscontroller]")
        if element:
            params = {
                "signature": element.get("data-n-a-sg"),
                "timestamp": element.get("data-n-a-ts"),
            }
            break

    if not params or not params.get("signature") or not params.get("timestamp"):
        return None

    payload = [
        "Fbv4je",
        (
            '["garturlreq",[["X","X",["X","X"],null,null,1,1,"US:en",null,1,'
            f'null,null,null,null,null,0,1],"X","X",1,[1,1,1],1,1,null,0,0,null,0],'
            f'"{base64_str}",{params["timestamp"]},"{params["signature"]}"]'
        ),
    ]
    try:
        response = requests.post(
            "https://news.google.com/_/DotsSplashUi/data/batchexecute",
            headers={
                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            },
            data=f"f.req={quote(json.dumps([[payload]]))}",
            timeout=timeout,
        )
        response.raise_for_status()
        parsed_data = json.loads(response.text.split("\n\n")[1])[:-2]
        decoded_url = json.loads(parsed_data[0][2])[1]
    except (requests.RequestException, json.JSONDecodeError, IndexError, TypeError, KeyError):
        return None

    if decoded_url and "news.google.com" not in decoded_url:
        return str(decoded_url).strip()
    return None
# Edit config.py to add/modify queries — changes apply here automatically.
GOOGLE_NEWS_QUERIES = GOOGLE_NEWS_RSS_QUERIES

# Delay between requests to be respectful
REQUEST_DELAY = GOOGLE_NEWS_RSS_REQUEST_DELAY_SECONDS


def _is_http_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


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
    """Generate historical date windows from start_year to present."""
    windows = []
    current = datetime(start_year, 1, 1)
    now = datetime.utcnow()

    while current < now:
        end = current + timedelta(days=GOOGLE_NEWS_RSS_HISTORICAL_WINDOW_DAYS)
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


def _resolve_google_news_article_url(link: str) -> Optional[str]:
    """Resolve Google News wrapper links to the underlying article URL when possible."""
    if not link:
        return None

    link = link.strip()
    if "news.google.com" not in link:
        return link

    try:
        from googlenewsdecoder import new_decoderv1

        result = new_decoderv1(link)
        if isinstance(result, dict) and result.get("status") and result.get("decoded_url"):
            resolved = str(result["decoded_url"]).strip()
            if resolved.startswith(("http://", "https://")) and "news.google.com" not in resolved:
                return resolved
    except Exception as exc:
        logger.debug("Modern Google News RSS decode failed for %s: %s", link[:120], exc)

    try:
        from googlenewsdecoder.decoderv1 import decode_google_news_url

        resolved = decode_google_news_url(link)
        if resolved and resolved.startswith(("http://", "https://")) and "news.google.com" not in resolved:
            return str(resolved).strip()
    except Exception as exc:
        logger.debug("Fast Google News RSS decode failed for %s: %s", link[:120], exc)

    resolved = _resolve_google_news_article_url_with_timeouts(link)
    if resolved:
        return resolved

    return None


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
    discovery_metrics = {
        "rss_results_seen": 0,
        "source_rows_created": 0,
        "duplicates_skipped": 0,
        "invalid_url_skipped": 0,
        "out_of_window_skipped": 0,
        "semantic_skipped": 0,
    }
    policy = discovery_policy_for_source(SOURCE_NAME)
    if policy == QUERY_SCOPED_HIGH_RECALL and semantic_prefilter_allowed(SOURCE_NAME):
        raise RuntimeError(f"{SOURCE_NAME} must not use semantic pre-filters")

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
            f"from {effective_start} to present "
            f"({GOOGLE_NEWS_RSS_HISTORICAL_WINDOW_DAYS}-day windows, "
            f"{len(GOOGLE_NEWS_QUERIES)} query/country tuples)"
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
            discovery_metrics["rss_results_seen"] += len(items)

            for item in items:
                raw_link = item["link"]

                link = raw_link
                title = item["title"]
                description = item.get("description", "")
                source_name = item.get("source_name")

                # Dedup by source-event id / Google wrapper, but do not persist the
                # wrapper URL as an enrichment candidate. Phase 2 will discover a
                # real article URL via SERP/title matching.
                if not _is_http_url(link):
                    discovery_metrics["invalid_url_skipped"] += 1
                    continue
                if link in seen_urls:
                    discovery_metrics["duplicates_skipped"] += 1
                    continue

                seen_urls.add(link)

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
                                discovery_metrics["out_of_window_skipped"] += 1
                                continue
                        pub_date = parsed_pub_date.date().isoformat()

                total_matched += 1

                resolved_link = _resolve_google_news_article_url(link)
                source_event_id = resolved_link or link
                incident_id = make_incident_id(SOURCE_NAME, source_event_id)

                incident = BaseIncident(
                    incident_id=incident_id,
                    source=SOURCE_NAME,
                    source_event_id=source_event_id,
                    institution_name=None,
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
                    all_urls=[resolved_link] if resolved_link else [],
                    attack_type_hint=None,
                    status="suspected",
                    source_confidence="medium",
                    notes=(
                        f"search_lang={lang};search_country={country};"
                        f"source={source_name or 'unknown'};query={query[:50]};"
                        f"discovery_policy={policy}"
                    ),
                    raw_source_payload={
                        "discovery_policy": policy,
                        "query": query,
                        "search_lang": lang,
                        "search_country": country,
                        "google_news_source": source_name or "unknown",
                    },
                )

                all_incidents.append(incident)
                discovery_metrics["source_rows_created"] += 1

                if save_callback:
                    save_callback([incident])

            # Rate limit between queries
            time.sleep(REQUEST_DELAY)

        if not incremental and (window_idx + 1) % 3 == 0:
            logger.info(
                f"Progress: {window_idx + 1}/{len(date_windows)} windows, "
                f"{total_fetched} fetched, {total_matched} candidates, "
                f"{len(all_incidents)} unique incidents"
            )

    record_source_discovery_metrics(SOURCE_NAME, discovery_metrics)
    logger.info(
        f"Google News RSS complete: {total_fetched} items fetched, "
        f"{total_matched} candidates, {len(all_incidents)} unique incidents, "
        f"discovery_metrics={discovery_metrics}"
    )
    return all_incidents
