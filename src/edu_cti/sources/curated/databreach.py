"""
DataBreaches.net Education Sector archive ingestion.

This module crawls the DataBreaches.net education sector category archive,
which contains 490+ pages of historical education-related breach reports.

Supports incremental ingestion:
- First run: Fetches all pages (historical)
- Subsequent runs: Only fetches new articles since last ingestion
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Callable, Iterable, List, Optional

from bs4 import BeautifulSoup

from src.edu_cti.core import config
from src.edu_cti.core.db import (
    get_connection,
    init_db,
    get_last_pubdate,
    set_last_pubdate,
    source_event_exists,
    register_source_event,
)
from src.edu_cti.core.http import HttpClient
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.pagination import extract_last_page_from_numbers
from src.edu_cti.core.utils import now_utc_iso
from .common import (
    default_client,
    extract_date,
    fetch_html,
)

BASE_URL = "https://databreaches.net/category/education-sector/"
SOURCE_NAME = config.SOURCE_DATABREACHES
logger = logging.getLogger(__name__)


def _discover_last_page(client: HttpClient) -> int:
    soup = fetch_html(BASE_URL, client=client)
    if not soup:
        return 1
    pagination = soup.select_one("ul.page-numbers")
    return extract_last_page_from_numbers(pagination)


def _page_url(page_number: int) -> str:
    if page_number <= 1:
        return BASE_URL
    return f"{BASE_URL.rstrip('/')}/page/{page_number}/"


def _iter_pages(
    client: HttpClient,
    max_pages: Optional[int],
) -> Iterable[tuple[int, Optional[BeautifulSoup]]]:
    last_page = _discover_last_page(client)
    target_last = min(max_pages or last_page, last_page)
    logger.info(
        "DataBreaches pagination discovered: last=%s, target=%s",
        last_page,
        target_last,
    )

    page = 1
    next_url = _page_url(page)
    while page <= target_last:
        soup = fetch_html(next_url, client=client, allow_404=True)
        logger.debug("DataBreaches fetching page %s -> %s", page, next_url)
        yield page, soup
        if soup is None:
            break
        next_link = soup.select_one("a.next.page-numbers")
        if next_link and next_link.get("href"):
            next_url = next_link["href"]
        else:
            page += 1
            next_url = _page_url(page)
            continue
        page += 1


def _parse_date_for_comparison(date_str: Optional[str]) -> Optional[datetime]:
    """Parse date string to datetime for comparison."""
    if not date_str:
        return None
    try:
        # Handle ISO format dates (YYYY-MM-DD)
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def build_databreach_incidents(
    *,
    max_pages: Optional[int] = None,
    client: Optional[HttpClient] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
    incremental: bool = True,
) -> List[BaseIncident]:
    """
    Crawl the DataBreaches.net education sector archive.
    
    Supports incremental ingestion:
    - incremental=True (default): Only fetch articles newer than last_pubdate
    - incremental=False: Full historical scrape (all pages)
    
    Args:
        max_pages: Maximum number of pages to fetch (None = all pages or until last_pubdate)
        client: Optional HTTP client
        save_callback: Optional callback to save incidents incrementally
        incremental: If True, stop when reaching already-ingested articles
    """
    http_client = client or default_client()
    incidents: List[BaseIncident] = []
    ingested_at = now_utc_iso()
    seen_urls: set[str] = set()
    
    # Initialize database connection for incremental tracking
    conn = get_connection()
    init_db(conn)
    
    # Get last ingestion date for incremental mode
    last_pubdate = None
    last_pubdate_dt = None
    if incremental:
        last_pubdate = get_last_pubdate(conn, SOURCE_NAME)
        if last_pubdate:
            last_pubdate_dt = _parse_date_for_comparison(last_pubdate)
            logger.info(f"DataBreaches: Incremental mode - fetching articles newer than {last_pubdate}")
        else:
            logger.info("DataBreaches: No previous ingestion found - full historical mode")
    else:
        logger.info("DataBreaches: Full historical mode (incremental=False)")
    
    newest_date: Optional[str] = None
    newest_date_dt: Optional[datetime] = None
    reached_old_articles = False
    total_new = 0
    total_skipped = 0

    limit = max_pages if max_pages is not None else None
    for page_number, soup in _iter_pages(http_client, limit):
        if soup is None:
            break
        
        if reached_old_articles:
            logger.info(f"DataBreaches: Stopping at page {page_number} - reached already-ingested articles")
            break

        page_incidents: List[BaseIncident] = []
        for article in soup.select("article"):
            title_tag = article.select_one("h2 a")
            if not title_tag:
                continue

            title = title_tag.get_text(strip=True)
            article_url = title_tag.get("href", "").strip()
            if not article_url or article_url in seen_urls:
                continue

            summary_tag = article.select_one(".entry-summary") or article.select_one("p")
            summary = summary_tag.get_text(" ", strip=True) if summary_tag else ""

            seen_urls.add(article_url)

            time_tag = article.find("time")
            raw_date = ""
            if time_tag:
                raw_date = time_tag.get("datetime") or time_tag.get_text(strip=True)
            incident_date, date_precision = extract_date(raw_date)
            
            # Track newest date for updating last_pubdate
            if incident_date:
                article_date_dt = _parse_date_for_comparison(incident_date)
                if article_date_dt and (newest_date_dt is None or article_date_dt > newest_date_dt):
                    newest_date = incident_date
                    newest_date_dt = article_date_dt
            
            # INCREMENTAL CHECK: Stop if we've reached already-ingested articles
            if incremental and last_pubdate_dt and incident_date:
                article_date_dt = _parse_date_for_comparison(incident_date)
                if article_date_dt and article_date_dt <= last_pubdate_dt:
                    logger.info(f"DataBreaches: Reached article from {incident_date} (<= {last_pubdate}), stopping pagination")
                    reached_old_articles = True
                    break
            
            # Skip if already in source_events (deduplication)
            source_event_id = article_url.rstrip("/")
            if source_event_exists(conn, SOURCE_NAME, source_event_id):
                logger.debug(f"Skipping already-ingested article: {title[:50]}...")
                total_skipped += 1
                continue

            incident = BaseIncident(
                incident_id=make_incident_id(SOURCE_NAME, article_url),
                source=SOURCE_NAME,
                source_event_id=source_event_id,
                university_name="",
                victim_raw_name="",
                institution_type=None,
                country=None,
                region=None,
                city=None,
                incident_date=incident_date,
                date_precision=date_precision,
                source_published_date=incident_date,
                ingested_at=ingested_at,
                title=title,
                subtitle=summary or None,
                primary_url=None,
                all_urls=[article_url],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=f"news_source={SOURCE_NAME};page={page_number}",
            )
            
            # Register source event to prevent re-ingestion
            register_source_event(conn, SOURCE_NAME, source_event_id, incident.incident_id, ingested_at)
            
            page_incidents.append(incident)
            incidents.append(incident)
            total_new += 1
        
        # Save incidents from this page incrementally if callback provided
        if save_callback is not None and page_incidents:
            try:
                save_callback(page_incidents)
                logger.debug(f"DataBreaches: Saved {len(page_incidents)} incidents from page {page_number}")
            except Exception as e:
                logger.error(f"DataBreaches: Error saving page {page_number}: {e}", exc_info=True)
        
        # Commit after each page
        conn.commit()
        
        logger.info(f"DataBreaches: Page {page_number} - {len(page_incidents)} new incidents")
    
    # Update last_pubdate to newest article we saw
    if newest_date:
        set_last_pubdate(conn, SOURCE_NAME, newest_date)
        logger.info(f"DataBreaches: Updated last_pubdate to {newest_date}")
    
    conn.commit()
    conn.close()
    
    logger.info(f"DataBreaches: Complete - {total_new} new, {total_skipped} skipped (already ingested)")
    return incidents
