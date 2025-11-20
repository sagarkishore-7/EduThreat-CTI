from __future__ import annotations

import logging
from typing import Callable, Iterable, List, Optional, Sequence
from urllib.parse import urlencode

from bs4 import BeautifulSoup

from src.edu_cti.core.http import HttpClient
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.utils import now_utc_iso
from .common import (
    DEFAULT_MAX_PAGES,
    default_client,
    extract_date,
    fetch_html,
    matches_keywords,
    prepare_keywords,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://krebsonsecurity.com"
SOURCE_NAME = "krebsonsecurity"


def _search_url(keyword: str, page: int = 1) -> str:
    """Build search URL for a keyword and page number."""
    params = {"s": keyword}
    if page > 1:
        return f"{BASE_URL}/page/{page}/?{urlencode(params)}"
    return f"{BASE_URL}/?{urlencode(params)}"


def _get_next_page_url(soup: BeautifulSoup) -> Optional[str]:
    """
    Extract the 'Older posts' link from the pagination navigation.
    Returns the URL if found, None otherwise.
    """
    nav = soup.select_one('nav#nav-below.navigation')
    if not nav:
        return None
    
    older_posts_link = nav.select_one('div.nav-previous.alignleft a')
    if older_posts_link:
        href = older_posts_link.get("href", "").strip()
        if href:
            return href
    return None


def _iter_pages(
    client: HttpClient,
    keyword: str,
    max_pages: Optional[int],
) -> Iterable[tuple[int, Optional[BeautifulSoup]]]:
    """
    Iterate through search result pages by following 'Older posts' pagination.
    Continues until no more 'Older posts' link is found or max_pages limit is reached.
    Fetches all pages up to max_pages (inclusive) if limit is specified.
    """
    page_num = 1
    current_url = _search_url(keyword, page_num)
    pages_fetched = 0
    
    while True:
        # Check max_pages limit BEFORE fetching
        # If we've already fetched max_pages pages, stop
        if max_pages is not None and page_num > max_pages:
            logger.info(
                f"KrebsOnSecurity keyword '{keyword}' fetched {pages_fetched} pages "
                f"(stopped at limit of {max_pages} pages)"
            )
            break
        
        soup = fetch_html(current_url, client=client, allow_404=True)
        logger.debug(f"KrebsOnSecurity keyword '{keyword}' fetching page {page_num} -> {current_url}")
        
        if soup is None:
            logger.warning(f"KrebsOnSecurity keyword '{keyword}' page {page_num} returned None (404 or error)")
            break
        
        yield page_num, soup
        pages_fetched += 1
        
        # Look for 'Older posts' link to continue pagination
        next_url = _get_next_page_url(soup)
        if not next_url:
            logger.info(
                f"KrebsOnSecurity keyword '{keyword}' reached last page at page {page_num} "
                f"(fetched {pages_fetched} pages total)"
            )
            break
        
        page_num += 1
        current_url = next_url


def build_krebsonsecurity_incidents(
    *,
    max_pages: Optional[int] = None,
    keywords: Optional[Sequence[str]] = None,
    client: Optional[HttpClient] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
) -> List[BaseIncident]:
    """
    Crawl KrebsOnSecurity search results for EDU keywords and capture matching articles.
    Uses search URL format: https://krebsonsecurity.com/?s=keyword
    Follows pagination via 'Older posts' links until no more pages exist or max_pages limit is reached.
    Supports incremental saving via save_callback - saves after each page is processed.
    
    Args:
        save_callback: Optional callback to save incidents incrementally.
                      Called after each page is processed with incidents from that page.
    """
    http_client = client or default_client()
    prepared_keywords = prepare_keywords(keywords)
    incidents: List[BaseIncident] = []
    ingested_at = now_utc_iso()
    seen_urls: set[str] = set()

    # Search for each keyword separately and combine results
    search_keywords = prepared_keywords if prepared_keywords else ["university"]
    
    for keyword in search_keywords:
        logger.info(f"KrebsOnSecurity searching for keyword: '{keyword}'")
        
        for page_num, soup in _iter_pages(http_client, keyword, max_pages):
            if soup is None:
                continue

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

                text_blob = " ".join(filter(None, [title, summary]))
                if not matches_keywords(text_blob, prepared_keywords):
                    continue

                seen_urls.add(article_url)

                time_tag = article.find("time")
                raw_date = ""
                if time_tag:
                    raw_date = time_tag.get("datetime") or time_tag.get_text(strip=True)
                incident_date, date_precision = extract_date(raw_date)

                incident = BaseIncident(
                    incident_id=make_incident_id(SOURCE_NAME, article_url),
                    source=SOURCE_NAME,
                    source_event_id=article_url.rstrip("/"),
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
                    # Phase 1: primary_url=None, all URLs in all_urls (Phase 2 will select best URL)
                    primary_url=None,
                    all_urls=[article_url],
                    leak_site_url=None,
                    source_detail_url=None,  # News articles don't have CTI detail pages
                    screenshot_url=None,
                    attack_type_hint=None,
                    status="suspected",
                    source_confidence="medium",
                    notes="news_source=krebsonsecurity",
                )
                page_incidents.append(incident)
                incidents.append(incident)
            
            # Save incidents from this page incrementally if callback provided
            if save_callback is not None and page_incidents:
                try:
                    save_callback(page_incidents)
                    logger.debug(f"KrebsOnSecurity: Saved {len(page_incidents)} incidents from page {page_num} for keyword '{keyword}'")
                except Exception as e:
                    logger.error(f"KrebsOnSecurity: Error saving page {page_num} for keyword '{keyword}': {e}", exc_info=True)
                    # Continue processing even if save fails

    logger.info(f"KrebsOnSecurity found {len(incidents)} total incidents")
    return incidents
