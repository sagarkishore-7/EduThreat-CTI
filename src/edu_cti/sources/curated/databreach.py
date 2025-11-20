from __future__ import annotations

import logging
from typing import Callable, Iterable, List, Optional

from bs4 import BeautifulSoup

from src.edu_cti.core import config
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


def build_databreach_incidents(
    *,
    max_pages: Optional[int] = None,
    client: Optional[HttpClient] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
) -> List[BaseIncident]:
    """
    Crawl the entire DataBreaches.net education sector archive (default) or up to max_pages.
    Supports incremental saving via save_callback - saves after each page is processed.
    
    Args:
        max_pages: Maximum number of pages to fetch (None = all pages)
        client: Optional HTTP client
        save_callback: Optional callback to save incidents incrementally.
                      Called after each page is processed with incidents from that page.
    """
    http_client = client or default_client()
    incidents: List[BaseIncident] = []
    ingested_at = now_utc_iso()
    seen_urls: set[str] = set()

    limit = max_pages if max_pages is not None else None
    for page_number, soup in _iter_pages(http_client, limit):
        if soup is None:
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
                source_detail_url=None,  # Archive sources don't have CTI detail pages
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=f"news_source={SOURCE_NAME};page={page_number}",
            )
            page_incidents.append(incident)
            incidents.append(incident)
        
        # Save incidents from this page incrementally if callback provided
        if save_callback is not None and page_incidents:
            try:
                save_callback(page_incidents)
                logger.debug(f"DataBreaches: Saved {len(page_incidents)} incidents from page {page_number}")
            except Exception as e:
                logger.error(f"DataBreaches: Error saving page {page_number}: {e}", exc_info=True)
                # Continue processing even if save fails

    return incidents

