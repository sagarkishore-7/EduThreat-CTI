from __future__ import annotations

import logging
from typing import Callable, Iterable, List, Optional, Sequence
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from bs4 import BeautifulSoup

from src.edu_cti.core import config
from src.edu_cti.core.http import HttpClient
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.utils import now_utc_iso
from .common import (
    is_cancelled,
    default_client,
    extract_date,
    fetch_html,
    matches_keywords,
    prepare_keywords,
    prepare_search_queries,
)

SOURCE_NAME = config.SOURCE_THERECORD
BASE_URL = "https://therecord.media/search-results"
logger = logging.getLogger(__name__)


def _search_url(term: str) -> str:
    """Build the initial search URL with term parameter."""
    params = {"term": term}
    return f"{BASE_URL}?{urlencode(params)}"


def _find_next_page_link(soup: BeautifulSoup, current_url: str) -> Optional[str]:
    """
    Find the next page link from the pagination HTML.
    Returns the URL of the next page, or None if there's no next page.
    Handles both direct links and JavaScript-based pagination (href="#").
    """
    if not soup:
        return None
    
    # Find the pagination container
    pagination = soup.select_one("ul.ais-Pagination-list")
    if not pagination:
        return None
    
    # Find the next page button
    next_page_item = pagination.select_one("li.ais-Pagination-item--nextPage")
    if not next_page_item:
        return None
    
    # Check if the next page button is disabled
    if "ais-Pagination-item--disabled" in next_page_item.get("class", []):
        return None
    
    # Find the link inside the next page button
    next_link = next_page_item.find("a", class_="ais-Pagination-link", href=True)
    if not next_link:
        return None
    
    href = next_link.get("href", "").strip()
    
    # If href is "#" or empty, construct URL by incrementing page number
    if not href or href == "#":
        # Try to get current page from URL first
        parsed = urlparse(current_url)
        params = parse_qs(parsed.query)
        current_page = 1
        
        # Extract page from URL if present
        if "page" in params and params["page"]:
            try:
                current_page = int(params["page"][0])
            except (ValueError, IndexError):
                pass
        else:
            # Fallback: find current page from pagination HTML
            current_page_item = pagination.select_one("li.ais-Pagination-item--selected")
            if current_page_item:
                page_link = current_page_item.find("a", class_="ais-Pagination-link")
                if page_link:
                    aria_label = page_link.get("aria-label", "")
                    # Extract page number from aria-label like "Page 1"
                    parts = [int(x) for x in aria_label.split() if x.isdigit()]
                    if parts:
                        current_page = parts[0]
        
        # Increment to next page
        next_page_num = current_page + 1
        
        # Construct URL with page parameter
        params["page"] = [str(next_page_num)]
        # Preserve term parameter
        new_query = urlencode(params, doseq=True)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
    
    # Make absolute URL if relative
    return urljoin(current_url, href)


def _select_nodes(soup: BeautifulSoup) -> List[BeautifulSoup]:
    """Select article nodes from The Record's search results."""
    # Articles are in li.ais-Hits-item elements
    nodes = soup.select("li.ais-Hits-item")
    return nodes


def _iter_pages(
    client: HttpClient,
    term: str,
    max_pages: Optional[int],
) -> Iterable[tuple[int, Optional[BeautifulSoup]]]:
    """
    Iterate through pages by following the 'next page' button in pagination.
    Uses HttpClient with Playwright-based wait_selector for JS-rendered Algolia content.
    Continues until max_pages is reached (if specified) or no more next page exists.
    """
    current_url = _search_url(term)
    page_number = 1

    while True:
        # Check for cancellation
        if is_cancelled():
            logger.info("The Record term '%s' cancelled at page %s", term, page_number)
            break

        # Check if we've reached the max_pages limit
        if max_pages is not None and page_number > max_pages:
            logger.info(
                "The Record term '%s' reached max_pages limit (%s)",
                term,
                max_pages,
            )
            break

        # Fetch the current page
        logger.debug("The Record term '%s' fetching page %s from %s", term, page_number, current_url)
        soup = client.get_soup(current_url, wait_selector="li.ais-Hits-item")

        # Check if we got results
        nodes = []
        if soup is not None:
            nodes = _select_nodes(soup)

        if soup is None:
            logger.warning("The Record term '%s' failed to fetch page %s", term, page_number)
            break
        
        # Verify we got results
        if nodes:
            logger.info(f"Found {len(nodes)} articles on page {page_number} for term '{term}'")
        else:
            logger.warning(
                "The Record term '%s' page %s returned no articles (Algolia may not have loaded)",
                term,
                page_number,
            )
        
        yield page_number, soup
        
        # Find the next page link
        next_url = _find_next_page_link(soup, current_url)
        if not next_url:
            logger.info(
                "The Record term '%s' reached end of pagination at page %s",
                term,
                page_number,
            )
            break
        
        # Move to next page
        current_url = next_url
        page_number += 1


def build_therecord_incidents(
    *,
    search_terms: Optional[Sequence[str]] = None,
    max_pages: Optional[int] = None,
    keywords: Optional[Sequence[str]] = None,
    client: Optional[HttpClient] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
) -> List[BaseIncident]:
    """
    Query The Record's on-site search using education keywords for EDU incidents.
    Uses keywords from config as search terms if search_terms not provided.
    Supports incremental saving via save_callback - saves after each page is processed.
    
    Args:
        save_callback: Optional callback to save incidents incrementally.
                      Called after each page is processed with incidents from that page.
    """
    http_client = client or default_client()
    prepared_keywords = prepare_keywords(keywords)
    # Use keywords from config as search terms if not provided
    terms = list(search_terms or prepare_search_queries())
    incidents: List[BaseIncident] = []
    seen_urls: set[str] = set()
    ingested_at = now_utc_iso()

    for term in terms:
        if is_cancelled():
            logger.info("The Record scraping cancelled before term '%s'", term)
            break
        # If max_pages is None, fetch all pages (None means no limit)
        page_limit = max_pages if max_pages is not None else None
        for page_number, soup in _iter_pages(http_client, term, page_limit):
            if soup is None:
                break

            page_incidents: List[BaseIncident] = []
            for node in _select_nodes(soup):
                # Find the article link (a.article-tile, can be --brief or --primary variant)
                link = node.find("a", class_=lambda x: x and "article-tile" in x, href=True)
                if not link:
                    continue
                
                # Get article URL (may be relative, need to make absolute)
                href = link.get("href", "").strip()
                if not href:
                    continue
                article_url = urljoin("https://therecord.media", href)
                if article_url in seen_urls:
                    continue

                # Extract title from h2.article-tile__title
                title = ""
                title_elem = node.select_one("h2.article-tile__title")
                if title_elem:
                    # Get all text from title, including highlighted parts
                    title = title_elem.get_text(" ", strip=True)
                    # Remove "Brief" label if present
                    title = title.replace("Brief", "").strip()

                # Extract summary/snippet from span.ais-Snippet
                summary = ""
                snippet_elem = node.select_one("span.ais-Snippet")
                if snippet_elem:
                    # Get all text from snippet, including highlighted parts
                    summary = snippet_elem.get_text(" ", strip=True)

                # Combine title and summary for keyword matching
                text_blob = " ".join(filter(None, [title, summary]))
                if not matches_keywords(text_blob, prepared_keywords):
                    continue

                seen_urls.add(article_url)

                # Extract date from span.article-tile__meta__date
                raw_date = ""
                date_elem = node.select_one("span.article-tile__meta__date")
                if date_elem:
                    raw_date = date_elem.get_text(strip=True)
                # Also check for time tag as fallback
                if not raw_date:
                    time_tag = node.find("time")
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
                    title=title or None,
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
                    notes=f"news_source={SOURCE_NAME};term={term};page={page_number}",
                )
                page_incidents.append(incident)
                incidents.append(incident)
            
            # Save incidents from this page incrementally if callback provided
            if save_callback is not None and page_incidents:
                try:
                    save_callback(page_incidents)
                    logger.debug(f"The Record: Saved {len(page_incidents)} incidents from page {page_number} for term '{term}'")
                except Exception as e:
                    logger.error(f"The Record: Error saving page {page_number} for term '{term}': {e}", exc_info=True)
                    # Continue processing even if save fails

    return incidents

