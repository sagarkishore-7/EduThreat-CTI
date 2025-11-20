from __future__ import annotations

import logging
import random
import time
from typing import Callable, Iterable, List, Optional, Sequence
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup

from src.edu_cti.core import config
from src.edu_cti.core.http import HttpClient
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.utils import now_utc_iso
from .common import (
    default_client,
    extract_date,
    fetch_html,
    matches_keywords,
    prepare_keywords,
)

# Selenium imports for Dark Reading
try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

SOURCE_NAME = config.SOURCE_DARKREADING
BASE_URL = "https://www.darkreading.com"
logger = logging.getLogger(__name__)


def _build_search_url(term: str, page: Optional[int] = None) -> str:
    """Build search URL with optional page parameter."""
    params = {"q": term}
    url = f"{BASE_URL}/search?{urlencode(params)}"
    if page and page > 1:
        url += f"&page={page}"
    return url


def _discover_last_page(soup: Optional[BeautifulSoup]) -> int:
    """Extract the last page number from pagination navigation."""
    if not soup:
        return 1
    
    pagination = soup.select_one("nav[aria-label='Pagination Navigation']")
    if not pagination:
        return 1
    
    max_page = 1
    # Look for all page number links
    page_links = pagination.select("a.Pagination-PageNumber")
    for link in page_links:
        href = link.get("href", "")
        if "page=" in href:
            try:
                # Extract page number from href like "/search?q=university&page=95"
                page_val = int(href.split("page=")[-1].split("&")[0])
                max_page = max(max_page, page_val)
            except (ValueError, IndexError):
                continue
    
    # Also check the text content of the last visible page number
    try:
        last_visible = pagination.select("a.Pagination-PageNumber:not(.Pagination-PageNumber_current)")
        if last_visible:
            last_text = last_visible[-1].get_text(strip=True)
            if last_text.isdigit():
                max_page = max(max_page, int(last_text))
    except Exception:
        pass
    
    logger.info("Dark Reading detected last page=%s", max_page)
    return max_page


def _has_search_results(soup: BeautifulSoup) -> bool:
    """Check if the page contains search results (handles ad popups)."""
    # Look for the SearchResult-Content div which contains the actual articles
    search_result_content = soup.select_one("div.SearchResult-Content")
    if search_result_content:
        # Check if there are actual article previews
        content_previews = search_result_content.select("div.ContentPreview.SearchResult-ContentPreview")
        return len(content_previews) > 0
    return False


def _extract_articles_from_page(soup: BeautifulSoup) -> List[BeautifulSoup]:
    """Extract article nodes from SearchResult-Content."""
    articles = []
    
    # Find the SearchResult-Content container
    search_result_content = soup.select_one("div.SearchResult-Content")
    if not search_result_content:
        return articles
    
    # Extract all ContentPreview nodes from SearchResult-ContentList
    content_lists = search_result_content.select("div.SearchResult-ContentList")
    for content_list in content_lists:
        previews = content_list.select("div.ContentPreview.SearchResult-ContentPreview")
        articles.extend(previews)
    
    logger.debug(f"Dark Reading extracted {len(articles)} articles from page")
    return articles


def _fetch_page_with_selenium(url: str, wait_for_results: bool = True) -> Optional[BeautifulSoup]:
    """
    Use Selenium with bot evading mechanisms to fetch a page.
    Handles ad popups by waiting for search results to appear.
    """
    if not SELENIUM_AVAILABLE:
        logger.warning("Selenium not available for Dark Reading")
        return None
    
    try:
        options = uc.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        # Random user agent
        user_agents = config.HTTP_USER_AGENTS
        if user_agents:
            options.add_argument(f"--user-agent={random.choice(user_agents)}")
        
        driver = uc.Chrome(options=options, version_main=None)
        try:
            logger.debug(f"Dark Reading: Fetching {url} with Selenium")
            driver.get(url)
            
            # Random delay to mimic human behavior
            time.sleep(random.uniform(2, 4))
            
            if wait_for_results:
                # Wait for search results to appear (handles ad popups)
                try:
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.SearchResult-Content"))
                    )
                    # Additional wait for content to render
                    time.sleep(random.uniform(1, 2))
                except TimeoutException:
                    logger.warning(f"Dark Reading: Timeout waiting for search results on {url}")
                    # Still try to get the page source
                    pass
            
            # Check if we have results
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            
            if wait_for_results and not _has_search_results(soup):
                logger.warning(f"Dark Reading: No search results found on {url} (may be ad popup)")
                # Try waiting a bit more
                time.sleep(random.uniform(3, 5))
                html = driver.page_source
                soup = BeautifulSoup(html, "html.parser")
            
            return soup
            
        finally:
            driver.quit()
            
    except Exception as e:
        logger.error(f"Dark Reading: Selenium fetch failed for {url}: {e}")
        return None


def _iter_pages(
    client: HttpClient,
    term: str,
    max_pages: Optional[int],
) -> Iterable[tuple[int, Optional[BeautifulSoup]]]:
    """
    Iterate through search result pages for a given term.
    First tries get_soup (faster), falls back to Selenium if no articles found or blocked.
    """
    # Fetch first page - try get_soup first (faster)
    first_url = _build_search_url(term, page=1)
    first = None
    
    # Try get_soup first
    logger.debug(f"Dark Reading: Trying get_soup for first page of term '{term}'")
    first = client.get_soup(first_url, use_selenium_fallback=True)
    
    # If get_soup returned None or no results found, try Selenium
    if first is None or not _has_search_results(first):
        logger.info(f"Dark Reading: get_soup failed or no results found, trying Selenium for term '{term}'")
    first = _fetch_page_with_selenium(first_url, wait_for_results=True)
    
    if first is None:
        logger.warning("Dark Reading failed to fetch initial page for term '%s'", term)
        return
    
    if not _has_search_results(first):
        logger.warning("Dark Reading: No search results found on first page for term '%s'", term)
        return
    
    yield 1, first
    
    # Discover total pages from pagination
    last_page = _discover_last_page(first)
    
    # Determine how many pages to fetch
    if max_pages is not None:
        limit = min(max_pages, last_page)
    else:
        limit = last_page
    
    logger.info(f"Dark Reading term '{term}': total pages={last_page}, fetching up to page {limit}")
    
    # For subsequent pages, try get_soup first, fallback to Selenium
    for page in range(2, limit + 1):
        page_url = _build_search_url(term, page=page)
        logger.debug(f"Dark Reading term '{term}': fetching page {page}")
        
        # Try get_soup first
        soup = client.get_soup(page_url, use_selenium_fallback=True)
        
        # If get_soup failed or no results, try Selenium
        if soup is None or not _has_search_results(soup):
            logger.info(f"Dark Reading: get_soup failed or no results on page {page}, trying Selenium")
        soup = _fetch_page_with_selenium(page_url, wait_for_results=True)
        
        if soup is None or not _has_search_results(soup):
            logger.warning(f"Dark Reading: Failed to fetch or no results on page {page} for term '{term}'")
            break
        
        yield page, soup
        
        # Random delay between pages to avoid detection
        if page < limit:
            time.sleep(random.uniform(2, 4))


def build_darkreading_incidents(
    *,
    search_terms: Optional[Sequence[str]] = None,
    max_pages: Optional[int] = None,
    keywords: Optional[Sequence[str]] = None,
    client: Optional[HttpClient] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
) -> List[BaseIncident]:
    """
    Crawl Dark Reading search results for EDU-related incidents.
    Uses search URL format: https://www.darkreading.com/search?q=university
    Supports incremental saving via save_callback - saves after each page is processed.
    
    Args:
        save_callback: Optional callback to save incidents incrementally.
                      Called after each page is processed with incidents from that page.
    """
    http_client = client or default_client()
    prepared_keywords = prepare_keywords(keywords)
    terms = list(search_terms or ["university", "school", "college"])
    incidents: List[BaseIncident] = []
    seen_urls: set[str] = set()
    ingested_at = now_utc_iso()

    for term in terms:
        logger.info(f"Dark Reading: Starting search for term '{term}'")
        # If max_pages is None, fetch all pages (None means no limit)
        page_limit = max_pages if max_pages is not None else None
        
        for page_number, soup in _iter_pages(http_client, term, page_limit):
            if soup is None:
                break

            # Extract articles from the page
            article_nodes = _extract_articles_from_page(soup)
            
            if not article_nodes:
                logger.warning(f"Dark Reading: No articles found on page {page_number} for term '{term}'")
                continue

            page_incidents: List[BaseIncident] = []
            for node in article_nodes:
                # Extract article link from ListPreview-Title
                title_link = node.select_one("a.ListPreview-Title[href]")
                if not title_link:
                    # Fallback: try any link in the preview
                    title_link = node.find("a", href=True)
                    if not title_link:
                        continue
                
                href = title_link.get("href", "").strip()
                if not href:
                    continue
                
                article_url = urljoin(BASE_URL, href)
                if not article_url or article_url in seen_urls:
                    continue

                # Extract title
                title = title_link.get_text(" ", strip=True)
                if not title:
                    continue

                # Extract summary/description (usually not available in preview, but check)
                summary = ""
                summary_sel = node.select_one("div.ListPreview-Description, .p-description, p")
                if summary_sel:
                    summary = summary_sel.get_text(" ", strip=True)

                # Combine title and summary for keyword matching
                text_blob = " ".join(filter(None, [title, summary]))
                if not matches_keywords(text_blob, prepared_keywords):
                    continue

                seen_urls.add(article_url)

                # Extract date from ListPreview-Date
                raw_date = ""
                date_elem = node.select_one("span.ListPreview-Date[data-testid='list-preview-date']")
                if date_elem:
                    raw_date = date_elem.get_text(strip=True)
                
                # Fallback: check for time tag
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
                logger.debug(f"Dark Reading: Extracted article '{title[:50]}...' from page {page_number}")
            
            # Save incidents from this page incrementally if callback provided
            if save_callback is not None and page_incidents:
                try:
                    save_callback(page_incidents)
                    logger.debug(f"Dark Reading: Saved {len(page_incidents)} incidents from page {page_number} for term '{term}'")
                except Exception as e:
                    logger.error(f"Dark Reading: Error saving page {page_number} for term '{term}': {e}", exc_info=True)
                    # Continue processing even if save fails

        logger.info(f"Dark Reading: Found {len([i for i in incidents if i.source == SOURCE_NAME])} incidents for term '{term}'")

    logger.info(f"Dark Reading: Total incidents collected: {len(incidents)}")
    return incidents

