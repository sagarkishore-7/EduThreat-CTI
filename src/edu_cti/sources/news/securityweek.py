from __future__ import annotations

import logging
import random
import re
import time
from typing import Callable, Iterable, List, Optional, Sequence
from urllib.parse import urlencode, urlparse, parse_qs

from bs4 import BeautifulSoup

from src.edu_cti.core import config
from src.edu_cti.core.http import HttpClient
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.pagination import extract_last_page_from_attr
from src.edu_cti.core.utils import now_utc_iso
from .common import (
    default_client,
    extract_date,
    fetch_html,
    matches_keywords,
    prepare_keywords,
)

# Selenium imports for SecurityWeek
try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

SOURCE_NAME = config.SOURCE_SECURITYWEEK
BASE_URL = "https://www.securityweek.com/"
logger = logging.getLogger(__name__)


def _search_url(term: str, page: int) -> str:
    params = {"s": term}
    if page > 1:
        params["page"] = page
    return f"{BASE_URL}?{urlencode(params)}"


def _fetch_page_with_selenium(url: str, wait_for_articles: bool = True) -> Optional[BeautifulSoup]:
    """
    Use Selenium with bot evading mechanisms to fetch a SecurityWeek search page.
    Waits for Algolia search results to load.
    """
    if not SELENIUM_AVAILABLE:
        logger.warning("Selenium not available for SecurityWeek")
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
            logger.debug(f"SecurityWeek: Fetching {url} with Selenium")
            driver.get(url)
            
            # Random delay to mimic human behavior
            time.sleep(random.uniform(2, 4))
            
            if wait_for_articles:
                # Wait for Algolia search results to appear
                try:
                    # Try multiple selectors - wait for any of them
                    wait = WebDriverWait(driver, 20)
                    found = False
                    for selector in [
                        "div#algolia-hits",
                        "li.ais-Hits-item",
                        "ol.ais-Hits-list"
                    ]:
                        try:
                            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                            found = True
                            break
                        except TimeoutException:
                            continue
                    
                    if not found:
                        logger.warning(f"SecurityWeek: None of the expected selectors found on {url}")
                    
                    # Additional wait for content to render
                    time.sleep(random.uniform(1, 2))
                    
                    # Verify articles actually exist
                    articles = driver.find_elements(By.CSS_SELECTOR, "li.ais-Hits-item")
                    if not articles:
                        logger.warning(f"SecurityWeek: No articles found after waiting on {url}")
                        # Wait a bit more and check again
                        time.sleep(random.uniform(2, 3))
                        articles = driver.find_elements(By.CSS_SELECTOR, "li.ais-Hits-item")
                        if not articles:
                            logger.warning(f"SecurityWeek: Still no articles found on {url}")
                        else:
                            logger.info(f"SecurityWeek: Found {len(articles)} articles after additional wait")
                    else:
                        logger.debug(f"SecurityWeek: Found {len(articles)} articles on {url}")
                except TimeoutException:
                    logger.warning(f"SecurityWeek: Timeout waiting for search results on {url}")
                    # Still try to get the page source
                    pass
            
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            
            # Verify articles exist in the parsed HTML
            if wait_for_articles:
                article_nodes = soup.select("li.ais-Hits-item")
                if not article_nodes:
                    logger.warning(f"SecurityWeek: No articles in parsed HTML from {url}")
            
            return soup
            
        finally:
            driver.quit()
            
    except Exception as e:
        logger.error(f"SecurityWeek: Selenium fetch failed for {url}: {e}")
        return None


def _discover_last_page(client: HttpClient, term: str) -> int:
    # Try get_soup first, fallback to Selenium if needed
    first_url = _search_url(term, 1)
    soup = client.get_soup(first_url, use_selenium_fallback=True)
    
    # If get_soup failed or no articles, try Selenium (Algolia is JS-rendered)
    if soup is None or not _select_article_nodes(soup):
        logger.info("SecurityWeek: get_soup failed or no articles, trying Selenium for pagination discovery")
        soup = _fetch_page_with_selenium(first_url, wait_for_articles=True)
    
    if not soup:
        return 1
    
    pagination = soup.select_one("ul.ais-Pagination-list")
    if not pagination:
        logger.warning("SecurityWeek: No pagination found")
        return 1
    
    # Extract from aria-label of last page link: "Last Page, Page 50"
    last_page_link = pagination.select_one("li.ais-Pagination-item--lastPage a[aria-label]")
    if last_page_link:
        aria_label = last_page_link.get("aria-label", "")
        # Extract number from "Last Page, Page 50"
        match = re.search(r'Page\s+(\d+)', aria_label)
        if match:
            max_page = int(match.group(1))
            logger.info("SecurityWeek term '%s' last page=%s (from aria-label)", term, max_page)
            return max_page
    
    # Fallback: use extract_last_page_from_attr
    last_from_attr = extract_last_page_from_attr(pagination)
    if last_from_attr > 1:
        logger.info("SecurityWeek term '%s' last page=%s (from attr)", term, last_from_attr)
        return last_from_attr

    # Fallback: parse href query param
    max_page = 1
    for link in pagination.select("a[href]"):
        qs = parse_qs(urlparse(link["href"]).query)
        if "page" in qs:
            try:
                max_page = max(max_page, int(qs["page"][0]))
            except (ValueError, IndexError):
                continue
    logger.info("SecurityWeek term '%s' last page=%s (from href)", term, max_page)
    return max_page


def _select_article_nodes(soup: BeautifulSoup) -> List[BeautifulSoup]:
    """Select article nodes from SecurityWeek search results."""
    # Primary selector for Algolia search results
    nodes = soup.select("li.ais-Hits-item")
    if nodes:
        return nodes
    
    # Fallback selectors for other page types
    selectors = [
        "article",
        ".td_module_16",
        ".td-block-span6",
    ]
    for sel in selectors:
        nodes = soup.select(sel)
        if nodes:
            return nodes
    return []


def _iter_pages(
    client: HttpClient,
    term: str,
    max_pages: Optional[int],
) -> Iterable[tuple[int, Optional[BeautifulSoup]]]:
    """
    Iterate through search result pages for a given term.
    First tries get_soup (faster), falls back to Selenium if no articles found (Algolia JS).
    """
    # Discover total pages from first page
    last_page = _discover_last_page(client, term)
    
    # Determine how many pages to fetch
    if max_pages is not None:
        limit = min(max_pages, last_page)
    else:
        limit = last_page
    
    logger.info(
        "SecurityWeek crawling term '%s' up to %s pages (last=%s)",
        term,
        limit,
        last_page,
    )
    
    # Fetch first page - try get_soup first (faster)
    first_url = _search_url(term, 1)
    logger.debug(f"SecurityWeek: Trying get_soup for first page of term '{term}'")
    first_soup = client.get_soup(first_url, use_selenium_fallback=True)
    
    # If get_soup failed or no articles, try Selenium (Algolia is JS-rendered)
    article_nodes = []
    if first_soup is not None:
        article_nodes = _select_article_nodes(first_soup)
    
    if first_soup is None or not article_nodes:
        logger.info(f"SecurityWeek: get_soup failed or no articles, trying Selenium for first page")
        first_soup = _fetch_page_with_selenium(first_url, wait_for_articles=True)
        if first_soup is not None:
            article_nodes = _select_article_nodes(first_soup)
    
    if first_soup is None:
        logger.warning(f"SecurityWeek: Failed to fetch first page for term '{term}'")
        return
    
    # Verify articles exist
    if not article_nodes:
        logger.warning(f"SecurityWeek: No articles found on first page for term '{term}'")
        return
    
    logger.info(f"SecurityWeek: Found {len(article_nodes)} articles on page 1 for term '{term}'")
    yield 1, first_soup
    
    # For subsequent pages, try get_soup first, fallback to Selenium
    for page in range(2, limit + 1):
        page_url = _search_url(term, page)
        logger.debug(f"SecurityWeek term '{term}': fetching page {page}")
        
        # Try get_soup first
        soup = client.get_soup(page_url, use_selenium_fallback=True)
        
        # If get_soup failed or no articles, try Selenium
        article_nodes = []
        if soup is not None:
            article_nodes = _select_article_nodes(soup)
        
        if soup is None or not article_nodes:
            logger.info(f"SecurityWeek: get_soup failed or no articles on page {page}, trying Selenium")
            soup = _fetch_page_with_selenium(page_url, wait_for_articles=True)
            if soup is not None:
                article_nodes = _select_article_nodes(soup)
        
        if soup is None:
            logger.warning(f"SecurityWeek: Failed to fetch page {page} for term '{term}'")
            break
        
        # Verify articles exist
        if not article_nodes:
            logger.warning(f"SecurityWeek: No articles found on page {page} for term '{term}'")
            break
        
        logger.debug(f"SecurityWeek: Found {len(article_nodes)} articles on page {page} for term '{term}'")
        yield page, soup
        
        # Random delay between pages to avoid detection
        if page < limit:
            time.sleep(random.uniform(2, 4))


def build_securityweek_incidents(
    *,
    search_terms: Optional[Sequence[str]] = None,
    max_pages: Optional[int] = None,
    keywords: Optional[Sequence[str]] = None,
    client: Optional[HttpClient] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
) -> List[BaseIncident]:
    """
    Use SecurityWeek's site search (default query 'college') to collect EDU articles.
    Supports incremental saving via save_callback - saves after each page is processed.
    
    Args:
        save_callback: Optional callback to save incidents incrementally.
                      Called after each page is processed with incidents from that page.
    """
    http_client = client or default_client()
    prepared_keywords = prepare_keywords(keywords)
    terms = list(search_terms or ["college", "university", "school"])
    seen_urls: set[str] = set()
    incidents: List[BaseIncident] = []
    ingested_at = now_utc_iso()

    for term in terms:
        logger.info(f"SecurityWeek: Starting search for term '{term}'")
        # If max_pages is None, fetch all pages (None means no limit)
        page_limit = max_pages if max_pages is not None else None
        for page_number, soup in _iter_pages(http_client, term, page_limit):
            if soup is None:
                break

            article_nodes = _select_article_nodes(soup)
            logger.debug(f"SecurityWeek: Processing {len(article_nodes)} articles from page {page_number} for term '{term}'")
            
            page_incidents: List[BaseIncident] = []
            for node in article_nodes:
                # Extract article link from Algolia search results
                # Look for: <a href="..." class="ais-hits--title-link">
                title_link = node.select_one("a.ais-hits--title-link[href]")
                if not title_link:
                    # Fallback: try any link in the article
                    title_link = node.find("a", href=True)
                    if not title_link:
                        continue
                
                article_url = title_link.get("href", "").strip()
                if not article_url or article_url in seen_urls:
                    continue

                # Extract title from the link
                title = title_link.get_text(" ", strip=True)
                if not title:
                    continue

                # Extract summary/excerpt from Algolia results
                # Look for: <span class="suggestion-post-content ais-hits--content-snippet">
                summary = ""
                summary_tag = node.select_one("span.suggestion-post-content.ais-hits--content-snippet")
                if summary_tag:
                    summary = summary_tag.get_text(" ", strip=True)
                else:
                    # Fallback: try other selectors
                    for sel in (".entry-summary", ".td-excerpt", "p"):
                        tag = node.select_one(sel)
                        if tag:
                            summary = tag.get_text(" ", strip=True)
                            break

                text_blob = " ".join(filter(None, [title, summary]))
                if not matches_keywords(text_blob, prepared_keywords):
                    continue

                seen_urls.add(article_url)

                # Extract date from Algolia results
                # Look for: <time class="post-date updated" itemprop="datePublished">
                raw_date = ""
                time_tag = node.select_one("time.post-date.updated[itemprop='datePublished']")
                if time_tag:
                    raw_date = time_tag.get("datetime") or time_tag.get_text(strip=True)
                else:
                    # Fallback: try any time tag
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
                logger.debug(f"SecurityWeek: Extracted article '{title[:50]}...' from page {page_number}")
            
            # Save incidents from this page incrementally if callback provided
            if save_callback is not None and page_incidents:
                try:
                    save_callback(page_incidents)
                    logger.debug(f"SecurityWeek: Saved {len(page_incidents)} incidents from page {page_number} for term '{term}'")
                except Exception as e:
                    logger.error(f"SecurityWeek: Error saving page {page_number} for term '{term}': {e}", exc_info=True)
                    # Continue processing even if save fails

        logger.info(f"SecurityWeek: Found {len([i for i in incidents if i.source == SOURCE_NAME])} incidents for term '{term}'")

    logger.info(f"SecurityWeek: Total incidents collected: {len(incidents)}")
    return incidents

