from __future__ import annotations

import logging
import random
import re
import time
from typing import Callable, Iterable, List, Optional, Sequence
from urllib.parse import urlencode, quote_plus

from bs4 import BeautifulSoup

from src.edu_cti.core import config
from src.edu_cti.core.http import HttpClient
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.utils import now_utc_iso
from .common import (
    DEFAULT_MAX_PAGES,
    default_client,
    extract_date,
    matches_keywords,
    prepare_keywords,
)

# Selenium imports for The Hacker News (Google Custom Search)
try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

BASE_URL = "https://thehackernews.com"
CSE_BASE_URL = "https://cse.google.com/cse"
CSE_CX = "partner-pub-7983783048239650:3179771210"  # Fallback if not found on page
SOURCE_NAME = "thehackernews"
logger = logging.getLogger(__name__)


def _extract_form_params_from_homepage(driver) -> Optional[str]:
    """
    Extract the cx parameter from the search form on the homepage.
    Returns the cx value or None if not found.
    """
    try:
        # Find the search form - try primary selector first
        search_form = None
        try:
            search_form = driver.find_element(By.CSS_SELECTOR, "form#searchform")
        except Exception:
            # Try alternative selector
            try:
                search_form = driver.find_element(By.CSS_SELECTOR, "form[action*='google.com/cse']")
            except Exception:
                logger.warning("The Hacker News: Could not find search form on homepage")
                return None
        
        if not search_form:
            return None
        
        # Find the hidden cx input field
        try:
            cx_input = search_form.find_element(By.CSS_SELECTOR, "input[name='cx']")
            cx_value = cx_input.get_attribute("value")
            
            if cx_value:
                logger.debug(f"The Hacker News: Extracted cx parameter from homepage: {cx_value}")
                return cx_value
            else:
                logger.warning("The Hacker News: Could not extract cx parameter value from homepage, using fallback")
                return None
        except Exception as e:
            logger.warning(f"The Hacker News: Error finding cx input field: {e}, using fallback")
            return None
    except Exception as e:
        logger.warning(f"The Hacker News: Error extracting form params from homepage: {e}, using fallback")
        return None


def _build_search_url(term: str, cx: Optional[str] = None, page: int = 1) -> str:
    """
    Build Google Custom Search URL for The Hacker News.
    
    Args:
        term: Search term
        cx: Custom search engine ID (cx parameter). If None, uses fallback.
        page: Page number
    """
    # Use provided cx or fallback
    cx_value = cx or CSE_CX
    
    # URL format: https://cse.google.com/cse?q={term}&cx={cx}#gsc.tab=0&gsc.q={term}&gsc.page={page}
    params = {
        "q": term,
        "cx": cx_value,
    }
    url = f"{CSE_BASE_URL}?{urlencode(params)}#gsc.tab=0&gsc.q={quote_plus(term)}&gsc.page={page}"
    return url


def _detect_captcha(soup: BeautifulSoup, page_text: str = "") -> bool:
    """
    Detect if a CAPTCHA page is present in the HTML.
    Returns True if CAPTCHA is detected.
    """
    if not soup and not page_text:
        return False
    
    # Common CAPTCHA indicators
    captcha_indicators = [
        # Google reCAPTCHA
        "recaptcha",
        "g-recaptcha",
        "I'm not a robot",
        "verify you're not a robot",
        # Cloudflare
        "cf-browser-verification",
        "cf_challenge",
        "checking your browser",
        "just a moment",
        # Generic
        "captcha",
        "challenge",
        "verification",
        "unusual traffic",
        "automated requests",
        # Google specific
        "our systems have detected unusual traffic",
        "sorry, we have detected unusual traffic",
    ]
    
    # Combine soup text and page_text for comprehensive checking
    combined_text = ""
    if soup:
        combined_text = soup.get_text(" ", strip=True).lower()
    if page_text:
        combined_text += " " + page_text.lower()
    
    # Check for CAPTCHA in text
    for indicator in captcha_indicators:
        if indicator.lower() in combined_text:
            return True
    
    # Check for specific CAPTCHA elements
    if soup:
        captcha_selectors = [
            "div.g-recaptcha",
            "iframe[src*='recaptcha']",
            "div#cf-wrapper",
            "div.challenge-container",
            "div.rc-anchor",
            "[data-sitekey]",  # reCAPTCHA site key
        ]
        for selector in captcha_selectors:
            if soup.select_one(selector):
                return True
    
    return False


def _fetch_page_with_selenium(
    term: str, 
    page: int = 1, 
    cx: Optional[str] = None,
    wait_for_results: bool = True, 
    retry_count: int = 0,
    driver: Optional[object] = None
) -> Optional[BeautifulSoup]:
    """
    Use Selenium with bot evading mechanisms to fetch Google CSE results.
    Starts from the homepage, extracts form parameters dynamically, types in search bar,
    then navigates to Google CSE results.
    Handles JavaScript-rendered search results and CAPTCHA detection.
    
    Args:
        term: Search term to query
        page: Page number (default: 1)
        cx: Custom search engine ID (cx parameter). If None, will be extracted from homepage.
        wait_for_results: Whether to wait for search results to appear
        retry_count: Number of retries already attempted (for exponential backoff)
        driver: Optional existing Selenium driver to reuse. If None, creates a new one.
    
    Returns:
        BeautifulSoup object or None if failed/CAPTCHA detected
    """
    if not SELENIUM_AVAILABLE:
        logger.warning("Selenium not available for The Hacker News")
        return None
    
    should_close_driver = driver is None
    
    try:
        if driver is None:
            options = uc.ChromeOptions()
            # Use new headless mode for better compatibility
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--disable-extensions")
            
            # Additional bot evasion arguments
            # Note: excludeSwitches and useAutomationExtension removed for Chrome 142+ compatibility
            # undetected-chromedriver already handles bot detection evasion internally
            
            # Language and locale settings to appear more human-like
            options.add_argument("--lang=en-US")
            options.add_argument("--accept-lang=en-US,en;q=0.9")
            
            # Random user agent
            user_agents = config.HTTP_USER_AGENTS
            if user_agents:
                user_agent = random.choice(user_agents)
                options.add_argument(f"--user-agent={user_agent}")
            
            driver = uc.Chrome(options=options, version_main=None)
            
            # Set timeouts
            driver.set_page_load_timeout(30)
            driver.implicitly_wait(5)
            
            # Execute script to hide webdriver property
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                '''
            })
        
        logger.debug(f"The Hacker News: Fetching search results for term '{term}' page {page} with Selenium (retry {retry_count})")
        
        # Step 1: Navigate to homepage
        logger.debug(f"The Hacker News: Navigating to homepage {BASE_URL}")
        driver.get(BASE_URL)
        
        # Random delay to mimic human behavior
        base_delay = 2 if retry_count == 0 else 5
        time.sleep(random.uniform(base_delay, base_delay + 2))
        
        # Step 2: Wait for page to load and verify it loaded correctly
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            logger.debug(f"The Hacker News: Homepage loaded successfully")
        except TimeoutException:
            logger.error(f"The Hacker News: Timeout waiting for homepage to load")
            return None
        
        # Step 3: Extract form parameters (cx) from homepage
        extracted_cx = cx
        if not extracted_cx:
            logger.debug(f"The Hacker News: Extracting cx parameter from homepage form")
            extracted_cx = _extract_form_params_from_homepage(driver)
            if not extracted_cx:
                # Use fallback
                extracted_cx = CSE_CX
                logger.warning(f"The Hacker News: Could not extract cx parameter, using fallback: {extracted_cx}")
            else:
                logger.info(f"The Hacker News: Successfully extracted cx parameter: {extracted_cx}")
            
        # Step 4: Build Google CSE URL with extracted parameters and search term
            cse_url = _build_search_url(term, cx=extracted_cx, page=page)
        logger.info(f"The Hacker News: Built CSE URL for term '{term}' (page {page}): {cse_url}")
            
        # Step 5: Navigate directly to Google CSE URL
        try:
            driver.get(cse_url)
            logger.debug(f"The Hacker News: Navigated to CSE URL")
            
            # Random delay to mimic human behavior (longer on retries)
            time.sleep(random.uniform(base_delay, base_delay + 2))
        except Exception as e:
            logger.error(f"The Hacker News: Error navigating to CSE URL: {e}")
            return None
        
        # Step 5: Get page source and check for CAPTCHA
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        
        # Get page text for CAPTCHA detection (safely)
        page_text = ""
        try:
            body = driver.find_element(By.TAG_NAME, "body")
            page_text = body.text if body else ""
        except Exception:
            # If we can't get body text, use soup text
            page_text = soup.get_text(" ", strip=True)
        
        # Check for CAPTCHA
        if _detect_captcha(soup, page_text):
            logger.error(
                f"The Hacker News: CAPTCHA detected on search for term '{term}'. "
                f"Bot detection has been triggered. Consider: "
                f"1) Using residential proxies, 2) Increasing delays between requests, "
                f"3) Running at different times, 4) Reducing request frequency"
            )
            return None
        
        if wait_for_results:
            # Wait for Google CSE results to appear - try multiple selectors
            try:
                # Try multiple possible selectors for results
                result_found = False
                selectors = [
                    "div.gsc-expansionArea",
                    "div.gsc-webResult",
                    "div.gs-result",
                    "div.gsc-result",
                ]
                for selector in selectors:
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        result_found = True
                        break
                    except TimeoutException:
                        continue
                
                if result_found:
                    # Additional wait for content to render
                    time.sleep(random.uniform(1, 2))
                    # Re-check for CAPTCHA after waiting (sometimes it appears after initial load)
                    html = driver.page_source
                    soup = BeautifulSoup(html, "html.parser")
                    try:
                        body = driver.find_element(By.TAG_NAME, "body")
                        page_text = body.text if body else ""
                    except Exception:
                        page_text = soup.get_text(" ", strip=True)
                    if _detect_captcha(soup, page_text):
                        logger.error(f"The Hacker News: CAPTCHA detected after waiting on search for term '{term}'")
                        return None
                else:
                    logger.warning(f"The Hacker News: No results found with any selector on search for term '{term}'")
                    # Check for CAPTCHA before giving up
                    if _detect_captcha(soup, page_text):
                        logger.error(f"The Hacker News: CAPTCHA detected (no results found) on search for term '{term}'")
                        return None
                    # Still try to get the page source - maybe results are there but selector is wrong
                    time.sleep(1)
                    html = driver.page_source
                    soup = BeautifulSoup(html, "html.parser")
            except TimeoutException:
                logger.warning(f"The Hacker News: Timeout waiting for search results on search for term '{term}'")
                # Check for CAPTCHA
                if _detect_captcha(soup, page_text):
                    logger.error(f"The Hacker News: CAPTCHA detected (timeout) on search for term '{term}'")
                    return None
                # Still try to get the page source
                time.sleep(1)
                html = driver.page_source
                soup = BeautifulSoup(html, "html.parser")
        
        # Final CAPTCHA check before returning
        if _detect_captcha(soup, page_text):
            logger.error(f"The Hacker News: CAPTCHA detected in final check on search for term '{term}'")
            return None
        
        return soup
        
    except Exception as e:
        logger.error(f"The Hacker News: Selenium fetch failed for term '{term}': {e}")
        return None
    finally:
        # Only close driver if we created it
        if should_close_driver and driver:
            try:
                driver.quit()
            except Exception as e:
                logger.debug(f"The Hacker News: Error closing driver: {e}")


def _discover_last_page(soup: Optional[BeautifulSoup]) -> int:
    """Extract the last page number from Google CSE pagination."""
    if not soup:
        return 1
    
    # Look for pagination in div.gsc-cursor-box
    pagination = soup.select_one("div.gsc-cursor-box")
    if not pagination:
        return 1
    
    max_page = 1
    # Look for all page number divs
    page_divs = pagination.select("div.gsc-cursor-page")
    for page_div in page_divs:
        # Get text content (page number)
        page_text = page_div.get_text(strip=True)
        if page_text.isdigit():
            max_page = max(max_page, int(page_text))
    
    logger.info(f"The Hacker News detected last page={max_page}")
    return max_page


def _extract_articles_from_page(soup: BeautifulSoup) -> List[BeautifulSoup]:
    """
    Extract article nodes from Google CSE results.
    Also checks for CAPTCHA before extracting.
    """
    articles = []
    
    # Check for CAPTCHA first
    if _detect_captcha(soup):
        logger.warning("The Hacker News: CAPTCHA detected in page content, skipping article extraction")
        return articles
    
    # Try multiple selectors for Google CSE results
    # Google may change selectors or structure
    
    # First try: standard gsc-expansionArea
    expansion_area = soup.select_one("div.gsc-expansionArea")
    if expansion_area:
        results = expansion_area.select("div.gsc-webResult.gsc-result, div.gs-result")
        articles.extend(results)
    
    # Second try: direct results if expansion area not found
    if not articles:
        results = soup.select("div.gsc-webResult.gsc-result, div.gs-result, div.gsc-result")
        articles.extend(results)
    
    # Third try: any result-like div
    if not articles:
        results = soup.select("div[class*='gsc'], div[class*='gs-result']")
        articles.extend(results)
    
    logger.debug(f"The Hacker News extracted {len(articles)} articles from page")
    return articles


def _iter_pages(
    client: HttpClient,
    term: str,
    max_pages: Optional[int],
) -> Iterable[tuple[int, Optional[BeautifulSoup]]]:
    """
    Iterate through Google CSE search result pages for a given term.
    Uses Selenium to navigate from homepage, extract form parameters dynamically,
    type in search bar, and then scrape Google CSE results.
    Includes CAPTCHA detection and graceful handling.
    """
    MAX_RETRIES = 2  # Maximum retries for CAPTCHA/blocked pages
    captcha_encountered = False
    
    def _has_articles(soup: BeautifulSoup) -> bool:
        """Check if soup contains articles."""
        if soup is None:
            return False
        if _detect_captcha(soup):
            return False
        articles = _extract_articles_from_page(soup)
        return len(articles) > 0
    
    # Extract cx parameter once from homepage (will be reused for all pages)
    extracted_cx = None
    driver = None  # Will be created on first use
    
    # Fetch first page using Selenium (starting from homepage)
    first = None
    logger.info(f"The Hacker News: Starting search for term '{term}' from homepage")
    
    # Retry logic with Selenium
    for retry in range(MAX_RETRIES + 1):
        first = _fetch_page_with_selenium(
            term, 
            page=1, 
            cx=extracted_cx, 
            wait_for_results=True, 
            retry_count=retry,
            driver=driver
        )
        
        # Extract cx from driver if we got it but don't have it yet
        if first is not None and extracted_cx is None:
            # The function extracted cx internally, we can reuse driver for efficiency
            # But we'll extract it again on next pages if needed
            pass
        
        if first is not None:
            # Check if CAPTCHA was detected
            if _detect_captcha(first):
                captcha_encountered = True
                logger.error(f"The Hacker News: CAPTCHA detected on first page for term '{term}' (retry {retry})")
                if retry < MAX_RETRIES:
                    # Exponential backoff before retry
                    backoff_time = (2 ** retry) * random.uniform(5, 10)
                    logger.info(f"The Hacker News: Waiting {backoff_time:.1f}s before retry...")
                    time.sleep(backoff_time)
                    continue
                else:
                    logger.error(f"The Hacker News: CAPTCHA persists after {MAX_RETRIES} retries for term '{term}'")
                    if driver:
                        try:
                            driver.quit()
                        except Exception:
                            pass
                    return
            # Check if we have articles
            if _has_articles(first):
                break
            elif retry < MAX_RETRIES:
                backoff_time = (2 ** retry) * random.uniform(3, 7)
                logger.warning(f"The Hacker News: No articles found, retrying in {backoff_time:.1f}s...")
                time.sleep(backoff_time)
                continue
        elif retry < MAX_RETRIES:
            # Exponential backoff before retry
            backoff_time = (2 ** retry) * random.uniform(3, 7)
            logger.warning(f"The Hacker News: Failed to fetch first page, retrying in {backoff_time:.1f}s...")
            time.sleep(backoff_time)
    
    if first is None:
        logger.warning(f"The Hacker News failed to fetch initial page for term '{term}' after {MAX_RETRIES} retries")
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        return
    
    # Final check for articles
    article_nodes = _extract_articles_from_page(first)
    if not article_nodes:
        # Check if it's because of CAPTCHA
        if _detect_captcha(first):
            logger.error(f"The Hacker News: CAPTCHA detected - no search results for term '{term}'")
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
            return
        logger.warning(f"The Hacker News: No search results found on first page for term '{term}' (not CAPTCHA)")
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        return
    
    if captcha_encountered:
        logger.warning(f"The Hacker News: CAPTCHA encountered but page loaded successfully after retry for term '{term}'")
    
    yield 1, first
    
    # Discover total pages from pagination
    last_page = _discover_last_page(first)
    
    # Determine how many pages to fetch
    if max_pages is not None:
        limit = min(max_pages, last_page)
    else:
        limit = last_page
    
    logger.info(f"The Hacker News term '{term}': total pages={last_page}, fetching up to page {limit}")
    
    # For subsequent pages, use Selenium with the same approach (from homepage, then navigate to CSE)
    # Note: For efficiency, we could reuse the driver, but for safety we create a new one per page
    # to avoid any session issues
    for page in range(2, limit + 1):
        logger.debug(f"The Hacker News term '{term}': fetching page {page}")
        
        soup = None
        # Retry logic with Selenium
        for retry in range(MAX_RETRIES + 1):
            soup = _fetch_page_with_selenium(
                term, 
                page=page, 
                cx=extracted_cx, 
                wait_for_results=True, 
                retry_count=retry,
                driver=None  # Create new driver for each page for safety
            )
            if soup is not None:
                # Check for CAPTCHA
                if _detect_captcha(soup):
                    captcha_encountered = True
                    logger.error(f"The Hacker News: CAPTCHA detected on page {page} for term '{term}' (retry {retry})")
                    if retry < MAX_RETRIES:
                        backoff_time = (2 ** retry) * random.uniform(5, 10)
                        logger.info(f"The Hacker News: Waiting {backoff_time:.1f}s before retry...")
                        time.sleep(backoff_time)
                        continue
                    else:
                        logger.error(f"The Hacker News: CAPTCHA persists on page {page} after {MAX_RETRIES} retries. Stopping crawl for term '{term}'")
                        if driver:
                            try:
                                driver.quit()
                            except Exception:
                                pass
                        return  # Stop entire crawl if CAPTCHA persists
                # Check if we have articles
                if _has_articles(soup):
                    break
                elif retry < MAX_RETRIES:
                    backoff_time = (2 ** retry) * random.uniform(3, 7)
                    logger.warning(f"The Hacker News: No articles on page {page}, retrying in {backoff_time:.1f}s...")
                    time.sleep(backoff_time)
                    continue
            elif retry < MAX_RETRIES:
                backoff_time = (2 ** retry) * random.uniform(3, 7)
                logger.warning(f"The Hacker News: Failed to fetch page {page}, retrying in {backoff_time:.1f}s...")
                time.sleep(backoff_time)
        
        if soup is None:
            logger.warning(f"The Hacker News: Failed to fetch page {page} for term '{term}' after {MAX_RETRIES} retries")
            break
        
        # Verify articles exist
        article_nodes = _extract_articles_from_page(soup)
        if not article_nodes:
            # Check if it's because of CAPTCHA
            if _detect_captcha(soup):
                logger.error(f"The Hacker News: CAPTCHA detected - no articles on page {page} for term '{term}'")
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                return  # Stop crawl
            logger.warning(f"The Hacker News: No articles found on page {page} for term '{term}' (not CAPTCHA)")
            break
        
        yield page, soup
        
        # Longer random delay between pages to avoid detection (especially after CAPTCHA)
        if page < limit:
            delay_min = 4 if captcha_encountered else 2
            delay_max = 8 if captcha_encountered else 4
            time.sleep(random.uniform(delay_min, delay_max))


def build_thehackernews_incidents(
    *,
    search_terms: Optional[Sequence[str]] = None,
    max_pages: Optional[int] = None,
    keywords: Optional[Sequence[str]] = None,
    client: Optional[HttpClient] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
) -> List[BaseIncident]:
    """
    Crawl The Hacker News Google Custom Search results for EDU-related incidents.
    Uses Google CSE URL format: https://cse.google.com/cse?q={term}&cx={cx}#gsc.tab=0&gsc.q={term}&gsc.page={page}
    Supports incremental saving via save_callback - saves after each page is processed.
    
    Args:
        save_callback: Optional callback to save incidents incrementally.
                      Called after each page is processed with incidents from that page.
    """
    http_client = client or default_client()
    prepared_keywords = prepare_keywords(keywords)
    terms = list(search_terms or ["school", "university", "college"])
    incidents: List[BaseIncident] = []
    seen_urls: set[str] = set()
    ingested_at = now_utc_iso()

    for term in terms:
        logger.info(f"The Hacker News: Starting search for term '{term}'")
        # If max_pages is None, fetch all pages (None means no limit)
        page_limit = max_pages  # None = fetch all pages
        
        for page_number, soup in _iter_pages(http_client, term, page_limit):
            if soup is None:
                break

            # Extract articles from the page
            article_nodes = _extract_articles_from_page(soup)
            
            if not article_nodes:
                logger.warning(f"The Hacker News: No articles found on page {page_number} for term '{term}'")
                continue

            page_incidents: List[BaseIncident] = []
            for node in article_nodes:
                # Extract article link from gs-title
                title_link = node.select_one("a.gs-title[href]")
                if not title_link:
                    # Fallback: try data-ctorig attribute
                    title_link = node.select_one("a.gs-title[data-ctorig]")
                    if not title_link:
                        continue
                
                # Prefer data-ctorig (original URL) over href (may be Google tracking URL)
                article_url = title_link.get("data-ctorig", "").strip()
                if not article_url:
                    article_url = title_link.get("href", "").strip()
                
                if not article_url or article_url in seen_urls:
                    continue

                # Extract title
                title = title_link.get_text(" ", strip=True)
                if not title:
                    continue

                # Extract summary/description from gs-snippet
                summary = ""
                snippet_tag = node.select_one("div.gs-bidi-start-align.gs-snippet")
                if snippet_tag:
                    snippet_text = snippet_tag.get_text(" ", strip=True)
                    # Remove date prefix if present (e.g., "3 May 2025 ...")
                    # Date pattern: "DD MMM YYYY ..."
                    date_match = re.match(r"^(\d{1,2}\s+\w{3,9}\s+\d{4})\s+\.\.\.\s*", snippet_text)
                    if date_match:
                        summary = snippet_text[len(date_match.group(0)):].strip()
                    else:
                        summary = snippet_text

                # Combine title and summary for keyword matching
                text_blob = " ".join(filter(None, [title, summary]))
                if not matches_keywords(text_blob, prepared_keywords):
                    continue

                seen_urls.add(article_url)

                # Extract date from snippet (format: "3 May 2025 ...")
                raw_date = ""
                snippet_tag = node.select_one("div.gs-bidi-start-align.gs-snippet")
                if snippet_tag:
                    snippet_text = snippet_tag.get_text(" ", strip=True)
                    # Try to extract date from beginning of snippet
                    date_match = re.match(r"^(\d{1,2}\s+\w{3,9}\s+\d{4})", snippet_text)
                    if date_match:
                        raw_date = date_match.group(1)

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
                logger.debug(f"The Hacker News: Extracted article '{title[:50]}...' from page {page_number}")
            
            # Save incidents from this page incrementally if callback provided
            if save_callback is not None and page_incidents:
                try:
                    save_callback(page_incidents)
                    logger.debug(f"The Hacker News: Saved {len(page_incidents)} incidents from page {page_number} for term '{term}'")
                except Exception as e:
                    logger.error(f"The Hacker News: Error saving page {page_number} for term '{term}': {e}", exc_info=True)
                    # Continue processing even if save fails

        logger.info(f"The Hacker News: Found {len([i for i in incidents if i.source == SOURCE_NAME])} incidents for term '{term}'")

    logger.info(f"The Hacker News: Total incidents collected: {len(incidents)}")
    return incidents

