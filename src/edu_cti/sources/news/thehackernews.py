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
    SearchQueryMetrics,
    build_search_query_variants,
    default_client,
    extract_date,
    is_cancelled,
    matches_keywords,
    page_limit_for_query_variant,
    prepare_keywords,
    prepare_search_queries,
    record_news_query_metrics,
    should_continue_to_next_query_variant,
)

BASE_URL = "https://thehackernews.com"
CSE_BASE_URL = "https://cse.google.com/cse"
CSE_CX = "partner-pub-7983783048239650:3179771210"  # Fallback if not found on page
SOURCE_NAME = "thehackernews"
logger = logging.getLogger(__name__)
_NATIVE_DATE_RE = re.compile(r"\b([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})\b")


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


def _build_native_search_url(term: str) -> str:
    """Build The Hacker News' native search URL.

    The older collector used Google Programmable Search. That endpoint now
    often returns a JavaScript-only shell to non-browser fetchers, which made
    the source appear healthy while yielding zero real article links.
    """
    return f"{BASE_URL}/search?{urlencode({'q': term})}"


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
    Uses HttpClient with Playwright-based wait_selector for JS-rendered content.
    Includes CAPTCHA detection and graceful handling.
    """
    # Fetch first page
    first_url = _build_search_url(term, page=1)
    logger.info(f"The Hacker News: Starting search for term '{term}'")
    first = client.get_soup(first_url, wait_selector="div.gsc-expansionArea")

    if first is None:
        logger.warning(f"The Hacker News failed to fetch initial page for term '{term}'")
        return

    # Check for CAPTCHA
    if _detect_captcha(first):
        logger.error(f"The Hacker News: CAPTCHA detected on first page for term '{term}'")
        return

    # Check for articles
    article_nodes = _extract_articles_from_page(first)
    if not article_nodes:
        logger.warning(f"The Hacker News: No search results found on first page for term '{term}'")
        return

    yield 1, first

    # Discover total pages from pagination
    last_page = _discover_last_page(first)

    # Determine how many pages to fetch
    if max_pages is not None:
        limit = min(max_pages, last_page)
    else:
        limit = last_page

    logger.info(f"The Hacker News term '{term}': total pages={last_page}, fetching up to page {limit}")

    for page in range(2, limit + 1):
        if is_cancelled():
            logger.info("Source term '%s' cancelled at page %s", term, page)
            break
        page_url = _build_search_url(term, page=page)
        logger.debug(f"The Hacker News term '{term}': fetching page {page}")

        soup = client.get_soup(page_url, wait_selector="div.gsc-expansionArea")

        if soup is None:
            logger.warning(f"The Hacker News: Failed to fetch page {page} for term '{term}'")
            break

        # Check for CAPTCHA
        if _detect_captcha(soup):
            logger.error(f"The Hacker News: CAPTCHA detected on page {page} for term '{term}'. Stopping crawl.")
            return

        # Verify articles exist
        article_nodes = _extract_articles_from_page(soup)
        if not article_nodes:
            logger.warning(f"The Hacker News: No articles found on page {page} for term '{term}'")
            break

        yield page, soup

        # Random delay between pages to avoid detection
        if page < limit:
            time.sleep(random.uniform(2, 4))


def _extract_native_articles_from_page(soup: BeautifulSoup) -> List[BeautifulSoup]:
    """Extract article cards from The Hacker News native search page."""
    if _detect_captcha(soup):
        logger.warning("The Hacker News native search returned a challenge page")
        return []
    return list(soup.select("a.story-link[href]"))


def _native_article_fields(node: BeautifulSoup) -> Optional[dict[str, str]]:
    """Normalize a native search card into article fields."""
    article_url = (node.get("href") or "").strip()
    if not article_url or "thehackernews.com/" not in article_url:
        return None

    title_tag = node.select_one(".home-title")
    title = title_tag.get_text(" ", strip=True) if title_tag else ""
    if not title:
        image = node.select_one("img[alt]")
        title = (image.get("alt") or "").strip() if image else ""
    if not title:
        return None

    summary_tag = node.select_one(".home-desc")
    summary = summary_tag.get_text(" ", strip=True) if summary_tag else ""

    date_tag = node.select_one(".h-datetime")
    date_text = date_tag.get_text(" ", strip=True) if date_tag else ""
    date_match = _NATIVE_DATE_RE.search(date_text)
    raw_date = date_match.group(1) if date_match else ""

    tags_tag = node.select_one(".h-tags")
    tags = tags_tag.get_text(" ", strip=True) if tags_tag else ""

    return {
        "url": article_url.rstrip("/"),
        "title": title,
        "summary": summary,
        "raw_date": raw_date,
        "tags": tags,
    }


def _iter_native_pages(
    client: HttpClient,
    term: str,
    max_pages: Optional[int],
) -> Iterable[tuple[int, Optional[BeautifulSoup]]]:
    """Iterate The Hacker News native search pages.

    Native search currently returns the relevant result set on one HTML page.
    We intentionally fetch only page 1 because pagination/load-more is
    JavaScript-driven and the old CSE path is kept only as a fallback helper.
    """
    if max_pages == 0:
        return

    search_url = _build_native_search_url(term)
    logger.info("The Hacker News: native search for term %r", term)
    soup = client.get_soup(search_url)
    if soup is None:
        logger.warning("The Hacker News: native search failed for term %r", term)
        return

    articles = _extract_native_articles_from_page(soup)
    if not articles:
        logger.warning("The Hacker News: native search returned no article cards for term %r", term)
        return

    yield 1, soup


def build_thehackernews_incidents(
    *,
    search_terms: Optional[Sequence[str]] = None,
    max_pages: Optional[int] = None,
    keywords: Optional[Sequence[str]] = None,
    client: Optional[HttpClient] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
) -> List[BaseIncident]:
    """
    Crawl The Hacker News native search results for EDU-related incidents.
    The previous Google CSE path frequently returned a JavaScript-only shell,
    so the collector now uses https://thehackernews.com/search?q=... directly.
    Supports incremental saving via save_callback - saves after each page is processed.

    Args:
        save_callback: Optional callback to save incidents incrementally.
                      Called after each page is processed with incidents from that page.
    """
    http_client = client or default_client()
    prepared_keywords = prepare_keywords(keywords)
    terms = list(search_terms or prepare_search_queries())
    incidents: List[BaseIncident] = []
    seen_urls: set[str] = set()
    ingested_at = now_utc_iso()

    for original_term in terms:
        if is_cancelled():
            logger.info("Source scraping cancelled before term '%s'", original_term)
            break
        for variant in build_search_query_variants(SOURCE_NAME, original_term):
            term = variant.search_query
            metrics = SearchQueryMetrics(
                source=SOURCE_NAME,
                original_query=variant.original_query,
                search_query=term,
                variant_type=variant.variant_type,
                generated_url=_build_native_search_url(term),
            )
            logger.info(
                "The Hacker News: Starting search for term '%s' (%s)",
                variant.original_query,
                variant.variant_type,
            )
            page_limit = page_limit_for_query_variant(variant.variant_type, max_pages)
            for page_number, soup in _iter_native_pages(http_client, term, page_limit):
                if soup is None:
                    metrics.fetch_errors += 1
                    metrics.stop_reason = "fetch_failed"
                    break

                metrics.pages_fetched += 1
                # Extract articles from the page
                article_nodes = _extract_native_articles_from_page(soup)
                metrics.raw_hits += len(article_nodes)

                if not article_nodes:
                    logger.warning(f"The Hacker News: No articles found on page {page_number} for term '{term}'")
                    continue

                page_incidents: List[BaseIncident] = []
                for node in article_nodes:
                    fields = _native_article_fields(node)
                    if fields is None:
                        continue
                    article_url = fields["url"]
                    if not article_url:
                        continue
                    if article_url in seen_urls:
                        metrics.duplicate_skips += 1
                        continue

                    title = fields["title"]
                    summary = fields["summary"]

                    # Combine title and summary for keyword matching
                    text_blob = " ".join(filter(None, [title, summary, fields.get("tags", "")]))
                    if not matches_keywords(text_blob, prepared_keywords):
                        continue

                    seen_urls.add(article_url)

                    incident_date, date_precision = extract_date(fields["raw_date"])

                    incident = BaseIncident(
                        incident_id=make_incident_id(SOURCE_NAME, article_url),
                        source=SOURCE_NAME,
                        source_event_id=article_url.rstrip("/"),
                        institution_name="",
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
                        notes=(
                            f"news_source={SOURCE_NAME};search=native;term={variant.original_query};"
                            f"query_variant={variant.variant_type};page={page_number}"
                        ),
                    )
                    page_incidents.append(incident)
                    incidents.append(incident)
                    logger.debug(f"The Hacker News: Extracted article '{title[:50]}...' from page {page_number}")

                metrics.keyword_matched += len(page_incidents)
                # Save incidents from this page incrementally if callback provided
                if save_callback is not None and page_incidents:
                    try:
                        callback_result = save_callback(page_incidents)
                        if isinstance(callback_result, int):
                            metrics.saved_rows += max(callback_result, 0)
                            metrics.save_result_observed = True
                        logger.debug(f"The Hacker News: Saved {len(page_incidents)} incidents from page {page_number} for term '{term}'")
                    except Exception as e:
                        logger.error(f"The Hacker News: Error saving page {page_number} for term '{term}': {e}", exc_info=True)
                        # Continue processing even if save fails

            if metrics.pages_fetched == 0 and metrics.stop_reason == "completed":
                metrics.stop_reason = "no_pages"

            record_news_query_metrics(metrics)
            logger.info(
                "The Hacker News: term '%s' (%s) summary: %s pages, %s raw hits, %s matched, %s saved",
                variant.original_query,
                variant.variant_type,
                metrics.pages_fetched,
                metrics.raw_hits,
                metrics.keyword_matched,
                metrics.saved_rows,
            )
            if should_continue_to_next_query_variant(metrics):
                logger.info(
                    "The Hacker News term '%s' exact phrase probe complete; continuing to unquoted baseline",
                    variant.original_query,
                )
                continue
            break

    logger.info(f"The Hacker News: Total incidents collected: {len(incidents)}")
    return incidents
