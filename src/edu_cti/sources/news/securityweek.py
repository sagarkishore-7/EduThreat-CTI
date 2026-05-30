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
    SearchQueryMetrics,
    build_search_query_variants,
    default_client,
    extract_date,
    fetch_html,
    is_cancelled,
    matches_keywords,
    page_limit_for_query_variant,
    prepare_keywords,
    prepare_search_queries,
    record_news_query_metrics,
    should_continue_to_next_query_variant,
)

SOURCE_NAME = config.SOURCE_SECURITYWEEK
BASE_URL = "https://www.securityweek.com/"
logger = logging.getLogger(__name__)


def _search_url(term: str, page: int) -> str:
    params = {"s": term}
    if page > 1:
        params["page"] = page
    return f"{BASE_URL}?{urlencode(params)}"


def _discover_last_page(client: HttpClient, term: str) -> int:
    first_url = _search_url(term, 1)
    soup = client.get_soup(first_url, wait_selector="div#algolia-hits")

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
    Uses HttpClient with Playwright-based wait_selector for JS-rendered Algolia content.
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

    # Fetch first page
    first_url = _search_url(term, 1)
    logger.debug(f"SecurityWeek: Fetching first page for term '{term}'")
    first_soup = client.get_soup(first_url, wait_selector="div#algolia-hits")

    article_nodes = []
    if first_soup is not None:
        article_nodes = _select_article_nodes(first_soup)

    if first_soup is None:
        logger.warning(f"SecurityWeek: Failed to fetch first page for term '{term}'")
        return

    if not article_nodes:
        logger.warning(f"SecurityWeek: No articles found on first page for term '{term}'")
        return

    logger.info(f"SecurityWeek: Found {len(article_nodes)} articles on page 1 for term '{term}'")
    yield 1, first_soup

    for page in range(2, limit + 1):
        if is_cancelled():
            logger.info("Source term '%s' cancelled at page %s", term, page)
            break
        page_url = _search_url(term, page)
        logger.debug(f"SecurityWeek term '{term}': fetching page {page}")

        soup = client.get_soup(page_url, wait_selector="div#algolia-hits")

        article_nodes = []
        if soup is not None:
            article_nodes = _select_article_nodes(soup)

        if soup is None:
            logger.warning(f"SecurityWeek: Failed to fetch page {page} for term '{term}'")
            break

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
    terms = list(search_terms or prepare_search_queries())
    seen_urls: set[str] = set()
    incidents: List[BaseIncident] = []
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
                generated_url=_search_url(term, 1),
            )
            logger.info(
                "SecurityWeek: Starting search for term '%s' (%s)",
                variant.original_query,
                variant.variant_type,
            )
            # If max_pages is None, fetch all pages (None means no limit)
            page_limit = page_limit_for_query_variant(variant.variant_type, max_pages)
            for page_number, soup in _iter_pages(http_client, term, page_limit):
                if soup is None:
                    metrics.fetch_errors += 1
                    metrics.stop_reason = "fetch_failed"
                    break

                metrics.pages_fetched += 1
                article_nodes = _select_article_nodes(soup)
                metrics.raw_hits += len(article_nodes)
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
                    if not article_url:
                        continue
                    if article_url in seen_urls:
                        metrics.duplicate_skips += 1
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
                            f"news_source={SOURCE_NAME};term={variant.original_query};"
                            f"query_variant={variant.variant_type};page={page_number}"
                        ),
                    )
                    page_incidents.append(incident)
                    incidents.append(incident)
                    logger.debug(f"SecurityWeek: Extracted article '{title[:50]}...' from page {page_number}")

                metrics.keyword_matched += len(page_incidents)
                # Save incidents from this page incrementally if callback provided
                if save_callback is not None and page_incidents:
                    try:
                        callback_result = save_callback(page_incidents)
                        if isinstance(callback_result, int):
                            metrics.saved_rows += max(callback_result, 0)
                            metrics.save_result_observed = True
                        logger.debug(f"SecurityWeek: Saved {len(page_incidents)} incidents from page {page_number} for term '{term}'")
                    except Exception as e:
                        logger.error(f"SecurityWeek: Error saving page {page_number} for term '{term}': {e}", exc_info=True)
                        # Continue processing even if save fails

            if metrics.pages_fetched == 0 and metrics.stop_reason == "completed":
                metrics.stop_reason = "no_pages"

            record_news_query_metrics(metrics)
            logger.info(
                "SecurityWeek: term '%s' (%s) summary: %s pages, %s raw hits, %s matched, %s saved",
                variant.original_query,
                variant.variant_type,
                metrics.pages_fetched,
                metrics.raw_hits,
                metrics.keyword_matched,
                metrics.saved_rows,
            )
            if should_continue_to_next_query_variant(metrics):
                logger.info(
                    "SecurityWeek term '%s' exact phrase probe complete; continuing to unquoted baseline",
                    variant.original_query,
                )
                continue
            break

    logger.info(f"SecurityWeek: Total incidents collected: {len(incidents)}")
    return incidents
