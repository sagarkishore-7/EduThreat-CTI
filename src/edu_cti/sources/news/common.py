from __future__ import annotations

import re
import threading
from typing import Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.edu_cti.core import config
from src.edu_cti.core.http import HttpClient, build_http_client
from src.edu_cti.core.utils import (
    is_edu_keyword_in_text,
    parse_date_with_precision,
)

DEFAULT_NEWS_KEYWORDS: List[str] = config.NEWS_KEYWORDS
DEFAULT_EDUCATION_KEYWORDS: List[str] = config.EDUCATION_KEYWORDS
DEFAULT_SEARCH_QUERIES: List[str] = config.NEWS_SEARCH_QUERIES
CYBER_KEYWORDS: List[str] = [k.lower() for k in config.CYBER_KEYWORDS]
DEFAULT_MAX_PAGES = config.NEWS_MAX_PAGES

# Module-level cancel event — set by PipelineManager to stop news scraping mid-page
_cancel_event = threading.Event()


def is_cancelled() -> bool:
    """Check if scraping has been cancelled by the pipeline manager."""
    return _cancel_event.is_set()


def prepare_keywords(
    keywords: Optional[Sequence[str]] = None,
) -> List[str]:
    combined = list(keywords or DEFAULT_NEWS_KEYWORDS) + list(DEFAULT_EDUCATION_KEYWORDS)
    seen = set()
    prepared: List[str] = []
    for keyword in combined:
        lowered = keyword.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        prepared.append(lowered)
    return prepared


def prepare_search_queries(
    search_terms: Optional[Sequence[str]] = None,
) -> List[str]:
    """Return search queries. Prefer targeted cyber+edu queries over bare keywords."""
    if search_terms:
        return list(search_terms)
    return list(DEFAULT_SEARCH_QUERIES)


def _has_cyber_keyword(text: str) -> bool:
    """Check if text contains at least one cybersecurity-related keyword."""
    return _contains_keyword(text, CYBER_KEYWORDS)


def _normalize_signal_text(text: str) -> str:
    lowered = text.lower()
    lowered = re.sub(r"[-_/]+", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def _contains_keyword(text: str, keywords: Iterable[str]) -> bool:
    normalized = _normalize_signal_text(text)
    collapsed = normalized.replace(" ", "")

    for keyword in keywords:
        normalized_keyword = _normalize_signal_text(keyword)
        if not normalized_keyword:
            continue
        if normalized_keyword in normalized:
            return True

        compact_keyword = normalized_keyword.replace(" ", "")
        if compact_keyword and compact_keyword in collapsed:
            return True

    return False


def matches_keywords(text: str, keywords: Iterable[str]) -> bool:
    """Match text that contains BOTH an education keyword AND a cyber keyword.

    This prevents collecting irrelevant articles (sports, admissions, general
    university news) that happen to mention an educational institution.
    """
    if not text:
        return False
    lowered = text.lower()
    has_edu = is_edu_keyword_in_text(lowered) or _contains_keyword(lowered, keywords)
    if not has_edu:
        return False
    return _has_cyber_keyword(lowered)


def extract_date(raw: Optional[str]) -> Tuple[Optional[str], str]:
    if not raw:
        return None, "unknown"
    date_iso, precision = parse_date_with_precision(raw.strip())
    return (date_iso or None, precision)


def absolute_url(base: str, href: str) -> str:
    if not href:
        return ""
    return urljoin(base, href.strip())


def find_first_text(soup: BeautifulSoup, selector: str) -> str:
    tag = soup.select_one(selector)
    return tag.get_text(" ", strip=True) if tag else ""


def fetch_html(
    url: str,
    *,
    client: Optional[HttpClient] = None,
    allow_404: bool = False,
) -> Optional[BeautifulSoup]:
    """
    Fetch a page with the shared HttpClient. Returns BeautifulSoup or None if 404 allowed.
    """
    http_client = client or default_client()
    return http_client.get_soup(url, allow_404=allow_404)


def default_client() -> HttpClient:
    return build_http_client()
