from __future__ import annotations

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
DEFAULT_SEARCH_QUERIES: List[str] = config.NEWS_SEARCH_QUERIES
CYBER_KEYWORDS: List[str] = [k.lower() for k in config.CYBER_KEYWORDS]
DEFAULT_MAX_PAGES = config.NEWS_MAX_PAGES


def prepare_keywords(
    keywords: Optional[Sequence[str]] = None,
) -> List[str]:
    return [k.lower() for k in (keywords or DEFAULT_NEWS_KEYWORDS)]


def prepare_search_queries(
    search_terms: Optional[Sequence[str]] = None,
) -> List[str]:
    """Return search queries. Prefer targeted cyber+edu queries over bare keywords."""
    if search_terms:
        return list(search_terms)
    return list(DEFAULT_SEARCH_QUERIES)


def _has_cyber_keyword(text: str) -> bool:
    """Check if text contains at least one cybersecurity-related keyword."""
    return any(k in text for k in CYBER_KEYWORDS)


def matches_keywords(text: str, keywords: Iterable[str]) -> bool:
    """Match text that contains BOTH an education keyword AND a cyber keyword.

    This prevents collecting irrelevant articles (sports, admissions, general
    university news) that happen to mention an educational institution.
    """
    if not text:
        return False
    lowered = text.lower()
    has_edu = is_edu_keyword_in_text(lowered) or any(k in lowered for k in keywords)
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

