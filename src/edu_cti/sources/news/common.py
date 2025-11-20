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
DEFAULT_MAX_PAGES = config.NEWS_MAX_PAGES


def prepare_keywords(
    keywords: Optional[Sequence[str]] = None,
) -> List[str]:
    return [k.lower() for k in (keywords or DEFAULT_NEWS_KEYWORDS)]


def matches_keywords(text: str, keywords: Iterable[str]) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if is_edu_keyword_in_text(lowered):
        return True
    return any(k in lowered for k in keywords)


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

