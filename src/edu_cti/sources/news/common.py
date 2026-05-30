from __future__ import annotations

from dataclasses import asdict, dataclass
import re
import threading
from typing import Any, Iterable, List, Optional, Sequence, Tuple
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
EXACT_PHRASE_VARIANT = "exact_phrase"
UNQUOTED_VARIANT = "unquoted"
EXACT_PHRASE_MAX_PAGES = max(int(getattr(config, "NEWS_EXACT_PHRASE_MAX_PAGES", 2)), 0)
EXACT_PHRASE_ENABLED_SOURCES = {
    config.SOURCE_THERECORD,
    config.SOURCE_SECURITYWEEK,
    config.SOURCE_DARKREADING,
    "krebsonsecurity",
    "thehackernews",
}
_QUERY_OPERATOR_RE = re.compile(
    r"(^|\s)(site|after|before|inurl|intitle|source|filetype|ext):",
    re.IGNORECASE,
)

# Module-level cancel event — set by PipelineManager to stop news scraping mid-page
_cancel_event = threading.Event()
_query_metrics_lock = threading.Lock()
_query_metrics: list[dict[str, Any]] = []


@dataclass(frozen=True)
class SearchQueryVariant:
    original_query: str
    search_query: str
    variant_type: str


@dataclass
class SearchQueryMetrics:
    source: str
    original_query: str
    search_query: str
    variant_type: str
    generated_url: str = ""
    raw_hits: int = 0
    keyword_matched: int = 0
    saved_rows: int = 0
    duplicate_skips: int = 0
    pages_fetched: int = 0
    fetch_errors: int = 0
    stop_reason: str = "completed"
    save_result_observed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


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


def can_exact_phrase_query(query: str) -> bool:
    """Return True when a query is safe to retry as a quoted exact phrase."""
    stripped = query.strip()
    if not stripped or " " not in stripped:
        return False
    if stripped.startswith('"') or stripped.endswith('"'):
        return False
    if not stripped.isascii():
        return False
    if _QUERY_OPERATOR_RE.search(stripped):
        return False
    if any(token in stripped.upper().split() for token in {"AND", "OR", "NOT"}):
        return False
    return True


def build_search_query_variants(
    source_name: str,
    query: str,
    *,
    exact_phrase_enabled: Optional[bool] = None,
) -> list[SearchQueryVariant]:
    """Build source-specific search variants for broad news sources.

    Broad source searches can be noisy when a site tokenizes multi-word queries.
    We use quoted exact phrase as a small additive precision pass, then always
    run the original unquoted query as the baseline coverage path.
    """
    stripped = query.strip()
    if not stripped:
        return []

    enabled = (
        source_name in EXACT_PHRASE_ENABLED_SOURCES
        if exact_phrase_enabled is None
        else exact_phrase_enabled
    )
    variants: list[SearchQueryVariant] = []
    if enabled and can_exact_phrase_query(stripped):
        variants.append(
            SearchQueryVariant(
                original_query=stripped,
                search_query=f'"{stripped}"',
                variant_type=EXACT_PHRASE_VARIANT,
            )
        )
    variants.append(
        SearchQueryVariant(
            original_query=stripped,
            search_query=stripped,
            variant_type=UNQUOTED_VARIANT,
        )
    )
    return variants


def page_limit_for_query_variant(variant_type: str, max_pages: Optional[int]) -> Optional[int]:
    """Keep the unquoted crawl budget intact while capping exact-phrase probes."""
    if variant_type != EXACT_PHRASE_VARIANT:
        return max_pages
    if max_pages == 0:
        return 0
    if max_pages is None:
        return EXACT_PHRASE_MAX_PAGES
    return min(max_pages, EXACT_PHRASE_MAX_PAGES)


def should_continue_to_next_query_variant(metrics: SearchQueryMetrics) -> bool:
    """Exact phrase is additive; the unquoted baseline should still run."""
    return metrics.variant_type == EXACT_PHRASE_VARIANT


def record_news_query_metrics(metrics: SearchQueryMetrics) -> None:
    with _query_metrics_lock:
        _query_metrics.append(metrics.to_dict())


def consume_news_query_metrics(source_names: object = None) -> list[dict[str, Any]]:
    if source_names is None:
        wanted = None
    elif isinstance(source_names, str):
        wanted = {source_names}
    else:
        wanted = {str(name) for name in source_names}

    with _query_metrics_lock:
        if wanted is None:
            records = list(_query_metrics)
            _query_metrics.clear()
            return records

        kept: list[dict[str, Any]] = []
        remaining: list[dict[str, Any]] = []
        for record in _query_metrics:
            if record.get("source") in wanted:
                kept.append(record)
            else:
                remaining.append(record)
        _query_metrics[:] = remaining
        return kept


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
