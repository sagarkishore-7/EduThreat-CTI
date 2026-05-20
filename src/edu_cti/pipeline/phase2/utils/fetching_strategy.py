"""
Smart article fetching strategy for Phase 2 enrichment.

Implements domain-based rate limiting and random incident selection
to avoid bot detection and ensure efficient article fetching.
"""

import random
import time
import logging
import threading
import re
import os
import xml.etree.ElementTree as ET
from typing import List, Dict, Set, Optional, Tuple
from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import parse_qs, quote_plus, unquote, urlparse
import sqlite3

from bs4 import BeautifulSoup

from src.edu_cti.core.db import get_connection, get_broken_urls, mark_urls_as_broken
from src.edu_cti.core import metrics as _metrics
from src.edu_cti.core.deduplication import normalize_url
from src.edu_cti.core.config import EDUCATION_KEYWORDS, CYBER_KEYWORDS, SERP_MAX_ATTEMPTS
from src.edu_cti.core.oxylabs import OxylabsClient
from src.edu_cti.pipeline.phase2.utils.post_processing import is_headline_format
from src.edu_cti.sources.rss.googlenews_rss import _resolve_google_news_article_url
from src.edu_cti.pipeline.phase2.storage.article_fetcher import (
    ArticleFetcher,
    ArticleContent,
    BLOCKED_FETCH_DOMAINS,
    _env_timeout_ms_as_seconds,
)
from src.edu_cti.pipeline.phase2.storage.article_storage import (
    init_articles_table,
    save_article,
    article_exists,
)

logger = logging.getLogger(__name__)

_INVALID_SERP_NAME_RE = re.compile(r"^[^A-Za-z0-9]+$")
_NEWS_DISCOVERY_USER_AGENT = "Mozilla/5.0 (compatible; EduThreat-CTI/2.0; +https://edu-threat-cti)"
_INVALID_DISCOVERY_NAMES = {
    "unknown",
    "unknown institution",
    "n/a",
    "none",
    "unnamed",
    "undisclosed",
    "not disclosed",
    "?",
    "-",
    "",
}

try:
    from scrapling.fetchers import Fetcher as ScraplingFetcher
    SCRAPLING_DISCOVERY_AVAILABLE = True
    SCRAPLING_DISCOVERY_IMPORT_ERROR: str | None = None
except ImportError as exc:
    ScraplingFetcher = None
    SCRAPLING_DISCOVERY_AVAILABLE = False
    SCRAPLING_DISCOVERY_IMPORT_ERROR = str(exc)


def _env_flag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _append_url_to_incident(conn: sqlite3.Connection, incident_id: str, url: str) -> None:
    """Append url to incidents.all_urls (semicolon-separated) when not already present.

    Called after a SERP-discovered URL fetches successfully so that
    save_enrichment_result() recognises the URL as a valid primary_url candidate
    and doesn't fall back to the original broken/PDF source URL.
    """
    row = conn.execute(
        "SELECT all_urls FROM incidents WHERE incident_id = ?", (incident_id,)
    ).fetchone()
    if not row:
        return
    existing_raw = row[0] or ""
    existing = {u.strip() for u in existing_raw.split(";") if u.strip()}
    if url in existing:
        return
    existing.add(url)
    conn.execute(
        "UPDATE incidents SET all_urls = ? WHERE incident_id = ?",
        (";".join(sorted(existing)), incident_id),
    )


def _save_fetch_attempt(
    conn: sqlite3.Connection,
    incident_id: str,
    url: str,
    article: ArticleContent,
) -> None:
    """Persist both successful and failed article fetch attempts for auditability."""
    save_article(
        conn,
        incident_id=incident_id,
        url=url,
        article=article,
    )


def is_internal_placeholder_url(url: str) -> bool:
    """
    Return True for internal/non-fetchable placeholder URLs.

    These URLs are useful as metadata carriers inside the pipeline, but they
    should never be sent through fetch tiers like newspaper3k, Playwright, or
    Oxylabs. Comparitech synthetic URLs are the main current example.
    """
    if not url:
        return False

    try:
        parsed = urlparse(url)
    except Exception:
        return False

    if parsed.scheme in {"http", "https"}:
        return False

    return bool(parsed.scheme)


def filter_fetchable_urls(urls: List[str]) -> List[str]:
    """Drop internal placeholder URLs and keep only externally fetchable ones."""
    return [url for url in urls if not is_internal_placeholder_url(url)]


def _news_discovery_max_results() -> int:
    try:
        return max(1, int(os.environ.get("EDU_CTI_NEWS_DISCOVERY_MAX_RESULTS", "5")))
    except (TypeError, ValueError):
        return 5


def _news_discovery_decode_limit(max_results: int) -> int:
    try:
        configured = int(os.environ.get("EDU_CTI_NEWS_DISCOVERY_DECODE_LIMIT", "20"))
    except (TypeError, ValueError):
        configured = 20
    return max(max_results, configured)


def _strip_title_source_suffix(title: str) -> str:
    """Remove common trailing publisher suffixes from a news headline."""
    cleaned = re.sub(r"\s+[-–—|]\s+[^-–—|]{2,60}$", "", title or "").strip()
    return cleaned or (title or "").strip()


def _build_news_discovery_query(incident: Dict) -> Optional[Tuple[str, str]]:
    """Build a stable news-search query without exposing callers to provider details."""
    name = (incident.get("institution_name") or incident.get("victim_raw_name") or "").strip()
    title = (incident.get("title") or "").strip()

    if (
        name
        and name.lower() not in _INVALID_DISCOVERY_NAMES
        and not _INVALID_SERP_NAME_RE.fullmatch(name)
        and not is_headline_format(name, title)
    ):
        # Skip domain-format names (e.g. unila.edu.mx, saiedu.fi). They
        # consistently return low-quality/non-news results and waste provider work.
        if "." in name and " " not in name:
            logger.debug("News discovery skip: domain-format name %r", name)
            return None
        attack_hint = incident.get("attack_type_hint") or "cyberattack"
        incident_date = incident.get("incident_date") or ""
        year = incident_date[:4] if incident_date and len(incident_date) >= 4 else ""
        query_parts = [f'"{name}"', attack_hint]
        if year:
            query_parts.append(year)
        return " ".join(query_parts), f"institution '{name}'"

    if title:
        title_lower = title.lower()
        has_edu = any(k.lower() in title_lower for k in EDUCATION_KEYWORDS)
        has_cyber = any(k.lower() in title_lower for k in CYBER_KEYWORDS)
        if not has_edu or not has_cyber:
            logger.debug(
                "News discovery skip: title lacks edu+cyber keywords "
                "(edu=%s, cyber=%s): %r",
                has_edu,
                has_cyber,
                title[:80],
            )
            return None
        return f'"{title}"', f"title '{title[:60]}'"

    return None


def _build_news_discovery_queries(incident: Dict) -> List[Tuple[str, str]]:
    """Build ordered query variants for URL discovery.

    Title-only sources often include a publisher suffix or slightly different
    punctuation than Google News has indexed. Try the precise query first, then
    a stripped/unquoted variant before declaring the source unresolved.
    """
    query_spec = _build_news_discovery_query(incident)
    if not query_spec:
        return []

    query, log_label = query_spec
    queries: List[Tuple[str, str]] = [(query, log_label)]
    title = (incident.get("title") or "").strip()
    if log_label.startswith("title ") and title:
        stripped_title = _strip_title_source_suffix(title)
        if stripped_title and stripped_title != title:
            variants = [
                (f'"{stripped_title}"', f"stripped title '{stripped_title[:60]}'"),
                (stripped_title, f"loose title '{stripped_title[:60]}'"),
            ]
        else:
            variants = [(title, f"loose title '{title[:60]}'")]

        seen = {query}
        for variant_query, variant_label in variants:
            if variant_query not in seen:
                queries.append((variant_query, variant_label))
                seen.add(variant_query)

    return queries


def _is_blocked_discovered_url(url: str) -> bool:
    try:
        parsed_domain = urlparse(url).netloc.lower()
        base = ".".join(parsed_domain.split(".")[-2:]) if parsed_domain.count(".") >= 1 else parsed_domain
        return parsed_domain in BLOCKED_FETCH_DOMAINS or base in BLOCKED_FETCH_DOMAINS
    except Exception:
        return False


def _filter_discovered_urls(urls: List[str], max_results: int) -> List[str]:
    """Dedupe discovered article URLs and filter domains we never fetch."""
    filtered: List[str] = []
    seen: Set[str] = set()
    for raw_url in urls:
        if not raw_url:
            continue
        url = raw_url.strip()
        if not url.startswith(("http://", "https://")):
            continue
        normalized = normalize_url(url)
        if normalized in seen:
            continue
        if _is_blocked_discovered_url(url):
            continue
        seen.add(normalized)
        filtered.append(url)
        if len(filtered) >= max_results:
            break
    return filtered


def _fetch_discovery_url_with_scrapling(url: str) -> Optional[str]:
    """Fetch search/RSS pages with Scrapling, returning body text only."""
    if not SCRAPLING_DISCOVERY_AVAILABLE or ScraplingFetcher is None:
        logger.info(
            "Scrapling discovery unavailable: %s",
            SCRAPLING_DISCOVERY_IMPORT_ERROR or "import failed",
        )
        return None
    try:
        kwargs = {
            "timeout": _env_timeout_ms_as_seconds("EDU_CTI_SCRAPLING_DISCOVERY_TIMEOUT_MS", 20000),
            "stealthy_headers": True,
            "follow_redirects": True,
            "headers": {
                "Accept": "application/rss+xml, application/xml, text/xml, text/html",
                "User-Agent": _NEWS_DISCOVERY_USER_AGENT,
            },
        }
        try:
            response = ScraplingFetcher.get(url, **kwargs)
        except TypeError as exc:
            if "headers" not in str(exc):
                raise
            kwargs.pop("headers", None)
            response = ScraplingFetcher.get(url, **kwargs)
        status = int(getattr(response, "status", None) or getattr(response, "status_code", 0) or 0)
        if status >= 400:
            logger.debug("Scrapling discovery HTTP %s for %s", status, url[:120])
            return None
        body = getattr(response, "body", None)
        if isinstance(body, bytes):
            return body.decode("utf-8", errors="replace")
        if body:
            return str(body)
        text = getattr(response, "text", "")
        return text() if callable(text) else str(text or "")
    except Exception as exc:
        logger.debug("Scrapling discovery failed for %s: %s", url[:120], exc)
        return None


def _discover_google_news_rss_with_scrapling(query: str, max_results: int) -> List[str]:
    """Discover article URLs through Google News RSS without Oxylabs SERP quota."""
    rss_url = (
        "https://news.google.com/rss/search?"
        f"q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
    )
    xml_text = _fetch_discovery_url_with_scrapling(rss_url)
    if not xml_text:
        return []

    try:
        root = ET.fromstring(xml_text.encode("utf-8") if isinstance(xml_text, str) else xml_text)
    except ET.ParseError as exc:
        logger.debug("Google News RSS parse failed: %s", exc)
        return []

    urls: List[str] = []
    seen: Set[str] = set()
    for item in root.findall(".//item")[: _news_discovery_decode_limit(max_results)]:
        link = (item.findtext("link") or "").strip()
        if not link:
            continue
        resolved = _resolve_google_news_article_url(link) or link
        if "news.google.com" in resolved:
            continue
        if not resolved.startswith(("http://", "https://")) or _is_blocked_discovered_url(resolved):
            continue
        normalized = normalize_url(resolved)
        if normalized in seen:
            continue
        seen.add(normalized)
        urls.append(resolved)
        if len(urls) >= max_results:
            break

    return urls


def _extract_bing_result_url(link: str) -> Optional[str]:
    if not link:
        return None
    link = link.strip()
    parsed = urlparse(link)
    if parsed.netloc.lower().endswith("bing.com") and parsed.path.endswith("/news/apiclick.aspx"):
        target = (parse_qs(parsed.query).get("url") or [""])[0]
        return unquote(target) if target else None
    return link if link.startswith(("http://", "https://")) else None


def _discover_bing_news_rss_with_scrapling(query: str, max_results: int) -> List[str]:
    """Discover article URLs through Bing News RSS as a free no-key fallback."""
    if not _env_flag("EDU_CTI_ENABLE_BING_NEWS_DISCOVERY", "1"):
        return []

    rss_url = f"https://www.bing.com/news/search?q={quote_plus(query)}&format=rss"
    xml_text = _fetch_discovery_url_with_scrapling(rss_url)
    if not xml_text:
        return []

    try:
        root = ET.fromstring(xml_text.encode("utf-8") if isinstance(xml_text, str) else xml_text)
    except ET.ParseError as exc:
        logger.debug("Bing News RSS parse failed: %s", exc)
        return []

    urls: List[str] = []
    for item in root.findall(".//item")[: _news_discovery_decode_limit(max_results)]:
        target = _extract_bing_result_url(item.findtext("link") or "")
        if target:
            urls.append(target)
    return _filter_discovered_urls(urls, max_results)


def _extract_yahoo_result_url(href: str) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith("/"):
        return None
    if "r.search.yahoo.com" in href and "/RU=" in href:
        try:
            encoded = href.split("/RU=", 1)[1].split("/RK=", 1)[0]
            href = unquote(encoded)
        except Exception:
            return None
    if href.startswith(("http://", "https://")) and "yahoo.com" not in urlparse(href).netloc.lower():
        return href
    return None


def _discover_yahoo_news_with_scrapling(query: str, max_results: int) -> List[str]:
    """Optional Yahoo News HTML fallback. Disabled by default due consent walls."""
    search_url = f"https://news.search.yahoo.com/search?p={quote_plus(query)}"
    html = _fetch_discovery_url_with_scrapling(search_url)
    if not html:
        return []

    lower = html.lower()
    if "consent.yahoo.com" in lower or "privacy dashboard" in lower or "consent" in lower[:2000]:
        logger.info("Yahoo News discovery returned a consent page; skipping provider")
        return []

    soup = BeautifulSoup(html, "html.parser")
    urls: List[str] = []
    for anchor in soup.select("a[href]"):
        target = _extract_yahoo_result_url(anchor.get("href") or "")
        if target:
            urls.append(target)
    return _filter_discovered_urls(urls, max_results)


def _discover_articles_via_oxylabs_serp(query: str, max_results: int) -> List[str]:
    """Paid fallback discovery, disabled unless EDU_CTI_ENABLE_OXYLABS_SERP=1."""
    if not _env_flag("EDU_CTI_ENABLE_OXYLABS_SERP", "0"):
        return []
    client = OxylabsClient()
    results = client.search_news(query, max_results=max_results)
    return _filter_discovered_urls([r["url"] for r in results if r.get("url")], max_results)


def discover_articles_via_serp(incident: Dict) -> List[str]:
    """
    Discover article URLs for a URL-less incident.

    Provider order:
    1. Google News RSS fetched through Scrapling (free/default)
    2. Bing News RSS through Scrapling (free fallback, direct URLs)
    3. Yahoo News HTML through Scrapling (optional, disabled by default)
    4. Oxylabs Google News SERP (paid, opt-in only)

    Query modes:
    1. Named institution (institution_name is set): query = '"Name" attack_hint year'
    2. Title-based fallback (institution_name blank): query = title (quoted)
       Used when we have a headline but no accessible article text — finds the
       same story on an open source.

    Args:
        incident: Incident dict with institution_name, attack_type_hint, incident_date, title

    Returns:
        List of discovered article URLs (may be empty if no provider returns usable results)
    """
    query_specs = _build_news_discovery_queries(incident)
    if not query_specs:
        return []

    max_results = _news_discovery_max_results()
    _src_label = (incident.get("incident_id") or "unknown").split("_")[0]
    _metrics.increment("serp_queries_total", labels={"source": _src_label})

    provider_name = "google_news_rss_scrapling"
    urls: List[str] = []
    log_label = query_specs[0][1]
    if _env_flag("EDU_CTI_ENABLE_GOOGLE_NEWS_DISCOVERY", "1"):
        for query, candidate_label in query_specs:
            urls = _discover_google_news_rss_with_scrapling(query, max_results)
            if urls:
                log_label = candidate_label
                break
    if not urls:
        provider_name = "bing_news_rss_scrapling"
        for query, candidate_label in query_specs:
            urls = _discover_bing_news_rss_with_scrapling(query, max_results)
            if urls:
                log_label = candidate_label
                break
    if not urls and _env_flag("EDU_CTI_ENABLE_YAHOO_NEWS_DISCOVERY", "0"):
        provider_name = "yahoo_news_scrapling"
        for query, candidate_label in query_specs:
            urls = _discover_yahoo_news_with_scrapling(query, max_results)
            if urls:
                log_label = candidate_label
                break
    if not urls:
        provider_name = "oxylabs_serp"
        for query, candidate_label in query_specs:
            urls = _discover_articles_via_oxylabs_serp(query, max_results)
            if urls:
                log_label = candidate_label
                break
    if not urls:
        provider_name = "none"

    if urls:
        _metrics.increment("serp_urls_returned_total", value=len(urls), labels={"source": _src_label})
        logger.info(
            "News discovery: provider=%s found %s articles for %s",
            provider_name,
            len(urls),
            log_label,
        )
    else:
        _metrics.increment("serp_zero_results_total", labels={"source": _src_label})
        logger.info("News discovery: no results for %s", log_label)
    return urls


class DomainRateLimiter:
    """
    Tracks and enforces rate limits per domain to avoid bot detection.
    
    Maintains:
    - Last fetch time per domain
    - Fetch counts per domain within time windows
    - Blocked domains (temporarily or permanently)
    """
    
    def __init__(
        self,
        min_delay_seconds: float = 2.0,
        max_delay_seconds: float = 5.0,
        max_fetches_per_hour: int = 10,
        block_duration_seconds: int = 3600,  # 1 hour
    ):
        self.min_delay_seconds = min_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.max_fetches_per_hour = max_fetches_per_hour
        self.block_duration_seconds = block_duration_seconds
        
        # Track last fetch time per domain
        self.domain_last_fetch: Dict[str, datetime] = {}
        
        # Track fetch counts per domain within time windows
        self.domain_fetch_counts: Dict[str, List[datetime]] = defaultdict(list)
        
        # Blocked domains (domain -> block_until_time)
        self.domain_blocks: Dict[str, datetime] = {}
        
        # Permanently blocked domains
        self.permanently_blocked: Set[str] = set()
        self._lock = threading.Lock()
    
    def extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            # Remove port if present
            if ':' in domain:
                domain = domain.split(':')[0]
            return domain
        except Exception as e:
            logger.warning(f"Error extracting domain from {url}: {e}")
            return ""
    
    def is_domain_blocked(self, domain: str) -> bool:
        """Check if domain is currently blocked."""
        with self._lock:
            if domain in self.permanently_blocked:
                return True
            
            if domain in self.domain_blocks:
                block_until = self.domain_blocks[domain]
                if datetime.utcnow() < block_until:
                    return True
                else:
                    # Block expired, remove it
                    del self.domain_blocks[domain]
            
            return False
    
    def block_domain(self, domain: str, permanent: bool = False) -> None:
        """Block a domain temporarily or permanently."""
        with self._lock:
            if permanent:
                self.permanently_blocked.add(domain)
                logger.warning(f"Permanently blocked domain: {domain}")
            else:
                block_until = datetime.utcnow() + timedelta(seconds=self.block_duration_seconds)
                self.domain_blocks[domain] = block_until
                logger.warning(f"Temporarily blocked domain {domain} until {block_until}")
    
    def can_fetch_from_domain(self, domain: str) -> bool:
        """Check if we can fetch from this domain right now."""
        if not domain:
            return False
        
        # Check if blocked
        if self.is_domain_blocked(domain):
            return False
        
        # Check rate limit
        now = datetime.utcnow()
        one_hour_ago = now - timedelta(hours=1)
        
        # Clean up old fetch times
        with self._lock:
            if domain in self.domain_fetch_counts:
                self.domain_fetch_counts[domain] = [
                    fetch_time for fetch_time in self.domain_fetch_counts[domain]
                    if fetch_time > one_hour_ago
                ]
            
            # Check if we've exceeded rate limit
            recent_fetches = len(self.domain_fetch_counts.get(domain, []))
        if recent_fetches >= self.max_fetches_per_hour:
            logger.debug(f"Rate limit exceeded for domain {domain} ({recent_fetches} fetches in last hour)")
            _metrics.increment("domain_perm_blocked_total", labels={"domain": domain})
            return False
        
        return True
    
    def wait_if_needed(self, domain: str) -> None:
        """Wait if necessary to respect rate limits."""
        if not domain:
            return
        
        with self._lock:
            last_fetch = self.domain_last_fetch.get(domain)
        if last_fetch:
            elapsed = (datetime.utcnow() - last_fetch).total_seconds()
            
            # Random delay between min and max
            delay = random.uniform(self.min_delay_seconds, self.max_delay_seconds)
            
            if elapsed < delay:
                wait_time = delay - elapsed
                logger.debug(f"Waiting {wait_time:.2f}s before fetching from {domain}")
                _metrics.increment("domain_rate_limit_delays_total", labels={"domain": domain})
                time.sleep(wait_time)
    
    def record_fetch(self, domain: str, success: bool = True) -> None:
        """Record a fetch attempt for a domain."""
        if not domain:
            return
        
        now = datetime.utcnow()
        with self._lock:
            self.domain_last_fetch[domain] = now
            
            if success:
                self.domain_fetch_counts[domain].append(now)
            else:
                # Multiple failures might indicate bot detection
                # Track failures and potentially block domain
                pass


class SmartArticleFetchingStrategy:
    """
    Smart article fetching strategy that:
    
    1. Randomly selects incidents to enrich
    2. Keeps track of incident IDs for later LLM enrichment
    3. Picks URLs from different domains (avoids same domain repeatedly)
    4. Implements domain-based rate limiting to prevent bot detection
    5. Organizes fetching efficiently
    """
    
    def __init__(
        self,
        conn: sqlite3.Connection,
        rate_limiter: Optional[DomainRateLimiter] = None,
        article_fetcher: Optional[ArticleFetcher] = None,
    ):
        self.conn = conn
        self.rate_limiter = rate_limiter or DomainRateLimiter()
        self.article_fetcher = article_fetcher or ArticleFetcher()
        
        # Track which incident IDs we're processing
        self.processing_incident_ids: Set[str] = set()
        
        # Track fetched URLs to avoid duplicates
        self.fetched_urls: Set[str] = set()
        
        init_articles_table(conn)
    
    def get_random_incidents_for_enrichment(
        self,
        limit: int,
        exclude_domains: Optional[List[str]] = None,
    ) -> List[Dict]:
        """
        Get random incidents that need enrichment, prioritizing diversity in domains.
        
        Args:
            limit: Maximum number of incidents to return
            exclude_domains: List of domains to avoid (e.g., recently blocked)
            
        Returns:
            List of incident dictionaries with URLs
        """
        exclude_domains = exclude_domains or []
        
        # Get all unenriched incidents (with or without URLs).
        # LEFT JOIN articles so we know which already have fetched content —
        # those will be fast-pathed to the LLM queue without re-fetching.
        # Exclude incidents that have exhausted SERP attempts (unenrichable).
        query = """
            SELECT
                i.incident_id,
                i.all_urls,
                i.institution_name,
                i.victim_raw_name,
                i.title,
                i.source_published_date,
                i.attack_type_hint,
                i.notes,
                i.incident_date,
                i.city,
                i.region,
                i.country,
                CASE WHEN a.incident_id IS NOT NULL THEN 1 ELSE 0 END AS has_articles
            FROM incidents i
            LEFT JOIN (
                SELECT DISTINCT incident_id FROM articles WHERE fetch_successful = 1
            ) a ON a.incident_id = i.incident_id
            WHERE i.llm_enriched = 0
              AND (
                (i.all_urls IS NOT NULL AND i.all_urls != '')
                OR a.incident_id IS NOT NULL
                OR (
                    COALESCE(i.serp_attempt_count, 0) < ?
                    AND (
                        (i.institution_name IS NOT NULL AND i.institution_name != '')
                        OR (i.victim_raw_name IS NOT NULL AND i.victim_raw_name != '')
                    )
                )
              )
            ORDER BY
                -- Articles-ready incidents first so they drain the LLM queue fast
                has_articles DESC,
                RANDOM()
            LIMIT ?
        """

        cur = self.conn.execute(query, (SERP_MAX_ATTEMPTS, limit * 5,))
        rows = cur.fetchall()

        if not rows:
            return []

        # Group by domain and select diverse incidents
        domain_incidents: Dict[str, List[Dict]] = defaultdict(list)
        no_domain_incidents: List[Dict] = []

        for row in rows:
            incident_id = row["incident_id"]
            all_urls_str = row["all_urls"] or ""
            raw_all_urls = [url.strip() for url in all_urls_str.split(";") if url.strip()]
            all_urls = filter_fetchable_urls(raw_all_urls)

            has_articles = bool(row["has_articles"])
            incident_dict = {
                "incident_id": incident_id,
                "all_urls": all_urls,
                "institution_name": row["institution_name"] or row["victim_raw_name"] or "",
                "victim_raw_name": row["victim_raw_name"],
                "title": row["title"],
                "source_published_date": row["source_published_date"],
                "attack_type_hint": row["attack_type_hint"],
                "notes": row["notes"],
                "incident_date": row["incident_date"],
                "city": row["city"],
                "region": row["region"],
                "country": row["country"],
                "has_articles": has_articles,
            }

            placeholder_count = len(raw_all_urls) - len(all_urls)
            if placeholder_count > 0:
                logger.info(
                    f"Skipping {placeholder_count} internal placeholder URL(s) for {incident_id}"
                )

            if not all_urls and not has_articles:
                # URL-less but has metadata — will use SERP discovery
                has_metadata = bool(row["attack_type_hint"] or row["notes"]
                                    or row["institution_name"] or row["victim_raw_name"])
                if has_metadata:
                    no_domain_incidents.append(incident_dict)
                continue

            if has_articles and not all_urls:
                # Has saved articles but no URLs (e.g. comparitech synthetic) —
                # treat as no-domain so it goes straight to the fast-path queue.
                no_domain_incidents.append(incident_dict)
                continue

            # Find first valid URL domain
            domain = None
            for url in all_urls:
                d = self.rate_limiter.extract_domain(url)
                if d and d not in exclude_domains:
                    if self.rate_limiter.can_fetch_from_domain(d):
                        domain = d
                        break

            if domain:
                domain_incidents[domain].append(incident_dict)
            else:
                no_domain_incidents.append(incident_dict)
        
        # Select incidents: prioritize diversity across domains
        selected: List[Dict] = []
        selected_domains: Set[str] = set()
        
        # First pass: select one incident per domain (ensures diversity)
        for domain, incidents in domain_incidents.items():
            if len(selected) >= limit:
                break
            if domain not in selected_domains:
                incident = random.choice(incidents)
                selected.append(incident)
                selected_domains.add(domain)
        
        # Second pass: fill remaining slots randomly from any domain
        remaining = limit - len(selected)
        if remaining > 0:
            all_remaining = [
                inc for domain, incidents in domain_incidents.items()
                for inc in incidents if inc not in selected
            ] + no_domain_incidents
            
            if all_remaining:
                additional = random.sample(
                    all_remaining,
                    min(remaining, len(all_remaining))
                )
                selected.extend(additional)
        
        logger.info(
            f"Selected {len(selected)} incidents for fetching "
            f"(diversity: {len(selected_domains)} unique domains)"
        )
        
        return selected[:limit]
    
    def select_best_url_for_fetching(
        self,
        incident: Dict,
    ) -> Optional[str]:
        """
        Select the best URL to fetch for an incident.
        
        Prioritizes:
        1. URLs from domains we haven't fetched from recently
        2. URLs that aren't blocked
        3. URLs we haven't already fetched
        
        Args:
            incident: Incident dictionary with all_urls
            
        Returns:
            Best URL to fetch, or None if no suitable URL
        """
        all_urls = incident.get("all_urls", [])
        if not all_urls:
            return None
        
        # Score URLs by domain availability and freshness
        url_scores: List[Tuple[str, float]] = []
        
        for url in all_urls:
            # Skip if already fetched
            if url in self.fetched_urls:
                continue
            
            domain = self.rate_limiter.extract_domain(url)
            if not domain:
                continue
            
            # Check if domain is available
            if not self.rate_limiter.can_fetch_from_domain(domain):
                continue
            
            # Score: prefer domains we haven't used recently
            score = 1.0
            if domain in self.rate_limiter.domain_last_fetch:
                last_fetch = self.rate_limiter.domain_last_fetch[domain]
                hours_since = (datetime.utcnow() - last_fetch).total_seconds() / 3600
                score = min(1.0, hours_since / 24.0)  # Higher score if longer since last fetch
            
            url_scores.append((url, score))
        
        if not url_scores:
            return None
        
        # Select URL with highest score
        url_scores.sort(key=lambda x: x[1], reverse=True)
        return url_scores[0][0]
    
    def fetch_articles_for_incidents(
        self,
        incidents: List[Dict],
    ) -> Dict[str, List[ArticleContent]]:
        """
        Fetch articles for multiple incidents with domain-based rate limiting.
        
        Args:
            incidents: List of incident dictionaries to fetch articles for
            
        Returns:
            Dictionary mapping incident_id to list of ArticleContent objects
        """
        results: Dict[str, List[ArticleContent]] = {}
        
        # Track which incident IDs we're processing
        for incident in incidents:
            self.processing_incident_ids.add(incident["incident_id"])
        
        # Process incidents one by one with domain-based rate limiting
        for i, incident in enumerate(incidents, 1):
            incident_id = incident["incident_id"]
            raw_all_urls = incident["all_urls"]
            all_urls = filter_fetchable_urls(raw_all_urls)

            placeholder_count = len(raw_all_urls) - len(all_urls)
            if placeholder_count > 0:
                logger.info(
                    f"[{i}/{len(incidents)}] Ignoring {placeholder_count} internal placeholder URL(s) "
                    f"for {incident_id}"
                )

            # For URL-less incidents (e.g. Comparitech), discover articles via
            # low-cost news discovery first, with paid Oxylabs SERP only if enabled.
            if not all_urls:
                logger.info(
                    f"[{i}/{len(incidents)}] No URLs for {incident_id} — trying news discovery"
                )
                discovered = discover_articles_via_serp(incident)
                if discovered:
                    all_urls = discovered
                    incident = dict(incident, all_urls=all_urls)
                    # Persist discovered URLs so save_enrichment_result validates them.
                    for disc_url in discovered:
                        _append_url_to_incident(self.conn, incident_id, disc_url)
                else:
                    logger.info(f"[{i}/{len(incidents)}] SERP found no articles for {incident_id}")
                    results[incident_id] = []
                    continue

            logger.info(
                f"[{i}/{len(incidents)}] Fetching articles for incident {incident_id} "
                f"({len(all_urls)} URLs)"
            )
            
            incident_articles: List[ArticleContent] = []

            # Load previously-confirmed broken URLs so we don't retry them
            known_broken: Set[str] = get_broken_urls(self.conn, incident_id)
            newly_failed_urls: List[str] = []

            # Try to fetch from each URL, prioritizing different domains
            for url in all_urls:
                # Skip URLs that already failed all 4 tiers in a previous run
                if normalize_url(url) in known_broken:
                    logger.debug(f"Skipping known-broken URL: {url}")
                    continue

                domain = self.rate_limiter.extract_domain(url)

                if not domain:
                    logger.debug(f"Skipping URL with invalid domain: {url}")
                    continue

                # Check if we can fetch from this domain
                if not self.rate_limiter.can_fetch_from_domain(domain):
                    logger.debug(f"Domain {domain} is blocked or rate-limited, skipping {url}")
                    continue

                # Check if already fetched
                if url in self.fetched_urls:
                    logger.debug(f"URL already fetched: {url}")
                    continue

                # Wait if needed to respect rate limits
                self.rate_limiter.wait_if_needed(domain)

                # Fetch article
                try:
                    logger.info(f"Fetching article from {domain}: {url}")
                    article_content = self.article_fetcher.fetch_article(url)

                    # Record fetch attempt
                    success = article_content.fetch_successful
                    self.rate_limiter.record_fetch(domain, success=success)

                    try:
                        _save_fetch_attempt(
                            self.conn,
                            incident_id=incident_id,
                            url=url,
                            article=article_content,
                        )
                    except Exception as save_error:
                        logger.error(
                            f"Failed to save fetch attempt for {url}: {str(save_error)[:100]}",
                            exc_info=True
                        )

                    if success:
                        self.fetched_urls.add(url)
                        incident_articles.append(article_content)
                        logger.info(
                            f"Fetched article from {domain} "
                            f"({len(article_content.content)} chars)"
                        )
                    else:
                        error_msg = article_content.error_message or "Unknown error"
                        content_len = article_content.content_length or 0
                        logger.warning(
                            f"Failed to fetch from {domain}: {error_msg[:100]} "
                            f"(content_length: {content_len}, title: {article_content.title[:50] if article_content.title else 'None'})"
                        )
                        logger.debug(f"Fetch failed {incident_id} {domain}: {error_msg[:100]}")
                        newly_failed_urls.append(url)

                        # If multiple failures from same domain, consider blocking
                        if "403" in error_msg or "Forbidden" in error_msg:
                            logger.warning(f"403 error from {domain}, may be blocked")

                    # Small delay between URLs from same incident
                    time.sleep(random.uniform(0.5, 1.5))

                except Exception as e:
                    logger.error(
                        f"Exception fetching {url}: {str(e)[:100]}",
                        exc_info=True
                    )
                    logger.error(f"Fetch exception {incident_id} {domain}: {str(e)[:200]}")
                    self.rate_limiter.record_fetch(domain, success=False)
                    newly_failed_urls.append(url)
                    try:
                        _save_fetch_attempt(
                            self.conn,
                            incident_id=incident_id,
                            url=url,
                            article=ArticleContent(
                                url=url,
                                title="",
                                content="",
                                fetch_successful=False,
                                error_message=str(e),
                                content_length=0,
                            ),
                        )
                    except Exception as save_error:
                        logger.error(
                            f"Failed to save exception fetch attempt for {url}: {str(save_error)[:100]}",
                            exc_info=True,
                        )

            # Persist newly-failed URLs so they are skipped on the next pipeline run
            if newly_failed_urls:
                try:
                    mark_urls_as_broken(self.conn, incident_id, newly_failed_urls)
                    self.conn.commit()
                    logger.info(f"Marked {len(newly_failed_urls)} broken URL(s) for {incident_id}")
                except Exception as be:
                    logger.warning(f"Failed to persist broken URLs for {incident_id}: {be}")

            # If primary URLs all failed, fall back to discovery.
            if not incident_articles and all_urls:
                logger.info(
                    f"Primary URL(s) all failed for {incident_id} — trying SERP fallback"
                )
                serp_urls = discover_articles_via_serp(incident)
                for serp_url in serp_urls:
                    domain = self.rate_limiter.extract_domain(serp_url)
                    if not domain or not self.rate_limiter.can_fetch_from_domain(domain):
                        continue
                    if serp_url in self.fetched_urls:
                        continue
                    self.rate_limiter.wait_if_needed(domain)
                    try:
                        article_content = self.article_fetcher.fetch_article(serp_url)
                        self.rate_limiter.record_fetch(domain, success=article_content.fetch_successful)
                        try:
                            _save_fetch_attempt(
                                self.conn,
                                incident_id=incident_id,
                                url=serp_url,
                                article=article_content,
                            )
                        except Exception as save_error:
                            logger.error(
                                f"SERP fallback save error {serp_url}: {save_error}"
                            )
                        if article_content.fetch_successful:
                            self.fetched_urls.add(serp_url)
                            incident_articles.append(article_content)
                            logger.info(f"SERP fallback: fetched {domain} ({len(article_content.content)} chars)")
                            # Register the SERP URL in all_urls so save_enrichment_result
                            # won't reject it as the primary_url.
                            _append_url_to_incident(self.conn, incident_id, serp_url)
                    except Exception as e:
                        logger.debug(f"SERP fallback fetch error {serp_url}: {e}")
                        try:
                            _save_fetch_attempt(
                                self.conn,
                                incident_id=incident_id,
                                url=serp_url,
                                article=ArticleContent(
                                    url=serp_url,
                                    title="",
                                    content="",
                                    fetch_successful=False,
                                    error_message=str(e),
                                    content_length=0,
                                ),
                            )
                        except Exception as save_error:
                            logger.error(
                                f"SERP fallback exception save error {serp_url}: {save_error}"
                            )

            results[incident_id] = incident_articles

            if not incident_articles:
                logger.warning(
                    f"No articles fetched for incident {incident_id} "
                    f"(tried {len(all_urls)} URL(s))"
                )
                logger.warning(f"No articles for {incident_id} (tried {len(all_urls)} URLs)")
            else:
                logger.info(
                    f"Fetched {len(incident_articles)} articles for incident {incident_id}"
                )
            
            # Minimal delay between incidents — Oxylabs handles anti-bot rotation,
            # so long inter-incident sleeps are unnecessary and waste time.
            if i < len(incidents):
                time.sleep(0.2)
        
        return results
    
    def get_processing_incident_ids(self) -> Set[str]:
        """Get set of incident IDs currently being processed."""
        return self.processing_incident_ids.copy()
