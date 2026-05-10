"""
Article fetching module for Phase 2 enrichment.

Fetches and extracts article content from URLs for LLM processing.
Uses newspaper3k for primary article extraction, with curl_cffi/Playwright fallback.
Includes advanced bot detection bypass via TLS fingerprinting and headless browser.
"""

import json
import logging
import os
import time
import random
import re
import requests
from typing import List, Optional, Dict
from dataclasses import dataclass
from urllib.parse import urlparse, quote

from src.edu_cti.core.http import HttpClient, build_http_client
from src.edu_cti.core.oxylabs import OxylabsClient
from bs4 import BeautifulSoup

# Optional newspaper3k support for article extraction
try:
    import newspaper
    from newspaper import Article
    NEWSPAPER_AVAILABLE = True
except ImportError:
    NEWSPAPER_AVAILABLE = False

logger = logging.getLogger(__name__)

from src.edu_cti.core import metrics as _metrics


def _env_flag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


def _prefer_oxylabs_before_browser() -> bool:
    """Railway safe mode prefers the cloud fetch tier before local browser work."""
    if not _env_flag("PHASE2_RAILWAY_SAFE_MODE", "0"):
        return False
    return bool(os.environ.get("RAILWAY_SERVICE_ID") or os.environ.get("RAILWAY_ENVIRONMENT"))


def _fetch_domain(url: str) -> str:
    """Extract eTLD+1 domain label from a URL for metric labels."""
    try:
        host = urlparse(url).netloc.lower()
        parts = host.split(".")
        return ".".join(parts[-2:]) if len(parts) >= 2 else host
    except Exception:
        return "unknown"


def _classify_fetch_failure(result) -> str:
    """Map an ArticleContent failure to a short reason label for Prometheus."""
    if result is None:
        return "none"
    msg = (result.error_message or "").lower()
    if "403" in msg or "forbidden" in msg:
        return "403"
    if "timeout" in msg or "timed out" in msg:
        return "timeout"
    if "soft" in msg or "gate page" in msg or "blocked" in msg:
        return "soft_404"
    if "content" in msg and ("short" in msg or "empty" in msg or "threshold" in msg):
        return "empty_content"
    if "blocked" in msg or "social media" in msg or "ioc" in msg:
        return "blocked_domain"
    return "exception"


_STRUCTURED_AUTHOR_KEYS = {
    "author",
    "authors",
    "creator",
    "creators",
    "contributor",
    "contributors",
    "byline",
}

_STRUCTURED_PUBLISH_DATE_KEYS = {
    "datePublished",
    "date_published",
    "publicationDate",
    "publishDate",
    "publish_date",
    "publishedAt",
    "published_at",
    "publishTime",
    "publish_time",
    "pubDate",
    "pub_date",
    "firstPublished",
    "first_published",
    "dateCreated",
    "date_created",
}


def _resolve_google_news_url(url: str) -> str:
    """Resolve Google News redirect URLs to actual article URLs.

    Google News RSS <link> elements are opaque encoded redirects like
    ``https://news.google.com/rss/articles/CBMi...`` that return 400 when
    fetched directly.  Uses googlenewsdecoder to extract the real article URL.
    Falls back to the original URL if decoding fails.
    """
    if "news.google.com" not in url:
        return url
    try:
        from googlenewsdecoder import new_decoderv1
        result = new_decoderv1(url)
        if result and result.get("status") and result.get("decoded_url"):
            resolved = result["decoded_url"]
            logger.debug(f"Resolved Google News URL → {resolved[:120]}")
            return resolved
    except ImportError:
        logger.warning("googlenewsdecoder not installed — cannot resolve Google News URLs")
    except Exception as e:
        logger.debug(f"Failed to resolve Google News URL: {e}")
    return url


# Domains with Cloudflare protection (curl_cffi handles these via TLS impersonation)
CLOUDFLARE_PROTECTED_DOMAINS = [
    "darkreading.com",
    "securityweek.com",
    "bleepingcomputer.com",
    "databreaches.net",
]

# Domains that never yield usable article content for CTI extraction.
# These are immediately rejected before any fetch attempt.
BLOCKED_FETCH_DOMAINS = {
    # Social media — paywalled/login-gated, no article text
    "twitter.com", "x.com",
    "facebook.com", "fb.com",
    "linkedin.com",
    "instagram.com",
    "reddit.com",
    "tiktok.com",
    # IOC databases — not news articles, no education CTI value
    "threatfox.abuse.ch",
    "bazaar.abuse.ch",
    "urlhaus.abuse.ch",
    "abuse.ch",
    "search.censys.io",
    "shodan.io",
    "virustotal.com",
    "any.run",
    "tria.ge",
    "malwarebazaar.abuse.ch",
    "otx.alienvault.com",
    "nvd.nist.gov",
    # Hard paywall — all 4 fetch tiers return login-gate HTML.
    # SERP fallback (fetching_strategy.py) will find the same story on an open source.
    "securityweek.com",
    # Permanently unreachable / returns 0 useful chars across all tiers.
    "syracuse.com",
    # JS-heavy paywalled sites — Oxylabs returns HTML shell, extractor gets 0 chars.
    "hipaajournal.com",
    # Paywall / aggregator roundup pages — these are listicles covering hundreds of
    # incidents per page; they cause LLM output to exceed 8192 tokens mid-JSON.
    "techtarget.com",
    # Cloudflare-protected victim listing / data aggregator — returns Cloudflare
    # challenge or empty content across all 4 tiers. Articles link out to source
    # coverage that IS fetchable; the databreaches.net page itself is not needed.
    "databreaches.net",
    # Consistently returns 0 extractable chars across all tiers.
    "tuttoscuola.com",
    "nu.nl",
    "alaraby.co.uk",
    "ithome.com.tw",
}

# Session-scoped dynamic block list. When all 4 fetch tiers fail for a domain,
# we add it here so subsequent fetches to that domain short-circuit. This
# prevents the same broken site from costing 2-3 minutes per attempt across
# multiple incidents that happen to share its URLs.
# Cleared on container restart — Railway resets are frequent enough that
# truly-broken sites won't survive long-term, but truly-flaky ones get a
# fresh chance after each restart.
import threading as _threading_for_dyn_block
_DYNAMIC_FAILED_DOMAINS: set = set()
_DYNAMIC_FAILED_LOCK = _threading_for_dyn_block.Lock()


def _record_dynamic_domain_failure(domain: str) -> None:
    """Record that all 4 fetch tiers failed for this domain. Both the full
    netloc and its base domain are stored so www/non-www variants both block."""
    if not domain:
        return
    base = ".".join(domain.split(".")[-2:]) if domain.count(".") >= 1 else domain
    with _DYNAMIC_FAILED_LOCK:
        _DYNAMIC_FAILED_DOMAINS.add(domain)
        if base != domain:
            _DYNAMIC_FAILED_DOMAINS.add(base)


def _domain_failed_dynamically(domain: str) -> bool:
    if not domain:
        return False
    base = ".".join(domain.split(".")[-2:]) if domain.count(".") >= 1 else domain
    with _DYNAMIC_FAILED_LOCK:
        return domain in _DYNAMIC_FAILED_DOMAINS or base in _DYNAMIC_FAILED_DOMAINS


# Extend the blocked list at runtime without a code deploy:
# BLOCKED_FETCH_DOMAINS_EXTRA=domain1.com,domain2.com
_extra = os.environ.get("BLOCKED_FETCH_DOMAINS_EXTRA", "")
if _extra:
    BLOCKED_FETCH_DOMAINS = BLOCKED_FETCH_DOMAINS | {
        d.strip().lower() for d in _extra.split(",") if d.strip()
    }


# Phrases that indicate the page is a CAPTCHA / bot-gate / login wall rather
# than actual article content.  Checked against lowercased title + first 500
# chars of content so we don't false-positive on articles *about* CAPTCHAs.
_GATE_PAGE_SIGNALS = [
    "verify you are human",
    "verify that you are human",
    "please verify you are human",
    "checking your browser",
    "checking if the site connection is secure",
    "enable javascript and cookies",
    "please enable cookies",
    "ddos protection by cloudflare",
    "just a moment",           # Cloudflare challenge title
    "one more step",           # Cloudflare challenge
    "access to this page has been denied",
    "access denied",
    "you have been blocked",
    "this site is protected by recaptcha",
    "complete the security check",
    "bot verification",
    "human verification",
    "are you a robot",
    "i am not a robot",
    "prove you are not a robot",
    "subscribe to continue",
    "subscribe to read",
    "sign in to read",
    "log in to continue",
    "create a free account to continue",
    "register to read",
]


def _is_gate_page(title: str, content: str) -> bool:
    """Return True if the page looks like a CAPTCHA, bot-gate, or paywall rather than an article."""
    combined = ((title or "") + " " + (content or "")[:500]).lower()
    return any(signal in combined for signal in _GATE_PAGE_SIGNALS)


@dataclass
class ArticleContent:
    """Container for fetched article content."""
    
    url: str
    title: str
    content: str
    author: Optional[str] = None
    publish_date: Optional[str] = None
    fetch_successful: bool = True
    error_message: Optional[str] = None
    content_length: int = 0
    fetch_metadata: Optional[Dict[str, object]] = None


def _build_fetch_attempt_payload(
    tier: str,
    *,
    duration_seconds: float,
    result: Optional[ArticleContent],
    error_code: Optional[str] = None,
) -> dict[str, object]:
    success = bool(result and result.fetch_successful)
    effective_error_code = error_code
    if effective_error_code is None and not success:
        classified = _classify_fetch_failure(result)
        effective_error_code = "unknown_failure" if classified == "none" else classified

    return {
        "tier": tier,
        "success": success,
        "latency_ms": int(max(duration_seconds, 0.0) * 1000),
        "content_length": int((result.content_length if result else 0) or 0),
        "error_code": effective_error_code,
        "error_message": (result.error_message if result else None),
    }


class ArticleFetcher:
    """
    Fetches and extracts article content from URLs.
    
    Handles:
    - HTML parsing and content extraction
    - Common article structure patterns
    - Error handling and retries
    - Content cleaning
    """
    
    def __init__(self, http_client: Optional[HttpClient] = None):
        self.http_client = http_client or build_http_client()
    
    def _is_cloudflare_protected(self, url: str) -> bool:
        """Check if this domain has Cloudflare protection."""
        domain = urlparse(url).netloc.lower()
        return any(d in domain for d in CLOUDFLARE_PROTECTED_DOMAINS)

    def _get_archive_url(self, url: str) -> Optional[str]:
        """
        Check if a URL is available on archive.org (Wayback Machine).
        
        Tries multiple URL variations since archive.org is exact-match:
        - Original URL
        - Without www.
        - With www.
        - HTTP instead of HTTPS
        
        Args:
            url: Original URL to look up
            
        Returns:
            Archive URL if available, None otherwise
        """
        # Generate URL variations to try
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        path = parsed.path + ('?' + parsed.query if parsed.query else '')
        
        url_variations = [url]  # Start with original
        
        # Try without www
        if domain.startswith('www.'):
            no_www = f"{parsed.scheme}://{domain[4:]}{path}"
            url_variations.append(no_www)
        else:
            # Try with www
            with_www = f"{parsed.scheme}://www.{domain}{path}"
            url_variations.append(with_www)
        
        # Also try HTTP if HTTPS
        if parsed.scheme == 'https':
            for var_url in list(url_variations):
                http_url = var_url.replace('https://', 'http://')
                url_variations.append(http_url)
        
        # Try each variation
        for try_url in url_variations:
            wayback_api = f"https://archive.org/wayback/available?url={quote(try_url, safe='')}"
            try:
                resp = requests.get(wayback_api, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    snapshots = data.get("archived_snapshots", {})
                    closest = snapshots.get("closest", {})
                    if closest.get("available"):
                        archive_url = closest.get("url")
                        timestamp = closest.get("timestamp", "unknown")
                        logger.info(f"Found archive.org snapshot for {url} (timestamp: {timestamp})")
                        return archive_url
            except requests.RequestException as e:
                logger.debug(f"Archive.org API error for {try_url}: {e}")
            except Exception as e:
                logger.debug(f"Error checking archive.org for {try_url}: {e}")
        
        return None

    def _fetch_with_oxylabs(self, url: str) -> Optional[ArticleContent]:
        """
        Fetch article using Oxylabs Realtime API (replaces Zyte).

        Fetches rendered HTML via Oxylabs universal scraper, then extracts
        content using BeautifulSoup — same extraction logic as HttpClient.

        Args:
            url: URL to fetch

        Returns:
            ArticleContent if successful, None otherwise
        """
        client = OxylabsClient()
        if not client._is_configured():
            logger.warning("Oxylabs credentials not configured, skipping Oxylabs fallback")
            return None

        html = client.fetch_url(url, render_js=True)
        if not html:
            logger.info(f"Oxylabs: no content returned for {url}")
            return None

        soup = BeautifulSoup(html, "html.parser")
        title = self._extract_title(soup)
        content = self._extract_content(soup)
        author = self._extract_author(soup)
        publish_date = self._extract_publish_date(soup)
        content = self._clean_content(content)

        # Detect soft-404 pages (site returns 200 but content is a "not found" page)
        _404_signals = ["page can't be found", "page can\u2019t be found",
                        "page cannot be found", "not found", "404",
                        "no longer available", "nothing was found",
                        "page not found", "error 404"]
        title_lower = (title or "").lower()
        text_lower = (content or "").lower()[:300]
        is_soft_404 = any(s in title_lower or s in text_lower for s in _404_signals)
        if is_soft_404:
            logger.info(f"Oxylabs detected soft-404 for {url}: title='{(title or '')[:60]}'")
            return None

        if _is_gate_page(title, content):
            logger.info(f"Oxylabs: gate/CAPTCHA page detected for {url} — falling through to archive.org")
            return None

        min_length = 50 if "databreaches.net" in url.lower() else 100
        content_stripped = (content or "").strip()
        if content_stripped and len(content_stripped) >= min_length:
            logger.info(f"Oxylabs succeeded for {url} ({len(content_stripped)} chars)")
            return ArticleContent(
                url=url,
                title=title or "",
                content=content,
                author=author,
                publish_date=publish_date,
                fetch_successful=True,
                content_length=len(content_stripped),
            )
        else:
            logger.warning(
                f"Oxylabs: insufficient content for {url}: "
                f"{len(content_stripped)} chars (min={min_length})"
            )
            return None

    def _fetch_from_archive(self, original_url: str) -> Optional[ArticleContent]:
        """
        Attempt to fetch article from archive.org.
        
        Args:
            original_url: Original URL that couldn't be fetched
            
        Returns:
            ArticleContent if successful, None otherwise
        """
        archive_url = self._get_archive_url(original_url)
        if not archive_url:
            logger.debug(f"No archive.org snapshot found for {original_url}")
            return None
        
        logger.info(f"Fetching from archive.org: {archive_url}")
        
        # Try newspaper3k on the archive URL
        if NEWSPAPER_AVAILABLE:
            article_content = self._fetch_with_newspaper(archive_url)
            if article_content and article_content.fetch_successful:
                # Update URL to original for consistency
                article_content.url = original_url
                logger.info(f"Successfully fetched {original_url} from archive.org ({article_content.content_length} chars)")
                return article_content
        
        # Try simple HTTP fetch on archive URL
        try:
            resp = requests.get(archive_url, timeout=30, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            })
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                
                # Remove Wayback Machine toolbar/overlay
                for elem in soup.find_all(id=lambda x: x and 'wm-' in x):
                    elem.decompose()
                for elem in soup.find_all(class_=lambda x: x and 'wm-' in str(x)):
                    elem.decompose()
                
                # Extract content
                title = soup.find("title")
                title_text = title.get_text().strip() if title else ""
                
                # Try common article selectors
                article_elem = (
                    soup.find("article") or 
                    soup.find("div", class_="article-content") or
                    soup.find("div", class_="entry-content") or
                    soup.find("div", class_="post-content") or
                    soup.find("main")
                )
                
                if article_elem:
                    content = article_elem.get_text(separator=" ", strip=True)
                else:
                    # Fallback to body text
                    content = soup.get_text(separator=" ", strip=True)
                
                if len(content) > 200:  # Minimum content threshold
                    return ArticleContent(
                        url=original_url,
                        title=title_text,
                        content=content,
                        fetch_successful=True,
                        content_length=len(content)
                    )
        except Exception as e:
            logger.debug(f"Failed to fetch from archive.org: {e}")
        
        return None

    def fetch_article(self, url: str, max_retries: int = 3) -> ArticleContent:
        """
        Fetch and extract article content from a URL.

        Fallback chain (free local methods first, paid last):
        1. newspaper3k  — fast, free, article-specific extraction
        2. curl_cffi     — TLS fingerprint impersonation (Cloudflare bypass)
        3. Playwright    — full headless browser (JS rendering)
        4. Oxylabs API   — paid cloud scraper (anti-bot + JS rendering)
        5. archive.org   — Wayback Machine fallback for historical articles

        Args:
            url: URL to fetch
            max_retries: Maximum number of retry attempts

        Returns:
            ArticleContent object with extracted content
        """
        # Resolve Google News redirect URLs before fetching
        url = _resolve_google_news_url(url)

        from urllib.parse import urlparse
        domain = urlparse(url).netloc.lower()
        tier_attempts: list[dict[str, object]] = []

        # Reject domains that never contain usable article content
        base_domain = ".".join(domain.split(".")[-2:]) if domain.count(".") >= 1 else domain
        if domain in BLOCKED_FETCH_DOMAINS or base_domain in BLOCKED_FETCH_DOMAINS:
            logger.info(f"FETCH SKIP blocked domain={domain} url={url[:80]}")
            _metrics.increment("article_fetch_failure_total", labels={"tier": "newspaper3k", "source": _fetch_domain(url), "reason": "blocked_domain"})
            tier_attempts.append(
                _build_fetch_attempt_payload(
                    "precheck",
                    duration_seconds=0.0,
                    result=None,
                    error_code="blocked_domain",
                )
            )
            return ArticleContent(
                url=url, title="", content="", fetch_successful=False,
                error_message=f"Domain blocked (social media / IOC database): {domain}",
                content_length=0,
                fetch_metadata={"selected_tier": None, "tier_attempts": tier_attempts},
            )

        # Reject domains that have already failed all 4 tiers earlier in this
        # process. Without this, one slow-broken site (e.g. tudocelular.com)
        # can burn 6+ minutes across 3 URL variants × 4 tiers each. Cleared
        # on container restart, so flaky sites get a fresh chance per deploy.
        if _domain_failed_dynamically(domain):
            logger.info(f"FETCH SKIP dynamic block (failed all tiers earlier this session) domain={domain}")
            _metrics.increment("article_fetch_failure_total", labels={"tier": "newspaper3k", "source": _fetch_domain(url), "reason": "session_blocked"})
            tier_attempts.append(
                _build_fetch_attempt_payload(
                    "precheck",
                    duration_seconds=0.0,
                    result=None,
                    error_code="session_blocked",
                )
            )
            return ArticleContent(
                url=url, title="", content="", fetch_successful=False,
                error_message=f"Domain failed all tiers earlier in session: {domain}",
                content_length=0,
                fetch_metadata={"selected_tier": None, "tier_attempts": tier_attempts},
            )

        # Reject binary document URLs — they return raw binary content, not article HTML,
        # and can be enormous (10+ MB PDFs) causing RSS memory spikes that kill the worker.
        _url_path_lower = urlparse(url).path.lower()
        if _url_path_lower.endswith(('.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xlsx', '.xls', '.zip', '.gz')):
            logger.info(f"FETCH SKIP binary document url={url[:80]}")
            tier_attempts.append(
                _build_fetch_attempt_payload(
                    "precheck",
                    duration_seconds=0.0,
                    result=None,
                    error_code="binary_document",
                )
            )
            return ArticleContent(
                url=url, title="", content="", fetch_successful=False,
                error_message=f"Binary document URL skipped: {_url_path_lower[-10:]}",
                content_length=0,
                fetch_metadata={"selected_tier": None, "tier_attempts": tier_attempts},
            )

        logger.info(f"FETCH CHAIN START: {domain} — {url[:100]}")
        _src_label = _fetch_domain(url)

        # --- Tier 1: newspaper3k (free, fast) ---
        if NEWSPAPER_AVAILABLE:
            _t0 = time.time()
            article_content = self._fetch_with_newspaper(url)
            _dur = time.time() - _t0
            _lbl = {"tier": "newspaper3k", "source": _src_label}
            _metrics.increment("article_fetch_attempts_total", labels=_lbl)
            tier_attempts.append(
                _build_fetch_attempt_payload(
                    "newspaper3k",
                    duration_seconds=_dur,
                    result=article_content,
                )
            )
            if article_content and article_content.fetch_successful:
                logger.info(f"FETCH OK tier=newspaper3k domain={domain} chars={article_content.content_length}")
                _metrics.increment("article_fetch_success_total", labels=_lbl)
                _metrics.observe("article_fetch_duration_seconds", _dur, labels=_lbl)
                _metrics.observe("article_content_length_chars", float(article_content.content_length or 0), labels=_lbl)
                article_content.fetch_metadata = {
                    "selected_tier": "newspaper3k",
                    "tier_attempts": list(tier_attempts),
                }
                return article_content
            _metrics.increment("article_fetch_failure_total", labels={**_lbl, "reason": _classify_fetch_failure(article_content)})
            logger.info(f"FETCH FAIL tier=newspaper3k domain={domain}")

        def _try_httpclient() -> Optional[ArticleContent]:
            _t0 = time.time()
            http_content = self._fetch_with_browser(url)
            _dur = time.time() - _t0
            _lbl = {"tier": "httpclient", "source": _src_label}
            _metrics.increment("article_fetch_attempts_total", labels=_lbl)
            tier_attempts.append(
                _build_fetch_attempt_payload(
                    "httpclient",
                    duration_seconds=_dur,
                    result=http_content,
                )
            )
            if http_content and http_content.fetch_successful:
                logger.info(f"FETCH OK tier=HttpClient domain={domain} chars={http_content.content_length}")
                _metrics.increment("article_fetch_success_total", labels=_lbl)
                _metrics.observe("article_fetch_duration_seconds", _dur, labels=_lbl)
                _metrics.observe("article_content_length_chars", float(http_content.content_length or 0), labels=_lbl)
                http_content.fetch_metadata = {
                    "selected_tier": "httpclient",
                    "tier_attempts": list(tier_attempts),
                }
                return http_content
            _metrics.increment("article_fetch_failure_total", labels={**_lbl, "reason": _classify_fetch_failure(http_content)})
            logger.info(f"FETCH FAIL tier=HttpClient domain={domain}")
            return None

        def _try_oxylabs() -> Optional[ArticleContent]:
            _t0 = time.time()
            oxylabs_content = self._fetch_with_oxylabs(url)
            _dur = time.time() - _t0
            _lbl = {"tier": "oxylabs", "source": _src_label}
            _metrics.increment("article_fetch_attempts_total", labels=_lbl)
            tier_attempts.append(
                _build_fetch_attempt_payload(
                    "oxylabs",
                    duration_seconds=_dur,
                    result=oxylabs_content,
                )
            )
            if oxylabs_content and oxylabs_content.fetch_successful:
                logger.info(f"FETCH OK tier=Oxylabs domain={domain} chars={oxylabs_content.content_length}")
                _metrics.increment("article_fetch_success_total", labels=_lbl)
                _metrics.observe("article_fetch_duration_seconds", _dur, labels=_lbl)
                _metrics.observe("article_content_length_chars", float(oxylabs_content.content_length or 0), labels=_lbl)
                oxylabs_content.fetch_metadata = {
                    "selected_tier": "oxylabs",
                    "tier_attempts": list(tier_attempts),
                }
                return oxylabs_content
            _metrics.increment("article_fetch_failure_total", labels={**_lbl, "reason": _classify_fetch_failure(oxylabs_content)})
            logger.info(f"FETCH FAIL tier=Oxylabs domain={domain}")
            return None

        # Railway safe mode prefers Oxylabs before local browser work to avoid
        # burning memory on Chromium for cases the cloud fetcher can satisfy.
        if _prefer_oxylabs_before_browser():
            oxylabs_content = _try_oxylabs()
            if oxylabs_content:
                return oxylabs_content
            article_content = _try_httpclient()
            if article_content:
                return article_content
        else:
            # Standard tier order: try the local Chromium / curl_cffi fallback
            # first (free), then the paid Oxylabs cloud scraper.
            article_content = _try_httpclient()
            if article_content:
                return article_content
            oxylabs_content = _try_oxylabs()
            if oxylabs_content:
                return oxylabs_content

        # --- Tier 4: archive.org (free, historical fallback) ---
        _t0 = time.time()
        archive_content = self._fetch_from_archive(url)
        _dur = time.time() - _t0
        _lbl = {"tier": "archive_org", "source": _src_label}
        _metrics.increment("article_fetch_attempts_total", labels=_lbl)
        tier_attempts.append(
            _build_fetch_attempt_payload(
                "archive_org",
                duration_seconds=_dur,
                result=archive_content,
            )
        )
        if archive_content and archive_content.fetch_successful:
            logger.info(f"FETCH OK tier=archive.org domain={domain} chars={archive_content.content_length}")
            _metrics.increment("article_fetch_success_total", labels=_lbl)
            _metrics.observe("article_fetch_duration_seconds", _dur, labels=_lbl)
            _metrics.observe("article_content_length_chars", float(archive_content.content_length or 0), labels=_lbl)
            archive_content.fetch_metadata = {
                "selected_tier": "archive_org",
                "tier_attempts": list(tier_attempts),
            }
            return archive_content
        _metrics.increment("article_fetch_failure_total", labels={**_lbl, "reason": _classify_fetch_failure(archive_content)})

        # All methods failed — record this domain so subsequent fetches to it
        # short-circuit. Saves 1-3 minutes per future URL on the same domain.
        _record_dynamic_domain_failure(domain)
        logger.warning(f"FETCH FAILED ALL TIERS: domain={domain} url={url[:100]} (added to dynamic block list)")
        return ArticleContent(
            url=url,
            title="",
            content="",
            fetch_successful=False,
            error_message="All fetch methods failed (newspaper3k, curl_cffi/Playwright, Oxylabs, archive.org)",
            content_length=0,
            fetch_metadata={"selected_tier": None, "tier_attempts": tier_attempts},
        )

    def _fetch_with_browser(self, url: str) -> ArticleContent:
        """
        Fetch article using HttpClient (curl_cffi + Playwright).

        Uses the multi-tier fallback chain in HttpClient:
        1. curl_cffi with Chrome TLS fingerprint (bypasses most Cloudflare)
        2. Playwright headless with stealth patches (JS rendering)
        3. Plain requests (simple pages)

        Args:
            url: URL to fetch

        Returns:
            ArticleContent with extracted content
        """
        try:
            logger.debug(f"HttpClient: Fetching soup for {url}")
            soup = self.http_client.get_soup(url, allow_404=True)

            if soup is None:
                logger.warning(f"HttpClient: soup is None for {url}")
                return ArticleContent(
                    url=url,
                    title="",
                    content="",
                    fetch_successful=False,
                    error_message="HttpClient fetch failed or returned None",
                    content_length=0
                )
            
            # Extract content
            logger.debug(f"HttpClient: Extracting content from {url}")
            title = self._extract_title(soup)
            content = self._extract_content(soup)
            author = self._extract_author(soup)
            publish_date = self._extract_publish_date(soup)
            
            logger.debug(f"HttpClient: Extracted title length: {len(title) if title else 0}, content length: {len(content) if content else 0}")
            
            # Clean content
            content_before_clean = content
            content = self._clean_content(content)
            logger.debug(f"HttpClient: Content length after cleaning: {len(content) if content else 0} (was {len(content_before_clean) if content_before_clean else 0})")
            
            # Check for meaningful content - lower threshold for databreaches.net and similar sites
            min_length = 50 if "databreaches.net" in url.lower() else 100
            content_stripped = content.strip() if content else ""
            
            if not content or len(content_stripped) < min_length:
                logger.warning(
                    f"HttpClient: Content too short for {url}: "
                    f"length={len(content_stripped)}, min={min_length}, "
                    f"title={title[:50] if title else 'None'}, "
                    f"content_preview={content_stripped[:100] if content_stripped else 'None'}"
                )
                return ArticleContent(
                    url=url,
                    title=title or "",
                    content=content or "",
                    author=author,
                    publish_date=publish_date,
                    fetch_successful=False,
                    error_message=f"Extracted content too short or empty (length: {len(content_stripped)}, min: {min_length})",
                    content_length=len(content) if content else 0
                )
            
            if _is_gate_page(title, content):
                logger.info(f"HttpClient: gate/CAPTCHA page detected for {url} — falling through to next tier")
                return ArticleContent(
                    url=url, title=title or "", content="",
                    fetch_successful=False,
                    error_message="Gate/CAPTCHA page detected",
                    content_length=0
                )

            logger.info(f"HttpClient: Successfully extracted content from {url}: {len(content)} chars")
            return ArticleContent(
                url=url,
                title=title,
                content=content,
                author=author,
                publish_date=publish_date,
                fetch_successful=True,
                content_length=len(content)
            )
            
        except Exception as e:
            logger.error(f"Error fetching article from {url} with HttpClient: {e}", exc_info=True)
            return ArticleContent(
                url=url,
                title="",
                content="",
                fetch_successful=False,
                error_message=f"HttpClient exception: {str(e)}",
                content_length=0
            )
    
    def _fetch_with_newspaper(self, url: str) -> Optional[ArticleContent]:
        """
        Fetch article using newspaper3k library.
        
        This is the preferred method as newspaper3k is specifically
        designed for article extraction and handles many edge cases.
        
        Args:
            url: URL to fetch
            
        Returns:
            ArticleContent if successful, None otherwise
        """
        try:
            logger.debug(f"newspaper3k: Fetching {url}")
            article = Article(url, language='en')
            article.download()
            article.parse()
            
            # Minimum content threshold:
            # - databreaches.net: 50 chars (brief blotter-style posts)
            # - all others: 500 chars — anything shorter is a headline/snippet
            #   from a JS-rendered page (e.g. BBC, Guardian) and won't give the
            #   LLM enough to work with. Falling through to HttpClient/Oxylabs
            #   gets the real article body via JS rendering.
            min_length = 50 if "databreaches.net" in url.lower() else 500
            text_stripped = article.text.strip() if article.text else ""

            if not article.text or len(text_stripped) < min_length:
                logger.debug(
                    f"newspaper3k extracted content too short for {url}: "
                    f"length={len(text_stripped)}, min={min_length}, "
                    f"title={article.title[:50] if article.title else 'None'}, "
                    f"content_preview={text_stripped[:100] if text_stripped else 'None'}"
                )
                return None
            
            logger.debug(f"newspaper3k: Successfully extracted {len(text_stripped)} chars from {url}")
            
            # Extract publish date and normalize
            publish_date = None
            if article.publish_date:
                try:
                    # newspaper3k returns datetime objects
                    publish_date = article.publish_date.date().isoformat()
                except (AttributeError, ValueError):
                    # If it's already a string, try to normalize
                    if isinstance(article.publish_date, str):
                        publish_date = self._normalize_date_to_iso(article.publish_date)
                    else:
                        publish_date = None
            
            # Get authors (article.authors is a list)
            author = None
            if article.authors:
                author = ', '.join(article.authors) if len(article.authors) > 1 else article.authors[0]
            
            content = self._clean_content(article.text)

            if _is_gate_page(article.title, content):
                logger.info(f"newspaper3k: gate/CAPTCHA page detected for {url} — falling through to next tier")
                return None

            return ArticleContent(
                url=url,
                title=article.title or "",
                content=content,
                author=author,
                publish_date=publish_date,
                fetch_successful=True,
                content_length=len(content)
            )
            
        except Exception as e:
            # Log the specific error for debugging, but don't fail completely
            # We'll fall back to HttpClient which handles bot detection better
            error_msg = str(e)
            if '403' in error_msg or 'Forbidden' in error_msg:
                logger.debug(f"newspaper3k got 403 Forbidden for {url}, will try HttpClient fallback")
            else:
                logger.debug(f"newspaper3k failed for {url}: {e}, will try HttpClient fallback")
            return None
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract article title from soup."""
        # Try various title selectors
        title_selectors = [
            'h1.entry-title',
            'h1.post-title',
            'h1.article-title',
            'h1[class*="title"]',
            'article h1',
            '.article-header h1',
            'h1',
            'title'
        ]
        
        for selector in title_selectors:
            element = soup.select_one(selector)
            if element:
                title = element.get_text(strip=True)
                if title and len(title) > 10:  # Filter out short/non-article titles
                    return title
        
        # Fallback to meta title
        meta_title = soup.find('meta', property='og:title')
        if meta_title and meta_title.get('content'):
            return meta_title['content'].strip()
        
        meta_title = soup.find('meta', {'name': 'title'})
        if meta_title and meta_title.get('content'):
            return meta_title['content'].strip()
        
        # Last resort: page title
        title_tag = soup.find('title')
        if title_tag:
            return title_tag.get_text(strip=True)
        
        return ""
    
    def _extract_content(self, soup: BeautifulSoup) -> str:
        """
        Extract main article content from soup.
        
        Uses extensive selectors to cover global news sites, various CMS systems,
        and multiple fallback mechanisms for maximum compatibility.
        """
        # Remove unwanted elements that typically contain non-article content
        unwanted_tags = [
            'script', 'style', 'nav', 'header', 'footer', 'aside', 'iframe',
            'noscript', 'svg', 'canvas', 'video', 'audio', 'form', 'button',
            'input', 'select', 'textarea', 'label', 'fieldset', 'legend',
            'menu', 'menuitem', 'dialog', 'template'
        ]
        for element in soup(unwanted_tags):
            element.decompose()
        
        # Remove common non-content elements by class/id patterns
        unwanted_patterns = [
            '[class*="sidebar"]', '[id*="sidebar"]',
            '[class*="comment"]', '[id*="comment"]',
            '[class*="related"]', '[id*="related"]',
            '[class*="recommend"]', '[id*="recommend"]',
            '[class*="social"]', '[id*="social"]',
            '[class*="share"]', '[id*="share"]',
            '[class*="newsletter"]', '[id*="newsletter"]',
            '[class*="subscription"]', '[id*="subscription"]',
            '[class*="advertisement"]', '[id*="advertisement"]',
            '[class*="ad-"]', '[id*="ad-"]',
            '[class*="promo"]', '[id*="promo"]',
            '[class*="widget"]', '[id*="widget"]',
            '[class*="popup"]', '[id*="popup"]',
            '[class*="modal"]', '[id*="modal"]',
            '[class*="cookie"]', '[id*="cookie"]',
            '[class*="banner"]', '[id*="banner"]',
            '[class*="navigation"]', '[id*="navigation"]',
            '[class*="breadcrumb"]', '[id*="breadcrumb"]',
            '[class*="tags"]', '[id*="tags"]',
            '[class*="meta-"]', 
        ]
        for pattern in unwanted_patterns:
            try:
                for element in soup.select(pattern):
                    element.decompose()
            except Exception:
                continue
        
        # Comprehensive content selectors - ordered by specificity
        content_selectors = [
            # === SITE-SPECIFIC SELECTORS ===
            # Wordfence Blog
            'section.blog-post-content',
            '.blog-post-content .container',
            '.blog-post-content .row',
            
            # DarkReading / Informa TechTarget
            '.ArticleBase-BodyContent',
            '[data-testid="article-base-body-content"]',
            '.ContentParagraph',
            
            # Belgian/European news (lesoir.be, etc.)
            'article.r-article',
            'r-article--section',
            '.r-article--section',
            '.article__body',
            
            # SecurityWeek
            '.article-content',
            '.entry-content',
            
            # BleepingComputer
            '.articleBody',
            '.article_section',
            
            # The Record / Recorded Future
            '.post-content',
            '.story-body',
            
            # Krebs on Security
            '.post',
            '.entry',
            
            # The Hacker News
            '.story-content',
            '.home-right',
            
            # Ars Technica
            '.article-content',
            '.post-content',
            
            # Threatpost / SC Magazine
            '.article-content',
            '.post-body',
            
            # ZDNet / TechRepublic
            '.article-body',
            '.content-body',
            
            # Wired
            '.body__inner-container',
            '.article__body',
            
            # === CMS-SPECIFIC SELECTORS ===
            # WordPress (most common for security blogs)
            # databreaches.net specific - try these first
            'article .entry-content',
            '.entry-content',
            '.post .entry-content',
            '.wp-content',
            '.post-content',
            '.single-post-content',
            '.blog-post-content',
            '.hentry',
            '.type-post',
            '.format-standard',
            
            # Drupal
            '.field--name-body',
            '.node__content',
            '.content-body',
            
            # Joomla
            '.item-page',
            '.article-body',
            
            # Ghost
            '.post-full-content',
            '.kg-card-markdown',
            
            # Medium
            '.section-content',
            '[class*="postContent"]',
            
            # Substack
            '.post-content',
            '.body',
            
            # === SEMANTIC HTML5 SELECTORS ===
            'article .content',
            'article .body',
            'article .text',
            'article section',
            'main article',
            'main .content',
            '[role="article"]',
            '[role="main"]',
            
            # Section-based (common in Bootstrap/modern sites)
            'section.content',
            'section.post',
            'section.article',
            'section.entry',
            'section[class*="blog"]',
            'section[class*="post"]',
            'section[class*="article"]',
            'section[class*="content"]',
            
            # Container patterns (Bootstrap, Foundation)
            '.container .post',
            '.container .article',
            '.container .content',
            '.row .col-lg-8',  # Common blog layout
            '.row .col-md-8',
            '.col-12.col-lg-8',  # Bootstrap 5
            
            # === GENERIC CLASS PATTERNS ===
            # Article body patterns
            '[class*="article-body"]',
            '[class*="articleBody"]',
            '[class*="article_body"]',
            '[class*="article-content"]',
            '[class*="articleContent"]',
            '[class*="article_content"]',
            '[class*="article-text"]',
            '[class*="articleText"]',
            
            # Post body patterns
            '[class*="post-body"]',
            '[class*="postBody"]',
            '[class*="post_body"]',
            '[class*="post-content"]',
            '[class*="postContent"]',
            '[class*="post_content"]',
            
            # Story patterns
            '[class*="story-body"]',
            '[class*="storyBody"]',
            '[class*="story-content"]',
            '[class*="storyContent"]',
            
            # News patterns
            '[class*="news-body"]',
            '[class*="newsBody"]',
            '[class*="news-content"]',
            '[class*="newsContent"]',
            
            # Entry patterns
            '[class*="entry-content"]',
            '[class*="entryContent"]',
            '[class*="entry_content"]',
            
            # Content patterns
            '[class*="content-body"]',
            '[class*="contentBody"]',
            '[class*="main-content"]',
            '[class*="mainContent"]',
            '[class*="page-content"]',
            '[class*="pageContent"]',
            
            # Text patterns
            '[class*="rich-text"]',
            '[class*="richText"]',
            '[class*="prose"]',
            '[class*="text-content"]',
            
            # === ID-BASED SELECTORS ===
            '#article-body',
            '#article-content',
            '#articleBody',
            '#articleContent',
            '#post-body',
            '#post-content',
            '#postBody',
            '#postContent',
            '#story-body',
            '#story-content',
            '#main-content',
            '#content',
            '#main',
            
            # === MICRODATA/SCHEMA.ORG ===
            '[itemprop="articleBody"]',
            '[itemprop="text"]',
            
            # === FALLBACK SELECTORS ===
            'article',
            'main',
            '.content',
            '#content',
            '.main',
            '#main',
        ]
        
        content_parts = []
        
        for selector in content_selectors:
            try:
                elements = soup.select(selector)
                for element in elements:
                    # Get all text-containing elements
                    text_elements = element.find_all(['p', 'div', 'span', 'li', 'blockquote', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'], recursive=True)
                    for el in text_elements:
                        # Skip if it's inside a nested unwanted element
                        if el.find_parent(['nav', 'aside', 'footer', 'header']):
                            continue
                        
                        text = el.get_text(strip=True)
                        # Filter out very short text (likely navigation/UI elements)
                        if text and len(text) > 40 and text not in content_parts:
                            content_parts.append(text)
                    
                    # If we found substantial content, stop searching
                    total_len = len(' '.join(content_parts))
                    if total_len > 500:
                        break
                
                if content_parts and len(' '.join(content_parts)) > 300:
                    break
            except Exception:
                continue
        
        # Fallback: Get all paragraphs from the page
        if not content_parts or len(' '.join(content_parts)) < 200:
            paragraphs = soup.find_all('p')
            for p in paragraphs:
                # Skip if inside unwanted parent
                if p.find_parent(['nav', 'aside', 'footer', 'header', 'form']):
                    continue
                
                text = p.get_text(strip=True)
                if text and len(text) > 40 and text not in content_parts:
                    content_parts.append(text)
        
        # Final fallback: Get text from body if nothing else worked
        if not content_parts:
            body = soup.find('body')
            if body:
                text = body.get_text(separator=' ', strip=True)
                # Clean up excessive whitespace
                import re
                text = re.sub(r'\s+', ' ', text)
                if len(text) > 100:
                    content_parts.append(text[:10000])  # Limit to first 10k chars
        
        return ' '.join(content_parts)

    def _load_structured_metadata_payloads(self, soup: BeautifulSoup) -> List[Dict]:
        """Parse JSON-LD and common hydration blobs that may contain article metadata."""
        payloads: List[Dict] = []

        for script in soup.find_all("script"):
            raw = script.string or script.get_text()
            if not raw:
                continue

            script_type = (script.get("type") or "").lower()
            script_id = (script.get("id") or "").lower()
            parsed = None

            if "ld+json" in script_type or script_id == "__next_data__":
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    continue
            elif "application/json" in script_type and any(
                token in raw for token in ("datePublished", "publicationDate", "firstPublished", "author", "contributor")
            ):
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    continue
            elif any(token in raw for token in ("__INITIAL_STATE__", "__PRELOADED_STATE__", "__NUXT__")) and any(
                token in raw for token in ("datePublished", "publicationDate", "firstPublished", "author", "contributor")
            ):
                match = re.search(
                    r"window\.(?:__INITIAL_STATE__|__PRELOADED_STATE__|__NUXT__)\s*=\s*(\{.*\})\s*;?\s*$",
                    raw,
                    re.DOTALL,
                )
                if match:
                    try:
                        parsed = json.loads(match.group(1))
                    except json.JSONDecodeError:
                        parsed = None

            if parsed is not None:
                payloads.append(parsed)

        return payloads

    def _collect_structured_key_matches(
        self,
        node,
        target_keys: set[str],
        matches: List,
    ) -> None:
        """Recursively collect values for matching keys from structured metadata."""
        if isinstance(node, dict):
            for key, value in node.items():
                if key in target_keys and value not in (None, "", [], {}):
                    matches.append(value)
                self._collect_structured_key_matches(value, target_keys, matches)
        elif isinstance(node, list):
            for item in node:
                self._collect_structured_key_matches(item, target_keys, matches)

    def _extract_structured_metadata_values(self, soup: BeautifulSoup, target_keys: set[str]) -> List:
        """Return candidate metadata values from JSON-LD / hydration payloads."""
        matches: List = []
        for payload in self._load_structured_metadata_payloads(soup):
            self._collect_structured_key_matches(payload, target_keys, matches)
        return matches

    def _normalize_author_value(self, value) -> Optional[str]:
        """Normalize structured author values into a clean display string."""
        if value is None:
            return None

        if isinstance(value, list):
            authors = []
            for item in value:
                normalized = self._normalize_author_value(item)
                if normalized and normalized not in authors:
                    authors.append(normalized)
            return ", ".join(authors) if authors else None

        if isinstance(value, dict):
            for key in ("name", "title", "label", "text", "value"):
                if value.get(key):
                    return self._normalize_author_value(value.get(key))
            return None

        if not isinstance(value, str):
            return None

        cleaned = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
        cleaned = re.sub(r"^\s*by\s+", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,;|-")
        return cleaned or None

    def _normalize_structured_date_value(self, value) -> Optional[str]:
        """Normalize date strings or timestamps from structured metadata to ISO dates."""
        if value is None:
            return None

        if isinstance(value, dict):
            for key in ("content", "value", "@value", "text"):
                if value.get(key):
                    return self._normalize_structured_date_value(value.get(key))
            return None

        if isinstance(value, list):
            for item in value:
                normalized = self._normalize_structured_date_value(item)
                if normalized:
                    return normalized
            return None

        if isinstance(value, (int, float)):
            try:
                timestamp = float(value)
                if timestamp > 1_000_000_000_000:
                    timestamp /= 1000.0
                from datetime import datetime, timezone
                return datetime.fromtimestamp(timestamp, tz=timezone.utc).date().isoformat()
            except (OverflowError, OSError, ValueError):
                return None

        if isinstance(value, str):
            return self._normalize_date_to_iso(value)

        return None
    
    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract article author from soup."""
        for value in self._extract_structured_metadata_values(soup, _STRUCTURED_AUTHOR_KEYS):
            author = self._normalize_author_value(value)
            if author:
                return author

        author_selectors = [
            '[rel="author"]',
            '.author',
            '[class*="author"]',
            '[itemprop="author"]',
            'meta[property="article:author"]',
            'meta[name="article:author"]',
            'meta[name="author"]',
            'meta[property="author"]',
            'meta[name="parsely-author"]',
            'meta[name="dc.creator"]',
            'meta[name="dcterms.creator"]',
            'meta[name="twitter:creator"]',
            'meta[property="cXenseParse:author"]',
            'meta[name="cXenseParse:author"]',
            'meta[name="byl"]',
        ]
        
        for selector in author_selectors:
            element = soup.select_one(selector)
            if element:
                if element.name == 'meta':
                    author = element.get('content', '')
                else:
                    author = element.get_text(strip=True)
                
                if author:
                    normalized = self._normalize_author_value(author)
                    if normalized:
                        return normalized
        
        return None
    
    def _extract_publish_date(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract article publish date from soup and normalize to ISO format when possible.
        
        Returns:
            Date string in ISO format (YYYY-MM-DD) when possible, or original format if parsing fails
        """
        for value in self._extract_structured_metadata_values(soup, _STRUCTURED_PUBLISH_DATE_KEYS):
            normalized = self._normalize_structured_date_value(value)
            if normalized:
                return normalized

        date_selectors = [
            'time[datetime]',
            '[class*="date"]',
            '[class*="published"]',
            '[itemprop="datePublished"]',
            'meta[property="article:published_time"]',
            'meta[name="article:published_time"]',
            'meta[name="date"]',
            'meta[property="og:published_time"]',
            'meta[name="og:published_time"]',
            'meta[name="publish-date"]',
            'meta[name="pubdate"]',
            'meta[name="publish_date"]',
            'meta[name="pub_date"]',
            'meta[name="parsely-pub-date"]',
            'meta[name="cXenseParse:publishtime"]',
            'meta[name="dc.date"]',
            'meta[name="dcterms.created"]',
        ]
        
        raw_date = None
        for selector in date_selectors:
            element = soup.select_one(selector)
            if element:
                if element.name == 'meta':
                    raw_date = element.get('content', '')
                elif element.name == 'time':
                    raw_date = element.get('datetime', '') or element.get_text(strip=True)
                else:
                    raw_date = element.get_text(strip=True)
                
                if raw_date:
                    break
        
        if not raw_date:
            return None
        
        # Try to normalize to ISO format (YYYY-MM-DD) for easier LLM processing
        normalized = self._normalize_date_to_iso(raw_date)
        return normalized if normalized else raw_date  # Return original if normalization fails
    
    def _normalize_date_to_iso(self, date_str: str) -> Optional[str]:
        """
        Normalize date string to ISO format (YYYY-MM-DD) when possible.
        
        Args:
            date_str: Raw date string in various formats
            
        Returns:
            ISO format date string (YYYY-MM-DD) or None if parsing fails
        """
        if not date_str:
            return None
        
        date_str = date_str.strip()
        
        # Try parsing with dateutil if available (handles many formats)
        try:
            from dateutil import parser as date_parser
            dt = date_parser.parse(date_str)
            return dt.date().isoformat()  # Return YYYY-MM-DD format
        except (ImportError, ValueError, TypeError):
            pass
        
        # Try common ISO and RFC formats
        import re
        from datetime import datetime
        
        # ISO 8601 formats
        iso_patterns = [
            r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})',  # YYYY-MM-DDTHH:MM:SS
            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)',  # YYYY-MM-DDTHH:MM:SSZ
            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2})',  # YYYY-MM-DDTHH:MM:SS+00:00
        ]
        
        for pattern in iso_patterns:
            match = re.search(pattern, date_str)
            if match:
                date_part = match.group(1).split('T')[0]  # Extract YYYY-MM-DD part
                try:
                    # Validate it's a valid date
                    datetime.strptime(date_part, "%Y-%m-%d")
                    return date_part
                except ValueError:
                    continue
        
        # RFC 822/1123 formats (common in RSS feeds)
        rfc_formats = [
            "%a, %d %b %Y %H:%M:%S %z",  # RFC 822 with timezone
            "%a, %d %b %Y %H:%M:%S %Z",  # RFC 822 with GMT/UTC
            "%a, %d %b %Y %H:%M:%S",     # RFC 822 without timezone
        ]
        
        for fmt in rfc_formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.date().isoformat()
            except ValueError:
                continue
        
        # Common human-readable formats
        human_formats = [
            "%B %d, %Y",   # April 17, 2025
            "%b %d, %Y",   # Apr 17, 2025
            "%d %B %Y",    # 10 December 2021
            "%d %b %Y",    # 10 Dec 2021
            "%Y-%m-%d",    # 2025-08-11
            "%m/%d/%Y",    # 11/19/2025
            "%d/%m/%Y",    # 19/11/2025
        ]
        
        for fmt in human_formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.date().isoformat()
            except ValueError:
                continue
        
        # If we can't parse, return None to use original
        return None
    
    def _clean_content(self, content: str) -> str:
        """Clean extracted content."""
        # Remove excessive whitespace
        lines = content.split('\n')
        cleaned_lines = []
        for line in lines:
            line = line.strip()
            if line:
                cleaned_lines.append(line)
        
        # Join with single spaces
        cleaned = ' '.join(cleaned_lines)
        
        # Remove multiple consecutive spaces
        while '  ' in cleaned:
            cleaned = cleaned.replace('  ', ' ')
        
        return cleaned.strip()
    
    def fetch_multiple_articles(self, urls: List[str]) -> Dict[str, ArticleContent]:
        """
        Fetch multiple articles in parallel (sequential for now to avoid rate limits).
        
        Args:
            urls: List of URLs to fetch
            
        Returns:
            Dictionary mapping URL to ArticleContent
        """
        results = {}
        for url in urls:
            results[url] = self.fetch_article(url)
        
        return results
