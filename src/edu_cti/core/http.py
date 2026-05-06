"""
Advanced HTTP client with multi-tier bot evasion for EduThreat-CTI.

Scraping strategy (2 steps):
  Step 1: Search for news using keywords (requires JS rendering for search results)
  Step 2: Fetch article content (needs to bypass Cloudflare/bot detection)

Fallback chain:
  1. curl_cffi with TLS fingerprint impersonation (fastest, bypasses most Cloudflare)
  2. Playwright headless with stealth patches (JS rendering, search pages)
  3. Plain requests with retry (RSS feeds, APIs, simple pages)

All modes are headless-only (no display required on backend).
"""

from __future__ import annotations

import concurrent.futures
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Callable, Iterable, Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from src.edu_cti.core import config

logger = logging.getLogger(__name__)

# ── Optional imports ─────────────────────────────────────────────────

# curl_cffi: TLS fingerprint impersonation (best for Cloudflare bypass)
try:
    from curl_cffi import requests as cffi_requests

    CFFI_AVAILABLE = True
except ImportError:
    CFFI_AVAILABLE = False
    logger.debug("curl_cffi not available – install with: pip install curl_cffi")

# Playwright: headless browser for JS-rendered pages
try:
    from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext
    from playwright_stealth import Stealth

    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logger.debug("playwright not available – install with: pip install playwright playwright-stealth")

# Plain requests fallback
import requests as plain_requests

# ── Constants ────────────────────────────────────────────────────────

# Chrome versions that curl_cffi can impersonate (TLS fingerprint match)
CFFI_IMPERSONATE_TARGETS = [
    "chrome131",
    "chrome130",
    "chrome124",
    "chrome120",
    "chrome116",
    "chrome110",
    "chrome107",
    "chrome104",
    "chrome101",
    "chrome100",
    "chrome99",
]

# Realistic browser fingerprints (UA + viewport + platform)
BROWSER_PROFILES = [
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "viewport": {"width": 1920, "height": 1080},
        "platform": "macOS",
        "locale": "en-US",
    },
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "viewport": {"width": 1536, "height": 864},
        "platform": "Windows",
        "locale": "en-US",
    },
    {
        "ua": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "viewport": {"width": 1440, "height": 900},
        "platform": "Linux",
        "locale": "en-US",
    },
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
        "viewport": {"width": 2560, "height": 1440},
        "platform": "macOS",
        "locale": "en-US",
    },
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
        "viewport": {"width": 1366, "height": 768},
        "platform": "Windows",
        "locale": "en-US",
    },
]

# Known domains that need specific handling
CLOUDFLARE_DOMAINS = [
    "databreaches.net",
    "darkreading.com",
    "securityweek.com",
    "therecord.media",
    "bleepingcomputer.com",
]

# Domains that need JS rendering (search results, dynamic content)
JS_REQUIRED_DOMAINS = [
    "darkreading.com",
    "securityweek.com",
    "therecord.media",
    "thehackernews.com",
]


@dataclass
class HttpResponse:
    """HTTP response wrapper."""
    url: str
    status_code: int
    text: str
    headers: dict
    method_used: str = "requests"  # Track which method succeeded


class HttpClient:
    """
    Multi-tier HTTP client with advanced bot evasion.

    All operations are headless – no display or manual interaction required.

    Tier 1: curl_cffi with Chrome TLS fingerprint impersonation
        → Fastest, bypasses most Cloudflare challenges without a browser
        → Impersonates real Chrome TLS handshake at the socket level

    Tier 2: Playwright headless + stealth patches
        → Full JS rendering for search pages, Algolia, SPAs
        → playwright-stealth hides automation signals
        → Handles cookie consent, popups, interstitials automatically

    Tier 3: Plain requests with retry
        → RSS feeds, JSON APIs, simple HTML pages
        → Exponential backoff on failures
    """

    def __init__(
        self,
        *,
        timeout: int = config.REQUEST_TIMEOUT_SECONDS,
        user_agents: Optional[Iterable[str]] = None,
        min_delay: float = config.HTTP_MIN_DELAY,
        max_delay: float = config.HTTP_MAX_DELAY,
    ) -> None:
        self.timeout = timeout
        self.user_agents = list(user_agents or [p["ua"] for p in BROWSER_PROFILES])
        self.min_delay = min_delay
        self.max_delay = max_delay
        self._failed_domains: dict[str, int] = {}
        self._profile = random.choice(BROWSER_PROFILES)

        # Plain requests session (Tier 3)
        self.session = plain_requests.Session()

        # Playwright browser (lazy-initialized, reused across calls)
        self._pw = None
        self._stealth_cm = None
        self._browser: Browser | None = None
        self._browser_context: BrowserContext | None = None

        # Counter for periodic browser recycling. Chromium accumulates memory
        # across page loads (JS heaps, GPU/image decoder pools, DNS cache,
        # stealth-injected script state) even when pages are closed. Recycling
        # the browser every N fetches releases that memory back to the OS.
        self._browser_fetches_since_recycle = 0
        self._browser_recycle_threshold = int(
            os.environ.get("PLAYWRIGHT_RECYCLE_AFTER", "100")
        )

    def close(self) -> None:
        """Clean up browser resources."""
        # Shut down Playwright in its dedicated thread
        def _close_pw():
            if self._browser_context:
                try:
                    self._browser_context.close()
                except Exception:
                    pass
                self._browser_context = None
            if self._browser:
                try:
                    self._browser.close()
                except Exception:
                    pass
                self._browser = None
            if hasattr(self, '_stealth_cm') and self._stealth_cm:
                try:
                    self._stealth_cm.__exit__(None, None, None)
                except Exception:
                    pass
                self._stealth_cm = None
            self._pw = None

        if hasattr(self, '_pw_executor') and self._pw_executor:
            try:
                self._pw_executor.submit(_close_pw).result(timeout=10)
            except Exception:
                _close_pw()  # Fallback: close directly
            try:
                self._pw_executor.shutdown(wait=False)
            except Exception:
                pass
            self._pw_executor = None
        else:
            _close_pw()

    def __del__(self):
        self.close()

    # ── Internal helpers ─────────────────────────────────────────────

    def _random_headers(self) -> dict:
        ua = random.choice(self.user_agents)
        return {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
            "sec-ch-ua": '"Chromium";v="131", "Not_A Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": f'"{self._profile["platform"]}"',
        }

    def _sleep(self, lo: float | None = None, hi: float | None = None) -> None:
        time.sleep(random.uniform(lo or self.min_delay, hi or self.max_delay))

    @staticmethod
    def _domain(url: str) -> str:
        return urlparse(url).netloc.lower()

    def _needs_js(self, url: str) -> bool:
        domain = self._domain(url)
        return any(d in domain for d in JS_REQUIRED_DOMAINS)

    def _has_cloudflare(self, url: str) -> bool:
        domain = self._domain(url)
        return any(d in domain for d in CLOUDFLARE_DOMAINS)

    def _mark_failed(self, url: str) -> None:
        d = self._domain(url)
        self._failed_domains[d] = self._failed_domains.get(d, 0) + 1

    def _should_skip_requests(self, url: str) -> bool:
        d = self._domain(url)
        return self._failed_domains.get(d, 0) >= 2 or self._needs_js(url)

    # ── Tier 1: curl_cffi (TLS fingerprint impersonation) ────────────

    def _cffi_get(self, url: str, *, allow_404: bool = False) -> HttpResponse | None:
        """
        Fetch URL using curl_cffi with Chrome TLS fingerprint.

        This makes the request indistinguishable from a real Chrome browser
        at the TLS handshake level – the most effective Cloudflare bypass
        without running a full browser.
        """
        if not CFFI_AVAILABLE:
            return None

        target = random.choice(CFFI_IMPERSONATE_TARGETS)
        headers = self._random_headers()

        for attempt in range(3):
            try:
                self._sleep(0.1, 0.5)
                resp = cffi_requests.get(
                    url,
                    headers=headers,
                    impersonate=target,
                    timeout=self.timeout,
                    allow_redirects=True,
                )

                if allow_404 and resp.status_code == 404:
                    return None

                if resp.status_code == 200:
                    return HttpResponse(
                        url=str(resp.url),
                        status_code=resp.status_code,
                        text=resp.text,
                        headers=dict(resp.headers),
                        method_used=f"curl_cffi/{target}",
                    )

                if resp.status_code in (403, 503):
                    # Cloudflare challenge page – try different impersonation
                    target = random.choice(CFFI_IMPERSONATE_TARGETS)
                    logger.debug(f"curl_cffi got {resp.status_code}, retrying with {target}")
                    self._sleep(1.0, 3.0)
                    continue

                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"curl_cffi rate limited on {url}, waiting {wait}s")
                    time.sleep(wait)
                    continue

                # Other errors
                logger.debug(f"curl_cffi got {resp.status_code} for {url}")
                return HttpResponse(
                    url=str(resp.url),
                    status_code=resp.status_code,
                    text=resp.text,
                    headers=dict(resp.headers),
                    method_used=f"curl_cffi/{target}",
                )

            except Exception as e:
                logger.debug(f"curl_cffi attempt {attempt + 1} failed for {url}: {e}")
                self._sleep(1.0, 2.0)
                continue

        return None

    # ── Tier 2: Playwright (headless browser with stealth) ───────────

    def _recycle_browser_if_needed(self) -> None:
        """
        Close and reopen the Chromium browser every N fetches to release
        accumulated memory. MUST run inside the dedicated Playwright thread.

        Chromium accumulates memory across page loads (JS heap fragments,
        DNS cache, GPU/image decoder pools, stealth-injected script state)
        even when individual pages are closed. Without recycling, the
        Chromium child-process memory grows monotonically over hours and
        eventually OOMs the container.

        The Python heap is unaffected — this is a child-process leak that
        gc.collect() and malloc_trim() cannot reach.
        """
        if self._browser_fetches_since_recycle < self._browser_recycle_threshold:
            return
        if self._browser is None:
            self._browser_fetches_since_recycle = 0
            return
        logger.info(
            "Recycling Playwright browser after %d fetches to release Chromium memory",
            self._browser_fetches_since_recycle,
        )
        try:
            if self._browser_context is not None:
                try:
                    self._browser_context.close()
                except Exception as exc:
                    logger.debug("Browser context close failed (non-fatal): %s", exc)
                self._browser_context = None
            try:
                self._browser.close()
            except Exception as exc:
                logger.debug("Browser close failed (non-fatal): %s", exc)
            self._browser = None
            if self._stealth_cm:
                try:
                    self._stealth_cm.__exit__(None, None, None)
                except Exception as exc:
                    logger.debug("Stealth context exit failed (non-fatal): %s", exc)
                self._stealth_cm = None
            self._pw = None
        finally:
            self._browser_fetches_since_recycle = 0

    def _ensure_browser(self) -> BrowserContext:
        """Lazy-initialize Playwright browser with stealth configuration.

        IMPORTANT: Must only be called from the dedicated Playwright thread
        (via _playwright_get → _run_in_pw_thread) to avoid asyncio conflicts.
        """
        if self._browser_context is not None:
            return self._browser_context

        if not PLAYWRIGHT_AVAILABLE:
            raise RuntimeError("Playwright not available")

        profile = self._profile

        # Use Stealth wrapper to auto-apply evasion scripts to all pages
        stealth = Stealth()
        self._stealth_cm = stealth.use_sync(sync_playwright())
        self._pw = self._stealth_cm.__enter__()
        self._browser = self._pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--disable-background-networking",
                "--disable-default-apps",
                "--disable-extensions",
                "--disable-sync",
                "--disable-translate",
                "--metrics-recording-only",
                "--no-first-run",
                f"--window-size={profile['viewport']['width']},{profile['viewport']['height']}",
            ],
        )

        self._browser_context = self._browser.new_context(
            viewport=profile["viewport"],
            user_agent=profile["ua"],
            locale=profile["locale"],
            timezone_id="America/New_York",
            geolocation={"longitude": -73.935242, "latitude": 40.730610},
            permissions=["geolocation"],
            java_script_enabled=True,
            bypass_csp=True,
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "sec-ch-ua": '"Chromium";v="131", "Not_A Brand";v="24"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": f'"{profile["platform"]}"',
            },
        )

        return self._browser_context

    def _run_in_pw_thread(self, fn, *args, **kwargs):
        """
        Run a function in a dedicated thread that has no asyncio event loop.

        Playwright sync API checks asyncio.get_running_loop() on every call.
        When the pipeline runs in a thread spawned from FastAPI (asyncio app),
        Playwright detects the parent's loop and refuses to work. Running ALL
        Playwright operations in a fresh ThreadPoolExecutor thread avoids this
        because the new thread has zero asyncio state.
        """
        if not hasattr(self, '_pw_executor') or self._pw_executor is None:
            # Single-threaded executor: all Playwright calls go to the same thread,
            # keeping browser state (context, cookies) consistent.
            self._pw_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="playwright"
            )
        future = self._pw_executor.submit(fn, *args, **kwargs)
        return future.result(timeout=self.timeout + 60)

    def _playwright_get(
        self,
        url: str,
        *,
        allow_404: bool = False,
        wait_selector: str | None = None,
        wait_timeout: int = 20000,
    ) -> HttpResponse | None:
        """
        Fetch URL using Playwright headless browser with stealth.

        All Playwright operations run in an isolated thread to avoid
        asyncio event loop conflicts when running under FastAPI.
        """
        if not PLAYWRIGHT_AVAILABLE:
            return None

        try:
            return self._run_in_pw_thread(
                self._playwright_get_impl,
                url,
                allow_404=allow_404,
                wait_selector=wait_selector,
                wait_timeout=wait_timeout,
            )
        except concurrent.futures.TimeoutError:
            logger.warning(f"Playwright timed out for {url}")
            return None
        except Exception as e:
            logger.warning(f"Playwright failed for {url}: {e}")
            return None

    def _playwright_get_impl(
        self,
        url: str,
        *,
        allow_404: bool = False,
        wait_selector: str | None = None,
        wait_timeout: int = 20000,
    ) -> HttpResponse | None:
        """
        Internal Playwright fetch — runs inside the dedicated Playwright thread.
        """
        page = None
        try:
            # Periodically recycle the Chromium browser to release accumulated
            # memory in the child process. Runs inside the Playwright thread.
            self._recycle_browser_if_needed()
            ctx = self._ensure_browser()
            page = ctx.new_page()
            self._browser_fetches_since_recycle += 1

            # Navigate
            response = page.goto(url, timeout=self.timeout * 1000, wait_until="domcontentloaded")

            if response is None:
                page.close()
                return None

            status = response.status

            if allow_404 and status == 404:
                page.close()
                return None

            # Wait for page to stabilize (networkidle may fail on busy pages - that's OK)
            try:
                page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass

            # Handle Cloudflare challenge (wait for it to resolve)
            if status == 403 or self._is_cloudflare_challenge(page):
                logger.info(f"Cloudflare challenge detected on {url}, waiting...")
                resolved = self._wait_for_cloudflare(page)
                if not resolved:
                    logger.warning(f"Cloudflare challenge not resolved for {url}")
                    page.close()
                    return None

            # Dismiss cookie consent
            self._dismiss_cookies(page)

            # Simulate human behavior
            self._human_scroll(page)

            # Wait for specific content if requested
            if wait_selector:
                try:
                    page.wait_for_selector(wait_selector, timeout=wait_timeout)
                except Exception:
                    logger.debug(f"Selector '{wait_selector}' not found on {url}")

            # Small delay for any lazy-loaded content
            self._sleep(0.5, 1.5)

            content = page.content()
            final_url = page.url

            page.close()

            return HttpResponse(
                url=final_url,
                status_code=200,
                text=content,
                headers={},
                method_used="playwright",
            )

        except Exception as e:
            logger.warning(f"Playwright failed for {url}: {e}")
            try:
                if page:
                    page.close()
            except Exception:
                pass
            return None

    def _is_cloudflare_challenge(self, page: Page) -> bool:
        """Detect Cloudflare challenge page."""
        try:
            title = page.title().lower()
            if "just a moment" in title or "attention required" in title:
                return True
            # Check for Cloudflare turnstile or challenge elements
            cf_markers = page.query_selector_all(
                "#challenge-running, #challenge-form, .cf-turnstile, [id*='cf-challenge']"
            )
            return len(cf_markers) > 0
        except Exception:
            return False

    def _wait_for_cloudflare(self, page: Page, max_wait: int = 30) -> bool:
        """Wait for Cloudflare challenge to auto-resolve (up to max_wait seconds)."""
        for _ in range(max_wait // 2):
            time.sleep(2)
            if not self._is_cloudflare_challenge(page):
                logger.info("Cloudflare challenge resolved")
                return True
        return False

    def _dismiss_cookies(self, page: Page) -> None:
        """Dismiss cookie consent popups."""
        selectors = [
            "#onetrust-accept-btn-handler",
            ".onetrust-accept-btn-handler",
            "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
            "[data-testid='cookie-accept']",
            ".evidon-banner-acceptbutton",
            "#_evidon-accept-button",
            "button[id*='cookie-accept']",
            "button[class*='cookie-accept']",
            "button[id*='consent-accept']",
            "button[class*='accept-all']",
        ]

        for sel in selectors:
            try:
                btn = page.query_selector(sel)
                if btn and btn.is_visible():
                    btn.click()
                    logger.debug(f"Dismissed cookie banner: {sel}")
                    self._sleep(0.3, 0.7)
                    return
            except Exception:
                continue

        # Fallback: find buttons by text
        for text in ["Accept All", "Accept", "I Agree", "Allow All", "Got it", "OK"]:
            try:
                btn = page.get_by_role("button", name=text, exact=False).first
                if btn and btn.is_visible():
                    btn.click()
                    logger.debug(f"Dismissed cookie banner via text: {text}")
                    self._sleep(0.3, 0.7)
                    return
            except Exception:
                continue

    def _human_scroll(self, page: Page) -> None:
        """Simulate human-like scrolling behavior."""
        try:
            # Scroll down gradually
            for _ in range(random.randint(1, 3)):
                scroll_amount = random.randint(200, 600)
                page.evaluate(f"window.scrollBy(0, {scroll_amount})")
                self._sleep(0.3, 0.8)

            # Scroll back up slightly
            page.evaluate(f"window.scrollBy(0, -{random.randint(50, 200)})")
            self._sleep(0.2, 0.5)
        except Exception:
            pass

    # ── Tier 3: Plain requests ───────────────────────────────────────

    def _requests_get(
        self,
        url: str,
        *,
        allow_404: bool = False,
        allow_status: set[int] | None = None,
    ) -> HttpResponse | None:
        """Plain requests with retry and exponential backoff."""
        allow_status = allow_status or set()
        retries = 0

        while retries <= config.HTTP_MAX_RETRIES:
            self._sleep()
            try:
                resp = self.session.get(
                    url,
                    timeout=self.timeout,
                    headers=self._random_headers(),
                )
            except plain_requests.RequestException:
                retries += 1
                if retries > config.HTTP_MAX_RETRIES:
                    return None
                time.sleep(config.HTTP_BACKOFF_BASE * retries)
                continue

            if allow_404 and resp.status_code == 404:
                return None

            if resp.status_code == 200 or resp.status_code in allow_status:
                return HttpResponse(
                    url=resp.url,
                    status_code=resp.status_code,
                    text=resp.text,
                    headers=dict(resp.headers),
                    method_used="requests",
                )

            if resp.status_code in (403, 429, 503):
                self._mark_failed(url)
                retries += 1
                time.sleep(config.HTTP_BACKOFF_BASE * retries)
                continue

            if resp.status_code >= 400:
                retries += 1
                if retries > config.HTTP_MAX_RETRIES:
                    return HttpResponse(
                        url=resp.url,
                        status_code=resp.status_code,
                        text=resp.text,
                        headers=dict(resp.headers),
                        method_used="requests",
                    )
                time.sleep(config.HTTP_BACKOFF_BASE * retries)
                continue

            return HttpResponse(
                url=resp.url,
                status_code=resp.status_code,
                text=resp.text,
                headers=dict(resp.headers),
                method_used="requests",
            )

        return None

    # ── Public API ───────────────────────────────────────────────────

    def get(
        self,
        url: str,
        *,
        allow_status: Iterable[int] | None = None,
        to_soup: bool = False,
        allow_404: bool = False,
    ) -> BeautifulSoup | HttpResponse | None:
        """
        GET a URL with automatic fallback chain.

        For HTML pages: uses curl_cffi → Playwright → requests.
        For APIs/RSS: uses requests directly (or curl_cffi for Cloudflare sites).

        Returns HttpResponse or BeautifulSoup (if to_soup=True) or None.
        """
        allow_set = set(allow_status or [])

        # For non-HTML (APIs, RSS) – try requests first, then cffi
        if not to_soup and not self._needs_js(url):
            result = self._requests_get(url, allow_404=allow_404, allow_status=allow_set)
            if result is not None:
                return result
            result = self._cffi_get(url, allow_404=allow_404)
            if result is not None:
                return result
            return None

        # For HTML pages – use the full fallback chain
        result = self._smart_get(url, allow_404=allow_404)
        if result is None:
            return None

        if to_soup:
            return self._to_soup(result.text)
        return result

    def get_soup(
        self,
        url: str,
        *,
        allow_404: bool = False,
        allow_status: Iterable[int] | None = None,
        use_selenium_fallback: bool = True,  # Kept for backward compatibility
        wait_selector: str | None = None,
    ) -> BeautifulSoup | None:
        """
        Fetch page and return as BeautifulSoup.

        Uses the smart fallback chain:
        1. curl_cffi (TLS fingerprint – fast, bypasses most Cloudflare)
        2. Playwright headless (JS rendering – search pages, SPAs)
        3. Plain requests (simple pages, RSS feeds)

        Args:
            url: URL to fetch
            allow_404: Return None instead of raising on 404
            wait_selector: CSS selector to wait for (Playwright only, for JS-rendered content)
        """
        result = self._smart_get(url, allow_404=allow_404, wait_selector=wait_selector)
        if result is None:
            return None
        return self._to_soup(result.text)

    def get_soup_with_fallback(
        self,
        url: str,
        *,
        allow_404: bool = False,
        allow_status: Iterable[int] | None = None,
        check_content: Callable[[BeautifulSoup], bool] | None = None,
        use_selenium_fallback: bool = True,
        wait_selector: str | None = None,
    ) -> BeautifulSoup | None:
        """
        Fetch page with content validation.

        If check_content returns False, retries with Playwright.
        """
        soup = self.get_soup(url, allow_404=allow_404, wait_selector=wait_selector)

        if soup is not None and check_content is not None:
            if not check_content(soup):
                logger.info(f"Content check failed for {url}, retrying with Playwright")
                result = self._playwright_get(url, allow_404=allow_404, wait_selector=wait_selector)
                if result:
                    pw_soup = self._to_soup(result.text)
                    if pw_soup and (check_content is None or check_content(pw_soup)):
                        return pw_soup
                return None

        return soup

    # ── Smart routing ────────────────────────────────────────────────

    def _smart_get(
        self,
        url: str,
        *,
        allow_404: bool = False,
        wait_selector: str | None = None,
    ) -> HttpResponse | None:
        """
        Intelligently route request through the best tier.

        Routing logic:
        - Known JS-required domains → Playwright first, cffi fallback
        - Known Cloudflare domains → cffi first, Playwright fallback
        - Previously failed domains → skip requests, try cffi → Playwright
        - Everything else → cffi → requests → Playwright
        """
        domain = self._domain(url)

        # Route 1: JS-required domains (search pages, SPAs)
        if self._needs_js(url):
            logger.debug(f"JS-required domain: {domain}, using Playwright")
            result = self._playwright_get(
                url, allow_404=allow_404, wait_selector=wait_selector
            )
            if result:
                return result
            # Fallback to cffi (some content may still be in HTML)
            result = self._cffi_get(url, allow_404=allow_404)
            if result:
                return result
            return None

        # Route 2: Known Cloudflare domains
        if self._has_cloudflare(url):
            logger.debug(f"Cloudflare domain: {domain}, using curl_cffi")
            result = self._cffi_get(url, allow_404=allow_404)
            if result:
                return result
            # Fallback to Playwright
            result = self._playwright_get(
                url, allow_404=allow_404, wait_selector=wait_selector
            )
            if result:
                return result
            return None

        # Route 3: Previously failed domains
        if self._should_skip_requests(url):
            logger.debug(f"Previously failed domain: {domain}, skipping requests")
            result = self._cffi_get(url, allow_404=allow_404)
            if result:
                return result
            result = self._playwright_get(
                url, allow_404=allow_404, wait_selector=wait_selector
            )
            if result:
                return result
            return None

        # Route 4: Default – try everything
        # Try cffi first (fastest and handles most sites)
        result = self._cffi_get(url, allow_404=allow_404)
        if result:
            return result

        # Try plain requests
        result = self._requests_get(url, allow_404=allow_404)
        if result:
            return result

        # Last resort: Playwright
        result = self._playwright_get(
            url, allow_404=allow_404, wait_selector=wait_selector
        )
        if result:
            return result

        logger.warning(f"All tiers failed for {url}")
        return None

    # ── Utilities ────────────────────────────────────────────────────

    @staticmethod
    def _to_soup(html: str) -> BeautifulSoup | None:
        if not html:
            return None
        try:
            return BeautifulSoup(html, "lxml")
        except Exception:
            try:
                return BeautifulSoup(html, "html.parser")
            except Exception:
                return None


# ── Module-level convenience ─────────────────────────────────────────

_default_client: HttpClient | None = None


def build_http_client(**kwargs) -> HttpClient:
    """Create a new HttpClient instance."""
    return HttpClient(**kwargs)


def default_client() -> HttpClient:
    """Get or create the default shared HttpClient."""
    global _default_client
    if _default_client is None:
        _default_client = HttpClient()
    return _default_client
