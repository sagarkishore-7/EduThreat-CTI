"""
Proxy pool manager for EduThreat-CTI.

Provides a unified interface for proxy rotation, supporting:
- Free proxy lists (cost-effective default)
- curl_cffi for TLS fingerprint mimicking
- SOCKS5/HTTP proxy support
- Country-specific exit nodes
- Health checking and automatic failover

Cost-effective approach: Uses free proxy lists + curl_cffi TLS fingerprinting
as the default. Paid proxy services (BrightData, etc.) can be configured
via environment variables when needed.
"""

import os
import random
import time
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

# Try to import curl_cffi for TLS fingerprint mimicking
try:
    from curl_cffi import requests as curl_requests
    CURL_CFFI_AVAILABLE = True
except ImportError:
    CURL_CFFI_AVAILABLE = False
    logger.debug("curl_cffi not available. Install with: pip install curl_cffi")


# Browser impersonation profiles for curl_cffi
BROWSER_PROFILES = [
    "chrome120",
    "chrome119",
    "chrome116",
    "safari17_0",
    "safari15_5",
    "firefox120",
]


@dataclass
class ProxyEntry:
    """A single proxy with metadata."""
    url: str  # e.g., "http://1.2.3.4:8080" or "socks5://1.2.3.4:1080"
    country: Optional[str] = None
    protocol: str = "http"  # http, https, socks5
    last_used: float = 0.0
    fail_count: int = 0
    success_count: int = 0
    avg_latency_ms: float = 0.0
    is_healthy: bool = True

    @property
    def reliability_score(self) -> float:
        total = self.success_count + self.fail_count
        if total == 0:
            return 0.5  # Unknown
        return self.success_count / total


@dataclass
class ProxyPoolConfig:
    """Configuration for the proxy pool."""
    # Free proxy list URLs (scraped on init)
    free_proxy_list_urls: List[str] = field(default_factory=lambda: [
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
    ])
    # Paid proxy service (optional, overrides free proxies)
    paid_proxy_url: Optional[str] = None  # e.g., "http://user:pass@proxy.brightdata.com:22225"
    # Max failures before marking proxy as unhealthy
    max_failures: int = 3
    # Cooldown after failure (seconds)
    failure_cooldown: float = 300.0
    # Whether to use curl_cffi for TLS fingerprinting
    use_curl_cffi: bool = True
    # Refresh interval for free proxy lists (seconds)
    refresh_interval: float = 3600.0  # 1 hour


class ProxyPool:
    """
    Manages a pool of proxies with health checking, rotation, and
    country-based selection.

    Cost-effective design:
    - Default: curl_cffi TLS fingerprinting (no proxy needed for most sites)
    - Fallback: Free proxy lists for geo-restricted content
    - Optional: Paid proxy URL for high-reliability needs
    """

    def __init__(self, config: Optional[ProxyPoolConfig] = None):
        self.config = config or ProxyPoolConfig()
        self._proxies: List[ProxyEntry] = []
        self._country_proxies: Dict[str, List[ProxyEntry]] = {}
        self._last_refresh: float = 0.0
        self._request_count: int = 0

        # Load paid proxy if configured
        paid_url = self.config.paid_proxy_url or os.getenv("PROXY_URL")
        if paid_url:
            self._proxies.append(ProxyEntry(
                url=paid_url,
                protocol="http",
                is_healthy=True,
            ))
            logger.info("Paid proxy configured")

    def _should_refresh(self) -> bool:
        return (time.time() - self._last_refresh) > self.config.refresh_interval

    def refresh_free_proxies(self) -> int:
        """Fetch fresh proxies from free proxy list sources."""
        new_proxies = []
        for list_url in self.config.free_proxy_list_urls:
            try:
                resp = requests.get(list_url, timeout=10)
                if resp.status_code == 200:
                    for line in resp.text.strip().split("\n"):
                        line = line.strip()
                        if line and ":" in line:
                            proxy_url = f"http://{line}"
                            new_proxies.append(ProxyEntry(
                                url=proxy_url,
                                protocol="http",
                            ))
            except Exception as e:
                logger.debug(f"Failed to fetch proxy list from {list_url}: {e}")

        if new_proxies:
            # Keep paid proxies, replace free ones
            paid = [p for p in self._proxies if self.config.paid_proxy_url and p.url == self.config.paid_proxy_url]
            self._proxies = paid + new_proxies
            self._last_refresh = time.time()
            logger.info(f"Loaded {len(new_proxies)} free proxies")

        return len(new_proxies)

    def get_proxy(self, country: Optional[str] = None) -> Optional[ProxyEntry]:
        """
        Get a healthy proxy, optionally for a specific country.

        Args:
            country: ISO-2 country code for geo-specific proxy

        Returns:
            ProxyEntry or None if no proxies available
        """
        if self._should_refresh() and not self.config.paid_proxy_url:
            self.refresh_free_proxies()

        healthy = [p for p in self._proxies if p.is_healthy]

        if country and country in self._country_proxies:
            country_healthy = [p for p in self._country_proxies[country] if p.is_healthy]
            if country_healthy:
                healthy = country_healthy

        if not healthy:
            return None

        # Weighted random selection favoring higher reliability
        weights = [max(p.reliability_score, 0.1) for p in healthy]
        return random.choices(healthy, weights=weights, k=1)[0]

    def report_success(self, proxy: ProxyEntry, latency_ms: float = 0.0) -> None:
        """Report successful request through proxy."""
        proxy.success_count += 1
        proxy.last_used = time.time()
        if latency_ms > 0:
            # Running average
            proxy.avg_latency_ms = (proxy.avg_latency_ms * 0.8) + (latency_ms * 0.2)

    def report_failure(self, proxy: ProxyEntry) -> None:
        """Report failed request through proxy."""
        proxy.fail_count += 1
        if proxy.fail_count >= self.config.max_failures:
            proxy.is_healthy = False
            logger.debug(f"Proxy marked unhealthy: {proxy.url[:30]}...")

    def get_requests_proxy_dict(self, proxy: Optional[ProxyEntry] = None) -> Optional[Dict[str, str]]:
        """Get proxy dict compatible with requests library."""
        if proxy is None:
            proxy = self.get_proxy()
        if proxy is None:
            return None
        return {
            "http": proxy.url,
            "https": proxy.url,
        }


def curl_cffi_get(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    proxy: Optional[str] = None,
    timeout: int = 30,
    impersonate: Optional[str] = None,
) -> Optional[requests.Response]:
    """
    Make an HTTP GET request using curl_cffi with TLS fingerprint mimicking.

    This is the most cost-effective bot evasion technique — it mimics
    real browser TLS fingerprints without needing a full browser or proxy.

    Args:
        url: URL to fetch
        headers: Optional headers
        proxy: Optional proxy URL
        timeout: Request timeout
        impersonate: Browser to impersonate (e.g., "chrome120")

    Returns:
        Response object or None on failure
    """
    if not CURL_CFFI_AVAILABLE:
        logger.debug("curl_cffi not available, falling back to requests")
        return None

    if impersonate is None:
        impersonate = random.choice(BROWSER_PROFILES)

    try:
        kwargs = {
            "url": url,
            "impersonate": impersonate,
            "timeout": timeout,
            "allow_redirects": True,
        }
        if headers:
            kwargs["headers"] = headers
        if proxy:
            kwargs["proxies"] = {"http": proxy, "https": proxy}

        resp = curl_requests.get(**kwargs)
        return resp
    except Exception as e:
        logger.debug(f"curl_cffi request failed for {url}: {e}")
        return None


# Singleton proxy pool
_proxy_pool: Optional[ProxyPool] = None


def get_proxy_pool() -> ProxyPool:
    """Get or create the singleton proxy pool."""
    global _proxy_pool
    if _proxy_pool is None:
        config = ProxyPoolConfig(
            paid_proxy_url=os.getenv("PROXY_URL"),
            use_curl_cffi=CURL_CFFI_AVAILABLE,
        )
        _proxy_pool = ProxyPool(config)
    return _proxy_pool
