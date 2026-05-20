"""
Oxylabs API client for EduThreat-CTI.

Provides two capabilities:
- fetch_url(): Scrape a URL via Oxylabs Realtime API (replaces Zyte)
- search_news(): Google News SERP via Oxylabs (for URL-less incidents like Comparitech)

Auth: HTTP Basic with OXYLABS_USERNAME / OXYLABS_PASSWORD env vars.
"""

import logging
import os
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

OXYLABS_API_URL = "https://realtime.oxylabs.io/v1/queries"
_404_SIGNALS = [
    "page can't be found", "page can\u2019t be found",
    "page cannot be found", "not found", "404",
    "no longer available", "nothing was found",
    "page not found", "error 404",
]


def _env_flag(name: str, default: str = "1") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


class OxylabsClient:
    """Thin wrapper around the Oxylabs Realtime API."""

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 60,
    ):
        # Default timeout: 60s. Down from the original 120s (which had a
        # long tail of dead sites burning 2 minutes each), but kept high
        # enough to handle slow-rendering JS-heavy sites that legitimately
        # take 30-50s through Oxylabs (e.g. some Korean / Japanese news
        # portals with heavy client-side rendering). Override via env var
        # OXYLABS_TIMEOUT_SECONDS or constructor arg.
        self.username = username or os.getenv("OXYLABS_USERNAME", "")
        self.password = password or os.getenv("OXYLABS_PASSWORD", "")
        env_timeout = os.getenv("OXYLABS_TIMEOUT_SECONDS")
        if env_timeout:
            try:
                timeout = int(env_timeout)
            except ValueError:
                pass
        self.timeout = timeout

    def _is_enabled(self) -> bool:
        """Return whether Oxylabs calls are allowed in this runtime."""
        return _env_flag("EDU_CTI_OXYLABS_ENABLED", "1")

    def _is_configured(self) -> bool:
        return self._is_enabled() and bool(self.username and self.password)

    def fetch_url(self, url: str, render_js: bool = True) -> Optional[str]:
        """
        Fetch a URL via Oxylabs and return the rendered HTML.

        Args:
            url: The URL to scrape
            render_js: Whether to render JavaScript (uses browser rendering)

        Returns:
            HTML string, or None if failed
        """
        if not self._is_enabled():
            logger.info("Oxylabs fetch disabled by EDU_CTI_OXYLABS_ENABLED=0")
            return None
        if not self._is_configured():
            logger.warning("Oxylabs credentials not configured (OXYLABS_USERNAME/OXYLABS_PASSWORD)")
            return None

        payload: Dict = {
            "source": "universal",
            "url": url,
            "render": "html" if render_js else None,
        }
        if not render_js:
            del payload["render"]

        try:
            logger.info(f"Oxylabs fetch: {url[:100]}")
            resp = requests.post(
                OXYLABS_API_URL,
                auth=(self.username, self.password),
                json=payload,
                timeout=self.timeout,
            )

            if resp.status_code == 200:
                data = resp.json()
                content = data.get("results", [{}])[0].get("content", "")
                if content:
                    logger.info(f"Oxylabs fetch succeeded ({len(content)} chars): {url[:80]}")
                    return content
                else:
                    logger.warning(f"Oxylabs: empty content for {url}")
                    return None
            elif resp.status_code == 400:
                logger.warning(f"Oxylabs: bad request for {url}: {resp.text[:200]}")
            elif resp.status_code == 401:
                logger.error("Oxylabs: authentication failed — check OXYLABS_USERNAME/PASSWORD")
            elif resp.status_code == 429:
                logger.warning("Oxylabs: rate limited")
            else:
                logger.warning(f"Oxylabs: HTTP {resp.status_code} for {url}")

        except requests.Timeout:
            logger.warning(f"Oxylabs: request timed out after {self.timeout}s for {url}")
        except requests.RequestException as e:
            logger.warning(f"Oxylabs: request error for {url}: {e}")

        return None

    def search_news(
        self,
        query: str,
        max_results: int = 10,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> List[Dict]:
        """
        Search Google News via Oxylabs SERP API.

        Args:
            query: Search query string
            max_results: Maximum number of results to return
            date_from: Start date for filtering (YYYY-MM-DD), e.g. "2022-01-01"
            date_to: End date for filtering (YYYY-MM-DD), e.g. "2022-12-31"

        Returns:
            List of dicts with keys: url, title, description, source
        """
        if not self._is_enabled():
            logger.info("Oxylabs SERP disabled by EDU_CTI_OXYLABS_ENABLED=0")
            return []
        if not self._is_configured():
            logger.warning("Oxylabs credentials not configured (OXYLABS_USERNAME/OXYLABS_PASSWORD)")
            return []

        context = [{"key": "tbm", "value": "nws"}]

        # Add date range filter via Google's tbs (custom date range) parameter
        # Format: cdr:1,cd_min:MM/DD/YYYY,cd_max:MM/DD/YYYY
        if date_from or date_to:
            def _fmt(d: str) -> str:
                # Convert YYYY-MM-DD → M/D/YYYY for Google tbs format
                from datetime import datetime
                dt = datetime.strptime(d, "%Y-%m-%d")
                return f"{dt.month}/{dt.day}/{dt.year}"

            cd_min = _fmt(date_from) if date_from else "1/1/2000"
            cd_max = _fmt(date_to) if date_to else "12/31/2099"
            context.append({"key": "tbs", "value": f"cdr:1,cd_min:{cd_min},cd_max:{cd_max}"})

        payload = {
            "source": "google_search",
            "query": query,
            "context": context,
            "parse": True,
            "limit": max_results,
        }

        try:
            logger.info(f"Oxylabs news search: {query!r}")
            resp = requests.post(
                OXYLABS_API_URL,
                auth=(self.username, self.password),
                json=payload,
                timeout=self.timeout,
            )

            if resp.status_code == 200:
                data = resp.json()
                try:
                    content_results = (
                        data.get("results", [{}])[0]
                        .get("content", {})
                        .get("results", {})
                    )
                    # News SERP returns results under "main"; web SERP uses "organic"
                    articles = (
                        content_results.get("main")
                        or content_results.get("organic")
                        or []
                    )
                except (IndexError, AttributeError):
                    logger.warning(f"Oxylabs: unexpected SERP response structure for query {query!r}")
                    return []

                results = []
                for item in articles[:max_results]:
                    url = item.get("url") or item.get("link", "")
                    if url:
                        results.append({
                            "url": url,
                            "title": item.get("title", ""),
                            "description": item.get("desc", "") or item.get("description", ""),
                            "source": item.get("source", "") or item.get("domain", ""),
                        })

                logger.info(f"Oxylabs SERP: {len(results)} results for {query!r}")
                return results
            elif resp.status_code == 401:
                logger.error("Oxylabs: authentication failed — check OXYLABS_USERNAME/PASSWORD")
            else:
                logger.warning(f"Oxylabs SERP: HTTP {resp.status_code} for query {query!r}")

        except requests.Timeout:
            logger.warning(f"Oxylabs SERP: timed out for query {query!r}")
        except requests.RequestException as e:
            logger.warning(f"Oxylabs SERP: request error for query {query!r}: {e}")

        return []
