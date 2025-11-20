from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

import requests
from bs4 import BeautifulSoup

from src.edu_cti.core import config

# Optional selenium support for bot detection bypass
try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False


@dataclass
class HttpResponse:
    url: str
    status_code: int
    text: str
    headers: dict


class HttpClient:
    """
    Thin wrapper around requests.Session that handles:
      - rotating User-Agent headers
      - randomized delays between calls
      - retry with exponential backoff
      - conversion to BeautifulSoup when needed
    """

    def __init__(
        self,
        *,
        timeout: int = config.REQUEST_TIMEOUT_SECONDS,
        user_agents: Optional[Iterable[str]] = None,
        min_delay: float = config.HTTP_MIN_DELAY,
        max_delay: float = config.HTTP_MAX_DELAY,
    ) -> None:
        self.session = requests.Session()
        self.timeout = timeout
        self.user_agents = list(user_agents or config.HTTP_USER_AGENTS)
        self.min_delay = min_delay
        self.max_delay = max_delay

    def _random_headers(self) -> dict:
        ua = random.choice(self.user_agents)
        return {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }

    def _sleep_a_bit(self) -> None:
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)

    def get(
        self,
        url: str,
        *,
        allow_status: Optional[Iterable[int]] = None,
        to_soup: bool = False,
        allow_404: bool = False,
    ) -> BeautifulSoup | HttpResponse | None:
        """
        Perform a GET with retries. Returns BeautifulSoup if to_soup=True,
        HttpResponse otherwise. Returns None when allow_404 is True and the
        server responds 404.
        """
        retries = 0
        allow_status = set(allow_status or [])

        while True:
            self._sleep_a_bit()
            try:
                resp = self.session.get(
                    url,
                    timeout=self.timeout,
                    headers=self._random_headers(),
                )
            except requests.RequestException:
                retries += 1
                if retries > config.HTTP_MAX_RETRIES:
                    raise
                time.sleep(config.HTTP_BACKOFF_BASE * retries)
                continue

            if allow_404 and resp.status_code == 404:
                return None

            if resp.status_code >= 400 and resp.status_code not in allow_status:
                retries += 1
                if retries > config.HTTP_MAX_RETRIES:
                    resp.raise_for_status()
                time.sleep(config.HTTP_BACKOFF_BASE * retries)
                continue

            result = HttpResponse(
                url=resp.url,
                status_code=resp.status_code,
                text=resp.text,
                headers=resp.headers,
            )
            if to_soup:
                try:
                    return BeautifulSoup(resp.text, "lxml")
                except Exception:
                    return BeautifulSoup(resp.text, "html.parser")
            return result

    def _get_with_selenium(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fallback to undetected-chromedriver when requests gets blocked (403/429).
        """
        if not SELENIUM_AVAILABLE:
            return None
        
        try:
            options = uc.ChromeOptions()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-extensions")
            options.add_argument("--incognito")
            options.add_argument("--ignore-certificate-errors")
            options.add_experimental_option("excludeSwitches", ["enable-logging"])
            
            driver = uc.Chrome(options=options, version_main=None)
            try:
                driver.get(url)
                # Wait for page to load
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                time.sleep(random.uniform(1, 2))  # Random delay
                html = driver.page_source
                return BeautifulSoup(html, "html.parser")
            finally:
                driver.quit()
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Selenium fallback failed for {url}: {e}")
            return None

    def get_soup(
        self,
        url: str,
        *,
        allow_404: bool = False,
        allow_status: Optional[Iterable[int]] = None,
        use_selenium_fallback: bool = True,
    ) -> Optional[BeautifulSoup]:
        """
        Get page as BeautifulSoup. If requests fails with 403/429/503 and use_selenium_fallback=True,
        try undetected-chromedriver as fallback. 503 is included because some sites return it for bot detection.
        
        This method tries requests first (faster), and falls back to Selenium if:
        - Status code indicates blocking (403, 429, 503)
        - Request fails with other errors
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # Status codes that typically indicate bot blocking (503 can be bot detection when page works in browser)
        BLOCKED_STATUS_CODES = (403, 429, 503)
        
        # First, try a direct request to check status without raising
        try:
            self._sleep_a_bit()
            resp = self.session.get(url, timeout=self.timeout, headers=self._random_headers())
            
            # Check for blocked status codes - try selenium fallback
            if resp.status_code in BLOCKED_STATUS_CODES:
                if use_selenium_fallback and SELENIUM_AVAILABLE:
                    logger.info(f"Requests blocked ({resp.status_code}) for {url}, trying selenium fallback...")
                    selenium_result = self._get_with_selenium(url)
                    if selenium_result is not None:
                        return selenium_result
                    # If selenium also failed, log warning and return None to allow graceful handling
                    logger.warning(f"Selenium fallback failed for {url}, skipping...")
                    return None
                # No selenium available - log and return None for graceful handling
                logger.warning(f"Requests blocked ({resp.status_code}) for {url} and selenium not available, skipping...")
                return None
            
            # If not blocked, proceed normally
            if allow_404 and resp.status_code == 404:
                return None
            if resp.status_code >= 400 and resp.status_code not in (allow_status or set()):
                resp.raise_for_status()
            # Success - parse to soup
            try:
                return BeautifulSoup(resp.text, "lxml")
            except Exception:
                return BeautifulSoup(resp.text, "html.parser")
        except requests.HTTPError as e:
            # If we got blocked status and selenium is available, try fallback
            if e.response is not None and e.response.status_code in BLOCKED_STATUS_CODES:
                if use_selenium_fallback and SELENIUM_AVAILABLE:
                    logger.info(f"Requests blocked ({e.response.status_code}) for {url}, trying selenium fallback...")
                    selenium_result = self._get_with_selenium(url)
                    if selenium_result is not None:
                        return selenium_result
                    # Selenium failed - return None for graceful handling
                    logger.warning(f"Selenium fallback failed for {url}, skipping...")
                    return None
                # No selenium - return None for graceful handling
                logger.warning(f"Requests blocked ({e.response.status_code}) for {url} and selenium not available, skipping...")
                return None
            # For other HTTP errors, raise as before
            raise
        except requests.RequestException:
            # For other request errors, try selenium as last resort if enabled
            if use_selenium_fallback and SELENIUM_AVAILABLE:
                logger.warning(f"Request failed for {url}, trying selenium fallback...")
                return self._get_with_selenium(url)
            raise
    
    def get_soup_with_fallback(
        self,
        url: str,
        *,
        allow_404: bool = False,
        allow_status: Optional[Iterable[int]] = None,
        check_content: Optional[Callable[[BeautifulSoup], bool]] = None,
        use_selenium_fallback: bool = True,
    ) -> Optional[BeautifulSoup]:
        """
        Get page as BeautifulSoup with enhanced fallback logic.
        
        First tries get_soup (requests with automatic Selenium fallback for blocked requests).
        If content check function is provided and returns False (indicating no articles/content found),
        then tries Selenium as an additional fallback.
        
        Args:
            url: URL to fetch
            allow_404: If True, return None for 404 instead of raising
            allow_status: List of status codes to allow (won't raise error)
            check_content: Optional function(soup) -> bool that returns True if content is valid.
                          If returns False, will try Selenium fallback.
            use_selenium_fallback: Whether to use Selenium fallback
        
        Returns:
            BeautifulSoup or None
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # First try standard get_soup (which already has Selenium fallback for blocked requests)
        soup = self.get_soup(
            url,
            allow_404=allow_404,
            allow_status=allow_status,
            use_selenium_fallback=use_selenium_fallback,
        )
        
        # If we got soup and have a content checker, verify content is valid
        if soup is not None and check_content is not None:
            if not check_content(soup):
                # Content check failed - try Selenium as additional fallback
                if use_selenium_fallback and SELENIUM_AVAILABLE:
                    logger.info(f"Content check failed for {url}, trying selenium fallback...")
                    selenium_result = self._get_with_selenium(url)
                    if selenium_result is not None:
                        # Verify selenium result passes content check
                        if check_content(selenium_result):
                            return selenium_result
                        logger.warning(f"Selenium result for {url} also failed content check")
                    else:
                        logger.warning(f"Selenium fallback failed for {url}")
                else:
                    logger.warning(f"Content check failed for {url} and selenium not available")
                # Return None if content check failed and no valid selenium result
                return None
        
        return soup


def build_http_client() -> HttpClient:
    return HttpClient()

