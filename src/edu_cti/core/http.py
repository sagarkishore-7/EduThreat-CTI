from __future__ import annotations

import random
import time
import logging
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, List

import requests
from bs4 import BeautifulSoup

from src.edu_cti.core import config

logger = logging.getLogger(__name__)

# Optional selenium support for bot detection bypass
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.chrome.service import Service as ChromeService
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logger.warning("Selenium not available. Install with: pip install selenium")

# Try to import undetected_chromedriver as optional enhancement
try:
    import undetected_chromedriver as uc
    UC_AVAILABLE = True
except ImportError:
    UC_AVAILABLE = False
    logger.debug("undetected_chromedriver not available, using regular selenium")


# Bot detection bypass configurations
BOT_EVASION_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
]

VIEWPORT_SIZES = [
    (1920, 1080),
    (1440, 900),
    (1366, 768),
    (1536, 864),
    (2560, 1440),
]

# Sites known to have aggressive bot detection (Cloudflare)
# Note: These sites block Selenium entirely; handled at article_fetcher level
AGGRESSIVE_BOT_DETECTION_DOMAINS = []


@dataclass
class HttpResponse:
    url: str
    status_code: int
    text: str
    headers: dict


class HttpClient:
    """
    HTTP client with advanced bot detection bypass capabilities.
    
    Features:
    - Rotating User-Agent headers
    - Randomized delays between calls
    - Retry with exponential backoff
    - Selenium fallback with multiple evasion techniques
    - Non-headless browser fallback for stubborn sites
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
        self._failed_domains: dict = {}  # Track domains that failed with requests

    def _random_headers(self) -> dict:
        ua = random.choice(self.user_agents)
        return {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }

    def _sleep_a_bit(self) -> None:
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)

    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        from urllib.parse import urlparse
        return urlparse(url).netloc.lower()

    def _is_aggressive_domain(self, url: str) -> bool:
        """Check if domain has aggressive bot detection."""
        domain = self._get_domain(url)
        return any(d in domain for d in AGGRESSIVE_BOT_DETECTION_DOMAINS)

    def _should_use_selenium_first(self, url: str) -> bool:
        """Check if we should skip requests and use Selenium first for this domain."""
        domain = self._get_domain(url)
        # If this domain failed recently, use Selenium directly
        if domain in self._failed_domains:
            fail_count = self._failed_domains[domain]
            if fail_count >= 2:
                return True
        # Use Selenium first for known aggressive domains
        return self._is_aggressive_domain(url)

    def _mark_domain_failed(self, url: str) -> None:
        """Mark a domain as having failed with requests."""
        domain = self._get_domain(url)
        self._failed_domains[domain] = self._failed_domains.get(domain, 0) + 1

    def get(
        self,
        url: str,
        *,
        allow_status: Optional[Iterable[int]] = None,
        to_soup: bool = False,
        allow_404: bool = False,
    ) -> BeautifulSoup | HttpResponse | None:
        """
        Perform a GET with retries.
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

    def _create_stealth_driver(self, headless: bool = True) -> webdriver.Chrome:
        """
        Create a Chrome driver with stealth settings using regular Selenium.
        
        Args:
            headless: If True, run in headless mode. If False, show browser window.
        """
        if not SELENIUM_AVAILABLE:
            raise RuntimeError("Selenium not available")
        
        options = ChromeOptions()
        
        # Viewport size
        width, height = random.choice(VIEWPORT_SIZES)
        
        if headless:
            options.add_argument("--headless=new")
        else:
            # Non-headless mode - show the browser
            logger.info("Opening visible browser window for bot detection bypass...")
        
        # Essential stealth arguments
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"--window-size={width},{height}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-infobars")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--ignore-ssl-errors")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--disable-notifications")
        
        # Random user agent
        ua = random.choice(BOT_EVASION_USER_AGENTS)
        options.add_argument(f"--user-agent={ua}")
        
        # Language and locale
        options.add_argument("--lang=en-US,en")
        
        # Use experimental options to appear more human
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        
        prefs = {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
            "profile.default_content_setting_values.notifications": 2,
        }
        options.add_experimental_option("prefs", prefs)
        
        # Create driver with regular selenium
        driver = webdriver.Chrome(options=options)
        
        # Execute stealth scripts
        self._apply_stealth_scripts(driver)
        
        return driver

    def _apply_stealth_scripts(self, driver: webdriver.Chrome) -> None:
        """Apply JavaScript to make browser appear more human."""
        try:
            # Override navigator.webdriver
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                    
                    // Override navigator.plugins
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5]
                    });
                    
                    // Override navigator.languages
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['en-US', 'en']
                    });
                    
                    // Override chrome runtime
                    window.chrome = {
                        runtime: {}
                    };
                    
                    // Override permissions
                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ?
                            Promise.resolve({ state: Notification.permission }) :
                            originalQuery(parameters)
                    );
                """
            })
        except Exception as e:
            logger.debug(f"Could not apply stealth scripts: {e}")

    def _simulate_human_behavior(self, driver: webdriver.Chrome) -> None:
        """Simulate human-like behavior on the page."""
        try:
            actions = ActionChains(driver)
            
            # Random mouse movements
            for _ in range(random.randint(2, 5)):
                x = random.randint(100, 800)
                y = random.randint(100, 600)
                actions.move_by_offset(x, y)
                time.sleep(random.uniform(0.1, 0.3))
            
            # Random scroll
            scroll_amount = random.randint(100, 500)
            driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
            time.sleep(random.uniform(0.5, 1.0))
            
            # Scroll back up a bit
            driver.execute_script(f"window.scrollBy(0, -{scroll_amount // 2});")
            
        except Exception as e:
            logger.debug(f"Could not simulate human behavior: {e}")

    def _handle_cookie_consent(self, driver: webdriver.Chrome) -> None:
        """Try to accept cookie consent popups."""
        cookie_selectors = [
            "button[id*='accept']",
            "button[class*='accept']",
            "button[id*='cookie']",
            "button[class*='cookie']",
            "button[id*='consent']",
            "button[class*='consent']",
            "a[id*='accept']",
            "a[class*='accept']",
            "[id*='onetrust-accept']",
            ".onetrust-accept-btn-handler",
            "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
            "[data-testid='cookie-accept']",
            ".cookie-accept",
            "#cookie-accept",
        ]
        
        for selector in cookie_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elements:
                    if elem.is_displayed():
                        elem.click()
                        logger.debug(f"Clicked cookie consent button: {selector}")
                        time.sleep(0.5)
                        return
            except Exception:
                continue

    def _handle_cookie_consent(self, driver: webdriver.Chrome) -> None:
        """
        Handle cookie consent popups by clicking Accept/Agree buttons.
        
        Handles common cookie consent patterns including:
        - GDPR cookie banners
        - OneTrust consent
        - Cookiebot
        - TechTarget/DarkReading consent
        - Generic cookie accept buttons
        - Consent dialogs in iframes
        """
        # Common cookie consent button selectors - ordered by specificity
        cookie_accept_selectors = [
            # OneTrust (used by many news sites including DarkReading)
            "#onetrust-accept-btn-handler",
            ".onetrust-accept-btn-handler",
            "[id*='onetrust-accept']",
            ".ot-pc-refuse-all-handler",  # OneTrust refuse/accept all
            
            # TechTarget/DarkReading/SecurityWeek specific
            ".evidon-banner-acceptbutton",
            "#_evidon-accept-button",
            "#evidon-banner-acceptbutton",
            ".evidon-consent-button",
            "[class*='evidon'] button",
            
            # Cookiebot
            "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
            "#CybotCookiebotDialogBodyButtonAccept",
            "[id*='CybotCookiebotDialogBodyLevelButtonAccept']",
            
            # SP Consent (SourcePoint - used by DarkReading parent company)
            "[title='SP Consent Message']",
            ".sp_choice_type_11",  # Accept All button in SourcePoint
            ".sp_choice_type_ACCEPT_ALL",
            "[class*='sp_choice'] button",
            
            # Generic cookie consent buttons
            "[class*='cookie'] button[class*='accept']",
            "[class*='cookie'] [class*='accept']",
            "[class*='consent'] button[class*='accept']",
            "[class*='consent'] [class*='accept']",
            "[class*='gdpr'] button[class*='accept']",
            "[class*='gdpr'] [class*='accept']",
            
            # Text-based selectors (Accept All, I Accept, etc.)
            "button[class*='accept-all']",
            "[class*='accept-all']",
            "button[class*='acceptAll']",
            "[class*='acceptAll']",
            
            # Common button texts via aria-label
            "[aria-label*='Accept']",
            "[aria-label*='accept']",
            "[aria-label*='Accept all']",
            "[aria-label*='Accept cookies']",
            
            # Generic banner dismiss buttons
            "[class*='cookie-banner'] button",
            "[class*='cookie-notice'] button",
            "[class*='privacy-banner'] button",
            "[id*='cookie-banner'] button",
            "[id*='cookie-notice'] button",
            
            # TrustArc
            ".truste_accept_btn",
            "#truste-consent-button",
            ".trustarc-agree-btn",
            
            # Quantcast
            ".qc-cmp-button",
            ".qc-cmp2-summary-buttons button:first-child",
            
            # Generic patterns
            ".cc-accept",
            ".cc-btn.cc-allow",
            "#accept-cookies",
            ".accept-cookies",
            "button[data-action='accept']",
            "[data-testid*='accept']",
            "[data-testid*='cookie'] button",
        ]
        
        # Accept keywords to look for in button text
        accept_keywords = ['accept', 'agree', 'allow', 'ok', 'got it', 'i understand', 'continue', 'yes', 'consent']
        accept_exact_texts = ['accept all', 'accept', 'agree', 'i agree', 'allow all', 'allow', 'ok', 'got it', 
                             'accept cookies', 'accept all cookies', 'yes, i agree', 'i accept']
        
        def try_click_consent_buttons(context_name: str = "main") -> bool:
            """Try clicking consent buttons in current context (main page or iframe)."""
            for selector in cookie_accept_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elements:
                        try:
                            if elem.is_displayed() and elem.is_enabled():
                                btn_text = elem.text.lower().strip()
                                if any(word in btn_text for word in accept_keywords) or not btn_text:
                                    elem.click()
                                    logger.info(f"Accepted cookie consent in {context_name} using: {selector} (text: '{elem.text}')")
                                    time.sleep(0.5)
                                    return True
                        except Exception as e:
                            logger.debug(f"Failed to click {selector} in {context_name}: {e}")
                            continue
                except Exception:
                    continue
            
            # Try finding buttons by text content
            try:
                buttons = driver.find_elements(By.TAG_NAME, "button")
                for btn in buttons:
                    try:
                        btn_text = btn.text.lower().strip()
                        if btn.is_displayed() and btn.is_enabled():
                            if btn_text in accept_exact_texts:
                                btn.click()
                                logger.info(f"Accepted cookie consent in {context_name} via button text: '{btn.text}'")
                                time.sleep(0.5)
                                return True
                    except Exception:
                        continue
            except Exception:
                pass
            
            return False
        
        # First try in main page context
        accepted = try_click_consent_buttons("main page")
        
        # If not found, try looking in iframes (many consent dialogs are in iframes)
        if not accepted:
            try:
                iframes = driver.find_elements(By.TAG_NAME, "iframe")
                consent_iframe_hints = ['consent', 'cookie', 'privacy', 'gdpr', 'sp_message', 'evidon', 'onetrust', 'sourcepoint']
                
                for iframe in iframes:
                    try:
                        iframe_id = iframe.get_attribute("id") or ""
                        iframe_name = iframe.get_attribute("name") or ""
                        iframe_src = iframe.get_attribute("src") or ""
                        iframe_title = iframe.get_attribute("title") or ""
                        iframe_identifiers = f"{iframe_id} {iframe_name} {iframe_src} {iframe_title}".lower()
                        
                        # Check if this iframe might be a consent dialog
                        if any(hint in iframe_identifiers for hint in consent_iframe_hints) or iframe.is_displayed():
                            logger.debug(f"Checking iframe for consent: id={iframe_id}, title={iframe_title}")
                            driver.switch_to.frame(iframe)
                            
                            if try_click_consent_buttons(f"iframe:{iframe_id or iframe_title or 'unnamed'}"):
                                accepted = True
                                driver.switch_to.default_content()
                                break
                            
                            driver.switch_to.default_content()
                    except Exception as e:
                        logger.debug(f"Error checking iframe: {e}")
                        try:
                            driver.switch_to.default_content()
                        except:
                            pass
                        continue
            except Exception as e:
                logger.debug(f"Error searching iframes for consent: {e}")
                try:
                    driver.switch_to.default_content()
                except:
                    pass
        
        if accepted:
            logger.info("Cookie consent handled successfully")
            time.sleep(1)  # Extra wait for consent dialog to fully close
        else:
            logger.debug("No cookie consent popup found or already dismissed")

    def _handle_ad_popups(self, driver: webdriver.Chrome) -> None:
        """
        Close ad popups and overlays that may block content.
        
        Handles common popup patterns including:
        - SecurityWeek popmake ads
        - Modal overlays
        - Newsletter signup popups
        - Generic close buttons on overlays
        """
        # Common popup close button selectors
        popup_close_selectors = [
            # SecurityWeek specific (popmake plugin)
            ".pum-close",
            ".popmake-close",
            "button.pum-close",
            ".pum-container.active .pum-close",
            
            # Generic modal/popup close buttons
            ".modal-close",
            ".popup-close",
            ".overlay-close",
            "[class*='close-button']",
            "[class*='close-btn']",
            "[class*='closeButton']",
            "[aria-label='Close']",
            "[aria-label='close']",
            
            # Common overlay dismiss buttons
            ".dismiss",
            ".dismiss-button",
            "[class*='dismiss']",
            
            # Newsletter/subscription popups
            ".newsletter-close",
            ".subscribe-close",
            "[class*='newsletter'] [class*='close']",
            
            # Generic X buttons in popups
            ".popup button[class*='close']",
            ".modal button[class*='close']",
            ".overlay button[class*='close']",
            
            # Ad-specific close buttons
            ".ad-close",
            "[class*='ad-close']",
            ".close-ad",
        ]
        
        closed_count = 0
        for selector in popup_close_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elements:
                    try:
                        if elem.is_displayed():
                            elem.click()
                            logger.debug(f"Closed popup using: {selector}")
                            closed_count += 1
                            time.sleep(0.3)  # Brief pause after closing
                    except Exception:
                        continue
            except Exception:
                continue
        
        # Also try closing by pressing Escape key (works on many modals)
        if closed_count == 0:
            try:
                from selenium.webdriver.common.keys import Keys
                driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
                logger.debug("Sent Escape key to close potential popups")
                time.sleep(0.3)
            except Exception:
                pass
        
        # Click outside popups to dismiss them (click on body)
        try:
            # Find and click on the main content area to dismiss overlays
            body = driver.find_element(By.TAG_NAME, "body")
            actions = ActionChains(driver)
            actions.move_to_element_with_offset(body, 10, 10).click().perform()
            time.sleep(0.2)
        except Exception:
            pass
        
        if closed_count > 0:
            logger.info(f"Closed {closed_count} popup(s)/overlay(s)")

    def _get_with_selenium_advanced(
        self, 
        url: str, 
        headless: bool = True,
        wait_time: int = 15,
        simulate_human: bool = True,
    ) -> Optional[BeautifulSoup]:
        """
        Advanced Selenium fetching with bot evasion techniques.
        
        Args:
            url: URL to fetch
            headless: If True, run in headless mode
            wait_time: Seconds to wait for page load
            simulate_human: If True, simulate human-like behavior
        """
        if not SELENIUM_AVAILABLE:
            return None
        
        driver = None
        try:
            driver = self._create_stealth_driver(headless=headless)
            
            # Navigate to URL
            logger.debug(f"Selenium navigating to: {url}")
            driver.get(url)
            
            # Wait for body to load
            WebDriverWait(driver, wait_time).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Random initial delay
            time.sleep(random.uniform(2, 4))
            
            # Handle cookie consent
            self._handle_cookie_consent(driver)
            
            # Wait a bit for any delayed popups (ads often load after page)
            time.sleep(random.uniform(1, 2))
            
            # Close any ad popups or overlays
            self._handle_ad_popups(driver)
            
            # Simulate human behavior
            if simulate_human:
                self._simulate_human_behavior(driver)
            
            # Additional wait for dynamic content
            time.sleep(random.uniform(1, 2))
            
            # Try closing popups again (some appear after scrolling)
            self._handle_ad_popups(driver)
            
            # Check for common block indicators
            page_source = driver.page_source.lower()
            block_indicators = [
                "access denied",
                "blocked",
                "bot detected",
                "captcha",
                "please verify",
                "checking your browser",
                "just a moment",
                "cloudflare",
                "ddos protection",
            ]
            
            if any(indicator in page_source for indicator in block_indicators):
                logger.warning(f"Bot detection triggered on {url}")
                return None
            
            # Handle cookie consent again (some appear after page fully loads)
            self._handle_cookie_consent(driver)
            
            # Success - return the page content
            html = driver.page_source
            return BeautifulSoup(html, "html.parser")
            
        except Exception as e:
            logger.warning(f"Selenium failed for {url}: {e}")
            return None
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

    def _get_with_selenium(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fallback to undetected-chromedriver with multiple attempts.
        
        Strategy:
        1. Try headless with stealth settings
        2. Try headless with more aggressive human simulation
        3. Try non-headless (visible browser) as last resort
        """
        if not SELENIUM_AVAILABLE:
            logger.warning("Selenium not available for bot detection bypass")
            return None
        
        domain = self._get_domain(url)
        is_aggressive = self._is_aggressive_domain(url)
        
        # Attempt 1: Headless with stealth
        logger.info(f"Selenium attempt 1 (headless stealth) for {url}")
        result = self._get_with_selenium_advanced(url, headless=True, simulate_human=True)
        if result is not None:
            return result
        
        # Attempt 2: Headless with longer wait
        logger.info(f"Selenium attempt 2 (headless, longer wait) for {url}")
        result = self._get_with_selenium_advanced(url, headless=True, wait_time=25, simulate_human=True)
        if result is not None:
            return result
        
        # Attempt 3: Non-headless (visible browser) for aggressive domains
        if is_aggressive or domain in self._failed_domains:
            logger.info(f"Selenium attempt 3 (visible browser) for {url}")
            logger.info("Opening visible browser window - this may take a moment...")
            result = self._get_with_selenium_advanced(url, headless=False, wait_time=30, simulate_human=True)
            if result is not None:
                return result
        
        logger.warning(f"All Selenium attempts failed for {url}")
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
        Get page as BeautifulSoup with intelligent bot detection bypass.
        
        Strategy:
        1. For known aggressive domains, use Selenium directly
        2. Otherwise, try requests first
        3. Fall back to Selenium with multiple attempts if blocked
        """
        BLOCKED_STATUS_CODES = (403, 429, 503)
        
        # For aggressive domains or previously failed domains, skip requests
        if self._should_use_selenium_first(url):
            if use_selenium_fallback and SELENIUM_AVAILABLE:
                logger.info(f"Using Selenium directly for aggressive domain: {url}")
                return self._get_with_selenium(url)
            else:
                logger.warning(f"Aggressive domain {url} but Selenium not available")
                return None
        
        # Try requests first
        try:
            self._sleep_a_bit()
            resp = self.session.get(url, timeout=self.timeout, headers=self._random_headers())
            
            if resp.status_code in BLOCKED_STATUS_CODES:
                self._mark_domain_failed(url)
                if use_selenium_fallback and SELENIUM_AVAILABLE:
                    logger.info(f"Requests blocked ({resp.status_code}) for {url}, trying Selenium...")
                    return self._get_with_selenium(url)
                logger.warning(f"Blocked ({resp.status_code}) for {url}, Selenium not available")
                return None
            
            if allow_404 and resp.status_code == 404:
                return None
            if resp.status_code >= 400 and resp.status_code not in (allow_status or set()):
                resp.raise_for_status()
            
            try:
                return BeautifulSoup(resp.text, "lxml")
            except Exception:
                return BeautifulSoup(resp.text, "html.parser")
                
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code in BLOCKED_STATUS_CODES:
                self._mark_domain_failed(url)
                if use_selenium_fallback and SELENIUM_AVAILABLE:
                    logger.info(f"Requests blocked ({e.response.status_code}) for {url}, trying Selenium...")
                    return self._get_with_selenium(url)
                return None
            raise
        except requests.RequestException as e:
            logger.warning(f"Request failed for {url}: {e}")
            if use_selenium_fallback and SELENIUM_AVAILABLE:
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
        Get page as BeautifulSoup with content validation.
        """
        soup = self.get_soup(
            url,
            allow_404=allow_404,
            allow_status=allow_status,
            use_selenium_fallback=use_selenium_fallback,
        )
        
        if soup is not None and check_content is not None:
            if not check_content(soup):
                if use_selenium_fallback and SELENIUM_AVAILABLE:
                    logger.info(f"Content check failed for {url}, trying Selenium...")
                    selenium_result = self._get_with_selenium(url)
                    if selenium_result is not None and check_content(selenium_result):
                            return selenium_result
                    logger.warning(f"Selenium result also failed content check for {url}")
                return None
        
        return soup


def build_http_client() -> HttpClient:
    return HttpClient()
