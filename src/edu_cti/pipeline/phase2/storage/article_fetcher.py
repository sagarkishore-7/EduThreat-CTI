"""
Article fetching module for Phase 2 enrichment.

Fetches and extracts article content from URLs for LLM processing.
Uses newspaper3k for primary article extraction, with Selenium fallback.
Includes advanced bot detection bypass for sites like DarkReading.
"""

import logging
import time
import random
import requests
from typing import List, Optional, Dict
from dataclasses import dataclass
from urllib.parse import urlparse, quote

from src.edu_cti.core.http import HttpClient, build_http_client, SELENIUM_AVAILABLE
from bs4 import BeautifulSoup

# Optional newspaper3k support for article extraction
try:
    import newspaper
    from newspaper import Article
    NEWSPAPER_AVAILABLE = True
except ImportError:
    NEWSPAPER_AVAILABLE = False

logger = logging.getLogger(__name__)

# Domains where Selenium doesn't work (Cloudflare protection)
# For these, newspaper3k is the best option
CLOUDFLARE_PROTECTED_DOMAINS = []

"""[
    "darkreading.com",
    "securityweek.com",
    "bleepingcomputer.com",
]"""


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
        """Check if this domain has Cloudflare protection (Selenium doesn't help)."""
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
        
        Strategy:
        1. For Cloudflare-protected domains:
           a. Try newspaper3k first
           b. Fall back to archive.org if newspaper3k fails
        2. For other domains:
           a. Try newspaper3k first
           b. Fall back to Selenium if newspaper3k fails
        
        Args:
            url: URL to fetch
            max_retries: Maximum number of retry attempts
            
        Returns:
            ArticleContent object with extracted content
        """
        is_cloudflare = self._is_cloudflare_protected(url)
        
        # For Cloudflare-protected domains, try newspaper3k then archive.org
        # (Selenium always fails against Cloudflare)
        if is_cloudflare:
            logger.debug(f"Cloudflare-protected domain, using newspaper3k + archive fallback: {url}")
            
            # Try newspaper3k first
            if NEWSPAPER_AVAILABLE:
                article_content = self._fetch_with_newspaper(url)
                if article_content and article_content.fetch_successful:
                    return article_content
            
            # Try archive.org as fallback
            logger.info(f"Trying archive.org fallback for Cloudflare-protected site: {url}")
            archive_content = self._fetch_from_archive(url)
            if archive_content and archive_content.fetch_successful:
                return archive_content
            
            # All methods failed for Cloudflare site
            logger.warning(f"Cannot fetch Cloudflare-protected URL (no archive available): {url}")
            return ArticleContent(
                url=url,
                title="",
                content="",
                fetch_successful=False,
                error_message="Cloudflare-protected site, no archive available",
                content_length=0
            )
        
        # Try newspaper3k first (best for article extraction)
        if NEWSPAPER_AVAILABLE:
            article_content = self._fetch_with_newspaper(url)
            if article_content and article_content.fetch_successful:
                logger.debug(f"Successfully fetched {url} using newspaper3k")
                return article_content
            error_msg = article_content.error_message if article_content else "Unknown error"
            logger.info(f"newspaper3k failed for {url} ({error_msg}), trying Selenium fallback...")
        
        # Fall back to Selenium (handles bot detection for non-Cloudflare sites)
        if SELENIUM_AVAILABLE:
            article_content = self._fetch_with_selenium(url)
            if article_content and article_content.fetch_successful:
                return article_content
            logger.info(f"Selenium failed for {url}, trying archive.org fallback...")
        
        # Final fallback: Try archive.org (works for many historical articles)
        archive_content = self._fetch_from_archive(url)
        if archive_content and archive_content.fetch_successful:
            logger.info(f"Successfully fetched {url} from archive.org")
            return archive_content
        
        # All methods failed
        logger.warning(f"All fetch methods failed for {url} (no archive available)")
        return ArticleContent(
            url=url,
            title="",
            content="",
            fetch_successful=False,
            error_message="All fetch methods failed (newspaper3k, Selenium, archive.org)",
            content_length=0
        )

    def _fetch_with_selenium(self, url: str) -> ArticleContent:
        """
        Fetch article using Selenium with bot detection bypass.
        
        Args:
            url: URL to fetch
            
        Returns:
            ArticleContent with extracted content
        """
        try:
            logger.debug(f"Selenium: Fetching soup for {url}")
            soup = self.http_client.get_soup(url, allow_404=True, use_selenium_fallback=True)
            
            if soup is None:
                logger.warning(f"Selenium: soup is None for {url}")
                return ArticleContent(
                    url=url,
                    title="",
                    content="",
                    fetch_successful=False,
                    error_message="Selenium fetch failed or returned None",
                    content_length=0
                )
            
            # Extract content
            logger.debug(f"Selenium: Extracting content from {url}")
            title = self._extract_title(soup)
            content = self._extract_content(soup)
            author = self._extract_author(soup)
            publish_date = self._extract_publish_date(soup)
            
            logger.debug(f"Selenium: Extracted title length: {len(title) if title else 0}, content length: {len(content) if content else 0}")
            
            # Clean content
            content_before_clean = content
            content = self._clean_content(content)
            logger.debug(f"Selenium: Content length after cleaning: {len(content) if content else 0} (was {len(content_before_clean) if content_before_clean else 0})")
            
            # Check for meaningful content - lower threshold for databreaches.net and similar sites
            min_length = 50 if "databreaches.net" in url.lower() else 100
            content_stripped = content.strip() if content else ""
            
            if not content or len(content_stripped) < min_length:
                logger.warning(
                    f"Selenium: Content too short for {url}: "
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
            
            logger.info(f"Selenium: Successfully extracted content from {url}: {len(content)} chars")
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
            logger.error(f"Error fetching article from {url} with Selenium: {e}", exc_info=True)
            return ArticleContent(
                url=url,
                title="",
                content="",
                fetch_successful=False,
                error_message=f"Selenium exception: {str(e)}",
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
            
            # Check if we got meaningful content - lower threshold for databreaches.net
            min_length = 50 if "databreaches.net" in url.lower() else 100
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
            # We'll fall back to Selenium which handles bot detection better
            error_msg = str(e)
            if '403' in error_msg or 'Forbidden' in error_msg:
                logger.debug(f"newspaper3k got 403 Forbidden for {url}, will try Selenium fallback")
            else:
                logger.debug(f"newspaper3k failed for {url}: {e}, will try Selenium fallback")
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
    
    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract article author from soup."""
        author_selectors = [
            '[rel="author"]',
            '.author',
            '[class*="author"]',
            '[itemprop="author"]',
            'meta[property="article:author"]',
            'meta[name="author"]'
        ]
        
        for selector in author_selectors:
            element = soup.select_one(selector)
            if element:
                if element.name == 'meta':
                    author = element.get('content', '')
                else:
                    author = element.get_text(strip=True)
                
                if author:
                    return author
        
        return None
    
    def _extract_publish_date(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract article publish date from soup and normalize to ISO format when possible.
        
        Returns:
            Date string in ISO format (YYYY-MM-DD) when possible, or original format if parsing fails
        """
        date_selectors = [
            'time[datetime]',
            '[class*="date"]',
            '[class*="published"]',
            '[itemprop="datePublished"]',
            'meta[property="article:published_time"]',
            'meta[name="date"]',
            'meta[property="og:published_time"]',
            'meta[name="publish-date"]',
            'meta[name="pubdate"]',
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

