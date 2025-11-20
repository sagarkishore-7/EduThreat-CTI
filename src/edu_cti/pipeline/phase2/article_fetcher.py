"""
Article fetching module for Phase 2 enrichment.

Fetches and extracts article content from URLs for LLM processing.
Uses newspaper3k for primary article extraction, with Selenium fallback.
"""

import logging
from typing import List, Optional, Dict
from dataclasses import dataclass

from src.edu_cti.core.http import HttpClient, build_http_client
from bs4 import BeautifulSoup

# Optional newspaper3k support for article extraction
try:
    import newspaper
    from newspaper import Article
    NEWSPAPER_AVAILABLE = True
except ImportError:
    NEWSPAPER_AVAILABLE = False

logger = logging.getLogger(__name__)


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
    
    def fetch_article(self, url: str) -> ArticleContent:
        """
        Fetch and extract article content from a URL.
        
        Tries newspaper3k first for better article extraction,
        then falls back to Selenium if that fails.
        
        Args:
            url: URL to fetch
            
        Returns:
            ArticleContent object with extracted content
        """
        # Try newspaper3k first (best for article extraction)
        if NEWSPAPER_AVAILABLE:
            article_content = self._fetch_with_newspaper(url)
            if article_content and article_content.fetch_successful:
                logger.debug(f"Successfully fetched {url} using newspaper3k")
                return article_content
            logger.info(f"newspaper3k failed for {url}, trying Selenium fallback...")
        
        # Fall back to Selenium (handles bot detection)
        try:
            soup = self.http_client.get_soup(url, allow_404=True)
            
            if soup is None:
                return ArticleContent(
                    url=url,
                    title="",
                    content="",
                    fetch_successful=False,
                    error_message="Failed to fetch or parse URL",
                    content_length=0
                )
            
            # Extract title
            title = self._extract_title(soup)
            
            # Extract main content
            content = self._extract_content(soup)
            
            # Extract metadata
            author = self._extract_author(soup)
            publish_date = self._extract_publish_date(soup)
            
            # Clean content
            content = self._clean_content(content)
            
            # Only return successful if we have meaningful content
            if not content or len(content.strip()) < 100:
                return ArticleContent(
                    url=url,
                    title=title or "",
                    content=content or "",
                    author=author,
                    publish_date=publish_date,
                    fetch_successful=False,
                    error_message="Extracted content too short or empty",
                    content_length=len(content) if content else 0
                )
            
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
            logger.error(f"Error fetching article from {url}: {e}")
            return ArticleContent(
                url=url,
                title="",
                content="",
                fetch_successful=False,
                error_message=str(e),
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
            article = Article(url, language='en')
            article.download()
            article.parse()
            
            # Check if we got meaningful content
            if not article.text or len(article.text.strip()) < 100:
                logger.debug(f"newspaper3k extracted content too short for {url}")
                return None
            
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
        """Extract main article content from soup."""
        # Remove unwanted elements
        for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside', 'iframe']):
            element.decompose()
        
        # Try various content selectors
        content_selectors = [
            'article .entry-content',
            'article .post-content',
            'article .article-content',
            'article .content',
            '[class*="article-body"]',
            '[class*="post-body"]',
            '[class*="entry-content"]',
            'article',
            '[role="article"]',
            '.main-content',
            '#main-content'
        ]
        
        content_parts = []
        
        for selector in content_selectors:
            elements = soup.select(selector)
            for element in elements:
                # Get all paragraphs
                paragraphs = element.find_all(['p', 'div'], recursive=True)
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    # Filter out very short paragraphs (likely navigation/menus)
                    if text and len(text) > 50:
                        content_parts.append(text)
                
                # If we found substantial content, break
                if len(' '.join(content_parts)) > 500:
                    break
            
            if content_parts:
                break
        
        # If no structured content found, try getting all paragraphs
        if not content_parts:
            paragraphs = soup.find_all('p')
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text and len(text) > 50:
                    content_parts.append(text)
        
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

