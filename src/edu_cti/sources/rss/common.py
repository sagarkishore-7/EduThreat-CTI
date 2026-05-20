"""
Common utilities for RSS feed sources.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from xml.etree import ElementTree as ET

import requests

from src.edu_cti.core.http import HttpClient
from src.edu_cti.core.config import REQUEST_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)


def _looks_like_feed_xml(body: str) -> bool:
    body_prefix = (body or "").lstrip()[:20].lower()
    return body_prefix.startswith("<?xml") or body_prefix.startswith("<rss") or body_prefix.startswith("<feed")


def _fetch_rss_with_plain_requests(url: str) -> Optional[str]:
    """Fallback for RSS endpoints that dislike browser/TLS impersonation clients."""
    try:
        response = requests.get(
            url,
            timeout=REQUEST_TIMEOUT_SECONDS,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/rss+xml, application/xml, text/xml, */*",
            },
        )
    except requests.RequestException as exc:
        logger.debug("Plain RSS fallback failed for %s: %s", url, exc)
        return None
    body = response.text or ""
    if response.status_code != 200 and not _looks_like_feed_xml(body):
        logger.debug("Plain RSS fallback returned HTTP %s for %s", response.status_code, url)
        return None
    return body if _looks_like_feed_xml(body) else None


def default_client() -> HttpClient:
    """Create a default HTTP client for RSS feed fetching."""
    from src.edu_cti.core.http import HttpClient
    return HttpClient()


def fetch_rss_feed(url: str, client: Optional[HttpClient] = None) -> Optional[ET.Element]:
    """
    Fetch and parse an RSS feed from the given URL.
    
    Args:
        url: RSS feed URL
        client: Optional HTTP client (uses default if not provided)
        
    Returns:
        Parsed XML ElementTree root element, or None if fetch/parse fails
    """
    http_client = client or default_client()
    
    try:
        response = http_client.get(url)
        if response is None:
            logger.warning(f"Failed to fetch RSS feed {url}: HTTP unknown")
            return None

        body = response.text or ""
        looks_like_feed = _looks_like_feed_xml(body)
        if response.status_code != 200 and not looks_like_feed:
            status_code = response.status_code if response else "unknown"
            logger.warning(f"Failed to fetch RSS feed {url}: HTTP {status_code}")
            return None
        if response.status_code != 200 and looks_like_feed:
            logger.info(
                "Parsing RSS feed body from %s despite HTTP %s because the body is valid feed XML",
                url,
                response.status_code,
            )
        
        # Parse XML
        try:
            root = ET.fromstring(body)
            return root
        except ET.ParseError as e:
            fallback_body = _fetch_rss_with_plain_requests(url)
            if fallback_body:
                try:
                    return ET.fromstring(fallback_body)
                except ET.ParseError:
                    pass
            logger.error(f"Failed to parse RSS feed XML from {url}: {e}")
            return None
            
    except Exception as e:
        logger.error(f"Error fetching RSS feed {url}: {e}", exc_info=True)
        return None


def parse_rss_date(date_str: str) -> Optional[datetime]:
    """
    Parse RSS pubDate string to datetime object.
    
    Supports common RSS date formats:
    - RFC 822: "Wed, 19 Nov 2025 16:23:06 +0000"
    - RFC 1123: "Wed, 19 Nov 2025 16:23:06 GMT"
    - ISO 8601: "2025-11-19T16:23:06Z"
    
    Args:
        date_str: Date string from RSS feed
        
    Returns:
        datetime object in UTC, or None if parsing fails
    """
    if not date_str:
        return None
    
    date_str = date_str.strip()
    
    # Try common formats
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",  # RFC 822 with timezone (4-digit year)
        "%a, %d %b %Y %H:%M:%S %Z",  # RFC 822 with GMT/UTC
        "%a, %d %b %y %H:%M:%S %z",  # RFC 822 with 2-digit year
        "%a, %d %b %y %H:%M:%S %Z",  # RFC 822 with 2-digit year + GMT
        "%Y-%m-%dT%H:%M:%S.%fZ",     # ISO 8601 with milliseconds
        "%Y-%m-%dT%H:%M:%SZ",         # ISO 8601 UTC
        "%Y-%m-%dT%H:%M:%S.%f%z",    # ISO 8601 with ms + timezone
        "%Y-%m-%dT%H:%M:%S%z",        # ISO 8601 with timezone
        "%Y-%m-%d %H:%M:%S",          # Simple format
        "%Y-%m-%d",                    # Date only
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            # Ensure timezone-aware (UTC if not specified)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    
    logger.warning(f"Could not parse RSS date: {date_str}")
    return None


def is_within_max_age(pub_date: Optional[datetime], max_age_days: int = 1) -> bool:
    """
    Check if a publication date is within the maximum age limit.
    
    Args:
        pub_date: Publication datetime (UTC)
        max_age_days: Maximum age in days (default: 1)
        
    Returns:
        True if within max_age_days, False otherwise
    """
    if pub_date is None:
        return False
    
    now = datetime.now(timezone.utc)
    age = now - pub_date
    
    return age <= timedelta(days=max_age_days)


def extract_rss_categories(item: ET.Element) -> list[str]:
    """
    Extract category tags from an RSS item.
    
    Supports both <category> and <dc:subject> tags.
    
    Args:
        item: RSS item element
        
    Returns:
        List of category strings
    """
    categories = []
    
    # Standard RSS category tags
    for cat in item.findall("category"):
        text = cat.text
        if text:
            categories.append(text.strip())
    
    # Dublin Core subject tags (dc:subject)
    # Handle namespaces
    namespaces = {
        "dc": "http://purl.org/dc/elements/1.1/",
    }
    for subject in item.findall("dc:subject", namespaces):
        text = subject.text
        if text:
            categories.append(text.strip())
    
    return categories


def has_education_category(categories: list[str]) -> bool:
    """
    Check if categories list contains education-related categories.
    
    Args:
        categories: List of category strings
        
    Returns:
        True if "Education Sector" or similar is found
    """
    education_keywords = [
        "education sector",
        "education",
        "university",
        "school",
        "college",
        "academic",
    ]
    
    categories_lower = [cat.lower() for cat in categories]
    
    for keyword in education_keywords:
        if any(keyword in cat for cat in categories_lower):
            return True
    
    return False
