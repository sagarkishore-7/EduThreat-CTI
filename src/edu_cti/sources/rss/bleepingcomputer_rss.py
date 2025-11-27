"""
BleepingComputer RSS feed ingestion.

This module handles the BleepingComputer RSS feed, filtering for:
1. Security category articles only
2. Articles containing education-related keywords in title/description

Supports incremental ingestion via last_pubdate tracking.

BleepingComputer is a major cybersecurity news source that covers breaches
affecting educational institutions.

Feed URL: https://www.bleepingcomputer.com/feed/
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Callable, List, Optional
from xml.etree import ElementTree as ET

from src.edu_cti.core import config
from src.edu_cti.core.db import (
    get_connection,
    init_db,
    get_last_pubdate,
    set_last_pubdate,
    source_event_exists,
    register_source_event,
)
from src.edu_cti.core.http import HttpClient
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.utils import now_utc_iso, parse_date_with_precision
from .common import (
    default_client,
    fetch_rss_feed,
    parse_rss_date,
    is_within_max_age,
    extract_rss_categories,
)

RSS_FEED_URL = "https://www.bleepingcomputer.com/feed/"
SOURCE_NAME = "bleepingcomputer"
logger = logging.getLogger(__name__)


def has_security_category(categories: List[str]) -> bool:
    """
    Check if article has Security category.
    
    BleepingComputer uses categories like "Security", "Microsoft", "Software".
    We only want Security-related articles.
    """
    categories_lower = [cat.lower().strip() for cat in categories]
    return "security" in categories_lower


def contains_education_keywords(text: str) -> bool:
    """
    Check if text contains any education-related keywords.
    
    Uses the EDUCATION_KEYWORDS list from config.
    """
    if not text:
        return False
    
    text_lower = text.lower()
    
    for keyword in config.EDUCATION_KEYWORDS:
        # Use word boundary matching for short keywords
        if len(keyword) <= 5:
            pattern = rf'\b{re.escape(keyword)}\b'
            if re.search(pattern, text_lower):
                return True
        else:
            if keyword.lower() in text_lower:
                return True
    
    return False


def build_bleepingcomputer_rss_incidents(
    *,
    max_age_days: int = 30,
    client: Optional[HttpClient] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
    incremental: bool = True,
) -> List[BaseIncident]:
    """
    Fetch and parse BleepingComputer RSS feed, filtering for education sector articles.
    
    Supports incremental ingestion:
    - incremental=True (default): Skip articles older than last_pubdate
    - incremental=False: Process all articles within max_age_days
    
    Filter criteria:
    1. Must be in "Security" category
    2. Must contain education keywords in title or description
    3. Published within max_age_days (or newer than last_pubdate in incremental mode)
    4. Not already ingested (deduplication via database)
    
    Args:
        max_age_days: Maximum age of items to include (default: 30 days)
        client: Optional HTTP client
        save_callback: Optional callback to save incidents incrementally
        incremental: If True, skip articles older than last_pubdate
        
    Returns:
        List of BaseIncident objects
    """
    http_client = client or default_client()
    incidents: List[BaseIncident] = []
    ingested_at = now_utc_iso()
    
    # Initialize database connection
    conn = get_connection()
    init_db(conn)
    
    # Get last ingestion date for incremental mode
    last_pubdate = None
    last_pubdate_dt = None
    if incremental:
        last_pubdate = get_last_pubdate(conn, SOURCE_NAME)
        if last_pubdate:
            try:
                last_pubdate_dt = datetime.strptime(last_pubdate[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                logger.info(f"BleepingComputer: Incremental mode - processing articles newer than {last_pubdate}")
            except ValueError:
                last_pubdate_dt = None
        else:
            logger.info("BleepingComputer: No previous ingestion found - processing all articles")
    else:
        logger.info("BleepingComputer: Full mode (incremental=False)")
    
    # Fetch RSS feed
    logger.info(f"Fetching BleepingComputer RSS feed from {RSS_FEED_URL}")
    root = fetch_rss_feed(RSS_FEED_URL, client=http_client)
    
    if root is None:
        logger.error("Failed to fetch or parse BleepingComputer RSS feed")
        conn.close()
        return incidents
    
    # Find all items (RSS 2.0 structure)
    items = root.findall(".//item")
    if not items:
        channel = root.find("channel")
        if channel is not None:
            items = channel.findall("item")
    
    logger.info(f"Found {len(items)} items in BleepingComputer RSS feed")
    
    newest_date: Optional[datetime] = None
    education_matches = 0
    security_matches = 0
    total_skipped = 0
    
    for item in items:
        try:
            # Extract title
            title_elem = item.find("title")
            title = title_elem.text.strip() if title_elem is not None and title_elem.text else None
            
            if not title:
                continue
            
            # Extract link
            link_elem = item.find("link")
            article_url = link_elem.text.strip() if link_elem is not None and link_elem.text else None
            
            if not article_url:
                continue
            
            # Extract publication date
            pub_date_elem = item.find("pubDate")
            pub_date_str = pub_date_elem.text.strip() if pub_date_elem is not None and pub_date_elem.text else None
            
            pub_date = None
            if pub_date_str:
                pub_date = parse_rss_date(pub_date_str)
            
            # Track newest date for updating last_pubdate
            if pub_date and (newest_date is None or pub_date > newest_date):
                newest_date = pub_date
            
            # Filter by age (use max_age_days as upper bound)
            if not is_within_max_age(pub_date, max_age_days):
                logger.debug(f"Skipping '{title}' - too old (published: {pub_date_str})")
                continue
            
            # INCREMENTAL CHECK: Skip if older than last_pubdate
            if incremental and last_pubdate_dt and pub_date:
                if pub_date <= last_pubdate_dt:
                    total_skipped += 1
                    continue
            
            # Extract categories
            categories = extract_rss_categories(item)
            
            # Filter 1: Must be Security category
            if not has_security_category(categories):
                logger.debug(f"Skipping '{title}' - not Security category")
                continue
            
            security_matches += 1
            
            # Extract description for keyword matching
            desc_elem = item.find("description")
            description = ""
            if desc_elem is not None and desc_elem.text:
                description = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', desc_elem.text, flags=re.DOTALL)
                description = re.sub(r'<[^>]+>', '', description).strip()
            
            # Combined text for keyword matching
            search_text = f"{title} {description}"
            
            # Filter 2: Must contain education keywords
            if not contains_education_keywords(search_text):
                logger.debug(f"Skipping '{title}' - no education keywords")
                continue
            
            education_matches += 1
            
            # Extract GUID for deduplication
            guid_elem = item.find("guid")
            guid = guid_elem.text.strip() if guid_elem is not None and guid_elem.text else article_url
            
            # Check if already ingested (deduplication)
            if source_event_exists(conn, SOURCE_NAME, guid):
                logger.debug(f"Skipping '{title}' - already ingested")
                total_skipped += 1
                continue
            
            # Parse incident date
            incident_date = None
            date_precision = "unknown"
            if pub_date:
                incident_date = pub_date.strftime("%Y-%m-%d")
                date_precision = "day"
            
            # Extract author
            author_elem = item.find("{http://purl.org/dc/elements/1.1/}creator")
            if author_elem is None:
                author_elem = item.find("dc:creator", {"dc": "http://purl.org/dc/elements/1.1/"})
            author = author_elem.text.strip() if author_elem is not None and author_elem.text else None
            
            # Create incident
            incident_id = make_incident_id(SOURCE_NAME, guid)
            
            incident = BaseIncident(
                incident_id=incident_id,
                source=SOURCE_NAME,
                source_event_id=guid,
                university_name="",
                victim_raw_name="",
                institution_type=None,
                country=None,
                region=None,
                city=None,
                incident_date=incident_date,
                date_precision=date_precision,
                source_published_date=incident_date,
                ingested_at=ingested_at,
                title=title,
                subtitle=description[:500] if description else None,
                primary_url=None,
                all_urls=[article_url],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="high",
                notes=f"rss_source={SOURCE_NAME};categories={','.join(categories)};author={author or 'unknown'}",
            )
            
            # Register source event
            register_source_event(conn, SOURCE_NAME, guid, incident_id, ingested_at)
            
            incidents.append(incident)
            logger.info(f"✓ Collected education incident: {title}")
            
            # Save incrementally if callback provided
            if save_callback is not None:
                try:
                    save_callback([incident])
                except Exception as e:
                    logger.error(f"Error in save_callback for incident {incident_id}: {e}", exc_info=True)
        
        except Exception as e:
            logger.error(f"Error processing RSS item: {e}", exc_info=True)
            continue
    
    # Update last_pubdate to newest article we saw
    if newest_date:
        set_last_pubdate(conn, SOURCE_NAME, newest_date.strftime("%Y-%m-%d"))
        logger.info(f"BleepingComputer: Updated last_pubdate to {newest_date.strftime('%Y-%m-%d')}")
    
    conn.commit()
    conn.close()
    
    logger.info(f"BleepingComputer RSS: {security_matches} security, {education_matches} education, {len(incidents)} new, {total_skipped} skipped")
    return incidents
