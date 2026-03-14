"""
DataBreaches.net RSS feed ingestion.

This module handles the DataBreaches.net RSS feed, filtering for education sector
articles and converting them to BaseIncident objects.

Supports incremental ingestion via last_pubdate tracking.
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
    has_education_category,
)

RSS_FEED_URL = "https://databreaches.net/feed/"
SOURCE_NAME = f"{config.SOURCE_DATABREACHES}_rss"
logger = logging.getLogger(__name__)


def build_databreaches_rss_incidents(
    *,
    max_age_days: int = 30,
    client: Optional[HttpClient] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
    incremental: bool = True,
) -> List[BaseIncident]:
    """
    Fetch and parse DataBreaches.net RSS feed, filtering for education sector articles.
    
    Supports incremental ingestion:
    - incremental=True (default): Skip articles older than last_pubdate
    - incremental=False: Process all articles within max_age_days
    
    Only processes items:
    - Published within max_age_days (or newer than last_pubdate in incremental mode)
    - With "Education Sector" category
    - Not already ingested (deduplication via database)
    
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
                logger.info(f"DataBreaches RSS: Incremental mode - processing articles newer than {last_pubdate}")
            except ValueError:
                last_pubdate_dt = None
        else:
            logger.info("DataBreaches RSS: No previous ingestion found - processing all articles")
    else:
        logger.info("DataBreaches RSS: Full mode (incremental=False)")
    
    # Fetch RSS feed
    logger.info(f"Fetching DataBreaches RSS feed from {RSS_FEED_URL}")
    root = fetch_rss_feed(RSS_FEED_URL, client=http_client)
    
    if root is None:
        logger.error("Failed to fetch or parse RSS feed")
        conn.close()
        return incidents
    
    # Find all items (handle RSS 2.0 structure)
    items = root.findall(".//item")
    if not items:
        channel = root.find("channel")
        if channel is not None:
            items = channel.findall("item")
    
    logger.info(f"Found {len(items)} items in RSS feed")
    
    newest_date: Optional[datetime] = None
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
            
            # Filter by age (only process items within max_age_days)
            if not is_within_max_age(pub_date, max_age_days):
                logger.debug(f"Skipping item '{title}' - too old (published: {pub_date_str})")
                continue
            
            # INCREMENTAL CHECK: Skip if older than last_pubdate
            if incremental and last_pubdate_dt and pub_date:
                if pub_date <= last_pubdate_dt:
                    total_skipped += 1
                    continue
            
            # Extract categories
            categories = extract_rss_categories(item)
            
            # Filter by education category
            if not has_education_category(categories):
                logger.debug(f"Skipping item '{title}' - not education sector")
                continue
            
            # Extract description
            desc_elem = item.find("description")
            description = ""
            if desc_elem is not None and desc_elem.text:
                description = re.sub(r'<[^>]+>', '', desc_elem.text).strip()
            
            # Extract GUID (use as source_event_id for deduplication)
            guid_elem = item.find("guid")
            guid = guid_elem.text.strip() if guid_elem is not None and guid_elem.text else article_url
            
            # Check if already ingested (deduplication)
            if source_event_exists(conn, SOURCE_NAME, guid):
                logger.debug(f"Skipping item '{title}' - already ingested")
                total_skipped += 1
                continue
            
            # Parse incident date from publication date
            incident_date = None
            date_precision = "unknown"
            if pub_date:
                incident_date = pub_date.strftime("%Y-%m-%d")
                date_precision = "day"
            
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
                subtitle=description or None,
                primary_url=None,
                all_urls=[article_url],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=f"rss_source={SOURCE_NAME};categories={','.join(categories)}",
            )
            
            incidents.append(incident)
            logger.info(f"Collected incident: {title} (published: {pub_date_str})")
            
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
        logger.info(f"DataBreaches RSS: Updated last_pubdate to {newest_date.strftime('%Y-%m-%d')}")
    
    conn.commit()
    conn.close()
    
    logger.info(f"DataBreaches RSS: {len(incidents)} new, {total_skipped} skipped")
    return incidents
