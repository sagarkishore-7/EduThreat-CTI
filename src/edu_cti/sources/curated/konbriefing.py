"""
KonBriefing University Cyber Attacks listing ingestion.

This module scrapes the KonBriefing page that tracks cyber attacks on universities.
It's a single-page source (no pagination) but supports incremental ingestion
by tracking the last ingestion date.

URL: https://konbriefing.com/en-topics/cyber-attacks-universities.html
"""

import json
import logging
from datetime import datetime
from typing import Callable, List, Optional

from bs4 import BeautifulSoup, NavigableString
import pandas as pd

from src.edu_cti.core.db import (
    get_connection,
    init_db,
    get_last_pubdate,
    set_last_pubdate,
    source_event_exists,
    register_source_event,
)
from src.edu_cti.core.http import HttpClient, build_http_client
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.utils import parse_date_with_precision, now_utc_iso

LISTING_URL = "https://konbriefing.com/en-topics/cyber-attacks-universities.html"
SOURCE_NAME = "konbriefing"
logger = logging.getLogger(__name__)


def _text_after_img(img) -> str:
    """Get the date text that appears next to the country flag image."""
    sib = img.next_sibling
    while sib is not None:
        if isinstance(sib, NavigableString):
            t = str(sib).strip()
            if t:
                return t
        sib = sib.next_sibling

    parent_txt = img.parent.get_text(" ", strip=True)
    parent_txt = parent_txt.replace(img.get("alt", ""), "").strip()
    return parent_txt


def _extract_subtitle_and_links(art) -> tuple[str, list[str]]:
    """
    Extract subtitle + all absolute links for a KonBriefing article block.
    This is a simplified version of your thesis scraper, focused only on ingestion.
    """
    kbox = art.select_one("div.kbresbox1")
    if not kbox:
        return "", []

    top_blocks = [d for d in kbox.find_all("div", recursive=False)]
    block_b = top_blocks[1] if len(top_blocks) > 1 else None
    if not block_b:
        return "", []

    # Subtitle = first direct <div> text
    subtitle = ""
    for child in block_b.find_all("div", recursive=False):
        subtitle = child.get_text(" ", strip=True)
        break

    links: list[str] = []
    ml = block_b.find(
        lambda tag: tag.name == "div"
        and tag.get("style")
        and "margin-left" in tag["style"]
    )
    if ml:
        for a in ml.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("http://") or href.startswith("https://"):
                links.append(href)

    seen, uniq = set(), []
    for u in links:
        if u not in seen:
            seen.add(u)
            uniq.append(u)

    return subtitle, uniq


def _parse_date_for_comparison(date_str: Optional[str]) -> Optional[datetime]:
    """Parse date string to datetime for comparison."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def fetch_konbriefing_listing(client: Optional[HttpClient] = None) -> pd.DataFrame:
    """
    Scrape the KonBriefing EDU cyber-attacks page and return a raw DataFrame
    with listing-level metadata (no LLM, no article fetch).
    Uses HttpClient with automatic Selenium fallback if needed.
    """
    http_client = client or build_http_client()

    # Try get_soup first (faster), with automatic Selenium fallback if blocked
    soup = http_client.get_soup(LISTING_URL, use_selenium_fallback=True)
    
    if soup is None:
        raise Exception(f"Failed to fetch KonBriefing listing page: {LISTING_URL}")

    records = []
    for art in soup.select("article.portfolio-item"):
        img = art.find("img", alt=lambda x: x and x.startswith("Flag "))
        if not img:
            continue

        country = img["alt"].replace("Flag ", "").strip()
        raw_date = _text_after_img(img)
        date_iso, date_prec = parse_date_with_precision(raw_date)

        # bold title
        title_div = art.find(
            lambda tag: tag.name == "div"
            and tag.get("style")
            and "bold" in tag["style"].lower()
        )
        title = title_div.get_text(strip=True) if title_div else ""

        subtitle, links = _extract_subtitle_and_links(art)

        # very rough institution name from subtitle
        institution = ""
        for sep in ("–", "—", "-", "--"):
            if sep in subtitle:
                institution = subtitle.split(sep, 1)[0].strip()
                break
        if not institution and "," in subtitle:
            maybe = subtitle.split(",", 1)[0].strip()
            if len(maybe) > 3:
                institution = maybe

        primary_url = links[0] if links else ""

        records.append(
            {
                "listing_date": date_iso,
                "date_precision": date_prec,
                "country": country,
                "institution": institution,
                "subtitle": subtitle,
                "title": title,
                "primary_url": primary_url,
                "all_urls_json": json.dumps(links, ensure_ascii=False),
            }
        )

    return pd.DataFrame.from_records(records)


def build_konbriefing_base_incidents(
    client: Optional[HttpClient] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
    incremental: bool = True,
) -> List[BaseIncident]:
    """
    Build a list of BaseIncident objects from KonBriefing listing data.
    
    Supports incremental ingestion:
    - incremental=True (default): Only process incidents newer than last_pubdate
    - incremental=False: Process all incidents (full refresh)

    Notes:
    - incident_date: we use the listing date as a first approximation.
    - source_published_date: same as listing_date for now.
    - ingested_at: current UTC timestamp from the pipeline run.
    - status: "confirmed" (KonBriefing is curated).
    - source_confidence: "high".
    
    Args:
        client: Optional HTTP client
        save_callback: Optional callback to save incidents incrementally
        incremental: If True, skip incidents already ingested or older than last_pubdate
    """
    # Initialize database connection for incremental tracking
    conn = get_connection()
    init_db(conn)
    
    # Get last ingestion date for incremental mode
    last_pubdate = None
    last_pubdate_dt = None
    if incremental:
        last_pubdate = get_last_pubdate(conn, SOURCE_NAME)
        if last_pubdate:
            last_pubdate_dt = _parse_date_for_comparison(last_pubdate)
            logger.info(f"KonBriefing: Incremental mode - processing incidents newer than {last_pubdate}")
        else:
            logger.info("KonBriefing: No previous ingestion found - processing all incidents")
    else:
        logger.info("KonBriefing: Full mode (incremental=False) - processing all incidents")
    
    df = fetch_konbriefing_listing(client=client)
    incidents: List[BaseIncident] = []
    ingested_at = now_utc_iso()
    
    newest_date: Optional[str] = None
    newest_date_dt: Optional[datetime] = None
    total_new = 0
    total_skipped = 0

    for _, row in df.iterrows():
        all_urls: list[str] = []
        if row.get("all_urls_json"):
            try:
                all_urls = json.loads(row["all_urls_json"])
            except Exception:
                all_urls = []

        # Collect all URLs (including primary_url if it exists)
        if row.get("primary_url"):
            primary_url_val = row["primary_url"]
            if primary_url_val not in all_urls:
                all_urls.append(primary_url_val)

        incident_date = row.get("listing_date") or None
        date_precision = row.get("date_precision") or "unknown"

        institution = row.get("institution") or ""
        
        # Track newest date for updating last_pubdate
        if incident_date:
            incident_date_dt = _parse_date_for_comparison(incident_date)
            if incident_date_dt and (newest_date_dt is None or incident_date_dt > newest_date_dt):
                newest_date = incident_date
                newest_date_dt = incident_date_dt
        
        # INCREMENTAL CHECK: Skip if older than last ingestion date
        if incremental and last_pubdate_dt and incident_date:
            incident_date_dt = _parse_date_for_comparison(incident_date)
            if incident_date_dt and incident_date_dt <= last_pubdate_dt:
                total_skipped += 1
                continue
        
        # Use all URLs for unique string generation
        urls_str = ";".join(all_urls) if all_urls else ""
        unique_string = f"{institution}|{incident_date or ''}|{urls_str}"
        incident_id = make_incident_id(SOURCE_NAME, unique_string)
        
        # Skip if already in source_events (deduplication)
        source_event_id = unique_string
        if source_event_exists(conn, SOURCE_NAME, source_event_id):
            logger.debug(f"Skipping already-ingested incident: {institution[:50]}...")
            total_skipped += 1
            continue

        incident = BaseIncident(
            incident_id=incident_id,
            source=SOURCE_NAME,
            source_event_id=source_event_id,

            university_name=institution,
            victim_raw_name=institution,

            # For this page we can safely default to "University"
            institution_type="University",
            country=row.get("country") or None,
            region=None,
            city=None,

            incident_date=incident_date,
            date_precision=date_precision,

            source_published_date=incident_date,
            ingested_at=ingested_at,

            title=row.get("title") or None,
            subtitle=row.get("subtitle") or None,

            # Phase 1: primary_url=None, all URLs in all_urls (Phase 2 will select best URL)
            primary_url=None,
            all_urls=all_urls,

            # CTI / infra URLs – none for KonBriefing
            leak_site_url=None,
            source_detail_url=None,
            screenshot_url=None,

            # Basic classification
            attack_type_hint=None,
            status="confirmed",
            source_confidence="high",

            notes=None,
        )
        
        # Register source event to prevent re-ingestion
        register_source_event(conn, SOURCE_NAME, source_event_id, incident.incident_id, ingested_at)
        
        incidents.append(incident)
        total_new += 1
    
    # Save all incidents if callback provided
    if save_callback is not None and incidents:
        try:
            save_callback(incidents)
            logger.debug(f"KonBriefing: Saved {len(incidents)} incidents")
        except Exception as e:
            logger.error(f"KonBriefing: Error saving incidents: {e}", exc_info=True)
    
    # Update last_pubdate to newest incident we saw
    if newest_date:
        set_last_pubdate(conn, SOURCE_NAME, newest_date)
        logger.info(f"KonBriefing: Updated last_pubdate to {newest_date}")
    
    conn.commit()
    conn.close()
    
    logger.info(f"KonBriefing: Complete - {total_new} new, {total_skipped} skipped")
    return incidents
