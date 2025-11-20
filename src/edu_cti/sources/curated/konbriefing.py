# src/edu_cti/ingest/konbriefing.py
import json
from typing import Callable, List, Optional

from bs4 import BeautifulSoup, NavigableString
import pandas as pd

from src.edu_cti.core.http import HttpClient, build_http_client
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.utils import parse_date_with_precision, now_utc_iso

LISTING_URL = "https://konbriefing.com/en-topics/cyber-attacks-universities.html"


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
) -> List[BaseIncident]:
    """
    Build a list of BaseIncident objects from KonBriefing listing data.
    Supports incremental saving via save_callback - saves after processing all records.

    Notes:
    - incident_date: we use the listing date as a first approximation.
    - source_published_date: same as listing_date for now.
    - ingested_at: current UTC timestamp from the pipeline run.
    - status: "confirmed" (KonBriefing is curated).
    - source_confidence: "high".
    
    Args:
        save_callback: Optional callback to save incidents incrementally.
                      Called after processing all records (single page source).
    """
    df = fetch_konbriefing_listing(client=client)
    incidents: List[BaseIncident] = []
    ingested_at = now_utc_iso()

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
        # Use all URLs for unique string generation
        urls_str = ";".join(all_urls) if all_urls else ""
        unique_string = f"{institution}|{incident_date or ''}|{urls_str}"
        incident_id = make_incident_id("konbriefing", unique_string)

        incident = BaseIncident(
            incident_id=incident_id,
            source="konbriefing",
            source_event_id=None,  # KonBriefing has no native event ID

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

            # Enrichment URLs (news / official statements)
            # Phase 1: primary_url=None, all URLs in all_urls (Phase 2 will select best URL)
            primary_url=None,
            all_urls=all_urls,

            # CTI / infra URLs – none for KonBriefing
            leak_site_url=None,
            source_detail_url=None,
            screenshot_url=None,

            # Basic classification
            attack_type_hint=None,   # inferred later from article text
            status="confirmed",
            source_confidence="high",

            notes=None,
        )
        incidents.append(incident)
    
    # Save all incidents if callback provided (single page source, save after processing all)
    if save_callback is not None and incidents:
        try:
            save_callback(incidents)
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"KonBriefing: Saved {len(incidents)} incidents")
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"KonBriefing: Error saving incidents: {e}", exc_info=True)
            # Continue even if save fails

    return incidents
