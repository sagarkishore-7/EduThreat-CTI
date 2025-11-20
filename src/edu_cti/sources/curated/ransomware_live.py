import time
from typing import Callable, List, Dict, Any, Optional

from src.edu_cti.core.http import HttpClient, build_http_client
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.utils import now_utc_iso, parse_date_with_precision


BASE_URL = "https://api.ransomware.live/v2"
MAX_RETRIES = 3
BACKOFF_SECONDS = 2.0

SOURCE_NAME = "ransomwarelive"


def _safe_str(x: Any) -> str:
    return str(x) if x is not None else ""


def _get_json(path: str, client: Optional[HttpClient] = None) -> Any:
    """
    Fetch JSON from ransomware.live API using HttpClient.
    Note: This is an API endpoint, so Selenium fallback is not needed,
    but we use HttpClient for consistent retry/backoff handling.
    """
    http_client = client or build_http_client()
    url = BASE_URL + path
    attempt = 0
    while True:
        attempt += 1
        try:
            # Use HttpClient.get for API calls (not get_soup since it's JSON)
            resp = http_client.get(url, allow_status=[429], to_soup=False)
            if resp is None:
                raise Exception(f"Failed to fetch {url}")
            
            # Handle rate limiting (429)
            if resp.status_code == 429 and attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS * attempt)
                continue
            
            # Check for other errors
            if resp.status_code >= 400:
                raise Exception(f"HTTP {resp.status_code} error for {url}")
            
            # Parse JSON from response text
            import json
            return json.loads(resp.text)
        except Exception as e:
            if attempt >= MAX_RETRIES:
                raise
            time.sleep(BACKOFF_SECONDS * attempt)


def _guess_institution_type(name: str, description: str = "") -> Optional[str]:
    """
    Very rough guess based on name/description.
    Refined later in the LLM enrichment phase.
    """
    base = f"{name} {description}".lower()

    if any(k in base for k in ["school district", "county schools", "high school"]):
        return "School"

    if any(
        k in base
        for k in [
            "school",
            "schule",
            "école",
            "escuela",
            "colegio",
            "scuola",
            "skola",
        ]
    ):
        return "School"

    if any(
        k in base
        for k in [
            "university",
            "universität",
            "universidade",
            "universidad",
            "université",
            "università",
        ]
    ):
        return "University"

    if any(
        k in base
        for k in [
            "institute",
            "instituto",
            "institut",
            "research",
            "academy",
            "akademie",
            "akademia",
        ]
    ):
        return "Research Institute"

    return "Unknown"


def _extract_press_article_urls(press_field: Any) -> List[str]:
    """
    For enrichment we ONLY want real articles / external sources.
    - Ignore ransomware.live internal IDs.
    - Ignore screenshots/images.
    """
    urls: List[str] = []
    if not press_field:
        return urls

    candidates: List[str] = []

    if isinstance(press_field, dict):
        for key in ("source", "url", "link"):
            val = _safe_str(press_field.get(key))
            if val:
                candidates.append(val)
    elif isinstance(press_field, list):
        for item in press_field:
            if isinstance(item, str):
                candidates.append(item)
            elif isinstance(item, dict):
                for key in ("source", "url", "link"):
                    val = _safe_str(item.get(key))
                    if val:
                        candidates.append(val)

    for u in candidates:
        u = u.strip()
        if not u.startswith(("http://", "https://")):
            continue
        # Skip ransomware.live internal pages
        if "ransomware.live" in u:
            continue
        # Skip obvious images
        if u.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
            continue
        urls.append(u)

    # uniquify
    seen, uniq = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def fetch_sector_education_victims(client: Optional[HttpClient] = None) -> List[Dict[str, Any]]:
    """
    Uses /sectorvictims/Education to get education-sector victims.
    Response (per your sample) is a list of dicts.
    """
    data = _get_json("/sectorvictims/Education", client=client)
    if isinstance(data, list):
        victims = data
    elif isinstance(data, dict):
        victims = data.get("victims") or data.get("data") or []
    else:
        victims = []
    return victims


def build_ransomwarelive_incidents(
    client: Optional[HttpClient] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
) -> List[BaseIncident]:
    """
    Map ransomware.live Education victims to BaseIncident.
    Supports incremental saving via save_callback - saves after processing all API records.

    Sample fields from the API:
      activity, attackdate, discovered, domain, claim_url, url, group,
      country, description, infostealer, press, screenshot, victim
    
    Args:
        save_callback: Optional callback to save incidents incrementally.
                      Called after processing all records from API.
    """
    ingested_at = now_utc_iso()
    incidents: List[BaseIncident] = []

    records = fetch_sector_education_victims(client=client)
    seen_keys = set()

    for row in records:
        activity = _safe_str(row.get("activity"))
        if activity and activity.lower() != "education":
            continue

        victim_name = _safe_str(row.get("victim") or row.get("name") or row.get("company"))
        description = _safe_str(row.get("description") or "")
        if not victim_name:
            continue

        group = _safe_str(row.get("group") or "")
        raw_attackdate = _safe_str(row.get("attackdate") or "")
        raw_discovered = _safe_str(row.get("discovered") or "")
        country = _safe_str(row.get("country") or row.get("countrycode") or "")
        domain = _safe_str(row.get("domain") or "")

        uniq_key = f"{victim_name}|{domain}|{raw_attackdate}|{group}|{country}"
        if uniq_key in seen_keys:
            continue
        seen_keys.add(uniq_key)

        # Incident date (attackdate -> date part)
        incident_date, date_prec = (None, "unknown")
        if raw_attackdate:
            attack_date_part = raw_attackdate.split(" ", 1)[0]
            d, p = parse_date_with_precision(attack_date_part)
            incident_date, date_prec = (d or None, p)

        # Source published date (discovered -> date part, fallback incident_date)
        source_published_date = None
        if raw_discovered:
            disc_date_part = raw_discovered.split(" ", 1)[0]
            d2, _ = parse_date_with_precision(disc_date_part)
            source_published_date = d2 or None
        if not source_published_date:
            source_published_date = incident_date

        # URLs
        press_urls = _extract_press_article_urls(row.get("press"))
        # Phase 1: primary_url=None, all URLs in all_urls (Phase 2 will select best URL)
        all_urls = press_urls  # ONLY real article URLs for enrichment

        # Infra URLs kept separately for CTI
        detail_url = _safe_str(row.get("url") or "")
        claim_url = _safe_str(row.get("claim_url") or "")
        screenshot_url = _safe_str(row.get("screenshot") or "")

        # Source-native event id = slug from ransomware.live detail URL, if present
        source_event_id = ""
        if detail_url:
            source_event_id = detail_url.rstrip("/").rsplit("/", 1)[-1]
        elif claim_url:
            source_event_id = claim_url.rstrip("/").rsplit("/", 1)[-1]

        # Notes: group + infostealer brief summary
        note_parts = []
        if group:
            note_parts.append(f"group={group}")

        infostealer = row.get("infostealer")
        if isinstance(infostealer, dict) and infostealer:
            brief_elems = []
            for key in ("employees", "users", "thirdparties"):
                if key in infostealer:
                    brief_elems.append(f"{key}={infostealer[key]}")
            if brief_elems:
                note_parts.append("infostealer(" + ", ".join(brief_elems) + ")")

        notes = "; ".join(note_parts) if note_parts else None

        institution_type = _guess_institution_type(victim_name, description)

        incident_id = make_incident_id(
            SOURCE_NAME,
            f"{victim_name}|{domain}|{incident_date or ''}|{group}|{country}",
        )

        incident = BaseIncident(
            incident_id=incident_id,
            source=SOURCE_NAME,
            source_event_id=source_event_id or None,

            # naming
            university_name=victim_name,
            victim_raw_name=victim_name,

            # location
            institution_type=institution_type,
            country=country or None,
            region=None,
            city=None,

            # dates
            incident_date=incident_date,
            date_precision=date_prec,
            source_published_date=source_published_date,
            ingested_at=ingested_at,

            # text
            title=victim_name,
            subtitle=description[:200] or None,

            # enrichment URLs
            # Phase 1: primary_url=None, all URLs in all_urls (Phase 2 will select best URL)
            primary_url=None,
            all_urls=all_urls,

            # CTI URLs
            leak_site_url=claim_url or None,
            source_detail_url=detail_url or None,
            screenshot_url=screenshot_url or None,

            # classification
            attack_type_hint="ransomware",
            status="suspected",
            source_confidence="medium",

            # misc
            notes=notes,
        )
        incidents.append(incident)
    
    # Save all incidents if callback provided (API source, save after processing all records)
    if save_callback is not None and incidents:
        try:
            save_callback(incidents)
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"RansomwareLive: Saved {len(incidents)} incidents")
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"RansomwareLive: Error saving incidents: {e}", exc_info=True)
            # Continue even if save fails

    return incidents
