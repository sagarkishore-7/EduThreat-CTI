"""
Ransomware.live API source for EduThreat-CTI.

Fetches education-sector victims from the ransomware.live v2 API.
API endpoint: https://api.ransomware.live/v2/sectorvictims/Education

Key fields from API:
  victim       – victim name
  domain       – victim domain
  group        – ransomware group
  attackdate   – estimated attack date (ISO datetime, when group claims attack occurred)
  discovered   – when the claim was posted on the leak site (disclosure/discovery date)
  claim_url    – .onion link to the ransomware group's claim post
  screenshot   – screenshot of the claim page
  press        – list of press article URLs (used for enrichment)
  infostealer  – dict with {employees, users, thirdparties, infostealer_stats} counts
  description  – brief victim description from the group
  country      – ISO-2 country code
  data_size    – claimed data exfiltrated (string)
  ransom       – ransom demand info
"""

import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional

from src.edu_cti.core.http import HttpClient, build_http_client
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.utils import now_utc_iso, parse_date_with_precision

logger = logging.getLogger(__name__)

BASE_URL = "https://api.ransomware.live/v2"
MAX_RETRIES = 3
BACKOFF_SECONDS = 2.0

SOURCE_NAME = "ransomwarelive"


def _safe_str(x: Any) -> str:
    return str(x) if x is not None else ""


def _get_json(path: str, client: Optional[HttpClient] = None) -> Any:
    http_client = client or build_http_client()
    url = BASE_URL + path
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = http_client.get(url, allow_status=[429], to_soup=False)
            if resp is None:
                raise Exception(f"Failed to fetch {url}")
            if resp.status_code == 429 and attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS * attempt)
                continue
            if resp.status_code >= 400:
                raise Exception(f"HTTP {resp.status_code} for {url}")
            return json.loads(resp.text)
        except Exception as exc:
            if attempt >= MAX_RETRIES:
                raise
            logger.debug(f"ransomware.live fetch attempt {attempt} failed: {exc}")
            time.sleep(BACKOFF_SECONDS * attempt)


def _guess_institution_type(name: str, description: str = "") -> Optional[str]:
    base = f"{name} {description}".lower()
    if any(k in base for k in ["school district", "county schools", "high school"]):
        return "School"
    if any(k in base for k in ["school", "schule", "école", "escuela", "colegio", "scuola", "skola"]):
        return "School"
    if any(k in base for k in ["university", "universität", "universidade", "universidad", "université", "università"]):
        return "University"
    if any(k in base for k in ["institute", "instituto", "institut", "research", "academy", "akademie", "akademia"]):
        return "Research Institute"
    return "Unknown"


def _extract_press_article_urls(press_field: Any) -> List[str]:
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

    seen, uniq = set(), []
    for u in candidates:
        u = u.strip()
        if not u.startswith(("http://", "https://")):
            continue
        if "ransomware.live" in u:
            continue
        if u.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
            continue
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def _build_infostealer_note(infostealer: Any) -> Optional[str]:
    """Summarise the infostealer dict into a compact note fragment."""
    if not isinstance(infostealer, dict) or not infostealer:
        return None
    parts = []
    for key in ("employees", "users", "thirdparties"):
        if key in infostealer:
            parts.append(f"{key}={infostealer[key]}")
    # Top-3 stealer families by count
    stats = infostealer.get("infostealer_stats")
    if isinstance(stats, dict) and stats:
        top = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:3]
        families = ",".join(f"{k}({v})" for k, v in top)
        parts.append(f"stealers={families}")
    return "infostealer(" + "; ".join(parts) + ")" if parts else None


def _dedup_raw_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    from collections import defaultdict
    groups: dict = defaultdict(list)
    for row in records:
        domain = _safe_str(row.get("domain") or "").lower().strip()
        domain_root = domain.split(".")[0] if domain else ""
        date_key = _safe_str(row.get("attackdate") or "")[:10]
        if domain_root and date_key:
            groups[(domain_root, date_key)].append(row)
        else:
            groups[(id(row), date_key)].append(row)

    merged: List[Dict[str, Any]] = []
    for key, group in groups.items():
        if len(group) == 1:
            merged.append(group[0])
            continue
        primary = next(
            (r for r in group if "." not in _safe_str(r.get("victim") or r.get("name") or "")),
            group[0],
        )
        all_groups = sorted({_safe_str(r.get("group") or "") for r in group} - {""})
        primary = dict(primary)
        primary["all_groups"] = all_groups
        logger.info(
            "RansomwareLive pre-dedup: merged %d entries for domain '%s' on %s — groups: %s",
            len(group), key[0], key[1], all_groups,
        )
        merged.append(primary)
    return merged


def fetch_sector_education_victims(client: Optional[HttpClient] = None) -> List[Dict[str, Any]]:
    data = _get_json("/sectorvictims/Education", client=client)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("victims") or data.get("data") or []
    return []


def build_ransomwarelive_incidents(
    client: Optional[HttpClient] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
    incremental: bool = True,
) -> List[BaseIncident]:
    """
    Map ransomware.live Education victims to BaseIncident.

    Key data preservation:
    - attackdate  → incident_date  (when the group claims the attack occurred)
    - discovered  → discovery_date (when the claim was posted on the leak site)
    - claim_url   → leak_site_url  (.onion CTI reference)
    - screenshot  → screenshot_url
    - press       → all_urls       (real article URLs for Phase 2 enrichment)
    - infostealer → notes          (credentials/records stats as structured hint)
    - data_size   → notes          (claimed exfil size)
    """
    ingested_at = now_utc_iso()
    incidents: List[BaseIncident] = []

    records = fetch_sector_education_victims(client=client)
    records = _dedup_raw_records(records)
    seen_keys: set = set()

    for row in records:
        activity = _safe_str(row.get("activity"))
        if activity and activity.lower() != "education":
            continue

        victim_name = _safe_str(row.get("victim") or row.get("name") or row.get("company"))
        if not victim_name:
            continue

        description = _safe_str(row.get("description") or "")
        group = _safe_str(row.get("group") or "")
        raw_attackdate = _safe_str(row.get("attackdate") or "")
        raw_discovered = _safe_str(row.get("discovered") or "")
        country = _safe_str(row.get("country") or row.get("countrycode") or "")
        domain = _safe_str(row.get("domain") or "")

        uniq_key = f"{victim_name}|{domain}|{raw_attackdate}|{group}|{country}"
        if uniq_key in seen_keys:
            continue
        seen_keys.add(uniq_key)

        # incident_date = attackdate (when attack allegedly occurred — "day" precision)
        incident_date, date_prec = None, "unknown"
        if raw_attackdate:
            d, p = parse_date_with_precision(raw_attackdate.split("T", 1)[0].split(" ", 1)[0])
            incident_date, date_prec = d or None, p

        # discovery_date = discovered (when claim was published on leak site)
        discovery_date: Optional[str] = None
        if raw_discovered:
            d2, _ = parse_date_with_precision(raw_discovered.split("T", 1)[0].split(" ", 1)[0])
            discovery_date = d2 or None

        # source_published_date = discovery_date (closest to "publication" semantics)
        source_published_date = discovery_date or incident_date

        # Press article URLs for Phase 2 enrichment
        all_urls = _extract_press_article_urls(row.get("press"))

        # CTI infrastructure URLs
        detail_url = _safe_str(row.get("url") or "")
        claim_url = _safe_str(row.get("claim_url") or "")
        screenshot_url = _safe_str(row.get("screenshot") or "")

        source_event_id = ""
        if detail_url:
            source_event_id = detail_url.rstrip("/").rsplit("/", 1)[-1]
        elif claim_url:
            source_event_id = claim_url.rstrip("/").rsplit("/", 1)[-1]

        # Build structured notes: group, infostealer, data_size
        note_parts = []
        all_groups = row.get("all_groups")
        if all_groups:
            note_parts.append(f"groups={','.join(all_groups)};multi_claimed=true")
        elif group:
            note_parts.append(f"group={group}")

        is_note = _build_infostealer_note(row.get("infostealer"))
        if is_note:
            note_parts.append(is_note)

        data_size = _safe_str(row.get("data_size") or "").strip()
        if data_size and data_size.lower() not in {"null", "none", "n/a", ""}:
            note_parts.append(f"data_size={data_size}")

        ransom = row.get("ransom")
        if ransom and isinstance(ransom, (str, int, float)):
            note_parts.append(f"ransom={ransom}")

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
            institution_name=victim_name,
            victim_raw_name=victim_name,
            institution_type=institution_type,
            country=country or None,
            region=None,
            city=None,
            incident_date=incident_date,
            date_precision=date_prec,
            source_published_date=source_published_date,
            discovery_date=discovery_date,
            ingested_at=ingested_at,
            title=victim_name,
            subtitle=description[:200] or None,
            primary_url=None,
            all_urls=all_urls,
            leak_site_url=claim_url or None,
            source_detail_url=detail_url or None,
            screenshot_url=screenshot_url or None,
            attack_type_hint="ransomware",
            status="suspected",
            source_confidence="medium",
            notes=notes,
        )
        incidents.append(incident)

    if save_callback is not None and incidents:
        try:
            save_callback(incidents)
            logger.debug("RansomwareLive: saved %d incidents", len(incidents))
        except Exception as exc:
            logger.error("RansomwareLive: error saving incidents: %s", exc, exc_info=True)

    return incidents
