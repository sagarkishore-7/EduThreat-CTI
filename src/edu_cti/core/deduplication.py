"""
Cross-source deduplication module for EduThreat-CTI.

This module handles deduplication of incidents across different sources
based on URL matching. When the same incident appears in multiple sources
(e.g., the same article URL from different news sources), we keep the
incident with the highest source confidence and merge metadata.
"""

import logging
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

from src.edu_cti.core.models import BaseIncident

logger = logging.getLogger(__name__)

# Source confidence ranking (higher = better)
SOURCE_CONFIDENCE_RANK = {
    "high": 3,
    "medium": 2,
    "low": 1,
}

# Date precision ranking (higher = more precise)
_DATE_PRECISION_RANK = {
    "day": 5,
    "week": 4,
    "month": 3,
    "year": 2,
    "approximate": 1,
    "unknown": 0,
}

# Source type classification — determines field-level merge authority
_SOURCE_TYPE: Dict[str, str] = {
    # API sources: structured data direct from group/vendor infrastructure
    "ransomwarelive": "api",
    "ransomlook": "api",
    # Curated sources: HTML-scraped sites with dedicated education sections
    "konbriefing": "curated",
    "databreach": "curated",
    "comparitech": "curated",
    # News sources: keyword-searched security news sites
    "krebsonsecurity": "news",
    "thehackernews": "news",
    "therecord": "news",
    "securityweek": "news",
    "darkreading": "news",
    # RSS sources: feed-based aggregators
    "databreaches_rss": "rss",
    "bleepingcomputer": "rss",
    "cisa_rss": "rss",
    "international_rss": "rss",
    "googlenews_rss": "rss",
    "oxylabs_news": "rss",
}

# Fields where API sources are ground truth — data comes directly from the threat
# actor's own infrastructure (ransomware.live claim pages, leak sites, etc.)
_API_PREFERRED_FIELDS: Set[str] = {
    "attack_type_hint",  # API = definitively ransomware (we got this from the group itself)
    "threat_actor",      # API = group name published by the group, not LLM-guessed
    "leak_site_url",     # API = .onion URL direct from group post
    "screenshot_url",    # API = screenshot of the group's own claim page
    "source_detail_url", # API = CTI platform detail page (not a news article)
}

_SOURCE_SURVIVOR_RANK: Dict[str, int] = {
    "ransomwarelive": 50,
    "ransomlook": 40,
    "comparitech": 35,
    "konbriefing": 34,
    "databreach": 33,
    "oxylabs_news": 30,
    "googlenews_rss": 20,
}


def is_google_news_wrapper_url(url: str) -> bool:
    """Return True for Google News wrapper URLs that should not be treated as source articles."""
    if not url:
        return False
    try:
        parsed = urlparse(url.strip())
    except Exception:
        return False
    host = parsed.netloc.lower().lstrip("www.")
    path = parsed.path or ""
    return host == "news.google.com" and path.startswith("/rss/articles/")


def _pick_better_date(
    date1: Optional[str],
    prec1: str,
    date2: Optional[str],
    prec2: str,
) -> Tuple[Optional[str], str]:
    """Return whichever (date, precision) pair is more precise.

    If both are equally precise, prefer the non-null one. Falls back to
    (date1, prec1) when tied or date2 is absent.
    """
    rank1 = _DATE_PRECISION_RANK.get(prec1 or "unknown", 0)
    rank2 = _DATE_PRECISION_RANK.get(prec2 or "unknown", 0)
    if date2 and rank2 > rank1:
        return date2, prec2
    return date1, prec1


def normalize_url(url: str) -> str:
    """
    Normalize URL for comparison by removing:
    - Trailing slashes
    - Query parameters (optional, can be enabled)
    - Fragments
    - www. prefix
    
    Args:
        url: URL string to normalize
        
    Returns:
        Normalized URL string
    """
    if not url or not url.strip():
        return ""
    
    url = url.strip()
    
    # Parse URL
    try:
        parsed = urlparse(url)
    except Exception:
        # If parsing fails, return original
        return url
    
    # Normalize scheme and netloc (lowercase, remove www.)
    scheme = parsed.scheme.lower() if parsed.scheme else ""
    netloc = parsed.netloc.lower() if parsed.netloc else ""
    if netloc.startswith("www."):
        netloc = netloc[4:]
    
    # Keep path and normalize (remove trailing slash)
    path = parsed.path.rstrip("/") if parsed.path else ""

    # Strip tracking/analytics query params that don't change article identity
    TRACKING_PARAMS = {
        "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
        "ref", "source", "via", "fbclid", "gclid", "mc_cid", "mc_eid",
        "_ga", "_gl", "ncid", "ocid", "sr_share", "social",
    }
    if parsed.query:
        params = parse_qs(parsed.query, keep_blank_values=False)
        filtered = {k: v for k, v in params.items() if k.lower() not in TRACKING_PARAMS}
        query = urlencode(filtered, doseq=True) if filtered else ""
    else:
        query = ""
    fragment = ""  # Always remove fragments

    # Reconstruct normalized URL
    normalized = urlunparse((scheme, netloc, path, "", query, fragment))
    
    return normalized


def extract_urls_from_incident(incident: BaseIncident) -> Set[str]:
    """
    Extract all URLs from an incident (all_urls + other URL fields).
    
    Args:
        incident: BaseIncident object
        
    Returns:
        Set of normalized URLs
    """
    urls: Set[str] = set()
    
    # Collect from all_urls
    if incident.all_urls:
        for url in incident.all_urls:
            if is_google_news_wrapper_url(url):
                continue
            normalized = normalize_url(url)
            if normalized:
                urls.add(normalized)
    
    # Collect from primary_url (should be None in Phase 1, but check anyway)
    if incident.primary_url:
        if is_google_news_wrapper_url(incident.primary_url):
            normalized = ""
        else:
            normalized = normalize_url(incident.primary_url)
        if normalized:
            urls.add(normalized)
    
    # Collect from source_detail_url
    if incident.source_detail_url:
        normalized = normalize_url(incident.source_detail_url)
        if normalized:
            urls.add(normalized)
    
    return urls


def _merge_dates(sorted_incidents: List[BaseIncident]) -> dict:
    """Return the best incident_date, date_precision, source_published_date, and
    discovery_date across all incidents in confidence-sorted order."""
    best_date: Optional[str] = None
    best_prec: str = "unknown"
    best_pubdate: Optional[str] = None
    best_discovery: Optional[str] = None

    for inc in sorted_incidents:
        best_date, best_prec = _pick_better_date(
            best_date, best_prec,
            inc.incident_date, inc.date_precision or "unknown",
        )
        if not best_pubdate and inc.source_published_date:
            best_pubdate = inc.source_published_date
        if not best_discovery and getattr(inc, "discovery_date", None):
            best_discovery = inc.discovery_date

    return {
        "incident_date": best_date,
        "date_precision": best_prec,
        "source_published_date": best_pubdate,
        "discovery_date": best_discovery,
    }


def _pick_field(field: str, sorted_incidents: List[BaseIncident]) -> Any:
    """Pick the best non-null value for a field using source-type-aware priority.

    API sources win for structured CTI fields (data from group infrastructure).
    Curated/news sources win for descriptive fields (institution details, text).
    sorted_incidents must be pre-sorted by source confidence (highest first).
    """
    if field in _API_PREFERRED_FIELDS:
        type_order = ["api", "curated", "news", "rss"]
    else:
        type_order = ["curated", "news", "rss", "api"]

    by_type: Dict[str, List[BaseIncident]] = {t: [] for t in type_order}
    for inc in sorted_incidents:
        stype = _SOURCE_TYPE.get(inc.source, "news")
        if stype in by_type:
            by_type[stype].append(inc)

    for stype in type_order:
        for inc in by_type[stype]:
            val = getattr(inc, field, None)
            if val:
                return val
    return None


def merge_incidents(incidents: List[BaseIncident]) -> BaseIncident:
    """
    Merge multiple incidents into one, keeping the best information.
    
    Strategy:
    1. Use incident with highest source_confidence
    2. Merge all URLs from all incidents
    3. Keep most complete metadata (non-empty fields preferred)
    4. Combine sources in notes
    
    Args:
        incidents: List of incidents to merge
        
    Returns:
        Merged BaseIncident
    """
    if not incidents:
        raise ValueError("Cannot merge empty list of incidents")
    
    if len(incidents) == 1:
        return incidents[0]
    
    # Sort by confidence (highest first)
    sorted_incidents = sorted(
        incidents,
        key=lambda inc: (
            SOURCE_CONFIDENCE_RANK.get(inc.source_confidence, 0),
            _SOURCE_SURVIVOR_RANK.get(inc.source, 0),
        ),
        reverse=True,
    )
    
    primary = sorted_incidents[0]
    
    # Collect all URLs from all incidents
    all_urls_set: Set[str] = set()
    sources_seen: Set[str] = {primary.source}
    
    for inc in sorted_incidents:
        urls = extract_urls_from_incident(inc)
        all_urls_set.update(urls)
        sources_seen.add(inc.source)
    
    # Merge metadata using field-level source-type priority:
    # - API sources (ransomware.live) win for structured CTI fields (threat actor,
    #   attack type, claim URLs, screenshots) — ground truth from group infrastructure
    # - Curated/news sources win for descriptive fields (institution name, location,
    #   institution type, text) — LLM-extracted or editorial, better for these
    # - Dates use precision-aware selection regardless of source type
    merged_incident = BaseIncident(
        incident_id=primary.incident_id,
        source=primary.source,
        source_event_id=primary.source_event_id,

        # Victim naming: curated/news preferred (full institution name)
        institution_name=_pick_field("institution_name", sorted_incidents) or "",
        victim_raw_name=_pick_field("victim_raw_name", sorted_incidents),

        # Location: curated/news preferred (more granular for region/city)
        institution_type=_pick_field("institution_type", sorted_incidents),
        country=_pick_field("country", sorted_incidents),
        region=_pick_field("region", sorted_incidents),
        city=_pick_field("city", sorted_incidents),

        # Dates: precision-aware — most precise date wins regardless of source type
        # e.g. ransomware.live "day" precision beats konbriefing "approximate"
        **_merge_dates(sorted_incidents),
        ingested_at=primary.ingested_at,

        # Text: curated/news preferred (richer article context)
        title=_pick_field("title", sorted_incidents),
        subtitle=_pick_field("subtitle", sorted_incidents),

        # URLs: always merge all sources
        primary_url=None,  # Phase 1: always None
        all_urls=list(all_urls_set),

        # CTI URLs: API preferred — .onion, screenshots come from group infrastructure
        leak_site_url=_pick_field("leak_site_url", sorted_incidents),
        source_detail_url=_pick_field("source_detail_url", sorted_incidents),
        screenshot_url=_pick_field("screenshot_url", sorted_incidents),

        # Classification: API preferred — attack type is authoritative from the group itself
        attack_type_hint=_pick_field("attack_type_hint", sorted_incidents),
        # Threat actor: API preferred — group name from group's own post, not LLM guess
        threat_actor=_pick_field("threat_actor", sorted_incidents),

        status=primary.status,
        source_confidence=primary.source_confidence,

        # Notes: combine all source notes
        notes=f"merged_from={','.join(sorted(sources_seen))};{primary.notes or ''}".strip(";"),
    )
    
    return merged_incident


def deduplicate_by_urls(incidents: List[BaseIncident]) -> Tuple[List[BaseIncident], Dict[str, int]]:
    """
    Deduplicate incidents across sources based on URL matching.
    
    Strategy:
    1. Group incidents by normalized URLs
    2. For each group with multiple incidents, merge them
    3. Return deduplicated list
    
    Args:
        incidents: List of incidents to deduplicate
        
    Returns:
        Tuple of (deduplicated_incidents, stats_dict)
        stats_dict contains:
        - total_input: Total incidents before deduplication
        - total_output: Total incidents after deduplication
        - duplicates_merged: Number of duplicate groups merged
        - incidents_removed: Number of incidents removed (duplicates)
    """
    if not incidents:
        return [], {
            "total_input": 0,
            "total_output": 0,
            "duplicates_merged": 0,
            "incidents_removed": 0,
        }
    
    # Build URL -> incidents mapping
    url_to_incidents: Dict[str, List[BaseIncident]] = {}
    # Use incident_id as key since BaseIncident is not hashable
    incident_id_to_incident: Dict[str, BaseIncident] = {inc.incident_id: inc for inc in incidents}
    incident_id_to_urls: Dict[str, Set[str]] = {}
    
    for incident in incidents:
        urls = extract_urls_from_incident(incident)
        incident_id_to_urls[incident.incident_id] = urls
        
        for url in urls:
            if url not in url_to_incidents:
                url_to_incidents[url] = []
            url_to_incidents[url].append(incident)
    
    # Find incidents that share URLs (potential duplicates)
    # Use union-find approach to group incidents that share URLs
    incident_id_to_group: Dict[str, int] = {}
    group_id = 0
    
    # Assign each incident to a group based on shared URLs
    for incident in incidents:
        incident_id = incident.incident_id
        if incident_id in incident_id_to_group:
            continue  # Already assigned to a group
        
        # Find all incidents that share at least one URL with this one
        shared_incident_ids = set()
        for url in incident_id_to_urls[incident_id]:
            for shared_inc in url_to_incidents[url]:
                shared_incident_ids.add(shared_inc.incident_id)
        
        if len(shared_incident_ids) > 1:
            # Assign all shared incidents to the same group
            for shared_id in shared_incident_ids:
                if shared_id not in incident_id_to_group:
                    incident_id_to_group[shared_id] = group_id
            group_id += 1
    
    # Build groups dictionary
    incident_groups: Dict[int, List[BaseIncident]] = {}
    for incident_id, gid in incident_id_to_group.items():
        if gid not in incident_groups:
            incident_groups[gid] = []
        incident_groups[gid].append(incident_id_to_incident[incident_id])
    
    # Merge groups and collect standalone incidents
    merged_incidents: List[BaseIncident] = []
    processed_incident_ids: set[str] = set()
    
    # Process groups (duplicates)
    for group_id, group_incidents in incident_groups.items():
        # Only process if not already processed
        group_ids = {inc.incident_id for inc in group_incidents}
        if any(inc_id in processed_incident_ids for inc_id in group_ids):
            continue
        
        merged = merge_incidents(group_incidents)
        merged_incidents.append(merged)
        processed_incident_ids.update(group_ids)
        
        logger.debug(
            f"Merged {len(group_incidents)} incidents from sources: "
            f"{[inc.source for inc in group_incidents]}"
        )
    
    # Add standalone incidents (no duplicates)
    for incident in incidents:
        if incident.incident_id not in processed_incident_ids:
            merged_incidents.append(incident)
    
    # Calculate stats
    duplicates_merged = len(incident_groups)
    incidents_removed = len(incidents) - len(merged_incidents)
    
    stats = {
        "total_input": len(incidents),
        "total_output": len(merged_incidents),
        "duplicates_merged": duplicates_merged,
        "incidents_removed": incidents_removed,
    }
    
    logger.info(
        f"Deduplication complete: {stats['total_input']} -> {stats['total_output']} "
        f"({stats['duplicates_merged']} groups merged, {stats['incidents_removed']} removed)"
    )
    
    return merged_incidents, stats
