"""
Cross-source deduplication module for EduThreat-CTI.

This module handles deduplication of incidents across different sources
based on URL matching. When the same incident appears in multiple sources
(e.g., the same article URL from different news sources), we keep the
incident with the highest source confidence and merge metadata.
"""

import logging
from typing import Dict, List, Set, Tuple
from urllib.parse import urlparse, urlunparse

from src.edu_cti.core.models import BaseIncident

logger = logging.getLogger(__name__)

# Source confidence ranking (higher = better)
SOURCE_CONFIDENCE_RANK = {
    "high": 3,
    "medium": 2,
    "low": 1,
}


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
    
    # Optionally remove query and fragment for stricter matching
    # For now, we keep query params to avoid false positives
    query = parsed.query
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
            normalized = normalize_url(url)
            if normalized:
                urls.add(normalized)
    
    # Collect from primary_url (should be None in Phase 1, but check anyway)
    if incident.primary_url:
        normalized = normalize_url(incident.primary_url)
        if normalized:
            urls.add(normalized)
    
    # Collect from source_detail_url
    if incident.source_detail_url:
        normalized = normalize_url(incident.source_detail_url)
        if normalized:
            urls.add(normalized)
    
    return urls


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
        key=lambda inc: SOURCE_CONFIDENCE_RANK.get(inc.source_confidence, 0),
        reverse=True
    )
    
    primary = sorted_incidents[0]
    
    # Collect all URLs from all incidents
    all_urls_set: Set[str] = set()
    sources_seen: Set[str] = {primary.source}
    
    for inc in sorted_incidents:
        urls = extract_urls_from_incident(inc)
        all_urls_set.update(urls)
        sources_seen.add(inc.source)
    
    # Merge metadata: prefer non-empty values from higher confidence sources
    merged_incident = BaseIncident(
        incident_id=primary.incident_id,  # Keep primary's ID
        source=primary.source,  # Keep primary source
        source_event_id=primary.source_event_id,
        
        # Victim naming: prefer non-empty
        university_name=primary.university_name or next(
            (inc.university_name for inc in sorted_incidents if inc.university_name),
            ""
        ),
        victim_raw_name=primary.victim_raw_name or next(
            (inc.victim_raw_name for inc in sorted_incidents if inc.victim_raw_name),
            None
        ),
        
        # Location: prefer non-empty
        institution_type=primary.institution_type or next(
            (inc.institution_type for inc in sorted_incidents if inc.institution_type),
            None
        ),
        country=primary.country or next(
            (inc.country for inc in sorted_incidents if inc.country),
            None
        ),
        region=primary.region or next(
            (inc.region for inc in sorted_incidents if inc.region),
            None
        ),
        city=primary.city or next(
            (inc.city for inc in sorted_incidents if inc.city),
            None
        ),
        
        # Dates: prefer most precise
        incident_date=primary.incident_date or next(
            (inc.incident_date for inc in sorted_incidents if inc.incident_date),
            None
        ),
        date_precision=primary.date_precision if primary.incident_date else next(
            (inc.date_precision for inc in sorted_incidents if inc.incident_date),
            "unknown"
        ),
        source_published_date=primary.source_published_date or next(
            (inc.source_published_date for inc in sorted_incidents if inc.source_published_date),
            None
        ),
        ingested_at=primary.ingested_at,  # Keep primary's ingestion time
        
        # Text: prefer non-empty
        title=primary.title or next(
            (inc.title for inc in sorted_incidents if inc.title),
            None
        ),
        subtitle=primary.subtitle or next(
            (inc.subtitle for inc in sorted_incidents if inc.subtitle),
            None
        ),
        
        # URLs: merge all
        primary_url=None,  # Phase 1: always None
        all_urls=list(all_urls_set),
        
        # CTI URLs: prefer non-empty
        leak_site_url=primary.leak_site_url or next(
            (inc.leak_site_url for inc in sorted_incidents if inc.leak_site_url),
            None
        ),
        source_detail_url=primary.source_detail_url or next(
            (inc.source_detail_url for inc in sorted_incidents if inc.source_detail_url),
            None
        ),
        screenshot_url=primary.screenshot_url or next(
            (inc.screenshot_url for inc in sorted_incidents if inc.screenshot_url),
            None
        ),
        
        # Classification: keep primary's
        attack_type_hint=primary.attack_type_hint or next(
            (inc.attack_type_hint for inc in sorted_incidents if inc.attack_type_hint),
            None
        ),
        status=primary.status,  # Keep primary's status
        source_confidence=primary.source_confidence,  # Keep primary's confidence
        
        # Notes: combine sources
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

