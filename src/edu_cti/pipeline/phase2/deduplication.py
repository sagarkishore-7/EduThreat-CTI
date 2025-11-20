"""
Post-enrichment deduplication for EduThreat-CTI.

Handles deduplication after enrichment to prevent duplicate narratives
of the same incident from different sources.
"""

import logging
import sqlite3
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
try:
    from dateutil import parser as date_parser
except ImportError:
    # Fallback if dateutil not available
    date_parser = None

from src.edu_cti.pipeline.phase2.schemas import CTIEnrichmentResult
from src.edu_cti.pipeline.phase2.db import get_enrichment_result

logger = logging.getLogger(__name__)


def normalize_institution_name(name: str) -> str:
    """
    Normalize institution name for comparison.
    
    Normalizations:
    - Lowercase
    - Remove common prefixes/suffixes (University, College, etc.)
    - Remove punctuation
    - Remove extra whitespace
    - Remove common words (the, of, etc.)
    
    Args:
        name: Institution name to normalize
        
    Returns:
        Normalized name string
    """
    if not name:
        return ""
    
    import re
    
    # Lowercase
    normalized = name.lower().strip()
    
    # Remove common prefixes
    prefixes = [
        r"^university\s+of\s+",
        r"^the\s+university\s+of\s+",
        r"^the\s+",
        r"^university\s+",
        r"^college\s+of\s+",
        r"^college\s+",
        r"^school\s+of\s+",
        r"^school\s+",
    ]
    for prefix in prefixes:
        normalized = re.sub(prefix, "", normalized)
    
    # Remove common suffixes
    suffixes = [
        r"\s+university$",
        r"\s+college$",
        r"\s+school$",
        r"\s+institute$",
        r"\s+university\s+system$",
    ]
    for suffix in suffixes:
        normalized = re.sub(suffix, "", normalized)
    
    # Remove punctuation except spaces and hyphens
    normalized = re.sub(r"[^\w\s-]", "", normalized)
    
    # Remove extra whitespace
    normalized = re.sub(r"\s+", " ", normalized).strip()
    
    return normalized


def parse_incident_date(date_str: Optional[str]) -> Optional[datetime]:
    """
    Parse incident date string to datetime object.
    
    Args:
        date_str: Date string in various formats (YYYY-MM-DD, etc.)
        
    Returns:
        datetime object or None if parsing fails
    """
    if not date_str:
        return None
    
    try:
        # Try parsing with dateutil if available (handles various formats)
        if date_parser:
            return date_parser.parse(date_str)
    except (ValueError, TypeError):
        pass
    
    try:
        # Fallback to simple YYYY-MM-DD format
        return datetime.strptime(date_str, "%Y-%m-%d")
    except (ValueError, TypeError):
        logger.warning(f"Could not parse date: {date_str}")
        return None


def dates_within_window(
    date1: Optional[datetime],
    date2: Optional[datetime],
    days: int = 14,
) -> bool:
    """
    Check if two dates are within specified window.
    
    Args:
        date1: First date
        date2: Second date
        days: Window size in days (default: 14 for 2 weeks)
        
    Returns:
        True if dates are within window, False otherwise
    """
    if not date1 or not date2:
        return False
    
    delta = abs((date1 - date2).days)
    return delta <= days


def find_duplicate_institutions(
    conn: sqlite3.Connection,
    incident_id: str,
    institution_name: Optional[str],
    incident_date: Optional[str],
    window_days: int = 14,
) -> List[Dict]:
    """
    Find incidents with same normalized institution name within date window.
    
    Args:
        conn: Database connection
        incident_id: Current incident ID (exclude from results)
        institution_name: Institution name to search for
        incident_date: Incident date for window calculation
        window_days: Date window in days (default: 14 for 2 weeks)
        
    Returns:
        List of duplicate incident dicts with enrichment info
    """
    if not institution_name:
        return []
    
    normalized_name = normalize_institution_name(institution_name)
    if not normalized_name:
        return []
    
    # Parse incident date
    incident_dt = parse_incident_date(incident_date)
    
    # Get all enriched incidents
    cur = conn.execute(
        """
        SELECT incident_id, university_name, victim_raw_name, incident_date
        FROM incidents
        WHERE incident_id != ?
          AND llm_enriched = 1
          AND (university_name IS NOT NULL OR victim_raw_name IS NOT NULL)
        """,
        (incident_id,)
    )
    
    duplicates = []
    for row in cur.fetchall():
        # Check if institution name matches
        uni_name = row["university_name"] or row["victim_raw_name"] or ""
        normalized_uni_name = normalize_institution_name(uni_name)
        
        if normalized_uni_name == normalized_name:
            # Check date window
            row_date = parse_incident_date(row["incident_date"])
            
            if dates_within_window(incident_dt, row_date, window_days):
                # Get enrichment confidence from enrichment data
                enrichment = get_enrichment_result(conn, row["incident_id"])
                confidence = (
                    enrichment.extraction_confidence if enrichment else 0.0
                )
                
                duplicates.append({
                    "incident_id": row["incident_id"],
                    "university_name": uni_name,
                    "incident_date": row["incident_date"],
                    "confidence": confidence,
                })
    
    return duplicates


def deduplicate_by_institution(
    conn: sqlite3.Connection,
    window_days: int = 14,
) -> Dict[str, int]:
    """
    Deduplicate enriched incidents by normalized institution name within date window.
    
    This prevents duplicate narratives of the same incident from different sources.
    Keeps the incident with highest enrichment confidence.
    
    Args:
        conn: Database connection
        window_days: Date window in days (default: 14 for 2 weeks)
        
    Returns:
        Dictionary with deduplication statistics
    """
    logger.info("Starting post-enrichment deduplication by institution name...")
    
    # Get all enriched incidents
    cur = conn.execute(
        """
        SELECT incident_id, university_name, victim_raw_name, incident_date
        FROM incidents
        WHERE llm_enriched = 1
          AND (university_name IS NOT NULL OR victim_raw_name IS NOT NULL)
        ORDER BY ingested_at DESC
        """
    )
    
    all_incidents = cur.fetchall()
    processed_ids = set()
    removed_count = 0
    checked_count = 0
    
    for row in all_incidents:
        incident_id = row["incident_id"]
        
        if incident_id in processed_ids:
            continue
        
        institution_name = row["university_name"] or row["victim_raw_name"]
        incident_date = row["incident_date"]
        
        # Find duplicates
        duplicates = find_duplicate_institutions(
            conn,
            incident_id,
            institution_name,
            incident_date,
            window_days,
        )
        
        if not duplicates:
            processed_ids.add(incident_id)
            continue
        
        checked_count += 1
        
        # Get current incident's enrichment confidence
        current_enrichment = get_enrichment_result(conn, incident_id)
        current_confidence = (
            current_enrichment.extraction_confidence if current_enrichment else 0.0
        )
        
        # Collect all incident IDs to compare (including current)
        all_incident_ids = [incident_id] + [d["incident_id"] for d in duplicates]
        all_confidences = [current_confidence] + [d["confidence"] for d in duplicates]
        
        # Find incident with highest confidence
        best_idx = max(range(len(all_confidences)), key=lambda i: all_confidences[i])
        best_incident_id = all_incident_ids[best_idx]
        
        # Mark others for removal
        for idx, dup_id in enumerate(all_incident_ids):
            if idx != best_idx:
                # Mark as removed (we'll delete them)
                logger.info(
                    f"Removing duplicate incident {dup_id} "
                    f"(confidence: {all_confidences[idx]:.2f}) - "
                    f"keeping {best_incident_id} (confidence: {all_confidences[best_idx]:.2f})"
                )
                
                # Delete the duplicate incident
                conn.execute("DELETE FROM incidents WHERE incident_id = ?", (dup_id,))
                removed_count += 1
        
        # Mark all as processed
        processed_ids.update(all_incident_ids)
    
    conn.commit()
    
    stats = {
        "total_enriched": len(all_incidents),
        "checked": checked_count,
        "removed": removed_count,
        "remaining": len(all_incidents) - removed_count,
    }
    
    logger.info(
        f"Post-enrichment deduplication complete: "
        f"{stats['total_enriched']} total, {stats['checked']} checked, "
        f"{stats['removed']} removed, {stats['remaining']} remaining"
    )
    
    return stats

