"""
Database operations for the API.

Provides optimized queries for the REST API endpoints.
"""

import json
import sqlite3
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from pathlib import Path

from src.edu_cti.core.config import DB_PATH
from src.edu_cti.core.db import get_connection


def get_api_connection(read_only: bool = True) -> sqlite3.Connection:
    """
    Get a database connection for API use.
    
    Args:
        read_only: If True (default), opens read-only connection for better concurrency.
                  Set to False only when API needs to write (e.g., admin endpoints).
    
    Returns:
        SQLite connection optimized for API reads
    """
    # API connections are read-only by default for better concurrency
    # Read-only connections don't acquire write locks, allowing concurrent reads
    # even when background processes are writing
    conn = get_connection(DB_PATH, timeout=5.0, read_only=read_only)
    return conn


# ============================================================
# Incident Queries
# ============================================================

def get_incidents_paginated(
    conn: sqlite3.Connection,
    page: int = 1,
    per_page: int = 20,
    country: Optional[str] = None,
    attack_category: Optional[str] = None,
    ransomware_family: Optional[str] = None,
    threat_actor: Optional[str] = None,
    institution_type: Optional[str] = None,
    year: Optional[int] = None,
    enriched_only: bool = False,
    education_related_only: bool = True,  # Default to only education-related
    search: Optional[str] = None,
    sort_by: str = "incident_date",
    sort_order: str = "desc",
) -> Tuple[List[Dict], int]:
    """
    Get paginated list of incidents with optional filters.
    
    By default only shows education-related incidents (confirmed by LLM).
    
    Returns:
        Tuple of (incidents list, total count)
    """
    # Build WHERE clause
    conditions = []
    params = []
    
    # By default only show education-related incidents
    if education_related_only:
        conditions.append("ef.is_education_related = 1")
    
    if country:
        # Normalize country for filtering (handle both codes and names)
        from src.edu_cti.core.countries import normalize_country
        country_normalized = normalize_country(country) or country
        # Check both original and normalized values
        conditions.append("(i.country = ? OR ef.country = ? OR i.country = ? OR ef.country = ?)")
        params.extend([country, country, country_normalized, country_normalized])
    
    if attack_category:
        conditions.append("ef.attack_category = ?")
        params.append(attack_category)
    
    if ransomware_family:
        conditions.append("ef.ransomware_family = ?")
        params.append(ransomware_family)
    
    if threat_actor:
        conditions.append("ef.threat_actor_name = ?")
        params.append(threat_actor)
    
    if institution_type:
        conditions.append("(i.institution_type = ? OR ef.institution_type = ?)")
        params.extend([institution_type, institution_type])
    
    if year:
        conditions.append("strftime('%Y', i.incident_date) = ?")
        params.append(str(year))
    
    if enriched_only:
        conditions.append("i.llm_enriched = 1")
    
    if search:
        search_pattern = f"%{search}%"
        conditions.append("""
            (i.university_name LIKE ? 
             OR i.victim_raw_name LIKE ? 
             OR i.title LIKE ?
             OR ef.institution_name LIKE ?
             OR ef.threat_actor_name LIKE ?
             OR ef.enriched_summary LIKE ?)
        """)
        params.extend([search_pattern] * 6)
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    # Validate sort options
    valid_sort_columns = {
        "incident_date": "i.incident_date",
        "ingested_at": "i.ingested_at",
        "university_name": "i.university_name",
        "country": "i.country",
    }
    sort_column = valid_sort_columns.get(sort_by, "i.incident_date")
    sort_dir = "DESC" if sort_order.lower() == "desc" else "ASC"
    
    # Use JOIN when filtering education-related, LEFT JOIN otherwise
    join_type = "JOIN" if education_related_only else "LEFT JOIN"
    
    # Count total
    count_query = f"""
        SELECT COUNT(DISTINCT i.incident_id) as total
        FROM incidents i
        {join_type} incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE {where_clause}
    """
    cur = conn.execute(count_query, params)
    total = cur.fetchone()["total"]
    
    # Get paginated results
    offset = (page - 1) * per_page
    query = f"""
        SELECT DISTINCT
            i.incident_id,
            COALESCE(ef.institution_name, i.university_name, 'Unknown') as university_name,
            i.victim_raw_name,
            COALESCE(ef.institution_type, i.institution_type) as institution_type,
            COALESCE(ef.country, i.country) as country,
            COALESCE(ef.region, i.region) as region,
            COALESCE(ef.city, i.city) as city,
            i.incident_date,
            i.date_precision,
            i.title,
            i.attack_type_hint,
            ef.attack_category,
            ef.ransomware_family,
            ef.threat_actor_name,
            i.status,
            i.source_confidence,
            i.llm_enriched,
            i.llm_enriched_at,
            i.ingested_at
        FROM incidents i
        {join_type} incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE {where_clause}
        ORDER BY {sort_column} {sort_dir} NULLS LAST
        LIMIT ? OFFSET ?
    """
    params.extend([per_page, offset])
    
    cur = conn.execute(query, params)
    rows = cur.fetchall()
    
    # Get sources for each incident
    incidents = []
    for row in rows:
        incident = dict(row)
        
        # Get sources
        source_cur = conn.execute(
            "SELECT source FROM incident_sources WHERE incident_id = ?",
            (incident["incident_id"],)
        )
        incident["sources"] = [s["source"] for s in source_cur.fetchall()]
        
        incidents.append(incident)
    
    return incidents, total


def get_incident_by_id(
    conn: sqlite3.Connection,
    incident_id: str,
) -> Optional[Dict[str, Any]]:
    """Get full incident detail by ID."""
    # Get base incident
    cur = conn.execute(
        """
        SELECT * FROM incidents WHERE incident_id = ?
        """,
        (incident_id,)
    )
    row = cur.fetchone()
    
    if not row:
        return None
    
    incident = dict(row)
    
    # Parse all_urls
    all_urls_str = incident.get("all_urls") or ""
    incident["all_urls"] = [url.strip() for url in all_urls_str.split(";") if url.strip()]
    
    # Get enrichment data
    cur = conn.execute(
        "SELECT * FROM incident_enrichments_flat WHERE incident_id = ?",
        (incident_id,)
    )
    enrichment_row = cur.fetchone()
    
    if enrichment_row:
        enrichment = dict(enrichment_row)
        
        # Parse JSON fields
        if enrichment.get("timeline_json"):
            try:
                incident["timeline"] = json.loads(enrichment["timeline_json"])
            except:
                incident["timeline"] = None
        
        if enrichment.get("mitre_techniques_json"):
            try:
                incident["mitre_attack_techniques"] = json.loads(enrichment["mitre_techniques_json"])
            except:
                incident["mitre_attack_techniques"] = None
        
        if enrichment.get("systems_affected_codes"):
            try:
                incident["systems_affected"] = json.loads(enrichment["systems_affected_codes"])
            except:
                incident["systems_affected"] = None
        
        # Use enrichment institution_name as primary (LLM-extracted name)
        if enrichment.get("institution_name"):
            incident["university_name"] = enrichment["institution_name"]
        
        # Merge enrichment fields
        for key, value in enrichment.items():
            if key not in incident or incident[key] is None:
                incident[key] = value
    
    # Get full enrichment JSON for complete data
    cur = conn.execute(
        "SELECT enrichment_data FROM incident_enrichments WHERE incident_id = ?",
        (incident_id,)
    )
    enrichment_json_row = cur.fetchone()
    
    if enrichment_json_row:
        try:
            full_enrichment = json.loads(enrichment_json_row["enrichment_data"])
            # Add any fields not in flat table
            if "attack_dynamics" not in incident and "attack_dynamics" in full_enrichment:
                incident["attack_dynamics"] = full_enrichment.get("attack_dynamics")
            if "education_relevance" in full_enrichment:
                incident["education_relevance"] = full_enrichment.get("education_relevance")
        except:
            pass
    
    # Get sources
    source_cur = conn.execute(
        """
        SELECT source, source_event_id, first_seen_at, confidence
        FROM incident_sources WHERE incident_id = ?
        ORDER BY first_seen_at ASC
        """,
        (incident_id,)
    )
    incident["sources"] = [dict(s) for s in source_cur.fetchall()]
    
    return incident


# ============================================================
# Statistics Queries
# ============================================================

def get_dashboard_stats(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Get overall dashboard statistics - only education-related incidents."""
    stats = {}
    
    # Total incidents = only education-related (confirmed by LLM)
    cur = conn.execute("""
        SELECT COUNT(*) as count FROM incidents i
        JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE ef.is_education_related = 1
    """)
    stats["total_incidents"] = cur.fetchone()["count"]
    
    # Enriched incidents (same as total for education-related)
    stats["enriched_incidents"] = stats["total_incidents"]
    
    # Pending analysis (not yet processed by LLM)
    cur = conn.execute("SELECT COUNT(*) as count FROM incidents WHERE llm_enriched = 0")
    stats["pending_analysis"] = cur.fetchone()["count"]
    
    # Unenriched (processed but not education-related)
    cur = conn.execute("SELECT COUNT(*) as count FROM incidents WHERE llm_enriched = 1")
    total_processed = cur.fetchone()["count"]
    stats["unenriched_incidents"] = total_processed - stats["total_incidents"]
    
    # Ransomware incidents
    # Ransomware incidents - count by attack_category since ransomware_family often NULL
    cur = conn.execute(
        """
        SELECT COUNT(*) as count FROM incident_enrichments_flat 
        WHERE is_education_related = 1 
          AND attack_category LIKE '%ransomware%'
        """
    )
    stats["incidents_with_ransomware"] = cur.fetchone()["count"]
    
    # Data breach incidents
    cur = conn.execute(
        "SELECT COUNT(*) as count FROM incident_enrichments_flat WHERE data_breached = 1"
    )
    stats["incidents_with_data_breach"] = cur.fetchone()["count"]
    
    # Countries affected
    cur = conn.execute(
        "SELECT COUNT(DISTINCT country) as count FROM incidents WHERE country IS NOT NULL"
    )
    stats["countries_affected"] = cur.fetchone()["count"]
    
    # Unique threat actors
    cur = conn.execute(
        """
        SELECT COUNT(DISTINCT threat_actor_name) as count 
        FROM incident_enrichments_flat 
        WHERE threat_actor_name IS NOT NULL AND threat_actor_name != ''
        """
    )
    stats["unique_threat_actors"] = cur.fetchone()["count"]
    
    # Unique ransomware families (use threat_actor_name as fallback)
    cur = conn.execute(
        """
        SELECT COUNT(DISTINCT COALESCE(ransomware_family, threat_actor_name)) as count 
        FROM incident_enrichments_flat 
        WHERE is_education_related = 1
          AND attack_category LIKE '%ransomware%'
          AND (
            (ransomware_family IS NOT NULL AND ransomware_family != '')
            OR (threat_actor_name IS NOT NULL AND threat_actor_name != '')
          )
        """
    )
    stats["unique_ransomware_families"] = cur.fetchone()["count"]
    
    stats["last_updated"] = datetime.utcnow().isoformat()
    
    return stats


def get_incidents_by_country(
    conn: sqlite3.Connection,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """Get incident counts by country - only education-related."""
    from src.edu_cti.core.countries import normalize_country, get_country_code, get_flag_emoji
    
    cur = conn.execute(
        """
        SELECT 
            COALESCE(ef.country, i.country, 'Unknown') as category,
            COUNT(*) as count
        FROM incidents i
        JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE ef.is_education_related = 1
          AND (ef.country IS NOT NULL OR i.country IS NOT NULL)
        GROUP BY category
        ORDER BY count DESC
        LIMIT ?
        """,
        (limit,)
    )
    rows = cur.fetchall()
    
    # Calculate percentages and normalize countries
    total = sum(r["count"] for r in rows)
    result = []
    seen_countries = {}  # Track normalized names to merge duplicates
    
    for row in rows:
        country_raw = row["category"]
        if country_raw == "Unknown":
            country_name = "Unknown"
            country_code = None
        else:
            country_name = normalize_country(country_raw) or country_raw
            country_code = get_country_code(country_name)
        
        # Merge duplicates (e.g., "US" and "United States")
        if country_name in seen_countries:
            seen_countries[country_name]["count"] += row["count"]
        else:
            seen_countries[country_name] = {
                "category": country_name,
                "count": row["count"],
                "country_code": country_code,
                "flag_emoji": get_flag_emoji(country_name) if country_code else "🌍"
            }
    
    # Convert to list and recalculate percentages
    for country_name, data in seen_countries.items():
        result.append({
            "category": data["category"],
            "count": data["count"],
            "percentage": round(data["count"] / total * 100, 1) if total > 0 else 0,
            "country_code": data["country_code"],
            "flag_emoji": data["flag_emoji"]
        })
    
    # Re-sort by count after merging
    result.sort(key=lambda x: x["count"], reverse=True)
    
    return result


def get_incidents_by_attack_type(
    conn: sqlite3.Connection,
    limit: int = 15,
) -> List[Dict[str, Any]]:
    """Get incident counts by attack category - only education-related."""
    cur = conn.execute(
        """
        SELECT 
            COALESCE(ef.attack_category, i.attack_type_hint, 'unknown') as category,
            COUNT(*) as count
        FROM incidents i
        JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE ef.is_education_related = 1
        GROUP BY category
        ORDER BY count DESC
        LIMIT ?
        """,
        (limit,)
    )
    rows = cur.fetchall()
    
    total = sum(r["count"] for r in rows)
    result = []
    for row in rows:
        result.append({
            "category": row["category"],
            "count": row["count"],
            "percentage": round(row["count"] / total * 100, 1) if total > 0 else 0
        })
    
    return result


def get_incidents_by_ransomware_family(
    conn: sqlite3.Connection,
    limit: int = 15,
) -> List[Dict[str, Any]]:
    """Get incident counts by ransomware family.
    
    Note: Uses threat_actor_name as fallback since LLM often stores 
    ransomware family names there for ransomware incidents.
    """
    cur = conn.execute(
        """
        SELECT 
            COALESCE(ransomware_family, threat_actor_name) as category,
            COUNT(*) as count
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND attack_category LIKE '%ransomware%'
          AND (
            (ransomware_family IS NOT NULL AND ransomware_family != '')
            OR (threat_actor_name IS NOT NULL AND threat_actor_name != '')
          )
        GROUP BY category
        ORDER BY count DESC
        LIMIT ?
        """,
        (limit,)
    )
    rows = cur.fetchall()
    
    total = sum(r["count"] for r in rows)
    result = []
    for row in rows:
        result.append({
            "category": row["category"],
            "count": row["count"],
            "percentage": round(row["count"] / total * 100, 1) if total > 0 else 0
        })
    
    return result


def get_incidents_over_time(
    conn: sqlite3.Connection,
    months: int = 24,
) -> List[Dict[str, Any]]:
    """Get incident counts over time (by month) - only education-related."""
    cur = conn.execute(
        """
        SELECT 
            strftime('%Y-%m', i.incident_date) as date,
            COUNT(*) as count
        FROM incidents i
        JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE i.incident_date IS NOT NULL
          AND ef.is_education_related = 1
          AND i.incident_date >= date('now', '-' || ? || ' months')
        GROUP BY date
        ORDER BY date ASC
        """,
        (months,)
    )
    rows = cur.fetchall()
    
    return [{"date": row["date"], "count": row["count"]} for row in rows]


def get_recent_incidents(
    conn: sqlite3.Connection,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """Get most recent incidents - only education-related."""
    cur = conn.execute(
        """
        SELECT 
            i.incident_id,
            COALESCE(ef.institution_name, i.university_name, 'Unknown') as university_name,
            COALESCE(ef.country, i.country) as country,
            ef.attack_category,
            ef.ransomware_family,
            i.incident_date,
            i.title,
            ef.threat_actor_name
        FROM incidents i
        JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE ef.is_education_related = 1
        ORDER BY i.incident_date DESC NULLS LAST, i.ingested_at DESC
        LIMIT ?
        """,
        (limit,)
    )
    rows = cur.fetchall()
    
    return [dict(row) for row in rows]


def get_threat_actors(
    conn: sqlite3.Connection,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """Get threat actor summary with their activity."""
    cur = conn.execute(
        """
        SELECT 
            ef.threat_actor_name as name,
            COUNT(*) as incident_count,
            GROUP_CONCAT(DISTINCT COALESCE(ef.country, i.country)) as countries,
            GROUP_CONCAT(DISTINCT ef.ransomware_family) as ransomware_families,
            MIN(i.incident_date) as first_seen,
            MAX(i.incident_date) as last_seen
        FROM incident_enrichments_flat ef
        JOIN incidents i ON ef.incident_id = i.incident_id
        WHERE ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != ''
        GROUP BY ef.threat_actor_name
        ORDER BY incident_count DESC
        LIMIT ?
        """,
        (limit,)
    )
    rows = cur.fetchall()
    
    result = []
    for row in rows:
        result.append({
            "name": row["name"],
            "incident_count": row["incident_count"],
            "countries_targeted": [c for c in (row["countries"] or "").split(",") if c],
            "ransomware_families": [r for r in (row["ransomware_families"] or "").split(",") if r],
            "first_seen": row["first_seen"],
            "last_seen": row["last_seen"],
        })
    
    return result


def get_filter_options(conn: sqlite3.Connection) -> Dict[str, List]:
    """Get available filter options for the UI."""
    options = {}
    
    # Countries - normalize and deduplicate
    from src.edu_cti.core.countries import normalize_country
    cur = conn.execute(
        """
        SELECT DISTINCT country FROM incidents 
        WHERE country IS NOT NULL AND country != ''
        ORDER BY country
        """
    )
    raw_countries = [row["country"] for row in cur.fetchall()]
    # Normalize and deduplicate
    normalized_countries = set()
    for country in raw_countries:
        normalized = normalize_country(country) or country
        normalized_countries.add(normalized)
    options["countries"] = sorted(normalized_countries)
    
    # Attack categories
    cur = conn.execute(
        """
        SELECT DISTINCT attack_category FROM incident_enrichments_flat 
        WHERE attack_category IS NOT NULL AND attack_category != ''
        ORDER BY attack_category
        """
    )
    options["attack_categories"] = [row["attack_category"] for row in cur.fetchall()]
    
    # Ransomware families
    # Ransomware families (use threat_actor_name as fallback for ransomware incidents)
    cur = conn.execute(
        """
        SELECT DISTINCT COALESCE(ransomware_family, threat_actor_name) as family
        FROM incident_enrichments_flat 
        WHERE is_education_related = 1
          AND attack_category LIKE '%ransomware%'
          AND (
            (ransomware_family IS NOT NULL AND ransomware_family != '')
            OR (threat_actor_name IS NOT NULL AND threat_actor_name != '')
          )
        ORDER BY family
        """
    )
    options["ransomware_families"] = [row["family"] for row in cur.fetchall()]
    
    # Threat actors
    cur = conn.execute(
        """
        SELECT DISTINCT threat_actor_name FROM incident_enrichments_flat 
        WHERE threat_actor_name IS NOT NULL AND threat_actor_name != ''
        ORDER BY threat_actor_name
        """
    )
    options["threat_actors"] = [row["threat_actor_name"] for row in cur.fetchall()]
    
    # Institution types
    cur = conn.execute(
        """
        SELECT DISTINCT institution_type FROM incidents 
        WHERE institution_type IS NOT NULL AND institution_type != ''
        UNION
        SELECT DISTINCT institution_type FROM incident_enrichments_flat 
        WHERE institution_type IS NOT NULL AND institution_type != ''
        ORDER BY institution_type
        """
    )
    options["institution_types"] = [row["institution_type"] for row in cur.fetchall()]
    
    # Years
    cur = conn.execute(
        """
        SELECT DISTINCT strftime('%Y', incident_date) as year FROM incidents 
        WHERE incident_date IS NOT NULL
        ORDER BY year DESC
        """
    )
    options["years"] = [int(row["year"]) for row in cur.fetchall() if row["year"]]
    
    return options

