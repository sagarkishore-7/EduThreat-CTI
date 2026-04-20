"""
Database operations for the API.

Provides optimized queries for the REST API endpoints.
"""

import json
import logging
import sqlite3
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

from src.edu_cti.core.config import DB_PATH
from src.edu_cti.core.db import get_connection


def count_education_incidents(conn: sqlite3.Connection) -> int:
    """Count canonical education incidents backed by a live incident row."""
    cur = conn.execute(
        """
        SELECT COUNT(DISTINCT i.incident_id) as count
        FROM incidents i
        JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE ef.is_education_related = 1
        """
    )
    return cur.fetchone()["count"]


def _live_incident_exists(alias: str = "ef") -> str:
    """Return an EXISTS clause that filters out orphan enrichment rows."""
    return f"EXISTS (SELECT 1 FROM incidents i_live WHERE i_live.incident_id = {alias}.incident_id)"


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
    data_breached: bool = False,
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
        # Support broad category filtering (e.g. "ransomware" matches "ransomware_encryption", etc.)
        conditions.append("ef.attack_category LIKE ?")
        params.append(f"%{attack_category}%")
    
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
    
    if data_breached:
        conditions.append("ef.data_breached = 1")

    if enriched_only:
        conditions.append("i.llm_enriched = 1")
    
    if search:
        search_pattern = f"%{search}%"
        conditions.append("""
            (i.institution_name LIKE ? 
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
        "institution_name": "i.institution_name",
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
            COALESCE(ef.institution_name, i.institution_name, 'Unknown') as institution_name,
            COALESCE(ef.institution_type, i.institution_type) as institution_type,
            COALESCE(ef.country, i.country) as country,
            COALESCE(ef.country_code, i.country_code) as country_code,
            COALESCE(ef.region, i.region) as region,
            COALESCE(ef.city, i.city) as city,
            i.incident_date,
            i.date_precision,
            i.title,
            i.subtitle,
            ef.enriched_summary,
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
    
    # Batch-fetch sources for all incidents (avoid N+1 query)
    incidents = [dict(row) for row in rows]
    if incidents:
        ids = [inc["incident_id"] for inc in incidents]
        placeholders = ",".join("?" * len(ids))
        source_cur = conn.execute(
            f"SELECT incident_id, source FROM incident_sources WHERE incident_id IN ({placeholders})",
            ids,
        )
        sources_map: Dict[str, list] = {}
        for s_row in source_cur.fetchall():
            sources_map.setdefault(s_row["incident_id"], []).append(s_row["source"])
        for inc in incidents:
            inc["sources"] = sources_map.get(inc["incident_id"], [])

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
            except Exception:
                incident["timeline"] = None
        
        if enrichment.get("mitre_techniques_json"):
            try:
                incident["mitre_attack_techniques"] = json.loads(enrichment["mitre_techniques_json"])
            except Exception:
                incident["mitre_attack_techniques"] = None
        
        if enrichment.get("systems_affected_codes"):
            try:
                parsed = json.loads(enrichment["systems_affected_codes"])
                incident["systems_affected"] = parsed if isinstance(parsed, list) else ([parsed] if isinstance(parsed, str) and parsed else None)
            except Exception:
                incident["systems_affected"] = None

        if enrichment.get("data_categories"):
            try:
                parsed = json.loads(enrichment["data_categories"])
                incident["data_categories"] = parsed if isinstance(parsed, list) else ([parsed] if isinstance(parsed, str) and parsed else None)
            except Exception:
                incident["data_categories"] = None
        
        # Use enrichment institution_name as primary (LLM-extracted name)
        if enrichment.get("institution_name"):
            incident["institution_name"] = enrichment["institution_name"]

        # Prefer normalized location/institution fields from enrichment when present.
        for key in ("institution_type", "country", "country_code", "region", "city"):
            if enrichment.get(key):
                incident[key] = enrichment[key]
        
        # Merge enrichment fields — skip keys already parsed above to prevent
        # the raw DB string from overwriting a deliberately-parsed None.
        _SKIP_MERGE = {"data_categories", "systems_affected_codes", "timeline_json", "mitre_techniques_json"}
        for key, value in enrichment.items():
            if key in _SKIP_MERGE:
                continue
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
        except Exception:
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

    # Total incidents in database (all ingested)
    cur = conn.execute("SELECT COUNT(*) as count FROM incidents")
    stats["total_incidents"] = cur.fetchone()["count"]

    # Education-confirmed incidents (enriched + confirmed by LLM)
    stats["education_incidents"] = count_education_incidents(conn)

    # Enriched incidents (processed by LLM)
    cur = conn.execute("SELECT COUNT(*) as count FROM incidents WHERE llm_enriched = 1")
    stats["enriched_incidents"] = cur.fetchone()["count"]

    # Pending analysis (not yet processed by LLM)
    cur = conn.execute("SELECT COUNT(*) as count FROM incidents WHERE llm_enriched = 0")
    stats["unenriched_incidents"] = cur.fetchone()["count"]

    # Ransomware incidents
    cur = conn.execute(
        f"""
        SELECT COUNT(*) as count FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.attack_category LIKE '%ransomware%'
          AND {_live_incident_exists('ef')}
        """
    )
    stats["incidents_with_ransomware"] = cur.fetchone()["count"]

    # Data breach incidents
    cur = conn.execute(
        f"""
        SELECT COUNT(*) as count FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.data_breached = 1
          AND {_live_incident_exists('ef')}
        """
    )
    stats["incidents_with_data_breach"] = cur.fetchone()["count"]

    # Countries affected (education-related only)
    cur = conn.execute(
        f"""
        SELECT COUNT(DISTINCT ef.country) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1 AND ef.country IS NOT NULL AND ef.country != ''
          AND {_live_incident_exists('ef')}
        """
    )
    stats["countries_affected"] = cur.fetchone()["count"]

    # Unique threat actors
    cur = conn.execute(
        f"""
        SELECT COUNT(DISTINCT ef.threat_actor_name) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != ''
          AND {_live_incident_exists('ef')}
        """
    )
    stats["unique_threat_actors"] = cur.fetchone()["count"]

    # Unique ransomware families
    cur = conn.execute(
        f"""
        SELECT COUNT(DISTINCT COALESCE(ef.ransomware_family, ef.threat_actor_name)) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.attack_category LIKE '%ransomware%'
          AND (
            (ef.ransomware_family IS NOT NULL AND ef.ransomware_family != '')
            OR (ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != '')
          )
          AND {_live_incident_exists('ef')}
        """
    )
    stats["unique_ransomware_families"] = cur.fetchone()["count"]

    # Data sources count
    cur = conn.execute(
        "SELECT COUNT(DISTINCT source) as count FROM incident_sources"
    )
    stats["data_sources"] = cur.fetchone()["count"]

    # Average recovery time (days) for education incidents
    cur = conn.execute(
        f"""
        SELECT AVG(ef.recovery_timeframe_days) as avg_days
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.recovery_timeframe_days IS NOT NULL
          AND ef.recovery_timeframe_days > 0
          AND {_live_incident_exists('ef')}
        """
    )
    row = cur.fetchone()
    stats["avg_recovery_days"] = round(row["avg_days"], 1) if row["avg_days"] else None

    # Total financial impact (sum of estimated costs)
    cur = conn.execute(
        f"""
        SELECT SUM(ef.recovery_costs_max) as total
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.recovery_costs_max IS NOT NULL AND ef.recovery_costs_max > 0
          AND {_live_incident_exists('ef')}
        """
    )
    row = cur.fetchone()
    stats["total_financial_impact"] = row["total"] if row["total"] else 0

    # MITRE techniques count
    cur = conn.execute(
        f"""
        SELECT COUNT(*) as count FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND mitre_techniques_count IS NOT NULL AND mitre_techniques_count > 0
          AND {_live_incident_exists('incident_enrichments_flat')}
        """
    )
    stats["incidents_with_mitre"] = cur.fetchone()["count"]

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
            COALESCE(ef.attack_category, i.attack_type_hint) as category,
            COUNT(*) as count
        FROM incidents i
        JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE ef.is_education_related = 1
          AND COALESCE(ef.attack_category, i.attack_type_hint) IS NOT NULL
          AND COALESCE(ef.attack_category, i.attack_type_hint) != ''
        GROUP BY category
        ORDER BY count DESC
        LIMIT ?
        """,
        (limit * 3,)
    )
    rows = cur.fetchall()

    merged: dict[str, int] = {}
    for row in rows:
        canonical = _normalize_attack_category(row["category"])
        if canonical is None:
            continue
        merged[canonical] = merged.get(canonical, 0) + row["count"]

    total = sum(merged.values())
    return [
        {"category": name, "count": count,
         "percentage": round(count / total * 100, 1) if total > 0 else 0}
        for name, count in sorted(merged.items(), key=lambda x: -x[1])
    ][:limit]


def get_incidents_by_ransomware_family(
    conn: sqlite3.Connection,
    limit: int = 15,
) -> List[Dict[str, Any]]:
    """Get incident counts by ransomware family.
    
    Note: Uses threat_actor_name as fallback since LLM often stores 
    ransomware family names there for ransomware incidents.
    """
    cur = conn.execute(
        f"""
        SELECT 
            COALESCE(ef.ransomware_family, ef.threat_actor_name) as category,
            COUNT(*) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.attack_category LIKE '%ransomware%'
          AND (
            (ef.ransomware_family IS NOT NULL AND ef.ransomware_family != '')
            OR (ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != '')
          )
          AND {_live_incident_exists('ef')}
        GROUP BY category
        ORDER BY count DESC
        LIMIT ?
        """,
        (limit,)
    )
    rows = cur.fetchall()

    # Normalize names and merge duplicates (e.g. "clop" + "cl0p_clop" → "Cl0p")
    merged: dict[str, int] = {}
    for row in rows:
        canonical = _normalize_ransomware_family(row["category"])
        if canonical is None:
            continue
        merged[canonical] = merged.get(canonical, 0) + row["count"]

    total = sum(merged.values())
    result = [
        {
            "category": name,
            "count": count,
            "percentage": round(count / total * 100, 1) if total > 0 else 0,
        }
        for name, count in sorted(merged.items(), key=lambda x: -x[1])
    ]
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
          AND i.incident_date != ''
          AND ef.is_education_related = 1
          AND i.incident_date >= date('now', '-' || ? || ' months')
        GROUP BY date
        HAVING date IS NOT NULL
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
            COALESCE(ef.institution_name, i.institution_name, 'Unknown') as institution_name,
            COALESCE(ef.country, i.country) as country,
            ef.attack_category,
            ef.ransomware_family,
            i.incident_date,
            i.title,
            ef.enriched_summary,
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


# ============================================================
# Advanced Analytics Queries
# ============================================================

def get_attack_trends(
    conn: sqlite3.Connection,
    months: int = 36,
) -> List[Dict[str, Any]]:
    """Get attack trends over time by category (stacked area chart)."""
    cur = conn.execute(
        """
        SELECT
            strftime('%Y-%m', i.incident_date) as month,
            ef.attack_category,
            COUNT(*) as count
        FROM incidents i
        JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE ef.is_education_related = 1
          AND i.incident_date IS NOT NULL
          AND i.incident_date >= date('now', '-' || ? || ' months')
        GROUP BY month, ef.attack_category
        ORDER BY month ASC
        """,
        (months,)
    )
    return [dict(row) for row in cur.fetchall()]


def get_attack_vectors(
    conn: sqlite3.Connection,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """Get attack vector distribution."""
    cur = conn.execute(
        f"""
        SELECT
            COALESCE(ef.initial_access_vector, ef.attack_vector) as category,
            COUNT(*) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND COALESCE(ef.initial_access_vector, ef.attack_vector) IS NOT NULL
          AND COALESCE(ef.initial_access_vector, ef.attack_vector) != ''
          AND {_live_incident_exists('ef')}
        GROUP BY category
        ORDER BY count DESC
        LIMIT ?
        """,
        (limit * 3,)
    )
    rows = cur.fetchall()
    merged: dict[str, int] = {}
    for row in rows:
        canonical = _normalize_attack_vector(row["category"])
        if canonical is None:
            continue
        merged[canonical] = merged.get(canonical, 0) + row["count"]
    total = sum(merged.values())
    return [
        {"category": name, "count": count,
         "percentage": round(count / total * 100, 1) if total > 0 else 0}
        for name, count in sorted(merged.items(), key=lambda x: -x[1])
    ][:limit]


# MITRE ATT&CK technique ID → tactic mapping (covers the most common techniques)
# Source: https://attack.mitre.org/techniques/
_TECHNIQUE_TO_TACTIC: Dict[str, str] = {
    # Reconnaissance
    "T1595": "Reconnaissance", "T1592": "Reconnaissance", "T1589": "Reconnaissance",
    "T1590": "Reconnaissance", "T1591": "Reconnaissance", "T1598": "Reconnaissance",
    "T1597": "Reconnaissance", "T1596": "Reconnaissance", "T1593": "Reconnaissance",
    "T1594": "Reconnaissance",
    # Resource Development
    "T1583": "Resource Development", "T1584": "Resource Development", "T1587": "Resource Development",
    "T1585": "Resource Development", "T1586": "Resource Development", "T1588": "Resource Development",
    "T1608": "Resource Development",
    # Initial Access
    "T1189": "Initial Access", "T1190": "Initial Access", "T1133": "Initial Access",
    "T1200": "Initial Access", "T1566": "Initial Access", "T1091": "Initial Access",
    "T1195": "Initial Access", "T1199": "Initial Access", "T1078": "Initial Access",
    "T1659": "Initial Access",
    # Execution
    "T1059": "Execution", "T1609": "Execution", "T1610": "Execution",
    "T1203": "Execution", "T1559": "Execution", "T1106": "Execution",
    "T1053": "Execution", "T1129": "Execution", "T1072": "Execution",
    "T1569": "Execution", "T1204": "Execution", "T1047": "Execution",
    "T1651": "Execution",
    # Persistence
    "T1098": "Persistence", "T1197": "Persistence", "T1547": "Persistence",
    "T1037": "Persistence", "T1176": "Persistence", "T1554": "Persistence",
    "T1136": "Persistence", "T1543": "Persistence", "T1546": "Persistence",
    "T1133": "Persistence", "T1574": "Persistence", "T1525": "Persistence",
    "T1556": "Persistence", "T1137": "Persistence", "T1542": "Persistence",
    "T1053": "Persistence", "T1505": "Persistence", "T1205": "Persistence",
    # Privilege Escalation
    "T1548": "Privilege Escalation", "T1134": "Privilege Escalation",
    "T1068": "Privilege Escalation", "T1484": "Privilege Escalation",
    "T1611": "Privilege Escalation",
    # Defense Evasion
    "T1548": "Defense Evasion", "T1197": "Defense Evasion", "T1140": "Defense Evasion",
    "T1006": "Defense Evasion", "T1480": "Defense Evasion", "T1211": "Defense Evasion",
    "T1222": "Defense Evasion", "T1564": "Defense Evasion", "T1562": "Defense Evasion",
    "T1070": "Defense Evasion", "T1202": "Defense Evasion", "T1036": "Defense Evasion",
    "T1556": "Defense Evasion", "T1578": "Defense Evasion", "T1112": "Defense Evasion",
    "T1601": "Defense Evasion", "T1599": "Defense Evasion", "T1027": "Defense Evasion",
    "T1647": "Defense Evasion", "T1542": "Defense Evasion", "T1055": "Defense Evasion",
    "T1620": "Defense Evasion", "T1207": "Defense Evasion", "T1014": "Defense Evasion",
    "T1218": "Defense Evasion", "T1216": "Defense Evasion", "T1221": "Defense Evasion",
    "T1205": "Defense Evasion", "T1127": "Defense Evasion", "T1535": "Defense Evasion",
    "T1550": "Defense Evasion", "T1078": "Defense Evasion", "T1497": "Defense Evasion",
    "T1600": "Defense Evasion", "T1220": "Defense Evasion",
    # Credential Access
    "T1557": "Credential Access", "T1110": "Credential Access", "T1555": "Credential Access",
    "T1212": "Credential Access", "T1187": "Credential Access", "T1606": "Credential Access",
    "T1056": "Credential Access", "T1556": "Credential Access", "T1111": "Credential Access",
    "T1621": "Credential Access", "T1040": "Credential Access", "T1003": "Credential Access",
    "T1528": "Credential Access", "T1558": "Credential Access", "T1539": "Credential Access",
    "T1649": "Credential Access",
    # Discovery
    "T1087": "Discovery", "T1010": "Discovery", "T1217": "Discovery",
    "T1580": "Discovery", "T1538": "Discovery", "T1526": "Discovery",
    "T1619": "Discovery", "T1613": "Discovery", "T1622": "Discovery",
    "T1482": "Discovery", "T1083": "Discovery", "T1615": "Discovery",
    "T1046": "Discovery", "T1135": "Discovery", "T1040": "Discovery",
    "T1201": "Discovery", "T1120": "Discovery", "T1069": "Discovery",
    "T1057": "Discovery", "T1012": "Discovery", "T1018": "Discovery",
    "T1518": "Discovery", "T1082": "Discovery", "T1614": "Discovery",
    "T1016": "Discovery", "T1049": "Discovery", "T1033": "Discovery",
    "T1007": "Discovery", "T1124": "Discovery", "T1497": "Discovery",
    # Lateral Movement
    "T1210": "Lateral Movement", "T1534": "Lateral Movement", "T1570": "Lateral Movement",
    "T1563": "Lateral Movement", "T1021": "Lateral Movement", "T1091": "Lateral Movement",
    "T1080": "Lateral Movement", "T1550": "Lateral Movement",
    # Collection
    "T1557": "Collection", "T1560": "Collection", "T1123": "Collection",
    "T1119": "Collection", "T1185": "Collection", "T1115": "Collection",
    "T1530": "Collection", "T1602": "Collection", "T1213": "Collection",
    "T1005": "Collection", "T1039": "Collection", "T1025": "Collection",
    "T1074": "Collection", "T1114": "Collection", "T1056": "Collection",
    "T1113": "Collection", "T1125": "Collection",
    # Command and Control
    "T1071": "Command and Control", "T1092": "Command and Control",
    "T1132": "Command and Control", "T1001": "Command and Control",
    "T1568": "Command and Control", "T1573": "Command and Control",
    "T1008": "Command and Control", "T1105": "Command and Control",
    "T1104": "Command and Control", "T1095": "Command and Control",
    "T1571": "Command and Control", "T1572": "Command and Control",
    "T1090": "Command and Control", "T1219": "Command and Control",
    "T1205": "Command and Control", "T1102": "Command and Control",
    # Exfiltration
    "T1020": "Exfiltration", "T1030": "Exfiltration", "T1048": "Exfiltration",
    "T1041": "Exfiltration", "T1011": "Exfiltration", "T1052": "Exfiltration",
    "T1567": "Exfiltration", "T1029": "Exfiltration", "T1537": "Exfiltration",
    # Impact
    "T1531": "Impact", "T1485": "Impact", "T1486": "Impact",
    "T1565": "Impact", "T1491": "Impact", "T1561": "Impact",
    "T1499": "Impact", "T1657": "Impact", "T1495": "Impact",
    "T1490": "Impact", "T1498": "Impact", "T1496": "Impact",
    "T1489": "Impact", "T1529": "Impact",
}


def _resolve_tactic_from_technique_id(tech_id: str) -> Optional[str]:
    """Look up the MITRE tactic from a technique ID. Returns None if unknown."""
    if not tech_id:
        return None
    # Strip sub-technique suffix (e.g., T1566.001 → T1566)
    base_id = tech_id.split(".")[0].strip().upper()
    return _TECHNIQUE_TO_TACTIC.get(base_id)


_UNKNOWN_FAMILY_VALUES = {
    "unknown", "not known", "not_known", "unidentified", "n/a",
    "not applicable", "not_applicable", "unspecified", "undetermined", "",
}

_RANSOMWARE_CANONICAL: Dict[str, str] = {
    # Cl0p
    "cl0p": "Cl0p", "clop": "Cl0p", "cl0p_clop": "Cl0p", "cl0p/clop": "Cl0p",
    "cl0p clop": "Cl0p",
    # LockBit
    "lockbit": "LockBit", "lock_bit": "LockBit", "lockbit 2.0": "LockBit",
    "lockbit 3.0": "LockBit", "lockbit2": "LockBit", "lockbit3": "LockBit",
    "lockbit_2": "LockBit", "lockbit_3": "LockBit",
    # BlackCat / ALPHV
    "blackcat": "BlackCat/ALPHV", "alphv": "BlackCat/ALPHV",
    "blackcat_alphv": "BlackCat/ALPHV", "alphv_blackcat": "BlackCat/ALPHV",
    "blackcat/alphv": "BlackCat/ALPHV",
    # Black Basta
    "blackbasta": "Black Basta", "black_basta": "Black Basta",
    "black basta": "Black Basta",
    # Vice Society
    "vice_society": "Vice Society", "vice society": "Vice Society",
    # DoppelPaymer
    "doppelpaymer": "DoppelPaymer", "dopplepaymer": "DoppelPaymer",
    "doppel_paymer": "DoppelPaymer",
    # BabLock / Rorschach
    "bablock_rorschach": "BabLock/Rorschach", "bablock": "BabLock/Rorschach",
    "rorschach": "BabLock/Rorschach",
    # REvil / Sodinokibi
    "revil": "REvil", "sodinokibi": "REvil", "r_evil": "REvil",
    # NetWalker
    "netwalker": "NetWalker", "net_walker": "NetWalker",
    # TrickBot
    "trickbot": "TrickBot", "trick_bot": "TrickBot",
    # RansomHub
    "ransomhub": "RansomHub", "ransom_hub": "RansomHub",
    # AvosLocker
    "avoslocker": "AvosLocker", "avos_locker": "AvosLocker",
    # INC Ransom
    "inc": "INC Ransom", "inc_ransom": "INC Ransom", "inc ransom": "INC Ransom",
    # GandCrab
    "gandcrab": "GandCrab", "gand_crab": "GandCrab",
    # Straight capitalisation (single canonical form)
    "medusa": "Medusa", "ryuk": "Ryuk", "rhysida": "Rhysida", "akira": "Akira",
    "conti": "Conti", "hive": "Hive", "royal": "Royal", "fog": "Fog",
    "qilin": "Qilin", "snatch": "Snatch", "maze": "Maze", "monti": "Monti",
    "interlock": "Interlock", "funksec": "FunkSec", "avaddon": "Avaddon",
    "blacksuit": "BlackSuit", "black_suit": "BlackSuit",
    "sinobi": "Sinobi", "ako": "AKO", "cuba": "Cuba",
}


def _normalize_ransomware_family(name: Optional[str]) -> Optional[str]:
    """Return the canonical display name for a ransomware family, or None if unknown/excluded."""
    if not name:
        return None
    normalized_key = name.strip().lower().replace(" ", "_").replace("/", "_").replace("-", "_")
    # Also try without underscores for slash-joined variants stored literally
    alt_key = name.strip().lower()
    if alt_key in _UNKNOWN_FAMILY_VALUES or normalized_key in _UNKNOWN_FAMILY_VALUES:
        return None
    return (
        _RANSOMWARE_CANONICAL.get(alt_key)
        or _RANSOMWARE_CANONICAL.get(normalized_key)
        or name.strip()  # keep raw value if not in map (preserves new families)
    )


_ATTACK_CATEGORY_CANONICAL: Dict[str, str] = {
    "ransomware": "Ransomware",
    "ransomware_encryption": "Ransomware",
    "ransomware_double_extortion": "Ransomware (Double Extortion)",
    "ransomware_triple_extortion": "Ransomware (Triple Extortion)",
    "ransomware_data_leak_only": "Ransomware (Data Leak Only)",
    "data_breach": "Data Breach",
    "data breach": "Data Breach",
    "data_breach_external": "Data Breach",
    "data_breach_internal": "Data Breach (Insider)",
    "data_exposure_misconfiguration": "Data Exposure",
    "data_leak_accidental": "Data Exposure",
    "phishing": "Phishing",
    "spear_phishing": "Spear Phishing",
    "ddos": "DDoS",
    "denial_of_service": "DDoS",
    "dos": "DDoS",
    "malware": "Malware",
    "unauthorized_access": "Unauthorized Access",
    "account_takeover": "Account Takeover",
    "credential_theft": "Credential Theft",
    "credential_stuffing": "Credential Stuffing",
    "supply_chain": "Supply Chain Attack",
    "supply_chain_attack": "Supply Chain Attack",
    "social_engineering": "Social Engineering",
    "business_email_compromise": "Business Email Compromise",
    "bec": "Business Email Compromise",
    "vulnerability_exploitation": "Vulnerability Exploitation",
    "zero_day": "Zero-Day Exploit",
    "zero_day_exploitation": "Zero-Day Exploit",
    "insider_threat": "Insider Threat",
    "cryptojacking": "Cryptojacking",
    "defacement": "Web Defacement",
    "web_defacement": "Web Defacement",
    "man_in_the_middle": "Man-in-the-Middle",
}

_ATTACK_VECTOR_CANONICAL: Dict[str, str] = {
    "phishing": "Phishing",
    "spear_phishing": "Spear Phishing",
    "email": "Phishing",  # generic 'email' vector = phishing
    "malicious_attachment": "Malicious Attachment",
    "malicious_link": "Malicious Link",
    "rdp": "RDP",
    "remote_desktop": "RDP",
    "remote_desktop_protocol": "RDP",
    "vpn": "VPN Exploitation",
    "vpn_exploitation": "VPN Exploitation",
    "vpn_vulnerability": "VPN Exploitation",
    "credential_stuffing": "Credential Stuffing",
    "brute_force": "Brute Force",
    "sql_injection": "SQL Injection",
    "web_application": "Web Application",
    "supply_chain": "Supply Chain",
    "social_engineering": "Social Engineering",
    "zero_day": "Zero-Day Exploit",
    "drive_by_download": "Drive-By Download",
    "removable_media": "Removable Media",
    "usb": "Removable Media",
    "insider": "Insider",
    "stolen_credentials": "Stolen Credentials",
    "exposed_service": "Exposed Service",
    "misconfiguration": "Misconfiguration",
}

_INSTITUTION_TYPE_CANONICAL: Dict[str, str] = {
    "university": "University",
    "k12": "K-12 School",
    "k_12": "K-12 School",
    "k-12": "K-12 School",
    "school": "K-12 School",
    "college": "College",
    "community_college": "Community College",
    "school_district": "School District",
    "research_institution": "Research Institution",
    "research_university": "University",
    "vocational": "Vocational School",
    "vocational_school": "Vocational School",
    "teaching_hospital": "Teaching Hospital",
    "online_university": "Online University",
    "education_department": "Education Department",
    "education_ministry": "Education Ministry",
    "student_loan_servicer": "Student Loan Servicer",
    "education_nonprofit": "Education Nonprofit",
    "education_vendor": "Education Vendor",
    "consortium": "Consortium",
    "academy": "Academy",
    "seminary": "Seminary",
    "polytechnic": "Polytechnic",
}

_GENERIC_UNKNOWNS = {"unknown", "other", "n/a", "not applicable", "not_applicable",
                     "unspecified", "unidentified", "not known", "not_known", ""}


def _normalize_category(raw: Optional[str], canonical_map: Dict[str, str]) -> Optional[str]:
    """Generic normalizer: lowercase+underscore key lookup, fallback to title-cased raw."""
    if not raw:
        return None
    key = raw.strip().lower().replace(" ", "_").replace("-", "_")
    alt = raw.strip().lower()
    if key in _GENERIC_UNKNOWNS or alt in _GENERIC_UNKNOWNS:
        return None
    return canonical_map.get(key) or canonical_map.get(alt) or raw.strip().replace("_", " ").title()


def _normalize_attack_category(raw: Optional[str]) -> Optional[str]:
    return _normalize_category(raw, _ATTACK_CATEGORY_CANONICAL)


def _normalize_attack_vector(raw: Optional[str]) -> Optional[str]:
    return _normalize_category(raw, _ATTACK_VECTOR_CANONICAL)


def _normalize_institution_type(raw: Optional[str]) -> Optional[str]:
    return _normalize_category(raw, _INSTITUTION_TYPE_CANONICAL)


def _normalize_mitre_tactic(raw: str) -> str:
    """Normalize a MITRE tactic value to the canonical display name.

    Handles: snake_case ('initial_access'), tactic IDs ('TA0001'),
    lowercase ('lateral movement'), or already-correct ('Initial Access').
    """
    if not raw:
        return "Unknown"
    # Canonical names keyed by lowercase
    CANONICAL = {
        "initial access": "Initial Access",
        "execution": "Execution",
        "persistence": "Persistence",
        "privilege escalation": "Privilege Escalation",
        "defense evasion": "Defense Evasion",
        "credential access": "Credential Access",
        "discovery": "Discovery",
        "lateral movement": "Lateral Movement",
        "collection": "Collection",
        "command and control": "Command and Control",
        "exfiltration": "Exfiltration",
        "impact": "Impact",
        "reconnaissance": "Reconnaissance",
        "resource development": "Resource Development",
    }
    TACTIC_ID_MAP = {
        "TA0001": "Initial Access", "TA0002": "Execution",
        "TA0003": "Persistence", "TA0004": "Privilege Escalation",
        "TA0005": "Defense Evasion", "TA0006": "Credential Access",
        "TA0007": "Discovery", "TA0008": "Lateral Movement",
        "TA0009": "Collection", "TA0010": "Command and Control",
        "TA0011": "Exfiltration", "TA0040": "Impact",
        "TA0043": "Reconnaissance", "TA0042": "Resource Development",
    }
    s = raw.strip()
    # Direct tactic ID match
    if s.upper() in TACTIC_ID_MAP:
        return TACTIC_ID_MAP[s.upper()]
    # Normalize: replace underscores, lowercase, strip
    normalized = s.replace("_", " ").lower().strip()
    if normalized in CANONICAL:
        return CANONICAL[normalized]
    # Partial match fallback
    for key, name in CANONICAL.items():
        if key in normalized or normalized in key:
            return name
    return raw.title()  # Best-effort title case


def get_mitre_tactics(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Parse mitre_techniques_json and aggregate by tactic."""
    # Try flat table first, then fall back to enrichment JSON
    cur = conn.execute(
        f"""
        SELECT ef.mitre_techniques_json, ie.enrichment_data
        FROM incident_enrichments_flat ef
        LEFT JOIN incident_enrichments ie ON ef.incident_id = ie.incident_id
        WHERE ef.is_education_related = 1
          AND (ef.mitre_techniques_count > 0 OR ef.mitre_techniques_json IS NOT NULL)
          AND {_live_incident_exists('ef')}
        """
    )
    tactic_counts: Dict[str, int] = {}
    tactic_techniques: Dict[str, list] = {}
    rows_checked = 0
    rows_with_data = 0
    for row in cur.fetchall():
        rows_checked += 1
        try:
            techniques = None
            # Try flat table JSON first
            if row["mitre_techniques_json"]:
                raw_json = row["mitre_techniques_json"]
                techniques = json.loads(raw_json)
            # Fall back to enrichment_data JSON if flat is empty
            if (not techniques or techniques == []) and row["enrichment_data"]:
                enrichment = json.loads(row["enrichment_data"])
                techniques = enrichment.get("mitre_attack_techniques", [])
            if not techniques:
                continue
            rows_with_data += 1
            for t in techniques:
                # Handle both dict and string entries
                if isinstance(t, str):
                    # String like "T1078: Valid Accounts" — parse it
                    t_str = t.strip()
                    tech_id = ""
                    tech_name = ""
                    if t_str.startswith("T") and ":" in t_str:
                        parts = t_str.split(":", 1)
                        tech_id = parts[0].strip()
                        tech_name = parts[1].strip() if len(parts) > 1 else ""
                    elif t_str.startswith("T"):
                        tech_id = t_str
                    # Resolve tactic from technique ID
                    tactic = _resolve_tactic_from_technique_id(tech_id) or "Unknown"
                elif isinstance(t, dict):
                    raw_tactic = t.get("tactic")
                    tech_id = t.get("technique_id", "")
                    tech_name = t.get("technique_name", "")
                    # If tactic is null/empty, resolve from technique ID
                    if raw_tactic:
                        tactic = _normalize_mitre_tactic(raw_tactic)
                    else:
                        tactic = _resolve_tactic_from_technique_id(tech_id) or "Unknown"
                else:
                    continue
                tactic_counts[tactic] = tactic_counts.get(tactic, 0) + 1
                if tech_id and tactic not in tactic_techniques:
                    tactic_techniques[tactic] = []
                if tech_id:
                    entry = f"{tech_id}: {tech_name}"
                    if entry not in tactic_techniques.get(tactic, []):
                        if tactic not in tactic_techniques:
                            tactic_techniques[tactic] = []
                        tactic_techniques[tactic].append(entry)
        except Exception as e:
            logger.warning(f"Error parsing MITRE data: {e}")
            continue

    logger.info(f"MITRE heatmap: checked {rows_checked} rows, {rows_with_data} had technique data, {sum(tactic_counts.values())} total technique entries")

    result = []
    for tactic, count in sorted(tactic_counts.items(), key=lambda x: -x[1]):
        result.append({
            "tactic": tactic,
            "count": count,
            "techniques": tactic_techniques.get(tactic, [])
        })
    return result


def get_initial_access_methods(
    conn: sqlite3.Connection,
    limit: int = 12,
) -> List[Dict[str, Any]]:
    """Get initial access vector distribution."""
    cur = conn.execute(
        f"""
        SELECT
            CASE
                WHEN COALESCE(ef.initial_access_vector, ef.attack_vector) IN ('unknown', 'other', 'Unknown', 'Other')
                    THEN 'Unknown / Other'
                WHEN ef.initial_access_vector IS NULL AND ef.attack_vector IS NULL
                    THEN 'Unknown / Other'
                ELSE COALESCE(ef.initial_access_vector, ef.attack_vector)
            END as category,
            COUNT(*) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND (ef.initial_access_vector IS NOT NULL OR ef.attack_vector IS NOT NULL)
          AND {_live_incident_exists('ef')}
        GROUP BY category
        ORDER BY count DESC
        LIMIT ?
        """,
        (limit,)
    )
    rows = cur.fetchall()
    total = sum(r["count"] for r in rows)
    return [
        {"category": r["category"], "count": r["count"],
         "percentage": round(r["count"] / total * 100, 1) if total > 0 else 0}
        for r in rows
    ]


def get_system_impact_stats(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Aggregate boolean system impact columns."""
    systems = [
        ("email_system_affected", "Email System"),
        ("student_portal_affected", "Student Portal"),
        ("research_systems_affected", "Research Systems"),
        ("network_compromised", "Network"),
        ("cloud_services_affected", "Cloud Services"),
        ("hospital_systems_affected", "Hospital Systems"),
        ("critical_systems_affected", "Critical Systems"),
    ]
    result = []
    for col, label in systems:
        cur = conn.execute(
            f"""
            SELECT COUNT(*) as count
            FROM incident_enrichments_flat ef
            WHERE ef.is_education_related = 1 AND ef.{col} = 1
              AND {_live_incident_exists('ef')}
            """
        )
        count = cur.fetchone()["count"]
        if count > 0:
            result.append({"category": label, "count": count})
    result.sort(key=lambda x: -x["count"])
    return result


def get_ransomware_timeline(
    conn: sqlite3.Connection,
    limit: int = 15,
) -> List[Dict[str, Any]]:
    """Get ransomware family activity periods (first/last seen + count)."""
    cur = conn.execute(
        """
        SELECT
            COALESCE(ef.ransomware_family, ef.threat_actor_name) as family,
            COUNT(*) as incident_count,
            MIN(i.incident_date) as first_seen,
            MAX(i.incident_date) as last_seen
        FROM incident_enrichments_flat ef
        JOIN incidents i ON ef.incident_id = i.incident_id
        WHERE ef.is_education_related = 1
          AND ef.attack_category LIKE '%ransomware%'
          AND (
            (ef.ransomware_family IS NOT NULL AND ef.ransomware_family != '')
            OR (ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != '')
          )
        GROUP BY family
        ORDER BY incident_count DESC
        LIMIT ?
        """,
        (limit,)
    )
    merged: dict[str, dict] = {}
    for row in cur.fetchall():
        r = dict(row)
        canonical = _normalize_ransomware_family(r["family"])
        if canonical is None:
            continue
        if canonical not in merged:
            merged[canonical] = {"family": canonical, "incident_count": 0,
                                 "first_seen": r["first_seen"], "last_seen": r["last_seen"]}
        m = merged[canonical]
        m["incident_count"] += r["incident_count"]
        if r["first_seen"] and (not m["first_seen"] or r["first_seen"] < m["first_seen"]):
            m["first_seen"] = r["first_seen"]
        if r["last_seen"] and (not m["last_seen"] or r["last_seen"] > m["last_seen"]):
            m["last_seen"] = r["last_seen"]
    return sorted(merged.values(), key=lambda x: -x["incident_count"])[:limit]


def get_ransomware_families_detail(
    conn: sqlite3.Connection,
    limit: int = 15,
) -> List[Dict[str, Any]]:
    """Enhanced stats per ransomware family."""
    cur = conn.execute(
        """
        SELECT
            COALESCE(ef.ransomware_family, ef.threat_actor_name) as family,
            COUNT(*) as incident_count,
            SUM(CASE WHEN ef.data_exfiltrated = 1 THEN 1 ELSE 0 END) as exfiltration_count,
            AVG(ef.ransom_amount) as avg_ransom,
            GROUP_CONCAT(DISTINCT ef.country) as countries,
            MIN(i.incident_date) as first_seen,
            MAX(i.incident_date) as last_seen
        FROM incident_enrichments_flat ef
        JOIN incidents i ON ef.incident_id = i.incident_id
        WHERE ef.is_education_related = 1
          AND ef.attack_category LIKE '%ransomware%'
          AND (
            (ef.ransomware_family IS NOT NULL AND ef.ransomware_family != '')
            OR (ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != '')
          )
        GROUP BY family
        ORDER BY incident_count DESC
        LIMIT ?
        """,
        (limit,)
    )
    merged: dict[str, dict] = {}
    for row in cur.fetchall():
        r = dict(row)
        canonical = _normalize_ransomware_family(r["family"])
        if canonical is None:
            continue
        if canonical not in merged:
            merged[canonical] = {
                "family": canonical,
                "incident_count": 0,
                "exfiltration_count": 0,
                "avg_ransom": None,
                "countries": set(),
                "first_seen": r["first_seen"],
                "last_seen": r["last_seen"],
            }
        m = merged[canonical]
        m["incident_count"] += r["incident_count"]
        m["exfiltration_count"] += r["exfiltration_count"] or 0
        if r["countries"]:
            m["countries"].update(c for c in r["countries"].split(",") if c)
        if r["first_seen"] and (not m["first_seen"] or r["first_seen"] < m["first_seen"]):
            m["first_seen"] = r["first_seen"]
        if r["last_seen"] and (not m["last_seen"] or r["last_seen"] > m["last_seen"]):
            m["last_seen"] = r["last_seen"]

    result = []
    for m in sorted(merged.values(), key=lambda x: -x["incident_count"]):
        m["countries"] = sorted(m["countries"])
        m["exfiltration_rate"] = round(m["exfiltration_count"] / m["incident_count"] * 100, 1) if m["incident_count"] > 0 else 0
        result.append(m)
    return result


def get_ransom_economics(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Aggregate ransom demand/payment economics."""
    cur = conn.execute(
        f"""
        SELECT
            COUNT(*) as total_ransomware,
            SUM(CASE WHEN was_ransom_demanded = 1 THEN 1 ELSE 0 END) as demanded_count,
            SUM(CASE WHEN ransom_paid = 1 THEN 1 ELSE 0 END) as paid_count,
            SUM(ransom_amount) as total_demanded,
            AVG(CASE WHEN ransom_amount > 0 THEN ransom_amount END) as avg_demanded,
            MAX(ransom_amount) as max_demanded,
            SUM(ransom_paid_amount) as total_paid,
            AVG(CASE WHEN ransom_paid_amount > 0 THEN ransom_paid_amount END) as avg_paid
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.attack_category LIKE '%ransomware%'
          AND {_live_incident_exists('ef')}
        """
    )
    row = dict(cur.fetchone())
    # Normalize None to 0 for count fields
    for k in ["demanded_count", "paid_count", "total_ransomware"]:
        if row[k] is None:
            row[k] = 0
    row["payment_rate"] = round(row["paid_count"] / row["demanded_count"] * 100, 1) if row["demanded_count"] > 0 else 0
    return row


def get_ransomware_recovery_comparison(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Compare recovery metrics: ransomware vs non-ransomware."""
    result = {}
    for label, condition in [("ransomware", "ef.attack_category LIKE '%ransomware%'"), ("other", "ef.attack_category NOT LIKE '%ransomware%'")]:
        cur = conn.execute(
            f"""
            SELECT
                AVG(recovery_timeframe_days) as avg_recovery_days,
                AVG(downtime_days) as avg_downtime_days,
                SUM(CASE WHEN from_backup = 1 THEN 1 ELSE 0 END) * 100.0 / MAX(COUNT(*), 1) as backup_rate,
                SUM(CASE WHEN incident_response_firm IS NOT NULL AND incident_response_firm != '' THEN 1 ELSE 0 END) * 100.0 / MAX(COUNT(*), 1) as ir_firm_rate,
                SUM(CASE WHEN forensics_firm IS NOT NULL AND forensics_firm != '' THEN 1 ELSE 0 END) * 100.0 / MAX(COUNT(*), 1) as forensics_rate,
                COUNT(*) as total
            FROM incident_enrichments_flat ef
            WHERE ef.is_education_related = 1 AND {condition}
              AND {_live_incident_exists('ef')}
            """
        )
        row = cur.fetchone()
        result[label] = {
            "avg_recovery_days": round(row["avg_recovery_days"], 1) if row["avg_recovery_days"] else 0,
            "avg_downtime_days": round(row["avg_downtime_days"], 1) if row["avg_downtime_days"] else 0,
            "backup_rate": round(row["backup_rate"], 1) if row["backup_rate"] else 0,
            "ir_firm_rate": round(row["ir_firm_rate"], 1) if row["ir_firm_rate"] else 0,
            "forensics_rate": round(row["forensics_rate"], 1) if row["forensics_rate"] else 0,
            "total": row["total"],
        }
    return result


def get_ransomware_geo(
    conn: sqlite3.Connection,
    limit_families: int = 6,
    limit_countries: int = 5,
) -> List[Dict[str, Any]]:
    """Per-family geographic targeting (small multiples)."""
    # Get top families first
    cur = conn.execute(
        f"""
        SELECT COALESCE(ef.ransomware_family, ef.threat_actor_name) as family
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.attack_category LIKE '%ransomware%'
          AND (
            (ef.ransomware_family IS NOT NULL AND ef.ransomware_family != '')
            OR (ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != '')
          )
          AND {_live_incident_exists('ef')}
        GROUP BY family
        ORDER BY COUNT(*) DESC
        LIMIT ?
        """,
        (limit_families,)
    )
    raw_families = [row["family"] for row in cur.fetchall()]
    # Normalize and deduplicate family names
    seen: set[str] = set()
    families: list[str] = []
    raw_to_canonical: Dict[str, str] = {}
    for f in raw_families:
        canonical = _normalize_ransomware_family(f)
        if canonical and canonical not in seen:
            seen.add(canonical)
            families.append(canonical)
        if canonical:
            raw_to_canonical[f] = canonical

    result = []
    for canonical in families:
        # Collect all raw variants that map to this canonical name
        raw_variants = [r for r, c in raw_to_canonical.items() if c == canonical]
        if not raw_variants:
            raw_variants = [canonical]
        placeholders = ",".join("?" * len(raw_variants))
        cur = conn.execute(
            f"""
            SELECT
                COALESCE(ef.country, 'Unknown') as country,
                COUNT(*) as count
            FROM incident_enrichments_flat ef
            WHERE ef.is_education_related = 1
              AND (ef.ransomware_family IN ({placeholders}) OR ef.threat_actor_name IN ({placeholders}))
              AND {_live_incident_exists('ef')}
            GROUP BY country
            ORDER BY count DESC
            LIMIT ?
            """,
            raw_variants + raw_variants + [limit_countries]
        )
        countries = [dict(row) for row in cur.fetchall()]
        result.append({"family": canonical, "countries": countries})
    return result


def get_threat_actor_categories(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Threat actor category distribution from flat table."""
    cur = conn.execute(
        f"""
        SELECT ef.threat_actor_category, COUNT(*) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != ''
          AND ef.threat_actor_category IS NOT NULL AND ef.threat_actor_category != ''
          AND ef.threat_actor_category NOT IN ('unknown', 'Unknown', 'other')
          AND {_live_incident_exists('ef')}
        GROUP BY ef.threat_actor_category
        ORDER BY count DESC
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    total = sum(r["count"] for r in rows)
    return [
        {
            "category": r["threat_actor_category"],
            "count": r["count"],
            "percentage": round(r["count"] / total * 100, 1) if total > 0 else 0,
        }
        for r in rows
    ]


def get_threat_actor_motivations(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Threat actor motivation distribution from flat table."""
    cur = conn.execute(
        f"""
        SELECT ef.threat_actor_motivation, COUNT(*) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != ''
          AND ef.threat_actor_motivation IS NOT NULL AND ef.threat_actor_motivation != ''
          AND ef.threat_actor_motivation NOT IN ('unknown', 'Unknown', 'other')
          AND {_live_incident_exists('ef')}
        GROUP BY ef.threat_actor_motivation
        ORDER BY count DESC
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    total = sum(r["count"] for r in rows)
    return [
        {
            "category": r["threat_actor_motivation"],
            "count": r["count"],
            "percentage": round(r["count"] / total * 100, 1) if total > 0 else 0,
        }
        for r in rows
    ]


def get_threat_actor_timeline(
    conn: sqlite3.Connection,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """Monthly activity per threat actor (scatter chart)."""
    cur = conn.execute(
        f"""
        SELECT
            ef.threat_actor_name as actor,
            strftime('%Y-%m', i.incident_date) as month,
            COUNT(*) as count
        FROM incident_enrichments_flat ef
        JOIN incidents i ON ef.incident_id = i.incident_id
        WHERE ef.is_education_related = 1
          AND ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != ''
          AND i.incident_date IS NOT NULL
          AND ef.threat_actor_name IN (
            SELECT ef2.threat_actor_name
            FROM incident_enrichments_flat ef2
            WHERE ef2.is_education_related = 1
              AND ef2.threat_actor_name IS NOT NULL AND ef2.threat_actor_name != ''
              AND {_live_incident_exists('ef2')}
            GROUP BY ef2.threat_actor_name
            ORDER BY COUNT(*) DESC
            LIMIT ?
          )
        GROUP BY actor, month
        ORDER BY month ASC
        """,
        (limit,)
    )
    return [dict(row) for row in cur.fetchall()]


def get_actor_ransomware_matrix(
    conn: sqlite3.Connection,
    limit_actors: int = 10,
    limit_families: int = 8,
) -> Dict[str, Any]:
    """Actor-to-ransomware-family cross-tabulation."""
    # Get top actors
    cur = conn.execute(
        f"""
        SELECT ef.threat_actor_name
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != ''
          AND {_live_incident_exists('ef')}
        GROUP BY ef.threat_actor_name ORDER BY COUNT(*) DESC LIMIT ?
        """,
        (limit_actors,)
    )
    actors = [row["threat_actor_name"] for row in cur.fetchall()]

    # Get top families
    cur = conn.execute(
        f"""
        SELECT ef.ransomware_family as family
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1 AND ef.attack_category LIKE '%ransomware%'
          AND ef.ransomware_family IS NOT NULL AND ef.ransomware_family != ''
          AND {_live_incident_exists('ef')}
        GROUP BY family ORDER BY COUNT(*) DESC LIMIT ?
        """,
        (limit_families * 3,)  # fetch extra to account for normalization merges
    )
    seen_fam: set[str] = set()
    families: list[str] = []
    raw_family_map: Dict[str, str] = {}
    for row in cur.fetchall():
        canonical = _normalize_ransomware_family(row["family"])
        if canonical and canonical not in seen_fam:
            seen_fam.add(canonical)
            families.append(canonical)
            if len(families) >= limit_families:
                break
        if canonical:
            raw_family_map[row["family"]] = canonical

    if not actors or not families:
        return {"actors": actors, "families": families, "matrix": []}

    # Build matrix — query raw variants, normalize in Python
    actor_placeholders = ",".join("?" * len(actors))
    raw_variants = list(raw_family_map.keys()) or families
    family_placeholders = ",".join("?" * len(raw_variants))
    cur = conn.execute(
        f"""
        SELECT
            ef.threat_actor_name as actor,
            ef.ransomware_family as family,
            COUNT(*) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.threat_actor_name IN ({actor_placeholders})
          AND ef.ransomware_family IN ({family_placeholders})
          AND {_live_incident_exists('ef')}
        GROUP BY actor, family
        """,
        actors + raw_variants,
    )
    # Merge rows whose raw family normalizes to the same canonical name
    merged_matrix: dict[tuple, int] = {}
    for row in cur.fetchall():
        canonical = raw_family_map.get(row["family"]) or _normalize_ransomware_family(row["family"])
        if canonical and canonical in families:
            key = (row["actor"], canonical)
            merged_matrix[key] = merged_matrix.get(key, 0) + row["count"]
    matrix = [{"actor": k[0], "family": k[1], "count": v} for k, v in merged_matrix.items()]
    return {"actors": actors, "families": families, "matrix": matrix}


def get_actor_targeting(
    conn: sqlite3.Connection,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """Per-actor country distribution (stacked horizontal bars)."""
    cur = conn.execute(
        f"""
        SELECT
            ef.threat_actor_name as actor,
            COALESCE(ef.country, 'Unknown') as country,
            COUNT(*) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != ''
          AND ef.threat_actor_name IN (
            SELECT ef2.threat_actor_name
            FROM incident_enrichments_flat ef2
            WHERE ef2.is_education_related = 1
              AND ef2.threat_actor_name IS NOT NULL AND ef2.threat_actor_name != ''
              AND {_live_incident_exists('ef2')}
            GROUP BY ef2.threat_actor_name ORDER BY COUNT(*) DESC LIMIT ?
          )
          AND {_live_incident_exists('ef')}
        GROUP BY actor, country
        ORDER BY actor, count DESC
        """,
        (limit,)
    )
    # Group by actor
    actors_map: Dict[str, list] = {}
    for row in cur.fetchall():
        actor = row["actor"]
        if actor not in actors_map:
            actors_map[actor] = []
        actors_map[actor].append({"country": row["country"], "count": row["count"]})
    return [{"actor": actor, "countries": countries} for actor, countries in actors_map.items()]


def get_institution_types(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Institution type distribution."""
    cur = conn.execute(
        f"""
        SELECT
            ef.institution_type as category,
            COUNT(*) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.institution_type IS NOT NULL AND ef.institution_type != ''
          AND {_live_incident_exists('ef')}
        GROUP BY category
        ORDER BY count DESC
        """
    )
    rows = cur.fetchall()
    merged: dict[str, int] = {}
    for row in rows:
        canonical = _normalize_institution_type(row["category"])
        if canonical is None:
            continue
        merged[canonical] = merged.get(canonical, 0) + row["count"]
    total = sum(merged.values())
    return [
        {"category": name, "count": count,
         "percentage": round(count / total * 100, 1) if total > 0 else 0}
        for name, count in sorted(merged.items(), key=lambda x: -x[1])
    ]


def get_operational_impact(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Aggregate boolean operational impact columns."""
    ops = [
        ("teaching_disrupted", "Teaching Disrupted"),
        ("research_disrupted", "Research Disrupted"),
        ("admissions_disrupted", "Admissions Disrupted"),
        ("enrollment_disrupted", "Enrollment Disrupted"),
        ("payroll_disrupted", "Payroll Disrupted"),
        ("classes_cancelled", "Classes Cancelled"),
        ("exams_postponed", "Exams Postponed"),
    ]
    # Get total for percentage calc
    cur = conn.execute(
        f"""
        SELECT COUNT(*) as total FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND {_live_incident_exists('ef')}
        """
    )
    total = cur.fetchone()["total"]

    result = []
    for col, label in ops:
        cur = conn.execute(
            f"""
            SELECT COUNT(*) as count FROM incident_enrichments_flat ef
            WHERE ef.is_education_related = 1 AND ef.{col} = 1
              AND {_live_incident_exists('ef')}
            """
        )
        count = cur.fetchone()["count"]
        result.append({
            "category": label,
            "count": count,
            "percentage": round(count / total * 100, 1) if total > 0 else 0,
        })
    return result


def get_financial_impact_by_year(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Financial breakdown by year (stacked bar)."""
    cur = conn.execute(
        """
        SELECT
            strftime('%Y', i.incident_date) as year,
            SUM(ef.ransom_amount) as ransom_cost,
            SUM(ef.recovery_costs_max) as recovery_cost,
            SUM(ef.legal_costs) as legal_cost,
            SUM(ef.notification_costs) as notification_cost,
            COUNT(*) as incident_count
        FROM incident_enrichments_flat ef
        JOIN incidents i ON ef.incident_id = i.incident_id
        WHERE ef.is_education_related = 1
          AND i.incident_date IS NOT NULL
        GROUP BY year
        ORDER BY year ASC
        """
    )
    return [dict(row) for row in cur.fetchall()]


def get_data_impact_stats(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Breach metrics and record ranges."""
    cur = conn.execute(
        f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN data_breached = 1 THEN 1 ELSE 0 END) as breached_count,
            SUM(CASE WHEN data_exfiltrated = 1 THEN 1 ELSE 0 END) as exfiltrated_count,
            SUM(records_affected_exact) as total_records,
            AVG(CASE WHEN records_affected_exact > 0 THEN records_affected_exact END) as avg_records,
            MAX(records_affected_exact) as max_records,
            SUM(pii_records_leaked) as total_pii_leaked
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND {_live_incident_exists('ef')}
        """
    )
    row = dict(cur.fetchone())
    row["breach_rate"] = round(row["breached_count"] / row["total"] * 100, 1) if row["total"] > 0 else 0
    row["exfiltration_rate"] = round(row["exfiltrated_count"] / row["total"] * 100, 1) if row["total"] > 0 else 0
    return row


def get_regulatory_impact_stats(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Regulatory compliance aggregation."""
    cur = conn.execute(
        f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN gdpr_breach = 1 THEN 1 ELSE 0 END) as gdpr_count,
            SUM(CASE WHEN hipaa_breach = 1 THEN 1 ELSE 0 END) as hipaa_count,
            SUM(CASE WHEN ferpa_breach = 1 THEN 1 ELSE 0 END) as ferpa_count,
            SUM(CASE WHEN breach_notification_required = 1 THEN 1 ELSE 0 END) as notification_required,
            SUM(CASE WHEN notifications_sent = 1 THEN 1 ELSE 0 END) as notifications_sent,
            SUM(CASE WHEN fine_imposed = 1 THEN 1 ELSE 0 END) as fines_imposed,
            SUM(fine_amount) as total_fines,
            SUM(CASE WHEN lawsuits_filed = 1 THEN 1 ELSE 0 END) as lawsuits_count,
            SUM(CASE WHEN class_action = 1 THEN 1 ELSE 0 END) as class_action_count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND {_live_incident_exists('ef')}
        """
    )
    return dict(cur.fetchone())


def get_recovery_effectiveness(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Recovery effectiveness stats."""
    cur = conn.execute(
        f"""
        SELECT
            COUNT(*) as total,
            AVG(recovery_timeframe_days) as avg_recovery_days,
            AVG(downtime_days) as avg_downtime_days,
            SUM(CASE WHEN from_backup = 1 THEN 1 ELSE 0 END) as backup_count,
            SUM(CASE WHEN incident_response_firm IS NOT NULL AND incident_response_firm != '' THEN 1 ELSE 0 END) as ir_firm_count,
            SUM(CASE WHEN forensics_firm IS NOT NULL AND forensics_firm != '' THEN 1 ELSE 0 END) as forensics_count,
            SUM(CASE WHEN mfa_implemented = 1 THEN 1 ELSE 0 END) as mfa_post_count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND {_live_incident_exists('ef')}
        """
    )
    row = dict(cur.fetchone())
    total = row["total"] or 1
    row["backup_rate"] = round(row["backup_count"] / total * 100, 1)
    row["ir_firm_rate"] = round(row["ir_firm_count"] / total * 100, 1)
    row["forensics_rate"] = round(row["forensics_count"] / total * 100, 1)
    row["mfa_adoption_rate"] = round(row["mfa_post_count"] / total * 100, 1)
    return row


def get_transparency_metrics(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Disclosure metrics."""
    cur = conn.execute(
        f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN public_disclosure = 1 THEN 1 ELSE 0 END) as disclosed_count,
            AVG(CASE WHEN disclosure_delay_days > 0 THEN disclosure_delay_days END) as avg_delay_days,
            transparency_level,
            COUNT(*) as level_count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND {_live_incident_exists('ef')}
        GROUP BY transparency_level
        """
    )
    rows = cur.fetchall()
    total = sum(r["total"] for r in rows)
    levels = []
    total_disclosed = 0
    avg_delay = None
    for row in rows:
        levels.append({
            "level": row["transparency_level"] or "unknown",
            "count": row["level_count"],
        })
        total_disclosed += row["disclosed_count"] or 0
        if row["avg_delay_days"]:
            avg_delay = row["avg_delay_days"]

    return {
        "total": total,
        "disclosed_count": total_disclosed,
        "disclosure_rate": round(total_disclosed / total * 100, 1) if total > 0 else 0,
        "avg_delay_days": round(avg_delay, 1) if avg_delay else None,
        "levels": levels,
    }


def get_user_impact_totals(conn: sqlite3.Connection) -> Dict[str, Any]:
    """User category impact totals."""
    cur = conn.execute(
        f"""
        SELECT
            SUM(students_affected) as students,
            SUM(staff_affected) as staff,
            SUM(faculty_affected) as faculty,
            SUM(users_affected_exact) as total_individuals,
            COUNT(CASE WHEN students_affected > 0 OR staff_affected > 0 OR faculty_affected > 0 THEN 1 END) as incidents_with_data
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND {_live_incident_exists('ef')}
        """
    )
    return dict(cur.fetchone())


# ============================================================
# Extended Cross-Dimensional Analytics
# ============================================================

def get_institution_risk_matrix(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Cross-tabulation of institution_type × attack_category."""
    cur = conn.execute(
        f"""
        SELECT
            ef.institution_type,
            ef.attack_category,
            COUNT(*) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.institution_type IS NOT NULL AND ef.institution_type != '' AND ef.institution_type != 'unknown'
          AND ef.attack_category IS NOT NULL AND ef.attack_category != ''
          AND {_live_incident_exists('ef')}
        GROUP BY institution_type, attack_category
        HAVING count >= 2
        ORDER BY count DESC
        """
    )
    return [dict(row) for row in cur.fetchall()]


def get_recovery_by_attack_type(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Avg recovery and downtime days by attack category."""
    cur = conn.execute(
        f"""
        SELECT
            ef.attack_category,
            ROUND(AVG(recovery_timeframe_days), 1) as avg_recovery_days,
            ROUND(AVG(downtime_days), 1) as avg_downtime_days,
            COUNT(*) as incident_count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.attack_category IS NOT NULL AND ef.attack_category != ''
          AND (ef.recovery_timeframe_days > 0 OR ef.downtime_days > 0)
          AND {_live_incident_exists('ef')}
        GROUP BY attack_category
        HAVING incident_count >= 3
        ORDER BY avg_recovery_days DESC
        """
    )
    return [dict(row) for row in cur.fetchall()]


def get_attack_vector_by_institution(
    conn: sqlite3.Connection,
    limit: int = 8,
) -> Dict[str, Any]:
    """Attack vector distribution per institution type (top N institution types)."""
    # Get top institution types
    cur = conn.execute(
        f"""
        SELECT ef.institution_type, COUNT(*) as cnt
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.institution_type IS NOT NULL AND ef.institution_type != '' AND ef.institution_type != 'unknown'
          AND ef.attack_vector IS NOT NULL AND ef.attack_vector != '' AND ef.attack_vector != 'unknown'
          AND {_live_incident_exists('ef')}
        GROUP BY ef.institution_type
        ORDER BY cnt DESC
        LIMIT ?
        """,
        (limit,)
    )
    top_types = [row["institution_type"] for row in cur.fetchall()]
    if not top_types:
        return {"institution_types": [], "vectors": [], "data": []}

    placeholders = ",".join("?" * len(top_types))
    cur = conn.execute(
        f"""
        SELECT
            ef.institution_type,
            CASE
                WHEN ef.attack_vector IN ('unknown', 'other', 'Unknown', 'Other') THEN 'Unknown / Other'
                ELSE ef.attack_vector
            END as attack_vector,
            COUNT(*) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.institution_type IN ({placeholders})
          AND ef.attack_vector IS NOT NULL AND ef.attack_vector != ''
          AND {_live_incident_exists('ef')}
        GROUP BY institution_type, attack_vector
        ORDER BY institution_type, count DESC
        """,
        top_types
    )
    data = [dict(row) for row in cur.fetchall()]
    vectors = sorted(set(d["attack_vector"] for d in data))
    return {"institution_types": top_types, "vectors": vectors, "data": data}


def get_breach_severity_timeline(
    conn: sqlite3.Connection,
    months: int = 60,
) -> List[Dict[str, Any]]:
    """Monthly incident count + avg records breached over time."""
    cur = conn.execute(
        """
        SELECT
            strftime('%Y-%m', i.incident_date) as month,
            COUNT(*) as incident_count,
            AVG(CASE WHEN ef.records_affected_exact > 0 THEN ef.records_affected_exact END) as avg_records,
            SUM(CASE WHEN ef.data_breached = 1 THEN 1 ELSE 0 END) as breach_count
        FROM incidents i
        JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE ef.is_education_related = 1
          AND i.incident_date IS NOT NULL
          AND i.incident_date >= date('now', ? || ' months')
        GROUP BY month
        HAVING month IS NOT NULL
        ORDER BY month ASC
        """,
        (f"-{months}",)
    )
    return [dict(row) for row in cur.fetchall()]


def get_ransom_payment_by_year(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Ransom demanded vs paid amounts by year, with payment rate."""
    cur = conn.execute(
        """
        SELECT
            strftime('%Y', i.incident_date) as year,
            COUNT(*) as total_incidents,
            COUNT(CASE WHEN ef.was_ransom_demanded = 1 THEN 1 END) as demanded_count,
            COUNT(CASE WHEN ef.ransom_paid = 1 THEN 1 END) as paid_count,
            SUM(ef.ransom_amount) as total_demanded,
            SUM(ef.ransom_paid_amount) as total_paid,
            CASE WHEN COUNT(CASE WHEN ef.was_ransom_demanded = 1 THEN 1 END) > 0
                 THEN ROUND(COUNT(CASE WHEN ef.ransom_paid = 1 THEN 1 END) * 100.0 /
                      COUNT(CASE WHEN ef.was_ransom_demanded = 1 THEN 1 END), 1)
                 ELSE 0 END as payment_rate
        FROM incidents i
        JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE ef.is_education_related = 1
          AND ef.attack_category LIKE '%ransomware%'
          AND i.incident_date IS NOT NULL
        GROUP BY year
        HAVING year IS NOT NULL
        ORDER BY year ASC
        """
    )
    return [dict(row) for row in cur.fetchall()]


def get_ransomware_family_trend(
    conn: sqlite3.Connection,
    limit: int = 8,
) -> Dict[str, Any]:
    """Top N ransomware families by month (stacked area chart)."""
    cur = conn.execute(
        f"""
        SELECT COALESCE(ef.ransomware_family, ef.threat_actor_name) as family, COUNT(*) as cnt
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.attack_category LIKE '%ransomware%'
          AND (
            (ef.ransomware_family IS NOT NULL AND ef.ransomware_family != '')
            OR (ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != '')
          )
          AND {_live_incident_exists('ef')}
        GROUP BY family
        ORDER BY cnt DESC
        LIMIT ?
        """,
        (limit,)
    )
    raw_top = [row["family"] for row in cur.fetchall()]
    # Normalize and keep unique canonical names
    seen_fam: set[str] = set()
    top_families: list[str] = []
    raw_to_can: Dict[str, str] = {}
    for f in raw_top:
        c = _normalize_ransomware_family(f)
        if c:
            raw_to_can[f] = c
            if c not in seen_fam:
                seen_fam.add(c)
                top_families.append(c)

    if not top_families:
        return {"families": [], "data": []}

    # Fetch time-series for all raw variants at once
    all_raw = list(raw_to_can.keys())
    placeholders = ",".join("?" * len(all_raw))
    cur = conn.execute(
        f"""
        SELECT
            strftime('%Y-%m', i.incident_date) as month,
            COALESCE(ef.ransomware_family, ef.threat_actor_name) as family,
            COUNT(*) as count
        FROM incidents i
        JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE ef.is_education_related = 1
          AND COALESCE(ef.ransomware_family, ef.threat_actor_name) IN ({placeholders})
          AND i.incident_date IS NOT NULL
        GROUP BY month, family
        ORDER BY month ASC
        """,
        all_raw,
    )
    # Re-key rows by canonical name
    merged_data: dict[tuple, int] = {}
    for row in cur.fetchall():
        canonical = raw_to_can.get(row["family"], row["family"])
        key = (row["month"], canonical)
        merged_data[key] = merged_data.get(key, 0) + row["count"]
    data = [
        {"month": k[0], "family": k[1], "count": v}
        for k, v in sorted(merged_data.items(), key=lambda x: (x[0][0] or "", x[0][1] or ""))
        if k[0] is not None
    ]
    return {"families": top_families, "data": data}


def get_actor_institution_targeting(
    conn: sqlite3.Connection,
    limit: int = 12,
) -> Dict[str, Any]:
    """Top actors × institution type cross-tabulation."""
    cur = conn.execute(
        f"""
        SELECT ef.threat_actor_name, COUNT(*) as cnt
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != ''
          AND {_live_incident_exists('ef')}
        GROUP BY ef.threat_actor_name
        ORDER BY cnt DESC
        LIMIT ?
        """,
        (limit,)
    )
    actors = [row["threat_actor_name"] for row in cur.fetchall()]
    if not actors:
        return {"actors": [], "institution_types": [], "data": []}

    placeholders = ",".join("?" * len(actors))
    cur = conn.execute(
        f"""
        SELECT
            ef.threat_actor_name as actor,
            COALESCE(ef.institution_type, 'unknown') as institution_type,
            COUNT(*) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.threat_actor_name IN ({placeholders})
          AND {_live_incident_exists('ef')}
        GROUP BY actor, institution_type
        ORDER BY count DESC
        """,
        actors
    )
    data = [dict(row) for row in cur.fetchall()]
    inst_types = sorted(set(d["institution_type"] for d in data))
    return {"actors": actors, "institution_types": inst_types, "data": data}


def get_actor_ttp_profile(
    conn: sqlite3.Connection,
    limit: int = 8,
) -> Dict[str, Any]:
    """Top actors with their MITRE ATT&CK tactic distribution."""
    # Get top actors that have MITRE data
    cur = conn.execute(
        f"""
        SELECT ef.threat_actor_name, COUNT(*) as cnt
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != ''
          AND (ef.mitre_techniques_json IS NOT NULL OR ef.mitre_techniques_count > 0)
          AND {_live_incident_exists('ef')}
        GROUP BY ef.threat_actor_name
        ORDER BY cnt DESC
        LIMIT ?
        """,
        (limit,)
    )
    actors = [row["threat_actor_name"] for row in cur.fetchall()]
    if not actors:
        return {"actors": [], "tactics": [], "data": []}

    placeholders = ",".join("?" * len(actors))
    cur = conn.execute(
        f"""
        SELECT ef.threat_actor_name as actor, ef.mitre_techniques_json
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.threat_actor_name IN ({placeholders})
          AND ef.mitre_techniques_json IS NOT NULL
          AND {_live_incident_exists('ef')}
        """,
        actors
    )

    # Parse techniques and aggregate by actor + tactic
    actor_tactic_counts: Dict[str, Dict[str, int]] = {}
    for row in cur.fetchall():
        actor = row["actor"]
        if actor not in actor_tactic_counts:
            actor_tactic_counts[actor] = {}
        try:
            techniques = json.loads(row["mitre_techniques_json"])
            for t in techniques:
                if isinstance(t, dict):
                    raw_tactic = t.get("tactic")
                    tech_id = t.get("technique_id", "")
                    if raw_tactic:
                        tactic = _normalize_mitre_tactic(raw_tactic)
                    else:
                        tactic = _resolve_tactic_from_technique_id(tech_id) or "Unknown"
                elif isinstance(t, str) and t.startswith("T"):
                    tech_id = t.split(":")[0].strip()
                    tactic = _resolve_tactic_from_technique_id(tech_id) or "Unknown"
                else:
                    continue
                if tactic != "Unknown":
                    actor_tactic_counts[actor][tactic] = actor_tactic_counts[actor].get(tactic, 0) + 1
        except Exception:
            continue

    # Flatten to list
    data = []
    all_tactics = set()
    for actor, tactics in actor_tactic_counts.items():
        for tactic, count in tactics.items():
            data.append({"actor": actor, "tactic": tactic, "count": count})
            all_tactics.add(tactic)

    return {"actors": list(actor_tactic_counts.keys()), "tactics": sorted(all_tactics), "data": data}


def get_disclosure_timeline(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Disclosure delay over time, by country."""
    cur = conn.execute(
        """
        SELECT
            i.incident_date,
            ef.disclosure_delay_days,
            COALESCE(ef.country, 'Unknown') as country,
            ef.transparency_level
        FROM incidents i
        JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE ef.is_education_related = 1
          AND ef.disclosure_delay_days IS NOT NULL
          AND ef.disclosure_delay_days > 0
          AND ef.disclosure_delay_days < 1000
          AND i.incident_date IS NOT NULL
        ORDER BY i.incident_date ASC
        """
    )
    return [dict(row) for row in cur.fetchall()]


def get_breach_by_institution_type(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Breach rate and avg records per institution type."""
    cur = conn.execute(
        f"""
        SELECT
            ef.institution_type,
            COUNT(*) as total_incidents,
            SUM(CASE WHEN data_breached = 1 THEN 1 ELSE 0 END) as breach_count,
            ROUND(SUM(CASE WHEN data_breached = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as breach_rate,
            AVG(CASE WHEN records_affected_exact > 0 THEN records_affected_exact END) as avg_records,
            SUM(records_affected_exact) as total_records
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.institution_type IS NOT NULL AND ef.institution_type != '' AND ef.institution_type != 'unknown'
          AND {_live_incident_exists('ef')}
        GROUP BY institution_type
        HAVING total_incidents >= 3
        ORDER BY breach_rate DESC
        """
    )
    return [dict(row) for row in cur.fetchall()]


def get_raw_incident_data(
    conn: sqlite3.Connection,
    incident_id: Optional[str] = None,
    has_mitre: Optional[bool] = None,
    attack_category: Optional[str] = None,
    country: Optional[str] = None,
    has_enrichment: Optional[bool] = None,
    limit: int = 20,
    offset: int = 0,
) -> Dict[str, Any]:
    """
    Get raw incident data from both tables for debugging/inspection.
    Returns data from incident_enrichments_flat + incident_enrichments (JSON blob).
    """
    conditions = ["ef.is_education_related = 1"]
    params: list = []

    if incident_id:
        conditions.append("ef.incident_id LIKE ?")
        params.append(f"%{incident_id}%")
    if has_mitre is True:
        conditions.append("(ef.mitre_techniques_count > 0 OR ef.mitre_techniques_json IS NOT NULL)")
    elif has_mitre is False:
        conditions.append("(ef.mitre_techniques_count = 0 OR ef.mitre_techniques_count IS NULL)")
        conditions.append("ef.mitre_techniques_json IS NULL")
    if attack_category:
        conditions.append("ef.attack_category LIKE ?")
        params.append(f"%{attack_category}%")
    if country:
        conditions.append("ef.country LIKE ?")
        params.append(f"%{country}%")
    if has_enrichment is True:
        conditions.append("ie.enrichment_data IS NOT NULL")
    elif has_enrichment is False:
        conditions.append("ie.enrichment_data IS NULL")

    where_clause = " AND ".join(conditions)

    # Count total
    count_sql = f"""
        SELECT COUNT(*) as total
        FROM incident_enrichments_flat ef
        LEFT JOIN incident_enrichments ie ON ef.incident_id = ie.incident_id
        WHERE {where_clause}
    """
    total = conn.execute(count_sql, params).fetchone()["total"]

    # Fetch rows
    data_sql = f"""
        SELECT
            ef.*,
            ie.enrichment_data,
            i.incident_date,
            i.title
        FROM incident_enrichments_flat ef
        LEFT JOIN incident_enrichments ie ON ef.incident_id = ie.incident_id
        LEFT JOIN incidents i ON ef.incident_id = i.incident_id
        WHERE {where_clause}
        ORDER BY ef.incident_id
        LIMIT ? OFFSET ?
    """
    cur = conn.execute(data_sql, params + [limit, offset])
    rows = []
    for row in cur.fetchall():
        r = dict(row)
        # Parse enrichment_data JSON for display
        if r.get("enrichment_data"):
            try:
                r["enrichment_data"] = json.loads(r["enrichment_data"])
            except Exception:
                pass  # leave as string
        # Parse mitre_techniques_json for display
        if r.get("mitre_techniques_json"):
            try:
                r["mitre_techniques_json"] = json.loads(r["mitre_techniques_json"])
            except Exception:
                pass
        rows.append(r)

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "incidents": rows,
    }


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
        f"""
        SELECT DISTINCT ef.attack_category FROM incident_enrichments_flat ef
        WHERE ef.attack_category IS NOT NULL AND ef.attack_category != ''
          AND {_live_incident_exists('ef')}
        ORDER BY attack_category
        """
    )
    options["attack_categories"] = [row["attack_category"] for row in cur.fetchall()]
    
    # Ransomware families
    # Ransomware families (use threat_actor_name as fallback for ransomware incidents)
    cur = conn.execute(
        f"""
        SELECT DISTINCT COALESCE(ef.ransomware_family, ef.threat_actor_name) as family
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.attack_category LIKE '%ransomware%'
          AND (
            (ef.ransomware_family IS NOT NULL AND ef.ransomware_family != '')
            OR (ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != '')
          )
          AND {_live_incident_exists('ef')}
        ORDER BY family
        """
    )
    options["ransomware_families"] = sorted({
        c for row in cur.fetchall()
        if (c := _normalize_ransomware_family(row["family"])) is not None
    })
    
    # Threat actors
    cur = conn.execute(
        f"""
        SELECT DISTINCT ef.threat_actor_name FROM incident_enrichments_flat ef
        WHERE ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != ''
          AND {_live_incident_exists('ef')}
        ORDER BY threat_actor_name
        """
    )
    options["threat_actors"] = [row["threat_actor_name"] for row in cur.fetchall()]
    
    # Institution types
    cur = conn.execute(
        f"""
        SELECT DISTINCT institution_type FROM incidents 
        WHERE institution_type IS NOT NULL AND institution_type != ''
        UNION
        SELECT DISTINCT ef.institution_type FROM incident_enrichments_flat ef
        WHERE ef.institution_type IS NOT NULL AND ef.institution_type != ''
          AND {_live_incident_exists('ef')}
        ORDER BY institution_type
        """
    )
    options["institution_types"] = sorted({
        c for row in cur.fetchall()
        if (c := _normalize_institution_type(row["institution_type"])) is not None
    })
    
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


# ============================================================
# Interactive / Nivo Visualization Endpoints
# ============================================================


def get_attack_flow(conn: sqlite3.Connection) -> Dict[str, Any]:
    """3-column Sankey: Attack Vector → Attack Category → Impact Outcome."""
    cur = conn.execute(
        f"""
        SELECT
            COALESCE(
                NULLIF(ef.initial_access_vector, ''),
                NULLIF(ef.attack_vector, '')
            ) as vector,
            NULLIF(ef.attack_category, '') as category,
            CASE
                WHEN ef.data_exfiltrated = 1 AND ef.was_ransom_demanded = 1 THEN 'Breach + Ransom'
                WHEN ef.data_exfiltrated = 1 THEN 'Data Breach'
                WHEN ef.was_ransom_demanded = 1 THEN 'Ransom Only'
                ELSE 'Other Impact'
            END as outcome,
            COUNT(*) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND {_live_incident_exists('ef')}
        GROUP BY vector, category, outcome
        HAVING count >= 1
        ORDER BY count DESC
        """
    )
    rows = [dict(r) for r in cur.fetchall()]

    links = []

    # Prefix node IDs by level so the same label (e.g. "Phishing")
    # in two different Sankey columns doesn't create a circular link in d3-sankey.
    vec_cat: Dict[tuple, int] = {}
    cat_out: Dict[tuple, int] = {}
    for r in rows:
        v_raw, c_raw, o, cnt = r["vector"], r["category"], r["outcome"], r["count"]
        v = _normalize_attack_vector(v_raw)
        c = _normalize_attack_category(c_raw)
        if v is None or c is None:
            continue
        vid, cid, oid = f"vec:{v}", f"cat:{c}", f"out:{o}"
        vec_cat[(vid, cid)] = vec_cat.get((vid, cid), 0) + cnt
        cat_out[(cid, oid)] = cat_out.get((cid, oid), 0) + cnt

    for (vid, cid), cnt in vec_cat.items():
        if cnt >= 2:
            links.append({"source": vid, "target": cid, "value": cnt})
    for (cid, oid), cnt in cat_out.items():
        if cnt >= 2:
            links.append({"source": cid, "target": oid, "value": cnt})

    # Collect only nodes that appear in links
    used: set = set()
    for lnk in links:
        used.add(lnk["source"])
        used.add(lnk["target"])

    nodes = [{"id": n} for n in sorted(used)]
    return {"nodes": nodes, "links": links}


def get_mitre_sunburst(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Hierarchical MITRE: Tactic → Technique tree for sunburst."""
    cur = conn.execute(
        f"""
        SELECT ef.mitre_techniques_json
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.mitre_techniques_json IS NOT NULL
          AND ef.mitre_techniques_json != '[]'
          AND ef.mitre_techniques_json != ''
          AND {_live_incident_exists('ef')}
        """
    )

    tactic_tech: Dict[str, Dict[str, int]] = {}

    for row in cur.fetchall():
        try:
            techniques = json.loads(row["mitre_techniques_json"])
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(techniques, list):
            continue

        for tech in techniques:
            if not isinstance(tech, dict):
                continue
            tech_id = tech.get("technique_id", "")
            tech_name = tech.get("technique_name", tech_id)
            raw_tactic = tech.get("tactic")

            if raw_tactic:
                tactic = raw_tactic.replace("-", " ").title()
            else:
                tactic = _resolve_tactic_from_technique_id(tech_id) or "Unknown"

            label = f"{tech_id}: {tech_name}" if tech_id and tech_name else (tech_id or tech_name or "Unknown")

            if tactic not in tactic_tech:
                tactic_tech[tactic] = {}
            tactic_tech[tactic][label] = tactic_tech[tactic].get(label, 0) + 1

    children = []
    for tactic, techs in sorted(tactic_tech.items(), key=lambda x: -sum(x[1].values())):
        tech_children = [
            {"id": t, "value": c}
            for t, c in sorted(techs.items(), key=lambda x: -x[1])
        ]
        children.append({"id": tactic, "children": tech_children})

    return {"id": "MITRE ATT&CK", "children": children}


def get_actor_network(conn: sqlite3.Connection, min_incidents: int = 2) -> Dict[str, Any]:
    """Network graph: actors linked by shared ransomware families."""
    # Get actors with their ransomware families
    cur = conn.execute(
        f"""
        SELECT ef.threat_actor_name as actor, ef.ransomware_family as family, COUNT(*) as cnt
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != ''
          AND ef.ransomware_family IS NOT NULL AND ef.ransomware_family != ''
          AND {_live_incident_exists('ef')}
        GROUP BY actor, family
        HAVING cnt >= 1
        """
    )
    rows = [dict(r) for r in cur.fetchall()]

    # Build actor → families map
    actor_families: Dict[str, set] = {}
    actor_counts: Dict[str, int] = {}
    for r in rows:
        actor = r["actor"]
        if actor not in actor_families:
            actor_families[actor] = set()
            actor_counts[actor] = 0
        actor_families[actor].add(r["family"])
        actor_counts[actor] += r["cnt"]

    # Filter to actors with min_incidents
    actors = [a for a, c in actor_counts.items() if c >= min_incidents]

    # Build links: actors sharing a ransomware family
    links = []
    link_set: set = set()
    for i, a1 in enumerate(actors):
        for a2 in actors[i + 1:]:
            shared = actor_families.get(a1, set()) & actor_families.get(a2, set())
            if shared:
                key = tuple(sorted([a1, a2]))
                if key not in link_set:
                    link_set.add(key)
                    links.append({
                        "source": a1,
                        "target": a2,
                        "distance": max(30, 120 - len(shared) * 30),
                        "shared_families": sorted(shared),
                    })

    # Only include actors that appear in at least one link
    linked_actors = set()
    for lnk in links:
        linked_actors.add(lnk["source"])
        linked_actors.add(lnk["target"])

    # Add isolated high-count actors as well
    for a in actors:
        if actor_counts[a] >= 5:
            linked_actors.add(a)

    nodes = [
        {
            "id": a,
            "radius": min(28, max(8, actor_counts.get(a, 1) * 3)),
            "count": actor_counts.get(a, 0),
            "families": sorted(actor_families.get(a, set())),
        }
        for a in sorted(linked_actors)
    ]

    return {"nodes": nodes, "links": links}


def get_ransom_flow(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Sankey: Institution Type → Ransomware Family → Payment Outcome."""
    cur = conn.execute(
        f"""
        SELECT
            COALESCE(NULLIF(ef.institution_type, ''), 'Unknown') as inst_type,
            ef.ransomware_family as family,
            CASE
                WHEN ef.ransom_paid = 1 THEN 'Paid'
                WHEN ef.was_ransom_demanded = 1 AND (ef.ransom_paid = 0 OR ef.ransom_paid IS NULL) THEN 'Refused'
                ELSE 'Unknown Outcome'
            END as outcome,
            COUNT(*) as count,
            COALESCE(SUM(ef.ransom_amount), 0) as total_amount
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.attack_category LIKE '%ransomware%'
          AND {_live_incident_exists('ef')}
        GROUP BY inst_type, family, outcome
        HAVING count >= 1
        ORDER BY count DESC
        """
    )
    rows = [dict(r) for r in cur.fetchall()]

    nodes_set: set = set()
    inst_fam: Dict[tuple, Dict] = {}
    fam_out: Dict[tuple, Dict] = {}

    for r in rows:
        it, out = r["inst_type"], r["outcome"]
        fam = _normalize_ransomware_family(r["family"])
        if fam is None:
            continue  # skip unknown/unidentified families
        cnt, amt = r["count"], r["total_amount"]
        nodes_set.update([it, fam, out])

        k1 = (it, fam)
        if k1 not in inst_fam:
            inst_fam[k1] = {"count": 0, "amount": 0}
        inst_fam[k1]["count"] += cnt
        inst_fam[k1]["amount"] += amt

        k2 = (fam, out)
        if k2 not in fam_out:
            fam_out[k2] = {"count": 0, "amount": 0}
        fam_out[k2]["count"] += cnt
        fam_out[k2]["amount"] += amt

    links_by_count = []
    links_by_amount = []

    for (s, t), d in inst_fam.items():
        if d["count"] >= 1:
            links_by_count.append({"source": s, "target": t, "value": d["count"]})
            links_by_amount.append({"source": s, "target": t, "value": max(d["amount"], 1)})
    for (s, t), d in fam_out.items():
        if d["count"] >= 1:
            links_by_count.append({"source": s, "target": t, "value": d["count"]})
            links_by_amount.append({"source": s, "target": t, "value": max(d["amount"], 1)})

    used = set()
    for lnk in links_by_count:
        used.add(lnk["source"])
        used.add(lnk["target"])

    nodes = [{"id": n} for n in sorted(used)]
    return {
        "nodes": nodes,
        "links_by_count": links_by_count,
        "links_by_amount": links_by_amount,
    }


def get_country_attack_matrix(
    conn: sqlite3.Connection,
    limit_countries: int = 8,
    limit_categories: int = 6,
) -> Dict[str, Any]:
    """Country × Attack Category matrix for chord diagram."""
    # Top countries
    cur = conn.execute(
        f"""
        SELECT ef.country, COUNT(*) as cnt
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1 AND ef.country IS NOT NULL AND ef.country != ''
          AND {_live_incident_exists('ef')}
        GROUP BY country ORDER BY cnt DESC LIMIT ?
        """,
        (limit_countries,)
    )
    countries = [row["country"] for row in cur.fetchall()]

    # Top attack categories
    cur = conn.execute(
        f"""
        SELECT ef.attack_category, COUNT(*) as cnt
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1 AND ef.attack_category IS NOT NULL AND ef.attack_category != ''
          AND {_live_incident_exists('ef')}
        GROUP BY attack_category ORDER BY cnt DESC LIMIT ?
        """,
        (limit_categories,)
    )
    categories = [row["attack_category"] for row in cur.fetchall()]

    if not countries or not categories:
        return {"keys": [], "matrix": []}

    # All keys: countries + categories
    keys = countries + categories
    n = len(keys)
    matrix = [[0] * n for _ in range(n)]

    country_ph = ",".join("?" * len(countries))
    category_ph = ",".join("?" * len(categories))

    cur = conn.execute(
        f"""
        SELECT ef.country, ef.attack_category, COUNT(*) as cnt
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.country IN ({country_ph})
          AND ef.attack_category IN ({category_ph})
          AND {_live_incident_exists('ef')}
        GROUP BY ef.country, ef.attack_category
        """,
        countries + categories
    )

    country_idx = {c: i for i, c in enumerate(keys)}
    for row in cur.fetchall():
        ci = country_idx.get(row["country"])
        ai = country_idx.get(row["attack_category"])
        if ci is not None and ai is not None:
            matrix[ci][ai] = row["cnt"]
            matrix[ai][ci] = row["cnt"]

    return {"keys": keys, "matrix": matrix}
