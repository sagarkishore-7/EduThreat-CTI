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
                incident["systems_affected"] = json.loads(enrichment["systems_affected_codes"])
            except Exception:
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
    cur = conn.execute("""
        SELECT COUNT(*) as count FROM incidents i
        JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE ef.is_education_related = 1
    """)
    stats["education_incidents"] = cur.fetchone()["count"]

    # Enriched incidents (processed by LLM)
    cur = conn.execute("SELECT COUNT(*) as count FROM incidents WHERE llm_enriched = 1")
    stats["enriched_incidents"] = cur.fetchone()["count"]

    # Pending analysis (not yet processed by LLM)
    cur = conn.execute("SELECT COUNT(*) as count FROM incidents WHERE llm_enriched = 0")
    stats["unenriched_incidents"] = cur.fetchone()["count"]

    # Ransomware incidents
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
        """
        SELECT COUNT(*) as count FROM incident_enrichments_flat
        WHERE is_education_related = 1 AND data_breached = 1
        """
    )
    stats["incidents_with_data_breach"] = cur.fetchone()["count"]

    # Countries affected (education-related only)
    cur = conn.execute(
        """
        SELECT COUNT(DISTINCT ef.country) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1 AND ef.country IS NOT NULL AND ef.country != ''
        """
    )
    stats["countries_affected"] = cur.fetchone()["count"]

    # Unique threat actors
    cur = conn.execute(
        """
        SELECT COUNT(DISTINCT threat_actor_name) as count
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND threat_actor_name IS NOT NULL AND threat_actor_name != ''
        """
    )
    stats["unique_threat_actors"] = cur.fetchone()["count"]

    # Unique ransomware families
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

    # Data sources count
    cur = conn.execute(
        "SELECT COUNT(DISTINCT source) as count FROM incident_sources"
    )
    stats["data_sources"] = cur.fetchone()["count"]

    # Average recovery time (days) for education incidents
    cur = conn.execute(
        """
        SELECT AVG(recovery_timeframe_days) as avg_days
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND recovery_timeframe_days IS NOT NULL
          AND recovery_timeframe_days > 0
        """
    )
    row = cur.fetchone()
    stats["avg_recovery_days"] = round(row["avg_days"], 1) if row["avg_days"] else None

    # Total financial impact (sum of estimated costs)
    cur = conn.execute(
        """
        SELECT SUM(recovery_costs_max) as total
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND recovery_costs_max IS NOT NULL AND recovery_costs_max > 0
        """
    )
    row = cur.fetchone()
    stats["total_financial_impact"] = row["total"] if row["total"] else 0

    # MITRE techniques count
    cur = conn.execute(
        """
        SELECT COUNT(*) as count FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND mitre_techniques_count IS NOT NULL AND mitre_techniques_count > 0
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
        """
        SELECT
            COALESCE(attack_vector, 'unknown') as category,
            COUNT(*) as count
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND attack_vector IS NOT NULL AND attack_vector != ''
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
        """
        SELECT ef.mitre_techniques_json, ie.enrichment_data
        FROM incident_enrichments_flat ef
        LEFT JOIN incident_enrichments ie ON ef.incident_id = ie.incident_id
        WHERE ef.is_education_related = 1
          AND (ef.mitre_techniques_count > 0 OR ef.mitre_techniques_json IS NOT NULL)
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
        """
        SELECT
            CASE
                WHEN COALESCE(initial_access_vector, attack_vector) IN ('unknown', 'other', 'Unknown', 'Other')
                    THEN 'Unknown / Other'
                WHEN initial_access_vector IS NULL AND attack_vector IS NULL
                    THEN 'Unknown / Other'
                ELSE COALESCE(initial_access_vector, attack_vector)
            END as category,
            COUNT(*) as count
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND (initial_access_vector IS NOT NULL OR attack_vector IS NOT NULL)
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
            FROM incident_enrichments_flat
            WHERE is_education_related = 1 AND {col} = 1
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
    return [dict(row) for row in cur.fetchall()]


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
    result = []
    for row in cur.fetchall():
        r = dict(row)
        r["countries"] = [c for c in (r["countries"] or "").split(",") if c]
        r["exfiltration_rate"] = round(r["exfiltration_count"] / r["incident_count"] * 100, 1) if r["incident_count"] > 0 else 0
        result.append(r)
    return result


def get_ransom_economics(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Aggregate ransom demand/payment economics."""
    cur = conn.execute(
        """
        SELECT
            COUNT(*) as total_ransomware,
            SUM(CASE WHEN was_ransom_demanded = 1 THEN 1 ELSE 0 END) as demanded_count,
            SUM(CASE WHEN ransom_paid = 1 THEN 1 ELSE 0 END) as paid_count,
            SUM(ransom_amount) as total_demanded,
            AVG(CASE WHEN ransom_amount > 0 THEN ransom_amount END) as avg_demanded,
            MAX(ransom_amount) as max_demanded,
            SUM(ransom_paid_amount) as total_paid,
            AVG(CASE WHEN ransom_paid_amount > 0 THEN ransom_paid_amount END) as avg_paid
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND attack_category LIKE '%ransomware%'
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
    for label, condition in [("ransomware", "attack_category LIKE '%ransomware%'"), ("other", "attack_category NOT LIKE '%ransomware%'")]:
        cur = conn.execute(
            f"""
            SELECT
                AVG(recovery_timeframe_days) as avg_recovery_days,
                AVG(downtime_days) as avg_downtime_days,
                SUM(CASE WHEN from_backup = 1 THEN 1 ELSE 0 END) * 100.0 / MAX(COUNT(*), 1) as backup_rate,
                SUM(CASE WHEN incident_response_firm IS NOT NULL AND incident_response_firm != '' THEN 1 ELSE 0 END) * 100.0 / MAX(COUNT(*), 1) as ir_firm_rate,
                SUM(CASE WHEN forensics_firm IS NOT NULL AND forensics_firm != '' THEN 1 ELSE 0 END) * 100.0 / MAX(COUNT(*), 1) as forensics_rate,
                COUNT(*) as total
            FROM incident_enrichments_flat
            WHERE is_education_related = 1 AND {condition}
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
        """
        SELECT COALESCE(ransomware_family, threat_actor_name) as family
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND attack_category LIKE '%ransomware%'
          AND (
            (ransomware_family IS NOT NULL AND ransomware_family != '')
            OR (threat_actor_name IS NOT NULL AND threat_actor_name != '')
          )
        GROUP BY family
        ORDER BY COUNT(*) DESC
        LIMIT ?
        """,
        (limit_families,)
    )
    families = [row["family"] for row in cur.fetchall()]

    result = []
    for family in families:
        cur = conn.execute(
            """
            SELECT
                COALESCE(country, 'Unknown') as country,
                COUNT(*) as count
            FROM incident_enrichments_flat
            WHERE is_education_related = 1
              AND (ransomware_family = ? OR threat_actor_name = ?)
            GROUP BY country
            ORDER BY count DESC
            LIMIT ?
            """,
            (family, family, limit_countries)
        )
        countries = [dict(row) for row in cur.fetchall()]
        result.append({"family": family, "countries": countries})
    return result


def get_threat_actor_categories(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Get threat actor category distribution from enrichment JSON."""
    cur = conn.execute(
        """
        SELECT ie.enrichment_data
        FROM incident_enrichments ie
        JOIN incident_enrichments_flat ef ON ie.incident_id = ef.incident_id
        WHERE ef.is_education_related = 1
          AND ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != ''
        """
    )
    category_counts: Dict[str, int] = {}
    for row in cur.fetchall():
        try:
            data = json.loads(row["enrichment_data"])
            # Try common paths for actor category
            cat = None
            if "threat_actor" in data and isinstance(data["threat_actor"], dict):
                cat = data["threat_actor"].get("category") or data["threat_actor"].get("actor_type")
            if "attack_dynamics" in data and isinstance(data["attack_dynamics"], dict):
                cat = cat or data["attack_dynamics"].get("threat_actor_category")
            cat = cat or "unknown"
            category_counts[cat] = category_counts.get(cat, 0) + 1
        except Exception:
            category_counts["unknown"] = category_counts.get("unknown", 0) + 1

    total = sum(category_counts.values())
    return [
        {"category": cat, "count": count,
         "percentage": round(count / total * 100, 1) if total > 0 else 0}
        for cat, count in sorted(category_counts.items(), key=lambda x: -x[1])
    ]


def get_threat_actor_motivations(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Get threat actor motivation distribution from enrichment JSON."""
    cur = conn.execute(
        """
        SELECT ie.enrichment_data
        FROM incident_enrichments ie
        JOIN incident_enrichments_flat ef ON ie.incident_id = ef.incident_id
        WHERE ef.is_education_related = 1
          AND ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != ''
        """
    )
    motivation_counts: Dict[str, int] = {}
    for row in cur.fetchall():
        try:
            data = json.loads(row["enrichment_data"])
            mot = None
            if "threat_actor" in data and isinstance(data["threat_actor"], dict):
                mot = data["threat_actor"].get("motivation")
            if "attack_dynamics" in data and isinstance(data["attack_dynamics"], dict):
                mot = mot or data["attack_dynamics"].get("threat_actor_motivation")
            mot = mot or "unknown"
            motivation_counts[mot] = motivation_counts.get(mot, 0) + 1
        except Exception:
            motivation_counts["unknown"] = motivation_counts.get("unknown", 0) + 1

    total = sum(motivation_counts.values())
    return [
        {"category": cat, "count": count,
         "percentage": round(count / total * 100, 1) if total > 0 else 0}
        for cat, count in sorted(motivation_counts.items(), key=lambda x: -x[1])
    ]


def get_threat_actor_timeline(
    conn: sqlite3.Connection,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """Monthly activity per threat actor (scatter chart)."""
    cur = conn.execute(
        """
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
            SELECT threat_actor_name
            FROM incident_enrichments_flat
            WHERE is_education_related = 1 AND threat_actor_name IS NOT NULL AND threat_actor_name != ''
            GROUP BY threat_actor_name
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
        """
        SELECT threat_actor_name
        FROM incident_enrichments_flat
        WHERE is_education_related = 1 AND threat_actor_name IS NOT NULL AND threat_actor_name != ''
        GROUP BY threat_actor_name ORDER BY COUNT(*) DESC LIMIT ?
        """,
        (limit_actors,)
    )
    actors = [row["threat_actor_name"] for row in cur.fetchall()]

    # Get top families
    cur = conn.execute(
        """
        SELECT COALESCE(ransomware_family, 'unknown') as family
        FROM incident_enrichments_flat
        WHERE is_education_related = 1 AND attack_category LIKE '%ransomware%'
          AND ransomware_family IS NOT NULL AND ransomware_family != ''
        GROUP BY family ORDER BY COUNT(*) DESC LIMIT ?
        """,
        (limit_families,)
    )
    families = [row["family"] for row in cur.fetchall()]

    if not actors or not families:
        return {"actors": actors, "families": families, "matrix": []}

    # Build matrix
    actor_placeholders = ",".join("?" * len(actors))
    family_placeholders = ",".join("?" * len(families))
    cur = conn.execute(
        f"""
        SELECT
            threat_actor_name as actor,
            COALESCE(ransomware_family, 'unknown') as family,
            COUNT(*) as count
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND threat_actor_name IN ({actor_placeholders})
          AND COALESCE(ransomware_family, 'unknown') IN ({family_placeholders})
        GROUP BY actor, family
        """,
        actors + families
    )
    matrix = [dict(row) for row in cur.fetchall()]
    return {"actors": actors, "families": families, "matrix": matrix}


def get_actor_targeting(
    conn: sqlite3.Connection,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """Per-actor country distribution (stacked horizontal bars)."""
    cur = conn.execute(
        """
        SELECT
            ef.threat_actor_name as actor,
            COALESCE(ef.country, 'Unknown') as country,
            COUNT(*) as count
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != ''
          AND ef.threat_actor_name IN (
            SELECT threat_actor_name
            FROM incident_enrichments_flat
            WHERE is_education_related = 1 AND threat_actor_name IS NOT NULL AND threat_actor_name != ''
            GROUP BY threat_actor_name ORDER BY COUNT(*) DESC LIMIT ?
          )
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
        """
        SELECT
            COALESCE(institution_type, 'unknown') as category,
            COUNT(*) as count
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
        GROUP BY category
        ORDER BY count DESC
        """
    )
    rows = cur.fetchall()
    total = sum(r["count"] for r in rows)
    return [
        {"category": r["category"], "count": r["count"],
         "percentage": round(r["count"] / total * 100, 1) if total > 0 else 0}
        for r in rows
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
    cur = conn.execute("SELECT COUNT(*) as total FROM incident_enrichments_flat WHERE is_education_related = 1")
    total = cur.fetchone()["total"]

    result = []
    for col, label in ops:
        cur = conn.execute(
            f"SELECT COUNT(*) as count FROM incident_enrichments_flat WHERE is_education_related = 1 AND {col} = 1"
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
        """
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN data_breached = 1 THEN 1 ELSE 0 END) as breached_count,
            SUM(CASE WHEN data_exfiltrated = 1 THEN 1 ELSE 0 END) as exfiltrated_count,
            SUM(records_affected_exact) as total_records,
            AVG(CASE WHEN records_affected_exact > 0 THEN records_affected_exact END) as avg_records,
            MAX(records_affected_exact) as max_records,
            SUM(pii_records_leaked) as total_pii_leaked
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
        """
    )
    row = dict(cur.fetchone())
    row["breach_rate"] = round(row["breached_count"] / row["total"] * 100, 1) if row["total"] > 0 else 0
    row["exfiltration_rate"] = round(row["exfiltrated_count"] / row["total"] * 100, 1) if row["total"] > 0 else 0
    return row


def get_regulatory_impact_stats(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Regulatory compliance aggregation."""
    cur = conn.execute(
        """
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
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
        """
    )
    return dict(cur.fetchone())


def get_recovery_effectiveness(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Recovery effectiveness stats."""
    cur = conn.execute(
        """
        SELECT
            COUNT(*) as total,
            AVG(recovery_timeframe_days) as avg_recovery_days,
            AVG(downtime_days) as avg_downtime_days,
            SUM(CASE WHEN from_backup = 1 THEN 1 ELSE 0 END) as backup_count,
            SUM(CASE WHEN incident_response_firm IS NOT NULL AND incident_response_firm != '' THEN 1 ELSE 0 END) as ir_firm_count,
            SUM(CASE WHEN forensics_firm IS NOT NULL AND forensics_firm != '' THEN 1 ELSE 0 END) as forensics_count,
            SUM(CASE WHEN mfa_implemented = 1 THEN 1 ELSE 0 END) as mfa_post_count
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
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
        """
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN public_disclosure = 1 THEN 1 ELSE 0 END) as disclosed_count,
            AVG(CASE WHEN disclosure_delay_days > 0 THEN disclosure_delay_days END) as avg_delay_days,
            transparency_level,
            COUNT(*) as level_count
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
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
        """
        SELECT
            SUM(students_affected) as students,
            SUM(staff_affected) as staff,
            SUM(faculty_affected) as faculty,
            SUM(users_affected_exact) as total_individuals,
            COUNT(CASE WHEN students_affected > 0 OR staff_affected > 0 OR faculty_affected > 0 THEN 1 END) as incidents_with_data
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
        """
    )
    return dict(cur.fetchone())


# ============================================================
# Extended Cross-Dimensional Analytics
# ============================================================

def get_institution_risk_matrix(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Cross-tabulation of institution_type × attack_category."""
    cur = conn.execute(
        """
        SELECT
            institution_type,
            attack_category,
            COUNT(*) as count
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND institution_type IS NOT NULL AND institution_type != '' AND institution_type != 'unknown'
          AND attack_category IS NOT NULL AND attack_category != ''
        GROUP BY institution_type, attack_category
        HAVING count >= 2
        ORDER BY count DESC
        """
    )
    return [dict(row) for row in cur.fetchall()]


def get_recovery_by_attack_type(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Avg recovery and downtime days by attack category."""
    cur = conn.execute(
        """
        SELECT
            attack_category,
            ROUND(AVG(recovery_timeframe_days), 1) as avg_recovery_days,
            ROUND(AVG(downtime_days), 1) as avg_downtime_days,
            COUNT(*) as incident_count
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND attack_category IS NOT NULL AND attack_category != ''
          AND (recovery_timeframe_days > 0 OR downtime_days > 0)
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
        """
        SELECT institution_type, COUNT(*) as cnt
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND institution_type IS NOT NULL AND institution_type != '' AND institution_type != 'unknown'
          AND attack_vector IS NOT NULL AND attack_vector != '' AND attack_vector != 'unknown'
        GROUP BY institution_type
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
            institution_type,
            CASE
                WHEN attack_vector IN ('unknown', 'other', 'Unknown', 'Other') THEN 'Unknown / Other'
                ELSE attack_vector
            END as attack_vector,
            COUNT(*) as count
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND institution_type IN ({placeholders})
          AND attack_vector IS NOT NULL AND attack_vector != ''
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
        """
        SELECT COALESCE(ransomware_family, threat_actor_name) as family, COUNT(*) as cnt
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND attack_category LIKE '%ransomware%'
          AND (
            (ransomware_family IS NOT NULL AND ransomware_family != '')
            OR (threat_actor_name IS NOT NULL AND threat_actor_name != '')
          )
        GROUP BY family
        ORDER BY cnt DESC
        LIMIT ?
        """,
        (limit,)
    )
    top_families = [row["family"] for row in cur.fetchall()]
    if not top_families:
        return {"families": [], "data": []}

    placeholders = ",".join("?" * len(top_families))
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
        top_families
    )
    data = [dict(row) for row in cur.fetchall()]
    return {"families": top_families, "data": data}


def get_actor_institution_targeting(
    conn: sqlite3.Connection,
    limit: int = 12,
) -> Dict[str, Any]:
    """Top actors × institution type cross-tabulation."""
    cur = conn.execute(
        """
        SELECT threat_actor_name, COUNT(*) as cnt
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND threat_actor_name IS NOT NULL AND threat_actor_name != ''
        GROUP BY threat_actor_name
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
            threat_actor_name as actor,
            COALESCE(institution_type, 'unknown') as institution_type,
            COUNT(*) as count
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND threat_actor_name IN ({placeholders})
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
        """
        SELECT ef.threat_actor_name, COUNT(*) as cnt
        FROM incident_enrichments_flat ef
        WHERE ef.is_education_related = 1
          AND ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != ''
          AND (ef.mitre_techniques_json IS NOT NULL OR ef.mitre_techniques_count > 0)
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
        """
        SELECT
            institution_type,
            COUNT(*) as total_incidents,
            SUM(CASE WHEN data_breached = 1 THEN 1 ELSE 0 END) as breach_count,
            ROUND(SUM(CASE WHEN data_breached = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as breach_rate,
            AVG(CASE WHEN records_affected_exact > 0 THEN records_affected_exact END) as avg_records,
            SUM(records_affected_exact) as total_records
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND institution_type IS NOT NULL AND institution_type != '' AND institution_type != 'unknown'
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


# ============================================================
# Interactive / Nivo Visualization Endpoints
# ============================================================


def get_attack_flow(conn: sqlite3.Connection) -> Dict[str, Any]:
    """3-column Sankey: Attack Vector → Attack Category → Impact Outcome."""
    cur = conn.execute(
        """
        SELECT
            COALESCE(
                NULLIF(initial_access_vector, ''),
                NULLIF(attack_vector, ''),
                'Unknown'
            ) as vector,
            COALESCE(NULLIF(attack_category, ''), 'Unknown') as category,
            CASE
                WHEN data_exfiltrated = 1 AND was_ransom_demanded = 1 THEN 'Breach + Ransom'
                WHEN data_exfiltrated = 1 THEN 'Data Breach'
                WHEN was_ransom_demanded = 1 THEN 'Ransom Only'
                ELSE 'Other Impact'
            END as outcome,
            COUNT(*) as count
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
        GROUP BY vector, category, outcome
        HAVING count >= 1
        ORDER BY count DESC
        """
    )
    rows = [dict(r) for r in cur.fetchall()]

    links = []

    # Prefix node IDs by level so the same label (e.g. "Unknown", "Phishing")
    # in two different Sankey columns doesn't create a circular link in d3-sankey.
    vec_cat: Dict[tuple, int] = {}
    cat_out: Dict[tuple, int] = {}
    for r in rows:
        v, c, o, cnt = r["vector"], r["category"], r["outcome"], r["count"]
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
        """
        SELECT mitre_techniques_json
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND mitre_techniques_json IS NOT NULL
          AND mitre_techniques_json != '[]'
          AND mitre_techniques_json != ''
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
        """
        SELECT threat_actor_name as actor, ransomware_family as family, COUNT(*) as cnt
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND threat_actor_name IS NOT NULL AND threat_actor_name != ''
          AND ransomware_family IS NOT NULL AND ransomware_family != ''
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
        """
        SELECT
            COALESCE(NULLIF(institution_type, ''), 'Unknown') as inst_type,
            COALESCE(NULLIF(ransomware_family, ''), 'Unknown Family') as family,
            CASE
                WHEN ransom_paid = 1 THEN 'Paid'
                WHEN was_ransom_demanded = 1 AND (ransom_paid = 0 OR ransom_paid IS NULL) THEN 'Refused'
                ELSE 'Unknown Outcome'
            END as outcome,
            COUNT(*) as count,
            COALESCE(SUM(ransom_amount), 0) as total_amount
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND attack_category LIKE '%ransomware%'
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
        it, fam, out = r["inst_type"], r["family"], r["outcome"]
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
        """
        SELECT country, COUNT(*) as cnt
        FROM incident_enrichments_flat
        WHERE is_education_related = 1 AND country IS NOT NULL AND country != ''
        GROUP BY country ORDER BY cnt DESC LIMIT ?
        """,
        (limit_countries,)
    )
    countries = [row["country"] for row in cur.fetchall()]

    # Top attack categories
    cur = conn.execute(
        """
        SELECT attack_category, COUNT(*) as cnt
        FROM incident_enrichments_flat
        WHERE is_education_related = 1 AND attack_category IS NOT NULL AND attack_category != ''
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
        SELECT country, attack_category, COUNT(*) as cnt
        FROM incident_enrichments_flat
        WHERE is_education_related = 1
          AND country IN ({country_ph})
          AND attack_category IN ({category_ph})
        GROUP BY country, attack_category
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

