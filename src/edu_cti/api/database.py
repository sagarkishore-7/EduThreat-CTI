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
    for row in cur.fetchall():
        try:
            techniques = None
            # Try flat table JSON first
            if row["mitre_techniques_json"]:
                techniques = json.loads(row["mitre_techniques_json"])
            # Fall back to enrichment_data JSON if flat is empty
            if (not techniques or techniques == []) and row["enrichment_data"]:
                enrichment = json.loads(row["enrichment_data"])
                techniques = enrichment.get("mitre_attack_techniques", [])
            if not techniques:
                continue
            for t in techniques:
                raw_tactic = t.get("tactic") or "unknown"
                tactic = _normalize_mitre_tactic(raw_tactic)
                tactic_counts[tactic] = tactic_counts.get(tactic, 0) + 1
                tech_id = t.get("technique_id", "")
                tech_name = t.get("technique_name", "")
                if tech_id and tactic not in tactic_techniques:
                    tactic_techniques[tactic] = []
                if tech_id:
                    entry = f"{tech_id}: {tech_name}"
                    if entry not in tactic_techniques[tactic]:
                        tactic_techniques[tactic].append(entry)
        except Exception:
            continue

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
            COALESCE(initial_access_vector, attack_vector, 'unknown') as category,
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

