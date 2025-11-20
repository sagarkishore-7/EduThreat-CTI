"""
Database operations for Phase 2 LLM enrichment.

Handles storage and retrieval of enrichment results.
"""

import json
import sqlite3
import logging
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime

from src.edu_cti.core.db import get_connection
from src.edu_cti.pipeline.phase2.schemas import CTIEnrichmentResult
from src.edu_cti.core.config import DB_PATH

logger = logging.getLogger(__name__)


def init_incident_enrichments_table(conn: sqlite3.Connection) -> None:
    """
    Initialize the incident_enrichments table if it doesn't exist.
    
    This table stores the full enrichment JSON data and confidence scores.
    
    Args:
        conn: Database connection
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS incident_enrichments (
            incident_id TEXT PRIMARY KEY,
            enrichment_data TEXT NOT NULL,
            enrichment_version TEXT DEFAULT '1.0',
            enrichment_confidence REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE
        )
        """
    )
    conn.commit()


def get_unenriched_incidents(
    conn: sqlite3.Connection,
    limit: Optional[int] = None,
    include_enrichment_upgrades: bool = True,
) -> List[Dict]:
    """
    Get incidents that haven't been enriched yet.
    
    Also includes enriched incidents that have been marked for upgrade
    (when new URLs were added but enrichment wasn't updated yet).
    
    Args:
        conn: Database connection
        limit: Optional limit on number of incidents to return
        include_enrichment_upgrades: If True, include incidents marked for upgrade
        
    Returns:
        List of incident dicts ready for enrichment
    """
    query = """
        SELECT * FROM incidents
        WHERE llm_enriched = 0
          AND (all_urls IS NOT NULL AND all_urls != '')
        ORDER BY ingested_at DESC
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    cur = conn.execute(query)
    rows = cur.fetchall()
    
    incidents = []
    for row in rows:
        # Parse all_urls
        all_urls_str = row["all_urls"] or ""
        all_urls = [url.strip() for url in all_urls_str.split(";") if url.strip()]
        
        incident_dict = {
            "incident_id": row["incident_id"],
            "university_name": row["university_name"] or row["victim_raw_name"] or "Unknown",
            "victim_raw_name": row["victim_raw_name"],
            "institution_type": row["institution_type"],
            "country": row["country"],
            "region": row["region"],
            "city": row["city"],
            "incident_date": row["incident_date"],
            "date_precision": row["date_precision"] or "unknown",
            "source_published_date": row["source_published_date"],
            "ingested_at": row["ingested_at"],
            "title": row["title"],
            "subtitle": row["subtitle"],
            "primary_url": row["primary_url"],
            "all_urls": all_urls,
            "attack_type_hint": row["attack_type_hint"],
            "status": row["status"] or "suspected",
            "source_confidence": row["source_confidence"] or "medium",
            "notes": row["notes"],
        }
        incidents.append(incident_dict)
    
    return incidents


def should_upgrade_enrichment(
    conn: sqlite3.Connection,
    incident_id: str,
    new_confidence: float,
) -> bool:
    """
    Check if enrichment should be upgraded based on confidence.
    
    Args:
        conn: Database connection
        incident_id: Incident ID to check
        new_confidence: New enrichment confidence score
        
    Returns:
        True if new enrichment has higher confidence, False otherwise
    """
    existing_enrichment = get_enrichment_result(conn, incident_id)
    if not existing_enrichment:
        return True  # No existing enrichment - should save
    
    existing_confidence = existing_enrichment.extraction_confidence
    return new_confidence > existing_confidence


def save_enrichment_result(
    conn: sqlite3.Connection,
    incident_id: str,
    enrichment_result: CTIEnrichmentResult,
    force_replace: bool = False,
) -> bool:
    """
    Save LLM enrichment result to database.
    
    If incident already has enrichment, only replace if new confidence is higher
    or force_replace=True.
    
    Args:
        conn: Database connection
        incident_id: Incident ID to update
        enrichment_result: CTIEnrichmentResult to save
        force_replace: If True, replace existing enrichment regardless of confidence
        
    Returns:
        True if enrichment was saved, False if skipped (lower confidence)
    """
    # Ensure table exists before any queries
    init_incident_enrichments_table(conn)
    
    # Check if we should upgrade existing enrichment
    if not force_replace:
        existing_enrichment = get_enrichment_result(conn, incident_id)
        if existing_enrichment:
            should_upgrade = should_upgrade_enrichment(
                conn, incident_id, enrichment_result.extraction_confidence
            )
            if not should_upgrade:
                logger.info(
                    f"Skipping enrichment for incident {incident_id} - "
                    f"existing confidence is higher ({existing_enrichment.extraction_confidence:.2f} "
                    f"vs {enrichment_result.extraction_confidence:.2f})"
                )
                return False
    
    now = datetime.utcnow().isoformat()
    
    # Convert enrichment result to JSON for storage
    enrichment_json = enrichment_result.model_dump_json(indent=2)
    
    # Update incident with enrichment data
    # Set primary_url from enrichment result
    primary_url = enrichment_result.primary_url
    
    # Build summary from enriched_summary
    summary = enrichment_result.enriched_summary
    
    # Build timeline JSON
    timeline_json = json.dumps([event.model_dump() for event in enrichment_result.timeline])
    
    # Build MITRE ATT&CK JSON
    mitre_json = json.dumps([tech.model_dump() for tech in enrichment_result.mitre_attack_techniques])
    
    # Build attack dynamics JSON
    attack_dynamics_json = None
    if enrichment_result.attack_dynamics:
        attack_dynamics_json = enrichment_result.attack_dynamics.model_dump_json(indent=2)
    
    # Update incident record
    conn.execute(
        """
        UPDATE incidents
        SET 
            llm_enriched = 1,
            llm_enriched_at = ?,
            primary_url = ?,
            llm_summary = ?,
            llm_timeline = ?,
            llm_mitre_attack = ?,
            llm_attack_dynamics = ?,
            last_updated_at = ?
        WHERE incident_id = ?
        """,
        (
            now,
            primary_url,
            summary,
            timeline_json,
            mitre_json,
            attack_dynamics_json,
            now,
            incident_id,
        )
    )
    
    # Table already created at the start of function - check if record exists for insert vs update
    cur = conn.execute(
        "SELECT incident_id FROM incident_enrichments WHERE incident_id = ?",
        (incident_id,)
    )
    exists = cur.fetchone() is not None
    
    if exists:
        conn.execute(
            """
            UPDATE incident_enrichments
            SET enrichment_data = ?, enrichment_confidence = ?, updated_at = ?
            WHERE incident_id = ?
            """,
            (enrichment_json, enrichment_result.extraction_confidence, now, incident_id)
        )
    else:
        conn.execute(
            """
            INSERT INTO incident_enrichments
            (incident_id, enrichment_data, enrichment_confidence, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (incident_id, enrichment_json, enrichment_result.extraction_confidence, now, now)
        )
    
    # Note: Article cleanup (removing non-primary articles) is handled in enrichment.py
    # after enrichment is complete. This ensures we only keep the selected primary article.
    
    conn.commit()
    logger.info(
        f"Saved enrichment result for incident {incident_id} "
        f"(confidence: {enrichment_result.extraction_confidence:.2f})"
    )
    return True


def get_enrichment_result(
    conn: sqlite3.Connection,
    incident_id: str,
) -> Optional[CTIEnrichmentResult]:
    """
    Retrieve enrichment result for an incident.
    
    Args:
        conn: Database connection
        incident_id: Incident ID to retrieve
        
    Returns:
        CTIEnrichmentResult if found, None otherwise
    """
    # Ensure table exists before querying
    init_incident_enrichments_table(conn)
    
    cur = conn.execute(
        """
        SELECT enrichment_data FROM incident_enrichments
        WHERE incident_id = ?
        """,
        (incident_id,)
    )
    
    row = cur.fetchone()
    if not row:
        return None
    
    try:
        enrichment_data = json.loads(row["enrichment_data"])
        return CTIEnrichmentResult.model_validate(enrichment_data)
    except Exception as e:
        logger.error(f"Error parsing enrichment data for {incident_id}: {e}")
        return None


def mark_incident_skipped(
    conn: sqlite3.Connection,
    incident_id: str,
    reason: str,
) -> None:
    """
    Mark an incident as skipped (e.g., not education-related).
    
    This prevents re-processing in future runs.
    
    Args:
        conn: Database connection
        incident_id: Incident ID to mark
        reason: Reason for skipping
    """
    now = datetime.utcnow().isoformat()
    
    # Store skip reason in notes or create a separate field
    # For now, we'll set llm_enriched = 1 with a flag in notes
    conn.execute(
        """
        UPDATE incidents
        SET 
            llm_enriched = 1,
            llm_enriched_at = ?,
            notes = COALESCE(notes || ' | ', '') || 'LLM_ENRICHMENT_SKIPPED: ' || ?,
            last_updated_at = ?
        WHERE incident_id = ?
        """,
        (now, reason, now, incident_id)
    )
    
    conn.commit()
    logger.info(f"Marked incident {incident_id} as skipped: {reason}")


def revert_enrichment_for_incident(
    conn: sqlite3.Connection,
    incident_id: str,
) -> bool:
    """
    Revert LLM enrichment data for an incident.
    
    This removes all enrichment data and marks the incident as unenriched,
    allowing it to be processed again in future runs.
    
    Args:
        conn: Database connection
        incident_id: Incident ID to revert
        
    Returns:
        True if reverted successfully, False otherwise
    """
    try:
        # Remove enrichment data from incidents table
        conn.execute(
            """
            UPDATE incidents
            SET 
                llm_enriched = 0,
                llm_enriched_at = NULL,
                llm_summary = NULL,
                llm_timeline = NULL,
                llm_mitre_attack = NULL,
                llm_attack_dynamics = NULL,
                primary_url = NULL,
                last_updated_at = ?
            WHERE incident_id = ?
            """,
            (datetime.utcnow().isoformat(), incident_id)
        )
        
        # Remove from incident_enrichments table
        conn.execute(
            "DELETE FROM incident_enrichments WHERE incident_id = ?",
            (incident_id,)
        )
        
        # Delete articles for this incident (they can be re-fetched)
        conn.execute(
            "DELETE FROM articles WHERE incident_id = ?",
            (incident_id,)
        )
        
        conn.commit()
        logger.info(f"Reverted enrichment for incident {incident_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error reverting enrichment for incident {incident_id}: {e}")
        conn.rollback()
        return False


def revert_all_enriched_incidents(
    conn: sqlite3.Connection,
) -> int:
    """
    Revert all enriched incidents in the database.
    
    This removes all enrichment data and marks all incidents as unenriched,
    allowing them to be processed again in future runs.
    
    Args:
        conn: Database connection
        
    Returns:
        Number of incidents reverted
    """
    try:
        # Get all enriched incident IDs
        cur = conn.execute("SELECT incident_id FROM incidents WHERE llm_enriched = 1")
        incident_ids = [row["incident_id"] for row in cur.fetchall()]
        
        if not incident_ids:
            logger.info("No enriched incidents to revert")
            return 0
        
        # Remove enrichment data from incidents table
        conn.execute(
            """
            UPDATE incidents
            SET 
                llm_enriched = 0,
                llm_enriched_at = NULL,
                llm_summary = NULL,
                llm_timeline = NULL,
                llm_mitre_attack = NULL,
                llm_attack_dynamics = NULL,
                primary_url = NULL,
                last_updated_at = ?
            WHERE llm_enriched = 1
            """,
            (datetime.utcnow().isoformat(),)
        )
        
        # Clear incident_enrichments table
        conn.execute("DELETE FROM incident_enrichments")
        
        # Delete all articles (they can be re-fetched)
        conn.execute("DELETE FROM articles")
        
        conn.commit()
        logger.info(f"Reverted enrichment for {len(incident_ids)} incidents")
        return len(incident_ids)
        
    except Exception as e:
        logger.error(f"Error reverting enriched incidents: {e}")
        conn.rollback()
        return 0


def get_enrichment_stats(conn: sqlite3.Connection) -> Dict[str, int]:
    """
    Get statistics about enrichment progress.
    
    Args:
        conn: Database connection
        
    Returns:
        Dictionary with enrichment statistics
    """
    stats = {}
    
    # Total incidents
    cur = conn.execute("SELECT COUNT(*) as count FROM incidents")
    stats["total_incidents"] = cur.fetchone()["count"]
    
    # Enriched incidents
    cur = conn.execute("SELECT COUNT(*) as count FROM incidents WHERE llm_enriched = 1")
    stats["enriched_incidents"] = cur.fetchone()["count"]
    
    # Unenriched incidents
    cur = conn.execute("SELECT COUNT(*) as count FROM incidents WHERE llm_enriched = 0")
    stats["unenriched_incidents"] = cur.fetchone()["count"]
    
    # Incidents with URLs
    cur = conn.execute(
        """
        SELECT COUNT(*) as count FROM incidents 
        WHERE all_urls IS NOT NULL AND all_urls != ''
        """
    )
    stats["incidents_with_urls"] = cur.fetchone()["count"]
    
    # Unenriched with URLs (ready for processing)
    cur = conn.execute(
        """
        SELECT COUNT(*) as count FROM incidents 
        WHERE llm_enriched = 0 
          AND all_urls IS NOT NULL AND all_urls != ''
        """
    )
    stats["ready_for_enrichment"] = cur.fetchone()["count"]
    
    return stats

