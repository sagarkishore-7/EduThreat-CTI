"""
CSV export for Phase 2 enriched dataset.

Exports enriched incidents to CSV format similar to Phase 1 base_dataset.csv.
"""

import csv
import json
import sqlite3
import logging
from pathlib import Path
from typing import List, Dict, Optional

from src.edu_cti.core.db import get_connection
from src.edu_cti.pipeline.phase2.db import get_enrichment_result
from src.edu_cti.pipeline.phase2.schemas import CTIEnrichmentResult

logger = logging.getLogger(__name__)


def load_enriched_incidents_from_db(conn: sqlite3.Connection) -> List[Dict]:
    """
    Load all enriched incidents from database with enrichment data.
    
    Args:
        conn: Database connection
        
    Returns:
        List of incident dictionaries with enrichment data
    """
    query = """
        SELECT 
            i.*,
            ie.enrichment_data,
            ie.enrichment_confidence,
            GROUP_CONCAT(DISTINCT isrc.source) as sources
        FROM incidents i
        INNER JOIN incident_enrichments ie ON i.incident_id = ie.incident_id
        LEFT JOIN incident_sources isrc ON i.incident_id = isrc.incident_id
        WHERE i.llm_enriched = 1
        GROUP BY i.incident_id
        ORDER BY i.llm_enriched_at DESC
    """
    
    cur = conn.execute(query)
    rows = cur.fetchall()
    
    # Helper function to safely get values from sqlite3.Row
    def safe_get(row, key, default=None):
        """Safely get value from sqlite3.Row, returning default if key doesn't exist."""
        try:
            value = row[key]
            return default if value is None else value
        except (KeyError, IndexError):
            return default
    
    incidents = []
    for row in rows:
        # Parse all_urls
        all_urls_str = safe_get(row, "all_urls", "") or ""
        all_urls = [url.strip() for url in all_urls_str.split(";") if url.strip()]
        
        # Get sources (comma-separated from GROUP_CONCAT)
        sources_str = safe_get(row, "sources", "") or ""
        sources = [s.strip() for s in sources_str.split(",") if s.strip()] if sources_str else []
        primary_source = sources[0] if sources else "unknown"
        
        # Get enrichment data
        enrichment_data = None
        enrichment_data_str = safe_get(row, "enrichment_data")
        if enrichment_data_str:
            try:
                enrichment_data = json.loads(enrichment_data_str)
            except json.JSONDecodeError as e:
                logger.warning(f"Error parsing enrichment data for {safe_get(row, 'incident_id')}: {e}")
        
        incident_dict = {
            "incident_id": safe_get(row, "incident_id", ""),
            "source": primary_source,
            "sources": ";".join(sources) if sources else "",
            "university_name": safe_get(row, "university_name") or safe_get(row, "victim_raw_name") or "Unknown",
            "victim_raw_name": safe_get(row, "victim_raw_name"),
            "institution_type": safe_get(row, "institution_type"),
            "country": safe_get(row, "country"),
            "region": safe_get(row, "region"),
            "city": safe_get(row, "city"),
            "incident_date": safe_get(row, "incident_date"),
            "date_precision": safe_get(row, "date_precision", "unknown"),
            "source_published_date": safe_get(row, "source_published_date"),
            "ingested_at": safe_get(row, "ingested_at"),
            "title": safe_get(row, "title"),
            "subtitle": safe_get(row, "subtitle"),
            "primary_url": safe_get(row, "primary_url"),
            "all_urls": ";".join(all_urls) if all_urls else "",
            "attack_type_hint": safe_get(row, "attack_type_hint"),
            "status": safe_get(row, "status", "suspected"),
            "source_confidence": safe_get(row, "source_confidence", "medium"),
            "notes": safe_get(row, "notes"),
            # Enrichment fields
            "llm_enriched_at": safe_get(row, "llm_enriched_at"),
            "llm_summary": safe_get(row, "llm_summary"),
            "llm_timeline": safe_get(row, "llm_timeline"),
            "llm_mitre_attack": safe_get(row, "llm_mitre_attack"),
            "llm_attack_dynamics": safe_get(row, "llm_attack_dynamics"),
            "enrichment_confidence": safe_get(row, "enrichment_confidence"),
            # Enrichment details (from full enrichment_data)
            "enrichment_data": json.dumps(enrichment_data) if enrichment_data else "",
        }
        
        # Extract additional fields from enrichment_data if available
        if enrichment_data:
            try:
                enrichment = CTIEnrichmentResult.model_validate(enrichment_data)
                incident_dict.update({
                    "education_relevance_confidence": enrichment.education_relevance.confidence if enrichment.education_relevance else None,
                    "is_education_related": enrichment.education_relevance.is_education_related if enrichment.education_relevance else None,
                    "timeline_events_count": len(enrichment.timeline) if enrichment.timeline else 0,
                    "mitre_techniques_count": len(enrichment.mitre_attack_techniques) if enrichment.mitre_attack_techniques else 0,
                    "attack_type": enrichment.attack_dynamics.attack_type if enrichment.attack_dynamics else None,
                    "attack_family": enrichment.attack_dynamics.attack_family if enrichment.attack_dynamics else None,
                })
            except Exception as e:
                incident_id = safe_get(row, "incident_id", "unknown")
                logger.warning(f"Error extracting enrichment details for {incident_id}: {e}")
        
        incidents.append(incident_dict)
    
    return incidents


def write_enriched_csv(output_path: Path, incidents: List[Dict]) -> None:
    """
    Write enriched incidents to CSV file.
    
    Args:
        output_path: Path to output CSV file
        incidents: List of incident dictionaries with enrichment data
    """
    if not incidents:
        logger.warning("No enriched incidents to write")
        return
    
    # Define CSV columns (expanded for enriched data)
    fieldnames = [
        # Core fields (from Phase 1)
        "incident_id",
        "source",
        "sources",
        "university_name",
        "victim_raw_name",
        "institution_type",
        "country",
        "region",
        "city",
        "incident_date",
        "date_precision",
        "source_published_date",
        "ingested_at",
        "title",
        "subtitle",
        "primary_url",
        "all_urls",
        "attack_type_hint",
        "status",
        "source_confidence",
        "notes",
        # Enrichment metadata
        "llm_enriched_at",
        "enrichment_confidence",
        "education_relevance_confidence",
        "is_education_related",
        # Enrichment content
        "llm_summary",
        "timeline_events_count",
        "llm_timeline",
        "mitre_techniques_count",
        "llm_mitre_attack",
        "attack_type",
        "attack_family",
        "llm_attack_dynamics",
        # Full enrichment data (JSON)
        "enrichment_data",
    ]
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        
        for incident in incidents:
            # Ensure all fields are present (fill missing with empty string)
            row = {field: incident.get(field, "") for field in fieldnames}
            writer.writerow(row)
    
    logger.info(f"Wrote {len(incidents)} enriched incidents to {output_path}")


def export_enriched_dataset(
    db_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
) -> Optional[Path]:
    """
    Export enriched incidents to CSV.
    
    Args:
        db_path: Path to database (default: from config)
        output_path: Path to output CSV (default: data/processed/enriched_dataset.csv)
        
    Returns:
        Path to output CSV file, or None if no enriched incidents found
    """
    from src.edu_cti.core.config import DB_PATH
    from src.edu_cti.pipeline.phase1.base_io import PROC_DIR
    
    if db_path is None:
        db_path = DB_PATH
    
    if output_path is None:
        output_path = PROC_DIR / "enriched_dataset.csv"
    
    conn = get_connection(db_path)
    
    try:
        # Check if incident_enrichments table exists
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='incident_enrichments'"
        )
        table_exists = cur.fetchone() is not None
        
        if not table_exists:
            logger.warning("No enriched incidents found - incident_enrichments table does not exist")
            return None
        
        # Load enriched incidents
        logger.info("Loading enriched incidents from database...")
        incidents = load_enriched_incidents_from_db(conn)
        
        logger.info(f"Found {len(incidents)} enriched incidents")
        
        # Write to CSV
        if incidents:
            write_enriched_csv(output_path, incidents)
            logger.info(f"Enriched dataset exported to: {output_path}")
            return output_path
        else:
            logger.warning("No enriched incidents found to export")
            return None
        
    except Exception as e:
        logger.error(f"Error exporting enriched dataset: {e}", exc_info=True)
        return None
    finally:
        conn.close()

