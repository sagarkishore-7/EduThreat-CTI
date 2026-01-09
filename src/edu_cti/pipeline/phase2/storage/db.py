"""
Database operations for Phase 2 LLM enrichment.

Handles storage and retrieval of enrichment results with optimized structure
for CSV export and dashboard analytics.
"""

import json
import sqlite3
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

from src.edu_cti.core.db import get_connection
from src.edu_cti.pipeline.phase2.schemas import CTIEnrichmentResult
from src.edu_cti.core.config import DB_PATH

logger = logging.getLogger(__name__)


def init_incident_enrichments_table(conn: sqlite3.Connection) -> None:
    """
    Initialize the incident_enrichments table with optimized structure.
    
    This table stores:
    1. Full JSON data for complete record (enrichment_data)
    2. Flattened key fields as columns for fast queries and CSV export
    
    Args:
        conn: Database connection
    """
    # Create main enrichment table with JSON backup
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS incident_enrichments (
            incident_id TEXT PRIMARY KEY,
            enrichment_data TEXT NOT NULL,
            enrichment_version TEXT DEFAULT '2.0',
            enrichment_confidence REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE
        )
        """
    )
    
    # Create flattened enrichment fields table for fast queries and CSV export
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS incident_enrichments_flat (
            incident_id TEXT PRIMARY KEY,
            
            -- Education & Institution
            is_education_related INTEGER,
            institution_name TEXT,
            institution_type TEXT,
            country TEXT,
            region TEXT,
            city TEXT,
            
            -- Attack Details
            attack_category TEXT,
            attack_vector TEXT,
            initial_access_vector TEXT,
            initial_access_description TEXT,
            ransomware_family TEXT,
            threat_actor_name TEXT,
            threat_actor_claim_url TEXT,
            
            -- Ransom
            was_ransom_demanded INTEGER,
            ransom_amount REAL,
            ransom_currency TEXT,
            ransom_paid INTEGER,
            ransom_paid_amount REAL,
            
            -- Data Impact
            data_breached INTEGER,
            data_exfiltrated INTEGER,
            records_affected_exact INTEGER,
            records_affected_min INTEGER,
            records_affected_max INTEGER,
            pii_records_leaked INTEGER,
            
            -- System Impact
            systems_affected_codes TEXT,  -- JSON array
            critical_systems_affected INTEGER,
            network_compromised INTEGER,
            email_system_affected INTEGER,
            student_portal_affected INTEGER,
            research_systems_affected INTEGER,
            hospital_systems_affected INTEGER,
            cloud_services_affected INTEGER,
            third_party_vendor_impact INTEGER,
            vendor_name TEXT,
            
            -- Operational Impact
            teaching_impacted INTEGER,
            teaching_disrupted INTEGER,
            research_impacted INTEGER,
            research_disrupted INTEGER,
            admissions_disrupted INTEGER,
            enrollment_disrupted INTEGER,
            payroll_disrupted INTEGER,
            classes_cancelled INTEGER,
            exams_postponed INTEGER,
            downtime_days REAL,
            outage_duration_hours REAL,
            
            -- User Impact
            students_affected INTEGER,
            staff_affected INTEGER,
            faculty_affected INTEGER,
            users_affected_exact INTEGER,
            users_affected_min INTEGER,
            users_affected_max INTEGER,
            
            -- Financial Impact
            recovery_costs_min REAL,
            recovery_costs_max REAL,
            legal_costs REAL,
            notification_costs REAL,
            insurance_claim INTEGER,
            insurance_claim_amount REAL,
            business_impact TEXT,
            
            -- Regulatory Impact
            gdpr_breach INTEGER,
            hipaa_breach INTEGER,
            ferpa_breach INTEGER,
            breach_notification_required INTEGER,
            notifications_sent INTEGER,
            fine_imposed INTEGER,
            fine_amount REAL,
            lawsuits_filed INTEGER,
            class_action INTEGER,
            
            -- Recovery
            recovery_timeframe_days REAL,
            recovery_started_date TEXT,
            recovery_completed_date TEXT,
            from_backup INTEGER,
            mfa_implemented INTEGER,
            incident_response_firm TEXT,
            forensics_firm TEXT,
            
            -- Transparency
            public_disclosure INTEGER,
            public_disclosure_date TEXT,
            disclosure_delay_days REAL,
            transparency_level TEXT,
            
            -- Timeline & MITRE (stored as JSON for complex structures)
            timeline_json TEXT,
            timeline_events_count INTEGER,
            mitre_techniques_json TEXT,
            mitre_techniques_count INTEGER,
            
            -- Summary
            enriched_summary TEXT,
            extraction_notes TEXT,
            confidence REAL,
            
            -- Metadata
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            
            FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE
        )
        """
    )
    
    # Create indexes for common query patterns
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_enrichments_attack_category 
        ON incident_enrichments_flat(attack_category)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_enrichments_country 
        ON incident_enrichments_flat(country)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_enrichments_ransom_demanded 
        ON incident_enrichments_flat(was_ransom_demanded)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_enrichments_date 
        ON incident_enrichments_flat(created_at)
        """
    )
    
    conn.commit()


def _flatten_enrichment_for_db(
    enrichment: CTIEnrichmentResult,
    raw_json_data: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Flatten enrichment result into database columns.
    
    Uses both the CTIEnrichmentResult and the raw JSON data from LLM
    to capture all extracted fields.
    
    Args:
        enrichment: CTIEnrichmentResult to flatten
        raw_json_data: Raw JSON response from LLM extraction (contains additional fields)
        
    Returns:
        Dictionary with flattened fields for database insertion
    """
    raw = raw_json_data or {}
    
    # Helper to get value from raw_json_data with fallback
    def raw_get(key, default=None):
        return raw.get(key, default)
    
    # Normalize country to full name
    from src.edu_cti.core.countries import normalize_country
    country_raw = raw_get("country")
    country_normalized = normalize_country(country_raw) if country_raw else None
    
    flat = {
        'incident_id': None,  # Will be set by caller
        'is_education_related': enrichment.education_relevance.is_education_related if enrichment.education_relevance else raw_get("is_edu_cyber_incident"),
        'institution_name': enrichment.education_relevance.institution_identified if enrichment.education_relevance else raw_get("institution_name"),
        'institution_type': raw_get("institution_type"),
        'country': country_normalized,
        'region': raw_get("region"),
        'city': raw_get("city"),
        
        # Attack details - prefer raw JSON data for direct fields
        'attack_category': raw_get("attack_category"),
        'attack_vector': raw_get("attack_vector") or (enrichment.attack_dynamics.attack_vector if enrichment.attack_dynamics else None),
        'initial_access_vector': raw_get("initial_access_vector"),
        'initial_access_description': enrichment.initial_access_description or raw_get("initial_access_description"),
        'ransomware_family': raw_get("ransomware_family_or_group") or (enrichment.attack_dynamics.ransomware_family if enrichment.attack_dynamics else None),
        
        # Threat actor
        'threat_actor_name': raw_get("threat_actor_name"),
        'threat_actor_claim_url': raw_get("threat_actor_claim_url"),
        
        # Ransom - use raw JSON data for exact values
        'was_ransom_demanded': raw_get("was_ransom_demanded") if raw_get("was_ransom_demanded") is not None else (enrichment.attack_dynamics.ransom_demanded if enrichment.attack_dynamics else None),
        'ransom_amount': raw_get("ransom_amount") or raw_get("ransom_amount_exact"),
        'ransom_currency': raw_get("ransom_currency"),
        'ransom_paid': raw_get("ransom_paid") if raw_get("ransom_paid") is not None else (enrichment.attack_dynamics.ransom_paid if enrichment.attack_dynamics else None),
        'ransom_paid_amount': raw_get("ransom_paid_amount"),
        
        # Data impact - use raw JSON data for direct fields
        'data_breached': raw_get("data_breached"),
        'data_exfiltrated': raw_get("data_exfiltrated") if raw_get("data_exfiltrated") is not None else (enrichment.attack_dynamics.data_exfiltration if enrichment.attack_dynamics else (enrichment.data_impact.get("data_exfiltrated") if enrichment.data_impact else None)),
        'records_affected_exact': raw_get("records_affected_exact") or raw_get("pii_records_leaked") or (enrichment.data_impact.get("records_affected_exact") if enrichment.data_impact else None),
        'records_affected_min': raw_get("records_affected_min") or (enrichment.data_impact.get("records_affected_min") if enrichment.data_impact else None),
        'records_affected_max': raw_get("records_affected_max") or (enrichment.data_impact.get("records_affected_max") if enrichment.data_impact else None),
        'pii_records_leaked': raw_get("pii_records_leaked"),
        
        # System impact
        'systems_affected_codes': json.dumps(raw_get("systems_affected_codes")) if raw_get("systems_affected_codes") else (json.dumps(enrichment.system_impact.get("systems_affected")) if enrichment.system_impact and enrichment.system_impact.get("systems_affected") else None),
        'critical_systems_affected': raw_get("critical_systems_affected") if raw_get("critical_systems_affected") is not None else (enrichment.system_impact.get("critical_systems_affected") if enrichment.system_impact else None),
        'network_compromised': raw_get("network_compromised") if raw_get("network_compromised") is not None else (enrichment.system_impact.get("network_compromised") if enrichment.system_impact else None),
        'email_system_affected': raw_get("email_system_affected") if raw_get("email_system_affected") is not None else (enrichment.system_impact.get("email_system_affected") if enrichment.system_impact else None),
        'student_portal_affected': raw_get("student_portal_affected") if raw_get("student_portal_affected") is not None else (enrichment.system_impact.get("student_portal_affected") if enrichment.system_impact else None),
        'research_systems_affected': raw_get("research_systems_affected") if raw_get("research_systems_affected") is not None else (enrichment.system_impact.get("research_systems_affected") if enrichment.system_impact else None),
        'hospital_systems_affected': raw_get("hospital_systems_affected") if raw_get("hospital_systems_affected") is not None else (enrichment.system_impact.get("hospital_systems_affected") if enrichment.system_impact else None),
        'cloud_services_affected': raw_get("cloud_services_affected") if raw_get("cloud_services_affected") is not None else (enrichment.system_impact.get("cloud_services_affected") if enrichment.system_impact else None),
        'third_party_vendor_impact': raw_get("third_party_vendor_impact") if raw_get("third_party_vendor_impact") is not None else (enrichment.system_impact.get("third_party_vendor_impact") if enrichment.system_impact else None),
        'vendor_name': raw_get("vendor_name") or (enrichment.system_impact.get("vendor_name") if enrichment.system_impact else None),
        
        # Operational impact - use raw JSON data
        'teaching_impacted': raw_get("teaching_impacted"),
        'teaching_disrupted': raw_get("teaching_disrupted") if raw_get("teaching_disrupted") is not None else (enrichment.operational_impact_metrics.get("teaching_disrupted") if enrichment.operational_impact_metrics else None),
        'research_impacted': raw_get("research_impacted"),
        'research_disrupted': raw_get("research_disrupted") if raw_get("research_disrupted") is not None else (enrichment.operational_impact_metrics.get("research_disrupted") if enrichment.operational_impact_metrics else None),
        'admissions_disrupted': raw_get("admissions_disrupted") if raw_get("admissions_disrupted") is not None else (enrichment.operational_impact_metrics.get("admissions_disrupted") if enrichment.operational_impact_metrics else None),
        'enrollment_disrupted': raw_get("enrollment_disrupted") if raw_get("enrollment_disrupted") is not None else (enrichment.operational_impact_metrics.get("enrollment_disrupted") if enrichment.operational_impact_metrics else None),
        'payroll_disrupted': raw_get("payroll_disrupted") if raw_get("payroll_disrupted") is not None else (enrichment.operational_impact_metrics.get("payroll_disrupted") if enrichment.operational_impact_metrics else None),
        'classes_cancelled': raw_get("classes_cancelled") if raw_get("classes_cancelled") is not None else (enrichment.operational_impact_metrics.get("classes_cancelled") if enrichment.operational_impact_metrics else None),
        'exams_postponed': raw_get("exams_postponed") if raw_get("exams_postponed") is not None else (enrichment.operational_impact_metrics.get("exams_postponed") if enrichment.operational_impact_metrics else None),
        'downtime_days': raw_get("downtime_days") or (enrichment.operational_impact_metrics.get("downtime_days") if enrichment.operational_impact_metrics else None),
        'outage_duration_hours': raw_get("outage_duration_hours"),
        
        # User impact - use raw JSON data for exact counts
        'students_affected': raw_get("students_affected"),
        'staff_affected': raw_get("staff_affected"),
        'faculty_affected': raw_get("faculty_affected") if raw_get("faculty_affected") is not None else (enrichment.user_impact.get("faculty_affected") if enrichment.user_impact else None),
        'users_affected_exact': raw_get("users_affected_exact") or (enrichment.user_impact.get("users_affected_exact") if enrichment.user_impact else None),
        'users_affected_min': raw_get("users_affected_min") or (enrichment.user_impact.get("users_affected_min") if enrichment.user_impact else None),
        'users_affected_max': raw_get("users_affected_max") or (enrichment.user_impact.get("users_affected_max") if enrichment.user_impact else None),
        
        # Financial impact - use raw JSON data
        'recovery_costs_min': raw_get("recovery_costs_min") or (enrichment.financial_impact.get("recovery_costs_min") if enrichment.financial_impact else None),
        'recovery_costs_max': raw_get("recovery_costs_max") or (enrichment.financial_impact.get("recovery_costs_max") if enrichment.financial_impact else None),
        'legal_costs': raw_get("legal_costs") or (enrichment.financial_impact.get("legal_costs") if enrichment.financial_impact else None),
        'notification_costs': raw_get("notification_costs") or (enrichment.financial_impact.get("notification_costs") if enrichment.financial_impact else None),
        'insurance_claim': raw_get("insurance_claim") if raw_get("insurance_claim") is not None else (enrichment.financial_impact.get("insurance_claim") if enrichment.financial_impact else None),
        'insurance_claim_amount': raw_get("insurance_claim_amount") or (enrichment.financial_impact.get("insurance_claim_amount") if enrichment.financial_impact else None),
        'business_impact': raw_get("business_impact") or (enrichment.attack_dynamics.business_impact if enrichment.attack_dynamics else None),
        
        # Regulatory impact - use raw JSON data
        'gdpr_breach': raw_get("gdpr_breach") if raw_get("gdpr_breach") is not None else (enrichment.regulatory_impact.get("gdpr_breach") if enrichment.regulatory_impact else None),
        'hipaa_breach': raw_get("hipaa_breach") if raw_get("hipaa_breach") is not None else (enrichment.regulatory_impact.get("hipaa_breach") if enrichment.regulatory_impact else None),
        'ferpa_breach': raw_get("ferpa_breach") if raw_get("ferpa_breach") is not None else (enrichment.regulatory_impact.get("ferc_breach") if enrichment.regulatory_impact else None),
        'breach_notification_required': raw_get("breach_notification_required") if raw_get("breach_notification_required") is not None else (enrichment.regulatory_impact.get("breach_notification_required") if enrichment.regulatory_impact else None),
        'notifications_sent': raw_get("notifications_sent") if raw_get("notifications_sent") is not None else (enrichment.regulatory_impact.get("notifications_sent") if enrichment.regulatory_impact else None),
        'fine_imposed': raw_get("fine_imposed") if raw_get("fine_imposed") is not None else (enrichment.regulatory_impact.get("fine_imposed") if enrichment.regulatory_impact else None),
        'fine_amount': raw_get("fine_amount") or (enrichment.regulatory_impact.get("fine_amount") if enrichment.regulatory_impact else None),
        'lawsuits_filed': raw_get("lawsuits_filed") if raw_get("lawsuits_filed") is not None else (enrichment.regulatory_impact.get("lawsuits_filed") if enrichment.regulatory_impact else None),
        'class_action': raw_get("class_action") if raw_get("class_action") is not None else (enrichment.regulatory_impact.get("class_action") if enrichment.regulatory_impact else None),
        
        # Recovery - use raw JSON data for dates and metrics
        'recovery_timeframe_days': raw_get("recovery_timeframe_days") or (enrichment.attack_dynamics.recovery_timeframe_days if enrichment.attack_dynamics else (enrichment.recovery_metrics.get("recovery_timeframe_days") if enrichment.recovery_metrics else None)),
        'recovery_started_date': raw_get("recovery_started_date") or (enrichment.recovery_metrics.get("recovery_started_date") if enrichment.recovery_metrics else None),
        'recovery_completed_date': raw_get("recovery_completed_date") or raw_get("service_restoration_date") or (enrichment.recovery_metrics.get("recovery_completed_date") if enrichment.recovery_metrics else None),
        'from_backup': raw_get("from_backup") if raw_get("from_backup") is not None else (enrichment.recovery_metrics.get("from_backup") if enrichment.recovery_metrics else None),
        'mfa_implemented': raw_get("mfa_implemented") if raw_get("mfa_implemented") is not None else (enrichment.recovery_metrics.get("mfa_implemented") if enrichment.recovery_metrics else None),
        'incident_response_firm': raw_get("incident_response_firm") or (enrichment.recovery_metrics.get("incident_response_firm") if enrichment.recovery_metrics else None),
        'forensics_firm': raw_get("forensics_firm") or (enrichment.recovery_metrics.get("forensics_firm") if enrichment.recovery_metrics else None),
        
        # Transparency - use raw JSON data
        'public_disclosure': raw_get("public_disclosure") or raw_get("was_disclosed_publicly") if (raw_get("public_disclosure") is not None or raw_get("was_disclosed_publicly") is not None) else (enrichment.transparency_metrics.get("public_disclosure") if enrichment.transparency_metrics else None),
        'public_disclosure_date': raw_get("public_disclosure_date") or (enrichment.transparency_metrics.get("public_disclosure_date") if enrichment.transparency_metrics else None),
        'disclosure_delay_days': raw_get("disclosure_delay_days") or (enrichment.transparency_metrics.get("disclosure_delay_days") if enrichment.transparency_metrics else None),
        'transparency_level': raw_get("transparency_level") or (enrichment.transparency_metrics.get("transparency_level") if enrichment.transparency_metrics else None),
        
        # Timeline and MITRE - prefer from enrichment result (properly parsed)
        'timeline_json': json.dumps([e.model_dump() for e in enrichment.timeline]) if enrichment.timeline else (json.dumps(raw_get("timeline")) if raw_get("timeline") else None),
        'timeline_events_count': len(enrichment.timeline) if enrichment.timeline else (len(raw_get("timeline", [])) if raw_get("timeline") else 0),
        'mitre_techniques_json': json.dumps([t.model_dump() for t in enrichment.mitre_attack_techniques]) if enrichment.mitre_attack_techniques else (json.dumps(raw_get("mitre_attack_techniques")) if raw_get("mitre_attack_techniques") else None),
        'mitre_techniques_count': len(enrichment.mitre_attack_techniques) if enrichment.mitre_attack_techniques else (len(raw_get("mitre_attack_techniques", [])) if raw_get("mitre_attack_techniques") else 0),
        
        # Summary
        'enriched_summary': enrichment.enriched_summary or raw_get("enriched_summary"),
        'extraction_notes': enrichment.extraction_notes or raw_get("extraction_notes"),
        'confidence': raw_get("confidence"),
    }
    
    # Convert booleans to integers for SQLite
    for key, value in flat.items():
        if isinstance(value, bool):
            flat[key] = 1 if value else 0
    
    return flat


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
) -> bool:
    """
    Check if enrichment should be upgraded.
    
    Always returns True (no confidence-based comparison anymore).
    
    Args:
        conn: Database connection
        incident_id: Incident ID to check
        
    Returns:
        True (always allow upgrade)
    """
    # Always allow upgrade - no confidence-based comparison
    return True


def save_enrichment_result(
    conn: sqlite3.Connection,
    incident_id: str,
    enrichment_result: CTIEnrichmentResult,
    force_replace: bool = False,
    raw_json_data: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Save LLM enrichment result to database with optimized structure.
    
    Stores both:
    1. Full JSON in incident_enrichments table (for complete record)
    2. Flattened fields in incident_enrichments_flat table (for fast queries/CSV)
    
    Args:
        conn: Database connection
        incident_id: Incident ID to update
        enrichment_result: CTIEnrichmentResult to save
        force_replace: If True, replace existing enrichment regardless of confidence
        
    Returns:
        True if enrichment was saved, False if skipped
    """
    # Ensure tables exist
    init_incident_enrichments_table(conn)
    
    # Check if we should upgrade existing enrichment
    if not force_replace:
        existing_enrichment = get_enrichment_result(conn, incident_id)
        if existing_enrichment:
            logger.info(
                f"Existing enrichment found for incident {incident_id}, replacing it."
            )
    
    now = datetime.utcnow().isoformat()
    
    # Convert enrichment result to JSON for storage
    enrichment_json = enrichment_result.model_dump_json(indent=2)
    
    # Get incident data for flattened table (fallback values)
    cur = conn.execute(
        "SELECT institution_type, country, region, city FROM incidents WHERE incident_id = ?",
        (incident_id,)
    )
    incident_row = cur.fetchone()
    institution_type_fallback = incident_row["institution_type"] if incident_row else None
    country_fallback = incident_row["country"] if incident_row else None
    region_fallback = incident_row["region"] if incident_row else None
    city_fallback = incident_row["city"] if incident_row else None
    
    # Use LLM-extracted values if available (from raw JSON), otherwise fallback to incident table
    institution_type = raw_json_data.get("institution_type") if raw_json_data else institution_type_fallback
    
    # Normalize country to full name (not code)
    from src.edu_cti.core.countries import normalize_country
    country_raw = raw_json_data.get("country") if raw_json_data else country_fallback
    country = normalize_country(country_raw) if country_raw else None
    
    region = raw_json_data.get("region") if raw_json_data else region_fallback
    city = raw_json_data.get("city") if raw_json_data else city_fallback
    
    # Update incident with enrichment data
    primary_url = enrichment_result.primary_url
    summary = enrichment_result.enriched_summary
    
    # Build timeline JSON (handle None/empty)
    timeline_json = None
    if enrichment_result.timeline:
        timeline_json = json.dumps([event.model_dump() for event in enrichment_result.timeline])
    
    # Build MITRE ATT&CK JSON (handle None/empty)
    mitre_json = None
    if enrichment_result.mitre_attack_techniques:
        mitre_json = json.dumps([tech.model_dump() for tech in enrichment_result.mitre_attack_techniques])
    
    # Build attack dynamics JSON
    attack_dynamics_json = None
    if enrichment_result.attack_dynamics:
        attack_dynamics_json = enrichment_result.attack_dynamics.model_dump_json(indent=2)
    
    # Extract incident_date from LLM response (timeline first event or direct field)
    llm_incident_date = None
    llm_date_precision = None
    
    # Try to get incident_date from raw JSON data (direct extraction)
    if raw_json_data:
        llm_incident_date = raw_json_data.get("incident_date")
        llm_date_precision = raw_json_data.get("incident_date_precision")
    
    # Fallback: Try to extract from timeline (earliest event)
    if not llm_incident_date and enrichment_result.timeline:
        # Get the earliest date from timeline events
        dated_events = [e for e in enrichment_result.timeline if e.date]
        if dated_events:
            # Sort by date and get earliest
            earliest_event = min(dated_events, key=lambda e: e.date)
            llm_incident_date = earliest_event.date
            llm_date_precision = earliest_event.date_precision or "approximate"
    
    # Update incident record - include incident_date and country if LLM extracted them
    update_fields = """
        llm_enriched = 1,
        llm_enriched_at = ?,
        primary_url = ?,
        llm_summary = ?,
        llm_timeline = ?,
        llm_mitre_attack = ?,
        llm_attack_dynamics = ?,
        last_updated_at = ?
    """
    
    update_params = [
        now,
        primary_url,
        summary,
        timeline_json,
        mitre_json,
        attack_dynamics_json,
        now,
    ]
    
    # Also update country in incidents table if we have a normalized value
    if country:
        update_fields += ",\n        country = ?"
        update_params.append(country)
    
    # Update incident_date if LLM extracted it
    # Always use LLM-extracted date (it's more accurate than source_published_date)
    if llm_incident_date:
        update_fields += """,
        incident_date = ?,
        date_precision = ?
        """
        update_params.extend([llm_incident_date, llm_date_precision or "approximate"])
    
    update_params.append(incident_id)
    
    conn.execute(
        f"""
        UPDATE incidents
        SET {update_fields}
        WHERE incident_id = ?
        """,
        tuple(update_params)
    )
    
    # Save full JSON to incident_enrichments table
    cur = conn.execute(
        "SELECT incident_id FROM incident_enrichments WHERE incident_id = ?",
        (incident_id,)
    )
    exists = cur.fetchone() is not None
    
    if exists:
        conn.execute(
            """
            UPDATE incident_enrichments
            SET enrichment_data = ?, updated_at = ?
            WHERE incident_id = ?
            """,
            (enrichment_json, now, incident_id)
        )
    else:
        conn.execute(
            """
            INSERT INTO incident_enrichments
            (incident_id, enrichment_data, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (incident_id, enrichment_json, now, now)
        )
    
    # Flatten and save to incident_enrichments_flat table
    # Pass raw_json_data to capture all LLM-extracted fields
    flat_data = _flatten_enrichment_for_db(enrichment_result, raw_json_data)
    flat_data['incident_id'] = incident_id
    # Override with incident table fallbacks only if not already set from raw_json_data
    if not flat_data.get('institution_type'):
        flat_data['institution_type'] = institution_type
    if not flat_data.get('country'):
        flat_data['country'] = country
    if not flat_data.get('region'):
        flat_data['region'] = region
    if not flat_data.get('city'):
        flat_data['city'] = city
    flat_data['created_at'] = now
    flat_data['updated_at'] = now
    
    # Check if flat record exists
    cur = conn.execute(
        "SELECT incident_id FROM incident_enrichments_flat WHERE incident_id = ?",
        (incident_id,)
    )
    flat_exists = cur.fetchone() is not None
    
    # Define all columns in order
    all_columns = [
        'incident_id', 'is_education_related', 'institution_name', 'institution_type',
        'country', 'region', 'city', 'attack_category', 'attack_vector', 'initial_access_vector',
        'initial_access_description', 'ransomware_family', 'threat_actor_name', 'threat_actor_claim_url',
        'was_ransom_demanded', 'ransom_amount', 'ransom_currency', 'ransom_paid', 'ransom_paid_amount',
        'data_breached', 'data_exfiltrated', 'records_affected_exact', 'records_affected_min',
        'records_affected_max', 'pii_records_leaked', 'systems_affected_codes', 'critical_systems_affected',
        'network_compromised', 'email_system_affected', 'student_portal_affected', 'research_systems_affected',
        'hospital_systems_affected', 'cloud_services_affected', 'third_party_vendor_impact', 'vendor_name',
        'teaching_impacted', 'teaching_disrupted', 'research_impacted', 'research_disrupted',
        'admissions_disrupted', 'enrollment_disrupted', 'payroll_disrupted', 'classes_cancelled',
        'exams_postponed', 'downtime_days', 'outage_duration_hours', 'students_affected', 'staff_affected',
        'faculty_affected', 'users_affected_exact', 'users_affected_min', 'users_affected_max',
        'recovery_costs_min', 'recovery_costs_max', 'legal_costs', 'notification_costs', 'insurance_claim',
        'insurance_claim_amount', 'business_impact', 'gdpr_breach', 'hipaa_breach', 'ferpa_breach',
        'breach_notification_required', 'notifications_sent', 'fine_imposed', 'fine_amount',
        'lawsuits_filed', 'class_action', 'recovery_timeframe_days', 'recovery_started_date',
        'recovery_completed_date', 'from_backup', 'mfa_implemented', 'incident_response_firm',
        'forensics_firm', 'public_disclosure', 'public_disclosure_date', 'disclosure_delay_days',
        'transparency_level', 'timeline_json', 'timeline_events_count', 'mitre_techniques_json',
        'mitre_techniques_count', 'enriched_summary', 'extraction_notes', 'confidence',
        'created_at', 'updated_at'
    ]
    
    if flat_exists:
        # Update existing flat record
        update_fields = [f"{col} = ?" for col in all_columns if col != 'incident_id']
        update_values = [flat_data.get(col) for col in all_columns if col != 'incident_id']
        update_values.append(incident_id)
        
        conn.execute(
            f"""
            UPDATE incident_enrichments_flat
            SET {', '.join(update_fields)}
            WHERE incident_id = ?
            """,
            update_values
        )
    else:
        # Insert new flat record
        values = [flat_data.get(col) for col in all_columns]
        
        conn.execute(
            f"""
            INSERT INTO incident_enrichments_flat ({', '.join(all_columns)})
            VALUES ({', '.join(['?'] * len(all_columns))})
            """,
            values
        )
    
    conn.commit()
    logger.info(
        f"Saved enrichment result for incident {incident_id} (JSON + flattened)"
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


def get_enrichment_flat(
    conn: sqlite3.Connection,
    incident_id: str,
) -> Optional[Dict[str, Any]]:
    """
    Get flattened enrichment data for an incident (faster for queries/CSV).
    
    Args:
        conn: Database connection
        incident_id: Incident ID to retrieve
        
    Returns:
        Dictionary with flattened enrichment fields, or None if not found
    """
    init_incident_enrichments_table(conn)
    
    cur = conn.execute(
        """
        SELECT * FROM incident_enrichments_flat
        WHERE incident_id = ?
        """,
        (incident_id,)
    )
    
    row = cur.fetchone()
    if not row:
        return None
    
    # Convert sqlite3.Row to dict
    return dict(row)


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
        
        # Remove from incident_enrichments_flat table
        conn.execute(
            "DELETE FROM incident_enrichments_flat WHERE incident_id = ?",
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
        
        # Clear incident_enrichments_flat table
        conn.execute("DELETE FROM incident_enrichments_flat")
        
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
