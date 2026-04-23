"""
Database operations for Phase 2 LLM enrichment.

Handles storage and retrieval of enrichment results with optimized structure
for CSV export and dashboard analytics.
"""

import json
import sqlite3
import logging
import re
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

from src.edu_cti.core.countries import (
    COUNTRY_ALIASES,
    COUNTRY_NAME_TO_CODE,
    get_country_code,
    normalize_country,
)
from src.edu_cti.core.db import get_connection
from src.edu_cti.pipeline.phase2.schemas import CTIEnrichmentResult
from src.edu_cti.pipeline.phase2.utils.deduplication import choose_best_institution_name, clean_institution_name
from src.edu_cti.pipeline.phase2.extraction.json_to_schema_mapper import normalize_institution_type
from src.edu_cti.core.config import DB_PATH, SERP_MAX_ATTEMPTS

logger = logging.getLogger(__name__)

_COUNTRY_TEXT_PATTERNS = tuple(
    (
        re.compile(rf"(?<!\w){re.escape(label)}(?!\w)", re.IGNORECASE),
        normalize_country(label),
    )
    for label in sorted(
        set(COUNTRY_NAME_TO_CODE.keys()) | set(COUNTRY_ALIASES.keys()),
        key=len,
        reverse=True,
    )
    if label and not (len(label) == 2 and label.isalpha())
)

_REASONING_INSTITUTION_PATTERNS = (
    re.compile(
        r"\bVictim is (?P<name>[^.]+?)(?:,| which| who| that| managing| making| serving|$)",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:This incident|The incident) involves (?P<name>[^.]+?)(?:,| which| who| that|$)",
        re.IGNORECASE,
    ),
    re.compile(
        r"\binvolves (?P<name>[A-Z][^.]+?)(?:,| which| who| that|$)",
        re.IGNORECASE,
    ),
)

_CURATED_NOTE_PATTERNS = {
    "ransomware_family": re.compile(r"\bRansomware:\s*(?P<value>[^|\n]+)", re.IGNORECASE),
    "ransom_paid": re.compile(r"\bRansom paid:\s*(?P<value>[^|\n]+)", re.IGNORECASE),
    "ransom_amount": re.compile(r"\bRansom amount:\s*(?P<value>[^|\n]+)", re.IGNORECASE),
    "records_affected_exact": re.compile(r"\bRecords affected:\s*(?P<value>[^|\n]+)", re.IGNORECASE),
}


def _parse_curated_bool(value: Optional[str]) -> Optional[bool]:
    if not value:
        return None
    normalized = value.strip().lower()
    if normalized in {"yes", "true", "1", "paid"}:
        return True
    if normalized in {"no", "false", "0", "refused", "declined"}:
        return False
    return None


def _parse_curated_number(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    match = re.search(r"([0-9][0-9,]*(?:\.[0-9]+)?)", value)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def _parse_curated_integer(value: Optional[str]) -> Optional[int]:
    parsed = _parse_curated_number(value)
    return int(parsed) if parsed is not None else None


def _extract_curated_fields_from_notes(notes: Optional[str]) -> Dict[str, Any]:
    """
    Parse trusted, structured note fragments written by curated ingestion sources
    such as Comparitech.
    """
    if not notes:
        return {}

    extracted: Dict[str, Any] = {}

    family_match = _CURATED_NOTE_PATTERNS["ransomware_family"].search(notes)
    if family_match:
        family = family_match.group("value").strip()
        if family and family.lower() not in {"unknown", "none", "n/a"}:
            extracted["ransomware_family"] = family

    paid_match = _CURATED_NOTE_PATTERNS["ransom_paid"].search(notes)
    ransom_paid = _parse_curated_bool(paid_match.group("value")) if paid_match else None
    if ransom_paid is not None:
        extracted["ransom_paid"] = 1 if ransom_paid else 0
        extracted["was_ransom_demanded"] = 1

    amount_match = _CURATED_NOTE_PATTERNS["ransom_amount"].search(notes)
    ransom_amount = _parse_curated_number(amount_match.group("value")) if amount_match else None
    if ransom_amount is not None:
        extracted["ransom_amount"] = ransom_amount
        extracted["ransom_currency"] = "USD"
        extracted["was_ransom_demanded"] = 1
        if extracted.get("ransom_paid") == 1:
            extracted["ransom_paid_amount"] = ransom_amount

    records_match = _CURATED_NOTE_PATTERNS["records_affected_exact"].search(notes)
    records_affected = _parse_curated_integer(records_match.group("value")) if records_match else None
    if records_affected is not None:
        extracted["records_affected_exact"] = records_affected

    return extracted


def _apply_curated_note_fallbacks(flat_data: Dict[str, Any], notes: Optional[str]) -> None:
    """Backfill missing structured fields from trusted curated-source note fragments."""
    # Do not populate structured CTI for rows already marked as non-education.
    if flat_data.get("is_education_related") in {0, False}:
        return

    curated = _extract_curated_fields_from_notes(notes)
    if not curated:
        return

    for key, value in curated.items():
        if flat_data.get(key) in (None, "", [], {}):
            flat_data[key] = value


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
            country TEXT,  -- Full country name (normalized)
            country_code TEXT,  -- ISO 3166-1 alpha-2 code (e.g., "US", "GB")
            region TEXT,
            city TEXT,
            
            -- Incident Classification
            incident_severity TEXT,  -- critical / high / medium / low
            institution_size TEXT,   -- small_under_5k / medium_5k_20k / large_20k_50k / very_large_over_50k

            -- Attack Details
            attack_category TEXT,
            attack_vector TEXT,
            initial_access_vector TEXT,
            initial_access_description TEXT,
            ransomware_family TEXT,
            threat_actor_name TEXT,
            threat_actor_category TEXT,   -- ransomware_gang / apt_nation_state / hacktivist / etc.
            threat_actor_motivation TEXT, -- financial / espionage / hacktivism / etc.
            threat_actor_origin_country TEXT,
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
            
            -- Data Categories
            data_categories TEXT,  -- JSON array of category codes (e.g. ["student_pii","employee_ssn"])

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

            -- Threat Intelligence (new)
            malware_families TEXT,           -- JSON array
            attacker_tools TEXT,             -- JSON array
            threat_actor_aliases TEXT,       -- JSON array
            attack_campaign_name TEXT,
            cloud_provider TEXT,
            infrastructure_type TEXT,
            dwell_time_days REAL,
            mttd_hours REAL,
            mttr_hours REAL,

            -- Vulnerabilities (new)
            cve_ids TEXT,                    -- JSON array of CVE IDs
            cvss_scores TEXT,                -- JSON array of CVSS scores (REAL)
            vulnerability_names TEXT,        -- JSON array
            affected_products TEXT,          -- JSON array

            -- Financial (additional)
            total_cost_estimate REAL,

            -- Operational (additional)
            partial_service_days REAL,
            clinical_operations_disrupted INTEGER,
            graduation_delayed INTEGER,
            online_learning_disrupted INTEGER,

            -- Recovery (additional)
            backup_status TEXT,
            backup_age_days REAL,
            law_enforcement_involved INTEGER,
            law_enforcement_agency TEXT,
            detection_source TEXT,

            -- Transparency (additional)
            official_statement_url TEXT,

            -- Research Impact (new)
            research_projects_affected INTEGER,
            research_data_compromised INTEGER,
            publications_delayed INTEGER,
            grants_affected INTEGER,
            research_area TEXT,

            -- Regulatory (additional)
            regulatory_context TEXT,         -- JSON array of applicable regulations

            -- Data (additional)
            data_volume_gb REAL,

            -- Summary
            enriched_summary TEXT,
            extraction_notes TEXT,
            confidence REAL,

            -- Metadata
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            enriched_at TEXT,
            skip_reason TEXT,

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
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_enrichments_education
        ON incident_enrichments_flat(is_education_related)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_enrichments_threat_actor
        ON incident_enrichments_flat(threat_actor_name)
        """
    )
    # Composite indexes for common dashboard query patterns
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_enrichments_edu_incident
        ON incident_enrichments_flat(is_education_related, incident_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_enrichments_edu_country
        ON incident_enrichments_flat(is_education_related, country)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_enrichments_edu_attack
        ON incident_enrichments_flat(is_education_related, attack_category)
        """
    )

    # Add columns to existing tables if they don't exist (migration)
    for col, col_type in [
        ("country_code", "TEXT"), ("enriched_at", "TEXT"), ("skip_reason", "TEXT"),
        ("data_categories", "TEXT"),
        ("incident_severity", "TEXT"), ("institution_size", "TEXT"),
        ("threat_actor_category", "TEXT"), ("threat_actor_motivation", "TEXT"),
        ("threat_actor_origin_country", "TEXT"),
        # Threat intelligence
        ("malware_families", "TEXT"),
        ("attacker_tools", "TEXT"),
        ("threat_actor_aliases", "TEXT"),
        ("attack_campaign_name", "TEXT"),
        ("cloud_provider", "TEXT"),
        ("infrastructure_type", "TEXT"),
        ("dwell_time_days", "REAL"),
        ("mttd_hours", "REAL"),
        ("mttr_hours", "REAL"),
        # Vulnerabilities
        ("cve_ids", "TEXT"),
        ("cvss_scores", "TEXT"),
        ("vulnerability_names", "TEXT"),
        ("affected_products", "TEXT"),
        # Financial
        ("total_cost_estimate", "REAL"),
        # Operational
        ("partial_service_days", "REAL"),
        ("clinical_operations_disrupted", "INTEGER"),
        ("graduation_delayed", "INTEGER"),
        ("online_learning_disrupted", "INTEGER"),
        # Recovery
        ("backup_status", "TEXT"),
        ("backup_age_days", "REAL"),
        ("law_enforcement_involved", "INTEGER"),
        ("law_enforcement_agency", "TEXT"),
        ("detection_source", "TEXT"),
        # Transparency
        ("official_statement_url", "TEXT"),
        # Research impact
        ("research_projects_affected", "INTEGER"),
        ("research_data_compromised", "INTEGER"),
        ("publications_delayed", "INTEGER"),
        ("grants_affected", "INTEGER"),
        ("research_area", "TEXT"),
        # Regulatory
        ("regulatory_context", "TEXT"),
        # Data
        ("data_volume_gb", "REAL"),
    ]:
        try:
            conn.execute(f"ALTER TABLE incident_enrichments_flat ADD COLUMN {col} {col_type}")
        except sqlite3.OperationalError:
            pass  # Column already exists

    # Checkpoint table — tracks which incidents have had articles fetched so
    # a crashed pipeline resumes without re-fetching already-processed URLs.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_checkpoint (
            incident_id TEXT PRIMARY KEY,
            phase TEXT NOT NULL DEFAULT 'article_fetch',
            completed_at TEXT NOT NULL
        )
        """
    )

    conn.commit()


def checkpoint_mark(conn: sqlite3.Connection, incident_id: str, phase: str = "article_fetch") -> None:
    """Record that article fetching is complete for an incident."""
    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT INTO pipeline_checkpoint (incident_id, phase, completed_at)
        VALUES (?, ?, ?)
        ON CONFLICT(incident_id) DO UPDATE SET phase=excluded.phase, completed_at=excluded.completed_at
        """,
        (incident_id, phase, now),
    )
    conn.commit()


def checkpoint_get_fetched(conn: sqlite3.Connection) -> set:
    """Return set of incident_ids that have already completed article fetch."""
    cur = conn.execute("SELECT incident_id FROM pipeline_checkpoint WHERE phase = 'article_fetch'")
    return {row[0] for row in cur.fetchall()}


def checkpoint_clear(conn: sqlite3.Connection, incident_id: str) -> None:
    """Clear fetch checkpoint state once an incident reaches a terminal outcome."""
    conn.execute(
        "DELETE FROM pipeline_checkpoint WHERE incident_id = ?",
        (incident_id,),
    )
    conn.commit()


def _derive_country_from_context(
    conn: sqlite3.Connection,
    incident_id: str,
    incident_row: Optional[sqlite3.Row] = None,
) -> Optional[str]:
    """
    Recover a country only from explicit textual evidence.

    We intentionally require exactly one normalized country mention across the
    incident title/subtitle and the primary fetched article. If the context
    mentions multiple countries, we leave the field empty rather than guessing.
    """
    texts: List[str] = []

    if incident_row:
        texts.extend(
            value for value in (incident_row["title"], incident_row["subtitle"]) if value
        )

    article_row = conn.execute(
        """
        SELECT title, content
        FROM articles
        WHERE incident_id = ? AND fetch_successful = 1
        ORDER BY is_primary DESC, fetched_at DESC
        LIMIT 1
        """,
        (incident_id,),
    ).fetchone()
    if article_row:
        texts.extend(value for value in (article_row["title"], article_row["content"]) if value)

    if not texts:
        return None

    combined_text = "\n".join(texts)
    matches = {
        normalized
        for pattern, normalized in _COUNTRY_TEXT_PATTERNS
        if normalized and pattern.search(combined_text)
    }
    if len(matches) == 1:
        return next(iter(matches))
    return None


def _extract_institution_from_reasoning(reasoning: Optional[str]) -> Optional[str]:
    """Recover an institution label from the LLM's education-relevance reasoning."""
    if not reasoning:
        return None

    text = re.sub(r"\s+", " ", str(reasoning)).strip()
    if not text:
        return None

    for pattern in _REASONING_INSTITUTION_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        candidate = match.group("name").strip(" \"'“”.,;:-")
        cleaned = clean_institution_name(candidate)
        if cleaned:
            return cleaned
        if candidate:
            return candidate
    return None


def _get_primary_article_metadata(
    conn: sqlite3.Connection,
    incident_id: str,
    primary_url: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """Return stored metadata for the selected primary article when available."""
    if not primary_url:
        return {"author": None, "publish_date": None}

    try:
        row = conn.execute(
            """
            SELECT author, publish_date
            FROM articles
            WHERE incident_id = ? AND url = ?
            LIMIT 1
            """,
            (incident_id, primary_url),
        ).fetchone()
    except sqlite3.OperationalError:
        return {"author": None, "publish_date": None}

    if not row:
        return {"author": None, "publish_date": None}

    return {
        "author": row["author"],
        "publish_date": row["publish_date"],
    }


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
    
    country_raw = raw_get("country")
    country_normalized = normalize_country(country_raw) if country_raw else None
    country_code = get_country_code(country_normalized) if country_normalized else None

    # Derive data_breached: use explicit LLM field first, then infer from attack_category / data signals.
    # The LLM often classifies attack_category correctly but omits the boolean.
    _DATA_BREACH_CATS = {
        "data_breach_external", "data_breach_internal", "data_exposure_misconfiguration",
        "data_leak_accidental", "ransomware_double_extortion", "ransomware_triple_extortion",
        "ransomware_data_leak_only",
    }
    _llm_data_breached = raw_get("data_breached")
    if _llm_data_breached is not None:
        _derived_data_breached = _llm_data_breached
    elif (raw_get("attack_category") or "").lower() in _DATA_BREACH_CATS:
        _derived_data_breached = True
    elif raw_get("data_exfiltrated"):
        _derived_data_breached = True
    elif raw_get("data_categories"):
        _derived_data_breached = True
    elif raw_get("records_affected_exact") or raw_get("records_affected_min"):
        _derived_data_breached = True
    else:
        _derived_data_breached = None

    flat = {
        'incident_id': None,  # Will be set by caller
        'is_education_related': enrichment.education_relevance.is_education_related if enrichment.education_relevance else raw_get("is_edu_cyber_incident"),
        # institution_name is always overridden by save_enrichment_result() with the
        # resolved_institution_name it computed — set None here as a placeholder.
        'institution_name': None,
        'institution_type': normalize_institution_type(raw_get("institution_type")),
        'institution_size': raw_get("institution_size"),
        'country': country_normalized,
        'country_code': country_code,
        'region': raw_get("region"),
        'city': raw_get("city"),

        # Incident classification
        'incident_severity': raw_get("incident_severity"),

        # Attack details - prefer raw JSON data for direct fields
        'attack_category': raw_get("attack_category"),
        'attack_vector': raw_get("attack_vector") or (enrichment.attack_dynamics.attack_vector if enrichment.attack_dynamics else None),
        'initial_access_vector': raw_get("initial_access_vector"),
        'initial_access_description': enrichment.initial_access_description or raw_get("initial_access_description"),
        'ransomware_family': raw_get("ransomware_family_or_group") or raw_get("ransomware_family") or (enrichment.attack_dynamics.ransomware_family if enrichment.attack_dynamics else None),

        # Threat actor
        'threat_actor_name': raw_get("threat_actor_name"),
        'threat_actor_category': raw_get("threat_actor_category"),
        'threat_actor_motivation': raw_get("threat_actor_motivation"),
        'threat_actor_origin_country': raw_get("threat_actor_origin_country"),
        'threat_actor_claim_url': raw_get("threat_actor_claim_url"),
        
        # Ransom - use raw JSON data for exact values
        'was_ransom_demanded': raw_get("was_ransom_demanded") if raw_get("was_ransom_demanded") is not None else (enrichment.attack_dynamics.ransom_demanded if enrichment.attack_dynamics else None),
        'ransom_amount': (raw_get("ransom_amount") or raw_get("ransom_amount_exact")
                         or (enrichment.attack_dynamics.ransom_amount if enrichment.attack_dynamics else None)),
        'ransom_currency': raw_get("ransom_currency"),
        'ransom_paid': raw_get("ransom_paid") if raw_get("ransom_paid") is not None else (enrichment.attack_dynamics.ransom_paid if enrichment.attack_dynamics else None),
        'ransom_paid_amount': raw_get("ransom_paid_amount"),
        
        # Data impact
        'data_breached': _derived_data_breached,
        'data_exfiltrated': raw_get("data_exfiltrated") if raw_get("data_exfiltrated") is not None else (enrichment.attack_dynamics.data_exfiltration if enrichment.attack_dynamics else (enrichment.data_impact.get("data_exfiltrated") if enrichment.data_impact else None)),
        'records_affected_exact': raw_get("records_affected_exact") or raw_get("pii_records_leaked") or (enrichment.data_impact.get("records_affected_exact") if enrichment.data_impact else None),
        'records_affected_min': raw_get("records_affected_min") or (enrichment.data_impact.get("records_affected_min") if enrichment.data_impact else None),
        'records_affected_max': raw_get("records_affected_max") or (enrichment.data_impact.get("records_affected_max") if enrichment.data_impact else None),
        'pii_records_leaked': raw_get("pii_records_leaked"),
        'data_categories': json.dumps(
            raw_get("data_categories")
            or (enrichment.data_impact.get("data_types_affected") if enrichment.data_impact else None)
        ) if (raw_get("data_categories") or (enrichment.data_impact and enrichment.data_impact.get("data_types_affected"))) else None,
        
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
        # Schema uses *_usd suffixed names; legacy aliases kept for raw_get
        'recovery_costs_min': (raw_get("recovery_costs_min") or raw_get("recovery_cost_usd")
                               or (enrichment.financial_impact.get("recovery_costs_min") if enrichment.financial_impact else None)),
        'recovery_costs_max': (raw_get("recovery_costs_max")
                               or (enrichment.financial_impact.get("recovery_costs_max") if enrichment.financial_impact else None)),
        'legal_costs': (raw_get("legal_costs") or raw_get("legal_cost_usd")
                        or (enrichment.financial_impact.get("legal_costs") if enrichment.financial_impact else None)),
        'notification_costs': (raw_get("notification_costs") or raw_get("notification_cost_usd")
                               or (enrichment.financial_impact.get("notification_costs") if enrichment.financial_impact else None)),
        'insurance_claim': raw_get("insurance_claim") if raw_get("insurance_claim") is not None else (enrichment.financial_impact.get("insurance_claim") if enrichment.financial_impact else None),
        'insurance_claim_amount': (raw_get("insurance_claim_amount") or raw_get("insurance_payout_usd")
                                   or (enrichment.financial_impact.get("insurance_claim_amount") if enrichment.financial_impact else None)),
        'business_impact': raw_get("business_impact") or (enrichment.attack_dynamics.business_impact if enrichment.attack_dynamics else None),
        
        # Regulatory impact - use raw JSON data
        # Schema: gdpr/hipaa/ferpa_breach not standalone — derived from applicable_regulations
        # Prefer mapper-derived values from enrichment.regulatory_impact
        'gdpr_breach': (raw_get("gdpr_breach") if raw_get("gdpr_breach") is not None
                        else ("GDPR" in (raw_get("applicable_regulations") or []) or None)
                        if raw_get("applicable_regulations") else
                        (enrichment.regulatory_impact.get("gdpr_breach") if enrichment.regulatory_impact else None)),
        'hipaa_breach': (raw_get("hipaa_breach") if raw_get("hipaa_breach") is not None
                         else ("HIPAA" in (raw_get("applicable_regulations") or []) or None)
                         if raw_get("applicable_regulations") else
                         (enrichment.regulatory_impact.get("hipaa_breach") if enrichment.regulatory_impact else None)),
        'ferpa_breach': (raw_get("ferpa_breach") if raw_get("ferpa_breach") is not None
                         else ("FERPA" in (raw_get("applicable_regulations") or []) or None)
                         if raw_get("applicable_regulations") else
                         (enrichment.regulatory_impact.get("ferpa_breach") if enrichment.regulatory_impact else None)),
        'breach_notification_required': raw_get("breach_notification_required") if raw_get("breach_notification_required") is not None else (enrichment.regulatory_impact.get("breach_notification_required") if enrichment.regulatory_impact else None),
        # Schema uses "notification_sent" (singular)
        'notifications_sent': (raw_get("notification_sent") or raw_get("notifications_sent")
                               if (raw_get("notification_sent") is not None or raw_get("notifications_sent") is not None)
                               else (enrichment.regulatory_impact.get("notifications_sent") if enrichment.regulatory_impact else None)),
        'fine_imposed': raw_get("fine_imposed") if raw_get("fine_imposed") is not None else (enrichment.regulatory_impact.get("fine_imposed") if enrichment.regulatory_impact else None),
        # Schema uses "fine_amount_usd"
        'fine_amount': (raw_get("fine_amount_usd") or raw_get("fine_amount")
                        or (enrichment.regulatory_impact.get("fine_amount") if enrichment.regulatory_impact else None)),
        'lawsuits_filed': raw_get("lawsuits_filed") if raw_get("lawsuits_filed") is not None else (enrichment.regulatory_impact.get("lawsuits_filed") if enrichment.regulatory_impact else None),
        # Schema uses "class_action_filed"
        'class_action': (raw_get("class_action_filed") if raw_get("class_action_filed") is not None
                         else raw_get("class_action") if raw_get("class_action") is not None
                         else (enrichment.regulatory_impact.get("class_action") if enrichment.regulatory_impact else None)),
        
        # Recovery - use raw JSON data for dates and metrics
        # Schema uses "recovery_duration_days"; legacy alias "recovery_timeframe_days"
        'recovery_timeframe_days': (
            raw_get("recovery_duration_days") or raw_get("recovery_timeframe_days")
            or (enrichment.attack_dynamics.recovery_timeframe_days if enrichment.attack_dynamics else None)
            or (enrichment.recovery_metrics.get("recovery_timeframe_days") if enrichment.recovery_metrics else None)
        ),
        'recovery_started_date': raw_get("recovery_started_date") or (enrichment.recovery_metrics.get("recovery_started_date") if enrichment.recovery_metrics else None),
        'recovery_completed_date': raw_get("recovery_completed_date") or raw_get("service_restoration_date") or (enrichment.recovery_metrics.get("recovery_completed_date") if enrichment.recovery_metrics else None),
        'from_backup': (
            raw_get("from_backup")
            if raw_get("from_backup") is not None else (
                True if raw_get("recovery_method") in ("backup_restore", "partial_backup_partial_rebuild")
                else (enrichment.recovery_metrics.get("from_backup") if enrichment.recovery_metrics else None)
            )
        ),
        # Schema: mfa_implemented is a value in security_improvements array, not a standalone field
        'mfa_implemented': (raw_get("mfa_implemented")
                            if raw_get("mfa_implemented") is not None
                            else (True if "mfa_implemented" in (raw_get("security_improvements") or [])
                                       or "mfa_expanded" in (raw_get("security_improvements") or [])
                                  else (enrichment.recovery_metrics.get("mfa_implemented") if enrichment.recovery_metrics else None))),
        # Schema uses "ir_firm_engaged" / "forensics_firm_engaged"; legacy aliases kept
        'incident_response_firm': (
            raw_get("ir_firm_engaged") or raw_get("incident_response_firm")
            or (enrichment.recovery_metrics.get("incident_response_firm") if enrichment.recovery_metrics else None)
        ),
        'forensics_firm': (
            raw_get("forensics_firm_engaged") or raw_get("forensics_firm")
            or (enrichment.recovery_metrics.get("forensics_firm") if enrichment.recovery_metrics else None)
        ),
        
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

        # Threat intelligence (new fields)
        # Use isinstance guards so MagicMock stubs in tests don't reach json.dumps
        'malware_families': (json.dumps(enrichment.malware_families)
                             if isinstance(enrichment.malware_families, list) and enrichment.malware_families
                             else (json.dumps(raw_get("malware_families"))
                                   if isinstance(raw_get("malware_families"), list) and raw_get("malware_families")
                                   else None)),
        'attacker_tools': (json.dumps(enrichment.attacker_tools)
                           if isinstance(enrichment.attacker_tools, list) and enrichment.attacker_tools
                           else (json.dumps(raw_get("attacker_tools"))
                                 if isinstance(raw_get("attacker_tools"), list) and raw_get("attacker_tools")
                                 else None)),
        'threat_actor_aliases': (json.dumps(enrichment.threat_actor_aliases)
                                  if isinstance(enrichment.threat_actor_aliases, list) and enrichment.threat_actor_aliases
                                  else (json.dumps(raw_get("threat_actor_aliases"))
                                        if isinstance(raw_get("threat_actor_aliases"), list) and raw_get("threat_actor_aliases")
                                        else None)),
        'attack_campaign_name': (enrichment.attack_campaign_name
                                  if isinstance(enrichment.attack_campaign_name, str)
                                  else None) or raw_get("attack_campaign_name"),
        'cloud_provider': (enrichment.cloud_provider
                           if isinstance(enrichment.cloud_provider, str)
                           else None) or raw_get("cloud_provider"),
        'infrastructure_type': (enrichment.infrastructure_type
                                 if isinstance(enrichment.infrastructure_type, str)
                                 else None) or raw_get("infrastructure_type"),
        'dwell_time_days': (enrichment.dwell_time_days
                            if isinstance(enrichment.dwell_time_days, (int, float))
                            else None) or raw_get("dwell_time_days"),
        'mttd_hours': raw_get("mttd_hours") or (enrichment.recovery_metrics.get("mttd_hours") if isinstance(enrichment.recovery_metrics, dict) else None),
        'mttr_hours': raw_get("mttr_hours") or (enrichment.recovery_metrics.get("mttr_hours") if isinstance(enrichment.recovery_metrics, dict) else None),

        # Vulnerabilities (new fields — flattened from vulnerabilities_exploited list)
        'cve_ids': (json.dumps([v["cve_id"] for v in enrichment.vulnerabilities_exploited if isinstance(v, dict) and v.get("cve_id")])
                    if isinstance(enrichment.vulnerabilities_exploited, list) and enrichment.vulnerabilities_exploited
                    else (json.dumps([v.get("cve_id") for v in raw_get("vulnerabilities_exploited", []) if isinstance(v, dict) and v.get("cve_id")])
                          if isinstance(raw_get("vulnerabilities_exploited"), list) and raw_get("vulnerabilities_exploited")
                          else None)),
        'cvss_scores': (json.dumps([v["cvss_score"] for v in enrichment.vulnerabilities_exploited if isinstance(v, dict) and v.get("cvss_score") is not None])
                        if isinstance(enrichment.vulnerabilities_exploited, list) and enrichment.vulnerabilities_exploited
                        else None),
        'vulnerability_names': (json.dumps([v["vulnerability_name"] for v in enrichment.vulnerabilities_exploited if isinstance(v, dict) and v.get("vulnerability_name")])
                                 if isinstance(enrichment.vulnerabilities_exploited, list) and enrichment.vulnerabilities_exploited
                                 else None),
        'affected_products': (json.dumps([v["affected_product"] for v in enrichment.vulnerabilities_exploited if isinstance(v, dict) and v.get("affected_product")])
                               if isinstance(enrichment.vulnerabilities_exploited, list) and enrichment.vulnerabilities_exploited
                               else None),

        # Financial (additional)
        'total_cost_estimate': (raw_get("currency_normalized_cost_usd") or raw_get("estimated_total_cost_usd")
                                or (enrichment.financial_impact.get("total_cost_estimate") if enrichment.financial_impact else None)),

        # Operational (additional)
        'partial_service_days': raw_get("partial_service_days") or (enrichment.operational_impact_metrics.get("partial_service_days") if enrichment.operational_impact_metrics else None),
        'clinical_operations_disrupted': raw_get("clinical_operations_disrupted") if raw_get("clinical_operations_disrupted") is not None else (enrichment.operational_impact_metrics.get("clinical_operations_disrupted") if enrichment.operational_impact_metrics else None),
        'graduation_delayed': raw_get("graduation_delayed") if raw_get("graduation_delayed") is not None else (enrichment.operational_impact_metrics.get("graduation_delayed") if enrichment.operational_impact_metrics else None),
        'online_learning_disrupted': raw_get("online_learning_disrupted") if raw_get("online_learning_disrupted") is not None else (enrichment.operational_impact_metrics.get("online_learning_disrupted") if enrichment.operational_impact_metrics else None),

        # Recovery (additional)
        'backup_status': raw_get("backup_status") or (enrichment.recovery_metrics.get("backup_status") if enrichment.recovery_metrics else None),
        'backup_age_days': raw_get("backup_age_days") or (enrichment.recovery_metrics.get("backup_age_days") if enrichment.recovery_metrics else None),
        'law_enforcement_involved': raw_get("law_enforcement_involved") if raw_get("law_enforcement_involved") is not None else (enrichment.recovery_metrics.get("law_enforcement_involved") if enrichment.recovery_metrics else None),
        'law_enforcement_agency': (raw_get("law_enforcement_agency") or raw_get("law_enforcement_agencies")
                                   or (enrichment.recovery_metrics.get("law_enforcement_agency") if enrichment.recovery_metrics else None)),
        'detection_source': raw_get("detection_source") or (enrichment.recovery_metrics.get("detection_source") if enrichment.recovery_metrics else None),

        # Transparency (additional)
        'official_statement_url': (raw_get("official_statement_url")
                                   or (enrichment.transparency_metrics.get("official_statement_url") if enrichment.transparency_metrics else None)),

        # Research impact (new fields)
        'research_projects_affected': raw_get("research_projects_affected") if raw_get("research_projects_affected") is not None else (enrichment.research_impact.get("research_projects_affected") if enrichment.research_impact else None),
        'research_data_compromised': raw_get("research_data_compromised") if raw_get("research_data_compromised") is not None else (enrichment.research_impact.get("research_data_compromised") if enrichment.research_impact else None),
        'publications_delayed': raw_get("publications_delayed") if raw_get("publications_delayed") is not None else (enrichment.research_impact.get("publications_delayed") if enrichment.research_impact else None),
        'grants_affected': raw_get("grants_affected") if raw_get("grants_affected") is not None else (enrichment.research_impact.get("grants_affected") if enrichment.research_impact else None),
        'research_area': raw_get("research_area") or (enrichment.research_impact.get("research_area") if enrichment.research_impact else None),

        # Regulatory (additional)
        'regulatory_context': json.dumps(raw_get("applicable_regulations")) if raw_get("applicable_regulations") else (json.dumps(enrichment.regulatory_impact.get("regulatory_context")) if enrichment.regulatory_impact and enrichment.regulatory_impact.get("regulatory_context") else None),

        # Data (additional)
        'data_volume_gb': enrichment.data_volume_gb or raw_get("data_volume_gb"),

        # Summary
        'enriched_summary': enrichment.enriched_summary or raw_get("enriched_summary"),
        'extraction_notes': enrichment.extraction_notes or raw_get("extraction_notes"),
        'confidence': raw_get("confidence"),
    }
    
    # Columns whose values are intentionally JSON-serialised lists (stored as TEXT).
    _JSON_TEXT_COLS = {
        "systems_affected_codes", "timeline_json", "mitre_techniques_json",
        "malware_families", "attacker_tools", "threat_actor_aliases",
        "cve_ids", "cvss_scores", "vulnerability_names", "affected_products",
        "data_categories", "regulatory_context",
    }

    for key, value in flat.items():
        if isinstance(value, bool):
            flat[key] = 1 if value else 0
        elif isinstance(value, list) and key not in _JSON_TEXT_COLS:
            # LLM returned a scalar field as a list — coerce to first element
            flat[key] = value[0] if value else None

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
        SELECT
            i.*,
            CASE WHEN a.incident_id IS NOT NULL THEN 1 ELSE 0 END AS has_articles
        FROM incidents i
        LEFT JOIN (
            SELECT DISTINCT incident_id FROM articles WHERE fetch_successful = 1
        ) a ON a.incident_id = i.incident_id
        WHERE i.llm_enriched = 0
          AND (i.llm_excluded IS NULL OR i.llm_excluded = 0)
          AND (
            -- Has at least one URL, OR
            (i.all_urls IS NOT NULL AND i.all_urls != '')
            OR
            -- Has existing fetched articles in the articles table (resume after restart)
            a.incident_id IS NOT NULL
            OR
            -- URL-less but has a named institution (SERP discovery at fetch time).
            -- Exclude incidents that have exhausted their SERP attempts — they are
            -- unenrichable and will be deleted on the next enrichment run.
            (
              COALESCE(i.serp_attempt_count, 0) < 3
              AND (
                (i.institution_name IS NOT NULL AND i.institution_name != '')
                OR (i.victim_raw_name IS NOT NULL AND i.victim_raw_name != '')
              )
            )
          )
        ORDER BY
            -- Incidents with existing articles first (fast-path, no fetch needed)
            has_articles DESC,
            i.ingested_at DESC
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
            "institution_name": row["institution_name"] or row["victim_raw_name"] or "Unknown",
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
            # True when articles already exist in DB from a previous run.
            # The fetch phase uses this to skip re-fetching and push directly to the LLM queue.
            "has_articles": bool(row["has_articles"]),
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
        """
        SELECT institution_name, victim_raw_name, institution_type, country, region, city,
               title, subtitle, source_published_date, notes, all_urls
        FROM incidents
        WHERE incident_id = ?
        """,
        (incident_id,)
    )
    incident_row = cur.fetchone()
    institution_name_fallback = incident_row["institution_name"] if incident_row else None
    victim_name_fallback = incident_row["victim_raw_name"] if incident_row else None
    institution_type_fallback = incident_row["institution_type"] if incident_row else None
    country_fallback = incident_row["country"] if incident_row else None
    region_fallback = incident_row["region"] if incident_row else None
    city_fallback = incident_row["city"] if incident_row else None
    source_published_date_fallback = incident_row["source_published_date"] if incident_row else None
    notes_fallback = incident_row["notes"] if incident_row else None
    
    def _scalar(v):
        """Coerce list → first element; return other values unchanged."""
        return v[0] if isinstance(v, list) and v else (None if isinstance(v, list) else v)

    # Use LLM-extracted values if available (from raw JSON), otherwise fallback to incident table
    _raw_inst_type = _scalar(raw_json_data.get("institution_type")) if raw_json_data else institution_type_fallback
    institution_type = normalize_institution_type(_raw_inst_type)

    country_raw = _scalar(raw_json_data.get("country")) if raw_json_data else country_fallback
    if not country_raw:
        country_raw = _derive_country_from_context(conn, incident_id, incident_row)
        if isinstance(country_raw, tuple):
            country_raw = next((value for value in country_raw if value), None)
    country = normalize_country(country_raw) if country_raw else None
    country_code = get_country_code(country) if country else None

    region = _scalar(raw_json_data.get("region")) if raw_json_data else region_fallback
    city = _scalar(raw_json_data.get("city")) if raw_json_data else city_fallback
    # Prefer the LLM's extracted name directly — it followed the prompt instruction
    # "institution_name must be ONLY the victim institution label, not a headline".
    # Only fall back to multi-candidate scoring when the LLM returned null, because
    # the scoring function (tokens × 5) rewards length, letting a 15-word headline
    # beat a 2-letter LLM abbreviation like "OU".
    _reasoning_name = _extract_institution_from_reasoning(
        enrichment_result.education_relevance.reasoning
        if enrichment_result.education_relevance else (
            _scalar(raw_json_data.get("education_relevance_reasoning")) if raw_json_data else None
        )
    )
    _llm_name = (
        _scalar(raw_json_data.get("institution_name")) if raw_json_data else None
    ) or (
        enrichment_result.education_relevance.institution_identified
        if enrichment_result.education_relevance else None
    ) or _reasoning_name
    if _llm_name and str(_llm_name).strip():
        # Clean the LLM output (handles the rare case where the LLM still returned
        # a headline despite the instruction), then use it directly.
        _cleaned = clean_institution_name(str(_llm_name).strip())
        resolved_institution_name = _cleaned if _cleaned else str(_llm_name).strip()
    else:
        # LLM returned null — use best available name from ingestion-time data.
        resolved_institution_name = choose_best_institution_name(
            institution_name_fallback,
            victim_name_fallback,
            incident_row["title"] if incident_row else None,
            incident_row["subtitle"] if incident_row else None,
        )
    
    _raw_primary_url = enrichment_result.primary_url
    # Only allow a SERP-discovered URL as primary_url when the incident has no
    # known source URLs. If all_urls is non-empty, the primary_url must come from
    # that list — prevents Chinese mirror sites from overriding the original source.
    _all_urls_str = incident_row["all_urls"] if incident_row else None
    # all_urls is stored as semicolon-separated text, not JSON
    if _all_urls_str:
        try:
            _all_urls = json.loads(_all_urls_str)
        except (json.JSONDecodeError, ValueError):
            _all_urls = [u.strip() for u in _all_urls_str.split(";") if u.strip()]
    else:
        _all_urls = []
    if _all_urls and _raw_primary_url and _raw_primary_url not in _all_urls:
        logger.warning(
            "LLM primary_url %s not in all_urls for %s; using %s instead",
            _raw_primary_url, incident_id, _all_urls[0],
        )
        primary_url = _all_urls[0]
    else:
        primary_url = _raw_primary_url
    article_metadata = _get_primary_article_metadata(conn, incident_id, primary_url)
    article_publish_date = article_metadata.get("publish_date")

    publication_date = _scalar(raw_json_data.get("publication_date")) if raw_json_data else None
    if not publication_date:
        publication_date = article_publish_date

    # Update incident with enrichment data
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
    
    # Try to get incident_date and discovery_date from raw JSON data (direct extraction)
    llm_discovery_date = None
    if raw_json_data:
        llm_incident_date = _scalar(raw_json_data.get("incident_date"))
        llm_date_precision = _scalar(raw_json_data.get("incident_date_precision"))
        llm_discovery_date = _scalar(raw_json_data.get("discovery_date"))
    
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
    
    # Also update country and country_code in incidents table if we have normalized values
    if country:
        update_fields += ",\n        country = ?"
        update_params.append(country)
    if country_code:
        update_fields += ",\n        country_code = ?"
        update_params.append(country_code)
    if resolved_institution_name:
        update_fields += ",\n        institution_name = ?"
        update_params.append(resolved_institution_name)
        if not victim_name_fallback:
            update_fields += ",\n        victim_raw_name = ?"
            update_params.append(resolved_institution_name)
    if publication_date and not source_published_date_fallback:
        update_fields += ",\n        source_published_date = ?"
        update_params.append(publication_date)
    
    # Update incident_date if LLM extracted it — but guard against the LLM picking
    # a repost/mirror date that is far AFTER the article's source_published_date.
    # An incident cannot occur significantly after the article reporting it was written.
    if llm_incident_date:
        _apply_llm_date = True
        if source_published_date_fallback:
            try:
                from datetime import date as _date
                _src_dt = _date.fromisoformat(str(source_published_date_fallback)[:10])
                _llm_dt = _date.fromisoformat(str(llm_incident_date)[:10])
                if (_llm_dt - _src_dt).days > 90:
                    logger.warning(
                        "Skipping LLM incident_date %s (>90 days after source_published_date %s) for %s",
                        llm_incident_date, source_published_date_fallback, incident_id,
                    )
                    _apply_llm_date = False
            except (ValueError, TypeError):
                pass
        if _apply_llm_date:
            update_fields += """,
        incident_date = ?,
        date_precision = ?
        """
            update_params.extend([llm_incident_date, llm_date_precision or "approximate"])
    if llm_discovery_date:
        update_fields += ",\n        discovery_date = ?"
        update_params.append(llm_discovery_date)
    
    update_params.append(incident_id)

    # BEGIN IMMEDIATE acquires the write lock up-front, preventing "database is locked"
    # errors when multiple enrichment workers try to write at the same time.
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError:
        pass  # Already in a transaction — caller manages the transaction boundary

    try:
        conn.execute(
            f"""
            UPDATE incidents
            SET {update_fields}
            WHERE incident_id = ?
            """,
            tuple(update_params)
        )
    except Exception:
        conn.execute("ROLLBACK")
        raise

    # --- Post-enrichment dedup ---
    # If another already-enriched incident has the same primary_url + incident_date,
    # they're almost certainly the same event. Delete the current (weaker) one.
    if primary_url and llm_incident_date:
        existing = conn.execute(
            """
            SELECT incident_id FROM incidents
            WHERE primary_url = ?
              AND incident_date = ?
              AND incident_id != ?
              AND llm_enriched = 1
            LIMIT 1
            """,
            (primary_url, llm_incident_date, incident_id),
        ).fetchone()
        if existing:
            survivor_id = existing[0]
            logger.warning(
                f"POST-ENRICH DEDUP: '{incident_id}' shares primary_url+date with "
                f"'{survivor_id}' — deleting duplicate with explicit related-row cleanup"
            )
            hard_delete_incident(conn, incident_id)
            conn.commit()
            return  # Nothing more to do for this incident

    # Save full JSON to incident_enrichments table
    try:
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
    except Exception:
        conn.execute("ROLLBACK")
        raise

    # Flatten and save to incident_enrichments_flat table
    # Pre-coerce raw_json_data: grammar-constrained LLM may return scalar fields as lists.
    # _flatten_enrichment_for_db also applies a post-pass, but doing it here means
    # the individual _scalar() calls above (country, region, etc.) share the same
    # normalised source without duplicating coercion logic.
    _FLAT_ARRAY_KEYS = {
        "timeline", "mitre_attack_techniques", "systems_affected_codes",
        "data_categories_affected", "data_types", "operational_impacts",
        "security_improvements", "third_parties_involved", "other_edu_incidents",
        "iocs", "target_demographics", "attack_chain",
        "vulnerabilities_exploited", "malware_families", "attacker_tools",
        "threat_actor_aliases", "applicable_regulations",
    }
    if raw_json_data:
        raw_json_data = {
            k: (v[0] if isinstance(v, list) and v and k not in _FLAT_ARRAY_KEYS else
                (None if isinstance(v, list) and k not in _FLAT_ARRAY_KEYS else v))
            for k, v in raw_json_data.items()
        }
    flat_data = _flatten_enrichment_for_db(enrichment_result, raw_json_data)
    flat_data['incident_id'] = incident_id
    if resolved_institution_name:
        flat_data['institution_name'] = resolved_institution_name
    # Override with incident table fallbacks only if not already set from raw_json_data
    if not flat_data.get('institution_type'):
        flat_data['institution_type'] = institution_type
    if not flat_data.get('country'):
        flat_data['country'] = country
    if not flat_data.get('country_code'):
        flat_data['country_code'] = country_code
    if not flat_data.get('region'):
        flat_data['region'] = region
    if not flat_data.get('city'):
        flat_data['city'] = city
    _apply_curated_note_fallbacks(flat_data, notes_fallback)
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
        'incident_id', 'is_education_related', 'institution_name', 'institution_type', 'institution_size',
        'incident_severity',
        'country', 'country_code', 'region', 'city', 'attack_category', 'attack_vector', 'initial_access_vector',
        'initial_access_description', 'ransomware_family',
        'threat_actor_name', 'threat_actor_category', 'threat_actor_motivation', 'threat_actor_origin_country', 'threat_actor_claim_url',
        'was_ransom_demanded', 'ransom_amount', 'ransom_currency', 'ransom_paid', 'ransom_paid_amount',
        'data_breached', 'data_exfiltrated', 'records_affected_exact', 'records_affected_min',
        'records_affected_max', 'pii_records_leaked', 'data_categories', 'systems_affected_codes', 'critical_systems_affected',
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
        'mitre_techniques_count',
        # Threat intelligence (new)
        'malware_families', 'attacker_tools', 'threat_actor_aliases', 'attack_campaign_name',
        'cloud_provider', 'infrastructure_type', 'dwell_time_days', 'mttd_hours', 'mttr_hours',
        # Vulnerabilities (new)
        'cve_ids', 'cvss_scores', 'vulnerability_names', 'affected_products',
        # Financial (additional)
        'total_cost_estimate',
        # Operational (additional)
        'partial_service_days', 'clinical_operations_disrupted', 'graduation_delayed', 'online_learning_disrupted',
        # Recovery (additional)
        'backup_status', 'backup_age_days', 'law_enforcement_involved', 'law_enforcement_agency', 'detection_source',
        # Transparency (additional)
        'official_statement_url',
        # Research impact (new)
        'research_projects_affected', 'research_data_compromised', 'publications_delayed',
        'grants_affected', 'research_area',
        # Regulatory (additional)
        'regulatory_context',
        # Data (additional)
        'data_volume_gb',
        'enriched_summary', 'extraction_notes', 'confidence',
        'created_at', 'updated_at'
    ]

    try:
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
    except Exception:
        conn.execute("ROLLBACK")
        raise
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

    This prevents re-processing in future runs. Also inserts a minimal
    record into incident_enrichments_flat so the incident is trackable
    in queries and CSV exports (with is_education_related=0).

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

    # Insert minimal enrichment record so this incident appears in queries
    # that JOIN on incident_enrichments_flat (prevents silent data loss)
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO incident_enrichments_flat (
                incident_id, is_education_related, enriched_at, skip_reason
            ) VALUES (?, 0, ?, ?)
            """,
            (incident_id, now, reason)
        )
    except Exception as e:
        # Table might not have skip_reason column yet; try without it
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO incident_enrichments_flat (
                    incident_id, is_education_related, enriched_at
                ) VALUES (?, 0, ?)
                """,
                (incident_id, now)
            )
        except Exception:
            logger.debug(f"Could not insert skip record for {incident_id}: {e}")

    conn.commit()
    logger.info(f"Marked incident {incident_id} as skipped: {reason}")


def delete_incident(
    conn: sqlite3.Connection,
    incident_id: str,
    reason: str = "not_education_related",
) -> bool:
    """
    Soft-delete an incident: keep the incidents row but clear articles and
    enrichment data, and set llm_excluded=1 so Phase 2 won't reprocess it.

    This replaces the previous hard-DELETE approach.  Hard-deleting caused
    permanent data loss when stale articles (from a prior pipeline run) caused
    the LLM to misclassify a real education incident as "not edu". Keeping the
    row lets administrators review/restore and lets Phase 1 re-ingest via the
    source_events cascade reset if needed.

    The incident row is NOT deleted — only its LLM-derived artifacts are cleared.
    The public API never shows llm_excluded=1 rows (they have no enrichment data).

    Args:
        conn: Database connection (must be writable)
        incident_id: Incident ID to soft-delete
        reason: Short label for llm_excluded_reason (default: "not_education_related")

    Returns:
        True if soft-deleted, False on error
    """
    try:
        # Clear derived data (articles + enrichments) but keep the incident row.
        for table in [
            "incident_enrichments_flat",
            "incident_enrichments",
            "articles",
        ]:
            try:
                conn.execute(
                    f"DELETE FROM {table} WHERE incident_id = ?", (incident_id,)
                )
            except Exception:
                pass  # Table might not exist

        # Mark as excluded so Phase 2 won't pick it up again, and record why.
        conn.execute(
            """
            UPDATE incidents
            SET llm_excluded = 1,
                llm_excluded_reason = ?,
                llm_enriched = 1,
                llm_enriched_at = datetime('now')
            WHERE incident_id = ?
            """,
            (reason, incident_id),
        )
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Failed to soft-delete incident {incident_id}: {e}")
        conn.rollback()
        return False


def hard_delete_incident(conn: sqlite3.Connection, incident_id: str) -> None:
    """
    Fully delete an incident and all directly linked incident-scoped rows.

    This is a defensive fallback for older databases where FK cascades were not
    reliably enforced, preventing orphaned enrichment rows from surviving.
    The caller owns the surrounding transaction/commit.
    """
    for table in [
        "incident_enrichments_flat",
        "incident_enrichments",
        "articles",
        "incident_sources",
        "source_events",
        "pipeline_checkpoint",
        "incident_iocs",
        "incident_threat_actors",
    ]:
        try:
            conn.execute(f"DELETE FROM {table} WHERE incident_id = ?", (incident_id,))
        except Exception:
            pass
    conn.execute("DELETE FROM incidents WHERE incident_id = ?", (incident_id,))


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
            orphan_cleanup = purge_orphaned_incident_rows(conn)
            conn.commit()
            if orphan_cleanup["total_deleted"] > 0:
                logger.info(
                    f"No enriched incidents to revert, but purged {orphan_cleanup['total_deleted']} orphaned child rows"
                )
            else:
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

        orphan_cleanup = purge_orphaned_incident_rows(conn)
        
        conn.commit()
        logger.info(
            f"Reverted enrichment for {len(incident_ids)} incidents"
            + (
                f" and purged {orphan_cleanup['total_deleted']} orphaned child rows"
                if orphan_cleanup["total_deleted"] > 0 else ""
            )
        )
        return len(incident_ids)
        
    except Exception as e:
        logger.error(f"Error reverting enriched incidents: {e}")
        conn.rollback()
        return 0


def revert_enrichment_before_date(
    conn: sqlite3.Connection,
    before_date: str,
) -> int:
    """
    Revert enrichment for all incidents enriched before a given date.

    Useful when the extraction schema has changed and old enrichments
    need to be re-processed with the updated LLM prompts.

    Args:
        conn: Database connection
        before_date: ISO date string (e.g. '2026-03-15'). All incidents
                     with llm_enriched_at < this date will be reverted.

    Returns:
        Number of incidents reverted
    """
    try:
        # Find affected incidents
        cur = conn.execute(
            "SELECT incident_id FROM incidents WHERE llm_enriched = 1 AND llm_enriched_at < ?",
            (before_date,),
        )
        incident_ids = [row["incident_id"] for row in cur.fetchall()]

        if not incident_ids:
            orphan_cleanup = purge_orphaned_incident_rows(conn)
            conn.commit()
            if orphan_cleanup["total_deleted"] > 0:
                logger.info(
                    f"No enriched incidents found before {before_date}; purged {orphan_cleanup['total_deleted']} orphaned child rows"
                )
            else:
                logger.info(f"No enriched incidents found before {before_date}")
            return 0

        placeholders = ",".join("?" * len(incident_ids))
        now = datetime.utcnow().isoformat()

        # Reset enrichment fields on incidents table
        conn.execute(
            f"""
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
            WHERE incident_id IN ({placeholders})
            """,
            [now] + incident_ids,
        )

        # Remove from enrichment tables
        conn.execute(
            f"DELETE FROM incident_enrichments WHERE incident_id IN ({placeholders})",
            incident_ids,
        )
        conn.execute(
            f"DELETE FROM incident_enrichments_flat WHERE incident_id IN ({placeholders})",
            incident_ids,
        )

        # Delete articles so they can be re-fetched with current fetcher
        conn.execute(
            f"DELETE FROM articles WHERE incident_id IN ({placeholders})",
            incident_ids,
        )

        orphan_cleanup = purge_orphaned_incident_rows(conn)

        conn.commit()
        logger.info(
            f"Reverted enrichment for {len(incident_ids)} incidents enriched before {before_date}"
            + (
                f" and purged {orphan_cleanup['total_deleted']} orphaned child rows"
                if orphan_cleanup["total_deleted"] > 0 else ""
            )
        )
        return len(incident_ids)

    except Exception as e:
        logger.error(f"Error reverting enrichment before {before_date}: {e}")
        conn.rollback()
        return 0


def purge_orphaned_incident_rows(conn: sqlite3.Connection) -> Dict[str, int]:
    """
    Delete child-table rows whose incident_id no longer exists in incidents.

    This cleans up stale analytics residue from older runs / schemas where
    cascades did not fire reliably.
    """
    deleted: Dict[str, int] = {}
    total_deleted = 0
    tables = [
        "incident_enrichments_flat",
        "incident_enrichments",
        "articles",
        "incident_sources",
        "source_events",
        "pipeline_checkpoint",
        "incident_iocs",
        "incident_threat_actors",
    ]

    for table in tables:
        try:
            cur = conn.execute(
                f"""
                DELETE FROM {table}
                WHERE incident_id NOT IN (SELECT incident_id FROM incidents)
                """
            )
            count = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            deleted[table] = count
            total_deleted += count
        except Exception:
            deleted[table] = 0

    deleted["total_deleted"] = total_deleted
    return deleted


def reset_phantom_enrichments(conn: sqlite3.Connection) -> int:
    """
    Reset incidents that are marked llm_enriched=1 but have no actual LLM data.

    These are "phantom enriched" incidents — typically caused by fetch failures
    that were incorrectly marked as enriched in older pipeline versions.

    Returns:
        Number of incidents reset
    """
    try:
        # Find phantom enriched: llm_enriched=1 but no real LLM summary
        cur = conn.execute(
            """
            SELECT incident_id FROM incidents
            WHERE llm_enriched = 1
              AND (llm_summary IS NULL OR length(llm_summary) < 10)
            """
        )
        incident_ids = [row["incident_id"] for row in cur.fetchall()]

        if not incident_ids:
            logger.info("No phantom enriched incidents found")
            return 0

        placeholders = ",".join("?" * len(incident_ids))
        now = datetime.utcnow().isoformat()

        # Reset enrichment fields
        conn.execute(
            f"""
            UPDATE incidents
            SET
                llm_enriched = 0,
                llm_enriched_at = NULL,
                llm_summary = NULL,
                llm_timeline = NULL,
                llm_mitre_attack = NULL,
                llm_attack_dynamics = NULL,
                notes = NULL,
                last_updated_at = ?
            WHERE incident_id IN ({placeholders})
            """,
            [now] + incident_ids,
        )

        # Clean up enrichment tables
        conn.execute(
            f"DELETE FROM incident_enrichments WHERE incident_id IN ({placeholders})",
            incident_ids,
        )
        conn.execute(
            f"DELETE FROM incident_enrichments_flat WHERE incident_id IN ({placeholders})",
            incident_ids,
        )

        # Delete failed articles so they can be re-fetched
        conn.execute(
            f"DELETE FROM articles WHERE incident_id IN ({placeholders})",
            incident_ids,
        )

        conn.commit()
        logger.info(f"Reset {len(incident_ids)} phantom enriched incidents")
        return len(incident_ids)

    except Exception as e:
        logger.error(f"Error resetting phantom enrichments: {e}")
        conn.rollback()
        return 0


def purge_non_education_incidents(conn: sqlite3.Connection) -> Dict[str, int]:
    """
    Delete all incidents that the LLM classified as not education-related.

    These are incidents where is_education_related=0 in enrichments_flat,
    meaning the LLM processed them and determined they are not about
    cyberattacks on educational institutions.

    Also deletes orphan incidents that are enriched but have no record
    in enrichments_flat at all (should not happen, but defensive).

    Returns:
        Dict with counts: non_education_purged, orphan_purged, total_purged
    """
    try:
        # 1. Find non-education incidents in enrichments_flat
        #    Include NULL values — these are records where the LLM didn't
        #    return education relevance data (treated as non-education)
        cur = conn.execute(
            """
            SELECT ef.incident_id FROM incident_enrichments_flat ef
            WHERE ef.is_education_related = 0
               OR ef.is_education_related IS NULL
            """
        )
        non_edu_ids = [row["incident_id"] for row in cur.fetchall()]

        # 2. Find orphan enriched incidents (in incidents table but not in enrichments_flat)
        cur = conn.execute(
            """
            SELECT i.incident_id FROM incidents i
            WHERE i.llm_enriched = 1
              AND NOT EXISTS (
                  SELECT 1 FROM incident_enrichments_flat ef
                  WHERE ef.incident_id = i.incident_id
              )
            """
        )
        orphan_ids = [row["incident_id"] for row in cur.fetchall()]

        all_ids = list(set(non_edu_ids + orphan_ids))

        if not all_ids:
            logger.info("No non-education incidents to purge")
            return {"non_education_purged": 0, "orphan_purged": 0, "total_purged": 0}

        placeholders = ",".join("?" * len(all_ids))

        # Delete from all related tables
        for table in [
            "incident_enrichments_flat",
            "incident_enrichments",
            "articles",
            "incident_sources",
        ]:
            try:
                conn.execute(
                    f"DELETE FROM {table} WHERE incident_id IN ({placeholders})",
                    all_ids,
                )
            except Exception:
                pass  # Table might not exist or have different schema

        # Delete from incidents table
        conn.execute(
            f"DELETE FROM incidents WHERE incident_id IN ({placeholders})",
            all_ids,
        )

        conn.commit()
        result = {
            "non_education_purged": len(non_edu_ids),
            "orphan_purged": len(orphan_ids),
            "total_purged": len(all_ids),
        }
        logger.info(
            f"Purged {result['total_purged']} non-education incidents "
            f"({result['non_education_purged']} non-edu, {result['orphan_purged']} orphans)"
        )
        return result

    except Exception as e:
        logger.error(f"Error purging non-education incidents: {e}")
        conn.rollback()
        return {"non_education_purged": 0, "orphan_purged": 0, "total_purged": 0}


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
    
    # Actionable incidents ready for Phase 2.
    # Keep this in sync with get_unenriched_incidents() so manager loops don't
    # spin on rows that Phase 2 would immediately reject as non-actionable.
    cur = conn.execute(
        """
        SELECT COUNT(*) as count
        FROM incidents i
        LEFT JOIN (
            SELECT DISTINCT incident_id FROM articles WHERE fetch_successful = 1
        ) a ON a.incident_id = i.incident_id
        WHERE i.llm_enriched = 0
          AND (i.llm_excluded IS NULL OR i.llm_excluded = 0)
          AND (
            (i.all_urls IS NOT NULL AND i.all_urls != '')
            OR a.incident_id IS NOT NULL
            OR (
              COALESCE(i.serp_attempt_count, 0) < ?
              AND (
                (i.institution_name IS NOT NULL AND i.institution_name != '')
                OR (i.victim_raw_name IS NOT NULL AND i.victim_raw_name != '')
              )
            )
          )
        """,
        (SERP_MAX_ATTEMPTS,),
    )
    stats["ready_for_enrichment"] = cur.fetchone()["count"]
    
    return stats
