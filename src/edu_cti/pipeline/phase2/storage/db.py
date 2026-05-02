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
from src.edu_cti.pipeline.phase2.extraction.extraction_schema import EXTRACTION_SCHEMA
from src.edu_cti.pipeline.phase2.extraction.json_to_schema_mapper import normalize_institution_type
from src.edu_cti.pipeline.phase2.utils.post_processing import apply_post_processing, infer_confirmed_status, is_headline_format
from src.edu_cti.core.config import DB_PATH, SERP_MAX_ATTEMPTS

logger = logging.getLogger(__name__)

_EXTRACTION_SCHEMA_FIELDS = tuple(EXTRACTION_SCHEMA["properties"].keys())
_STORAGE_DEBUG_KEY = "_storage_debug"
_ENRICHMENT_STORAGE_VERSION = "3.1"
_FINAL_JSON_TEXT_FIELDS = {
    "systems_affected_codes",
    "timeline_json",
    "mitre_techniques_json",
    "malware_families",
    "attacker_tools",
    "threat_actor_aliases",
    "cve_ids",
    "cvss_scores",
    "vulnerability_names",
    "affected_products",
    "data_categories",
    "regulatory_context",
    "secondary_attack_categories",
    "attack_chain",
}


def _first_present(*values: Any) -> Any:
    """Return the first non-empty value while preserving False/0."""
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and value == "":
            continue
        if isinstance(value, (list, dict, tuple, set)) and not value:
            continue
        return value
    return None


def _extract_storage_debug(raw_json_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Return internal debug metadata attached by the enrichment layer."""
    if not raw_json_data or not isinstance(raw_json_data, dict):
        return {}
    debug = raw_json_data.get(_STORAGE_DEBUG_KEY)
    return debug if isinstance(debug, dict) else {}


def _strip_storage_debug(raw_json_data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Remove internal debug metadata before schema/raw-field processing."""
    if raw_json_data is None:
        return None
    if not isinstance(raw_json_data, dict):
        return raw_json_data
    return {
        key: value
        for key, value in raw_json_data.items()
        if key != _STORAGE_DEBUG_KEY
    }


def _build_raw_extraction_snapshot(raw_json_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Build a schema-shaped raw extraction snapshot with explicit nulls."""
    raw_snapshot = {field: None for field in _EXTRACTION_SCHEMA_FIELDS}
    if raw_json_data:
        for field in _EXTRACTION_SCHEMA_FIELDS:
            if field in raw_json_data:
                raw_snapshot[field] = raw_json_data[field]
    return raw_snapshot


def _extract_typed_enrichment_dict(final_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Support both wrapped final payloads and bare typed-enrichment JSON."""
    typed = final_payload.get("typed_enrichment")
    return typed if isinstance(typed, dict) else final_payload


def _build_final_enrichment_record(
    enrichment_result: CTIEnrichmentResult,
    raw_snapshot: Dict[str, Any],
    flat_data: Dict[str, Any],
    storage_debug: Dict[str, Any],
) -> str:
    """Persist the canonical post-processed enrichment record for analytics/debugging."""
    metadata = storage_debug.get("llm_metadata")
    canonical = _build_canonical_enrichment_record(flat_data)
    final_record = {
        "storage_version": _ENRICHMENT_STORAGE_VERSION,
        **canonical,
        "canonical": canonical,
        "typed_enrichment": enrichment_result.model_dump(mode="json", exclude_none=False),
        "raw_extraction": raw_snapshot,
        "analytics_projection": flat_data,
        "llm_metadata": metadata if isinstance(metadata, dict) else None,
    }
    return json.dumps(final_record, indent=2)


def _build_canonical_enrichment_record(flat_data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert the SQLite-shaped flat projection into a readable canonical JSON record."""
    canonical = dict(flat_data)

    for key in _FINAL_JSON_TEXT_FIELDS:
        value = canonical.get(key)
        if not isinstance(value, str):
            continue
        try:
            canonical[key] = json.loads(value)
        except Exception:
            continue

    timeline = canonical.get("timeline_json")
    if isinstance(timeline, list):
        canonical["timeline"] = timeline

    mitre = canonical.get("mitre_techniques_json")
    if isinstance(mitre, list):
        canonical["mitre_attack_techniques"] = mitre

    return canonical


def _derive_extraction_confidence(
    enrichment_result: CTIEnrichmentResult,
    raw_json_data: Optional[Dict[str, Any]] = None,
) -> Optional[float]:
    """Derive a stable confidence score from core field completeness when the LLM does not provide one."""
    raw = raw_json_data or {}

    def _present(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return value.strip() != ""
        if isinstance(value, (list, dict, tuple, set)):
            return len(value) > 0
        return True

    signals = [
        _present(raw.get("institution_name"))
        or _present(enrichment_result.education_relevance.institution_identified if enrichment_result.education_relevance else None),
        _present(raw.get("institution_type")),
        _present(raw.get("country")),
        _present(raw.get("incident_date")),
        _present(raw.get("attack_category")),
        _present(raw.get("attack_vector"))
        or _present(enrichment_result.attack_dynamics.attack_vector if enrichment_result.attack_dynamics else None),
        _present(raw.get("ransomware_family"))
        or _present(raw.get("threat_actor_name"))
        or _present(enrichment_result.attack_dynamics.ransomware_family if enrichment_result.attack_dynamics else None),
        _present(raw.get("records_affected_exact"))
        or _present(raw.get("pii_records_leaked"))
        or _present(raw.get("users_affected_exact")),
        bool(enrichment_result.timeline),
        bool(enrichment_result.mitre_attack_techniques),
        _present(enrichment_result.enriched_summary),
    ]

    if not signals:
        return None

    return round(sum(1 for present in signals if present) / len(signals), 2)


def _metadata_value(storage_debug: Dict[str, Any], key: str) -> Optional[str]:
    meta = storage_debug.get("llm_metadata")
    if not isinstance(meta, dict):
        return None
    value = meta.get(key)
    return str(value) if value is not None else None

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


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set:
    return {
        row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }


def init_incident_enrichments_table(conn: sqlite3.Connection) -> None:
    """
    Initialize the incident_enrichments table with optimized structure.
    
    This table stores:
    1. Canonical debug/reprocessing layers in incident_enrichments
    2. Flattened key fields as columns for fast queries and CSV export
    
    Args:
        conn: Database connection
    """
    # Create main enrichment table with canonical artifact layers
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS incident_enrichments (
            incident_id TEXT PRIMARY KEY,
            raw_response_payload TEXT,
            raw_extraction_json TEXT,
            final_enrichment_json TEXT,
            storage_metadata TEXT,
            enrichment_version TEXT DEFAULT '3.1',
            enrichment_confidence REAL,
            llm_provider TEXT,
            llm_model TEXT,
            extraction_mode TEXT,
            prompt_version TEXT,
            schema_version TEXT,
            mapper_version TEXT,
            post_processing_version TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE
        )
        """
    )
    for col, col_type in [
        ("raw_response_payload", "TEXT"),
        ("raw_extraction_json", "TEXT"),
        ("final_enrichment_json", "TEXT"),
        ("storage_metadata", "TEXT"),
        ("enrichment_version", "TEXT DEFAULT '3.1'"),
        ("enrichment_confidence", "REAL"),
        ("llm_provider", "TEXT"),
        ("llm_model", "TEXT"),
        ("extraction_mode", "TEXT"),
        ("prompt_version", "TEXT"),
        ("schema_version", "TEXT"),
        ("mapper_version", "TEXT"),
        ("post_processing_version", "TEXT"),
        ("created_at", "TEXT"),
        ("updated_at", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE incident_enrichments ADD COLUMN {col} {col_type}")
        except sqlite3.OperationalError:
            pass

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS incident_enrichment_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_id TEXT NOT NULL,
            raw_response_payload TEXT,
            raw_extraction_json TEXT,
            final_enrichment_json TEXT,
            storage_metadata TEXT,
            enrichment_version TEXT DEFAULT '3.1',
            enrichment_confidence REAL,
            llm_provider TEXT,
            llm_model TEXT,
            extraction_mode TEXT,
            prompt_version TEXT,
            schema_version TEXT,
            mapper_version TEXT,
            post_processing_version TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE
        )
        """
    )
    for col, col_type in [
        ("raw_response_payload", "TEXT"),
        ("raw_extraction_json", "TEXT"),
        ("final_enrichment_json", "TEXT"),
        ("storage_metadata", "TEXT"),
        ("enrichment_version", "TEXT DEFAULT '3.1'"),
        ("enrichment_confidence", "REAL"),
        ("llm_provider", "TEXT"),
        ("llm_model", "TEXT"),
        ("extraction_mode", "TEXT"),
        ("prompt_version", "TEXT"),
        ("schema_version", "TEXT"),
        ("mapper_version", "TEXT"),
        ("post_processing_version", "TEXT"),
        ("created_at", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE incident_enrichment_runs ADD COLUMN {col} {col_type}")
        except sqlite3.OperationalError:
            pass
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_enrichment_runs_incident
        ON incident_enrichment_runs(incident_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_enrichment_runs_created
        ON incident_enrichment_runs(created_at)
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
            access_vector TEXT,
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
            alumni_affected INTEGER,
            parents_affected INTEGER,
            applicants_affected INTEGER,
            patients_affected INTEGER,
            users_affected_exact INTEGER,
            users_affected_min INTEGER,
            users_affected_max INTEGER,

            -- Financial Impact
            recovery_costs_min REAL,
            recovery_costs_max REAL,
            ransom_amount_min REAL,
            ransom_amount_max REAL,
            legal_costs REAL,
            insurance_claim INTEGER,
            insurance_claim_amount REAL,
            business_impact TEXT,

            -- Regulatory Impact
            gdpr_breach INTEGER,
            hipaa_breach INTEGER,
            ferpa_breach INTEGER,
            breach_notification_required INTEGER,
            notifications_sent INTEGER,
            notifications_sent_date TEXT,
            dpa_notified INTEGER,
            investigation_opened INTEGER,
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
            
            -- Vulnerability convenience columns (full data in incident_vulnerabilities table)
            primary_cve_id TEXT,             -- highest-CVSS CVE from the list
            max_cvss_score REAL,             -- highest CVSS score across all exploited CVEs

            -- MITRE convenience column (full data in incident_mitre_techniques table)
            primary_mitre_technique_id TEXT, -- first technique_id from the list

            -- Timeline convenience column (full data in incident_timeline table)
            timeline_events_count INTEGER,   -- count of extracted timeline events

            -- Threat Intelligence
            malware_families TEXT,           -- JSON array
            attacker_tools TEXT,             -- JSON array
            threat_actor_aliases TEXT,       -- JSON array
            attack_campaign_name TEXT,
            cloud_provider TEXT,
            dwell_time_days REAL,
            mttd_hours REAL,
            mttr_hours REAL,

            -- Financial (additional)
            total_cost_estimate REAL,

            -- Operational (additional)
            clinical_operations_disrupted INTEGER,
            graduation_delayed INTEGER,
            online_learning_disrupted INTEGER,

            -- Recovery (additional)
            backup_status TEXT,
            backup_age_days REAL,
            law_enforcement_involved INTEGER,
            law_enforcement_agency TEXT,

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

            -- Attack classification (additional)
            secondary_attack_categories TEXT,  -- JSON array of secondary attack types
            attack_chain TEXT,                 -- JSON array of MITRE ATT&CK kill chain phases
            incident_date_precision TEXT,      -- exact / approximate / month_only / year_only / unknown
            encryption_extent TEXT,            -- full_encryption / partial_encryption / no_encryption / unknown
            disclosure_source TEXT,            -- institution_statement / media_report / attacker_leak_site / etc.

            -- Education-specific context (new)
            academic_period_affected TEXT,      -- start_of_semester / finals_week / enrollment_period / etc.
            dark_web_posting_confirmed INTEGER, -- confirmed posting on leak site/dark web
            prior_breach_same_institution INTEGER, -- institution was previously breached
            notification_delay_days INTEGER,    -- days from discovery to victim notification

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
    
    # Existing row-cleared Railway databases may still have older flat schemas.
    for col, col_type in [
        ("country_code", "TEXT"),
        ("enriched_at", "TEXT"),
        ("skip_reason", "TEXT"),
        ("data_categories", "TEXT"),
        ("incident_severity", "TEXT"),
        ("institution_size", "TEXT"),
        ("access_vector", "TEXT"),
        ("threat_actor_category", "TEXT"),
        ("threat_actor_motivation", "TEXT"),
        ("threat_actor_origin_country", "TEXT"),
        ("malware_families", "TEXT"),
        ("attacker_tools", "TEXT"),
        ("threat_actor_aliases", "TEXT"),
        ("attack_campaign_name", "TEXT"),
        ("cloud_provider", "TEXT"),
        ("dwell_time_days", "REAL"),
        ("mttd_hours", "REAL"),
        ("mttr_hours", "REAL"),
        ("primary_cve_id", "TEXT"),
        ("max_cvss_score", "REAL"),
        ("primary_mitre_technique_id", "TEXT"),
        ("timeline_events_count", "INTEGER"),
        ("total_cost_estimate", "REAL"),
        ("clinical_operations_disrupted", "INTEGER"),
        ("graduation_delayed", "INTEGER"),
        ("online_learning_disrupted", "INTEGER"),
        ("backup_status", "TEXT"),
        ("backup_age_days", "REAL"),
        ("law_enforcement_involved", "INTEGER"),
        ("law_enforcement_agency", "TEXT"),
        ("official_statement_url", "TEXT"),
        ("research_projects_affected", "INTEGER"),
        ("research_data_compromised", "INTEGER"),
        ("publications_delayed", "INTEGER"),
        ("grants_affected", "INTEGER"),
        ("research_area", "TEXT"),
        ("regulatory_context", "TEXT"),
        ("data_volume_gb", "REAL"),
        ("secondary_attack_categories", "TEXT"),
        ("attack_chain", "TEXT"),
        ("incident_date_precision", "TEXT"),
        ("encryption_extent", "TEXT"),
        ("disclosure_source", "TEXT"),
        ("alumni_affected", "INTEGER"),
        ("parents_affected", "INTEGER"),
        ("applicants_affected", "INTEGER"),
        ("patients_affected", "INTEGER"),
        ("ransom_amount_min", "REAL"),
        ("ransom_amount_max", "REAL"),
        ("notifications_sent_date", "TEXT"),
        ("dpa_notified", "INTEGER"),
        ("investigation_opened", "INTEGER"),
        ("academic_period_affected", "TEXT"),
        ("dark_web_posting_confirmed", "INTEGER"),
        ("prior_breach_same_institution", "INTEGER"),
        ("notification_delay_days", "INTEGER"),
    ]:
        try:
            conn.execute(f"ALTER TABLE incident_enrichments_flat ADD COLUMN {col} {col_type}")
        except sqlite3.OperationalError:
            pass

    # Create indexes after repairing legacy columns so stale schemas can heal
    # in place before SQLite validates indexed column names.
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

    # ── Junction tables for structured multi-value CTI fields ────────────────
    # Each row is one item; parent incident linked via incident_id FK.
    # DELETE + re-insert on re-enrichment keeps data consistent.

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS incident_vulnerabilities (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_id         TEXT NOT NULL,
            cve_id              TEXT,
            vulnerability_name  TEXT,
            affected_product    TEXT,
            cvss_score          REAL,
            exploit_in_wild     INTEGER,
            patch_available     INTEGER,
            FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_vulns_incident ON incident_vulnerabilities(incident_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_vulns_cve ON incident_vulnerabilities(cve_id)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS incident_mitre_techniques (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_id     TEXT NOT NULL,
            seq_order       INTEGER NOT NULL DEFAULT 0,
            technique_id    TEXT,
            technique_name  TEXT,
            tactic          TEXT,
            description     TEXT,
            sub_techniques  TEXT,   -- JSON array of sub-technique IDs
            FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_mitre_incident ON incident_mitre_techniques(incident_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_mitre_technique ON incident_mitre_techniques(technique_id)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS incident_timeline (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_id         TEXT NOT NULL,
            seq_order           INTEGER NOT NULL DEFAULT 0,
            event_date          TEXT,
            date_precision      TEXT,
            event_type          TEXT,
            event_description   TEXT,
            actor_attribution   TEXT,
            FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_timeline_incident ON incident_timeline(incident_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_timeline_type ON incident_timeline(event_type)"
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


# ── Junction-table helper utilities ──────────────────────────────────────────

def _normalize_vuln_list(raw: Any) -> list:
    """Coerce vulnerabilities_exploited to a plain list of dicts."""
    if not raw:
        return []
    items = [v.model_dump() if hasattr(v, "model_dump") else v for v in raw]
    return [v for v in items if isinstance(v, dict)]


def _vuln_convenience(raw: Any) -> dict:
    """Return primary_cve_id and max_cvss_score for the flat table."""
    vulns = _normalize_vuln_list(raw)
    if not vulns:
        return {"primary_cve_id": None, "max_cvss_score": None}
    scored = sorted(
        [v for v in vulns if v.get("cvss_score") is not None],
        key=lambda v: v["cvss_score"],
        reverse=True,
    )
    top = scored[0] if scored else vulns[0]
    return {
        "primary_cve_id": top.get("cve_id"),
        "max_cvss_score": top.get("cvss_score"),
    }


def _normalize_mitre_list(raw: Any) -> list:
    """Coerce mitre_attack_techniques to a plain list of dicts."""
    if not raw:
        return []
    items = [t.model_dump() if hasattr(t, "model_dump") else t for t in raw]
    return [t for t in items if isinstance(t, dict)]


def _primary_mitre_id(raw: Any) -> Optional[str]:
    techniques = _normalize_mitre_list(raw)
    if not techniques:
        return None
    return techniques[0].get("technique_id")


def _normalize_timeline_list(raw: Any) -> list:
    """Coerce timeline to a plain list of dicts."""
    if not raw:
        return []
    items = [e.model_dump() if hasattr(e, "model_dump") else e for e in raw]
    return [e for e in items if isinstance(e, dict)]


# ── Junction-table save helpers ───────────────────────────────────────────────

def _save_incident_vulnerabilities(
    conn: sqlite3.Connection, incident_id: str, raw: Any
) -> None:
    """Upsert vulnerability rows into incident_vulnerabilities."""
    conn.execute(
        "DELETE FROM incident_vulnerabilities WHERE incident_id = ?", [incident_id]
    )
    for v in _normalize_vuln_list(raw):
        conn.execute(
            """
            INSERT INTO incident_vulnerabilities
                (incident_id, cve_id, vulnerability_name, affected_product,
                 cvss_score, exploit_in_wild, patch_available)
            VALUES (?,?,?,?,?,?,?)
            """,
            [
                incident_id,
                v.get("cve_id"),
                v.get("vulnerability_name"),
                v.get("affected_product"),
                v.get("cvss_score"),
                int(bool(v.get("exploit_in_wild"))) if v.get("exploit_in_wild") is not None else None,
                int(bool(v.get("patch_available"))) if v.get("patch_available") is not None else None,
            ],
        )


def _save_incident_mitre_techniques(
    conn: sqlite3.Connection, incident_id: str, raw: Any
) -> None:
    """Upsert MITRE ATT&CK technique rows into incident_mitre_techniques."""
    conn.execute(
        "DELETE FROM incident_mitre_techniques WHERE incident_id = ?", [incident_id]
    )
    for i, t in enumerate(_normalize_mitre_list(raw)):
        sub = json.dumps(t["sub_techniques"]) if t.get("sub_techniques") else None
        conn.execute(
            """
            INSERT INTO incident_mitre_techniques
                (incident_id, seq_order, technique_id, technique_name,
                 tactic, description, sub_techniques)
            VALUES (?,?,?,?,?,?,?)
            """,
            [
                incident_id, i,
                t.get("technique_id"),
                t.get("technique_name"),
                t.get("tactic"),
                t.get("description"),
                sub,
            ],
        )


def _save_incident_timeline(
    conn: sqlite3.Connection, incident_id: str, raw: Any
) -> None:
    """Upsert timeline event rows into incident_timeline."""
    conn.execute(
        "DELETE FROM incident_timeline WHERE incident_id = ?", [incident_id]
    )
    for i, e in enumerate(_normalize_timeline_list(raw)):
        conn.execute(
            """
            INSERT INTO incident_timeline
                (incident_id, seq_order, event_date, date_precision,
                 event_type, event_description, actor_attribution)
            VALUES (?,?,?,?,?,?,?)
            """,
            [
                incident_id, i,
                e.get("date"),
                e.get("date_precision"),
                e.get("event_type"),
                e.get("event_description"),
                e.get("actor_attribution"),
            ],
        )


# ─────────────────────────────────────────────────────────────────────────────

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
    elif raw_get("data_exfiltrated") or raw_get("data_exfiltration"):
        _derived_data_breached = True
    elif raw_get("data_categories"):
        _derived_data_breached = True
    elif raw_get("records_affected_exact") or raw_get("records_affected_min") or raw_get("records_affected"):
        _derived_data_breached = True
    elif raw_get("data_volume_gb") and float(raw_get("data_volume_gb") or 0) > 0:
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
        'attack_vector': _first_present(
            raw_get("attack_vector"),
            raw_get("access_vector"),
            enrichment.attack_dynamics.attack_vector if enrichment.attack_dynamics else None,
        ),
        'access_vector': _first_present(
            raw_get("access_vector"),
            raw_get("attack_vector"),
            enrichment.attack_dynamics.attack_vector if enrichment.attack_dynamics else None,
        ),
        'initial_access_description': enrichment.initial_access_description or raw_get("initial_access_description"),
        'ransomware_family': _first_present(
            raw_get("ransomware_family"),
            raw_get("ransomware_family_or_group"),
            enrichment.attack_dynamics.ransomware_family if enrichment.attack_dynamics else None,
        ),

        # Threat actor
        'threat_actor_name': raw_get("threat_actor_name"),
        'threat_actor_category': raw_get("threat_actor_category"),
        'threat_actor_motivation': raw_get("threat_actor_motivation"),
        'threat_actor_origin_country': raw_get("threat_actor_origin_country"),
        'threat_actor_claim_url': raw_get("threat_actor_claim_url"),
        
        # Ransom - use raw JSON data for exact values
        'was_ransom_demanded': raw_get("was_ransom_demanded") if raw_get("was_ransom_demanded") is not None else (enrichment.attack_dynamics.ransom_demanded if enrichment.attack_dynamics else None),
        'ransom_amount': _first_present(
            raw_get("ransom_amount"),
            raw_get("ransom_amount_exact"),
            enrichment.attack_dynamics.ransom_amount if enrichment.attack_dynamics else None,
        ),
        'ransom_currency': raw_get("ransom_currency"),
        'ransom_paid': raw_get("ransom_paid") if raw_get("ransom_paid") is not None else (enrichment.attack_dynamics.ransom_paid if enrichment.attack_dynamics else None),
        'ransom_paid_amount': raw_get("ransom_paid_amount"),
        
        # Data impact
        'data_breached': _derived_data_breached,
        'data_exfiltrated': raw_get("data_exfiltrated") if raw_get("data_exfiltrated") is not None else (enrichment.attack_dynamics.data_exfiltration if enrichment.attack_dynamics else (enrichment.data_impact.get("data_exfiltrated") if enrichment.data_impact else None)),
        'records_affected_exact': _first_present(
            raw_get("records_affected_exact"),
            raw_get("pii_records_leaked"),
            enrichment.data_impact.get("records_affected_exact") if enrichment.data_impact else None,
        ),
        'records_affected_min': _first_present(
            raw_get("records_affected_min"),
            enrichment.data_impact.get("records_affected_min") if enrichment.data_impact else None,
        ),
        'records_affected_max': _first_present(
            raw_get("records_affected_max"),
            enrichment.data_impact.get("records_affected_max") if enrichment.data_impact else None,
        ),
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
        'cloud_services_affected': (
            raw_get("cloud_services_affected") if raw_get("cloud_services_affected") is not None
            else (enrichment.system_impact.get("cloud_services_affected") if enrichment.system_impact else None)
            or (True if (raw_get("cloud_provider") and str(raw_get("cloud_provider", "")).lower() not in ("none", "unknown", "")) else None)
        ),
        'third_party_vendor_impact': raw_get("third_party_vendor_impact") if raw_get("third_party_vendor_impact") is not None else (enrichment.system_impact.get("third_party_vendor_impact") if enrichment.system_impact else None),
        'vendor_name': _first_present(
            raw_get("vendor_name"),
            enrichment.system_impact.get("vendor_name") if enrichment.system_impact else None,
        ),
        
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
        'downtime_days': _first_present(
            raw_get("downtime_days"),
            enrichment.operational_impact_metrics.get("downtime_days") if enrichment.operational_impact_metrics else None,
        ),
        'outage_duration_hours': raw_get("outage_duration_hours"),
        
        # User impact - use raw JSON data for exact counts
        'students_affected': raw_get("students_affected"),
        'staff_affected': raw_get("staff_affected"),
        'faculty_affected': raw_get("faculty_affected") if raw_get("faculty_affected") is not None else (enrichment.user_impact.get("faculty_affected") if enrichment.user_impact else None),
        'alumni_affected': _first_present(
            raw_get("alumni_affected"),
            enrichment.user_impact.get("alumni_affected") if enrichment.user_impact else None,
        ),
        'parents_affected': _first_present(
            raw_get("parents_affected"),
            enrichment.user_impact.get("parents_affected") if enrichment.user_impact else None,
        ),
        'applicants_affected': _first_present(
            raw_get("applicants_affected"),
            enrichment.user_impact.get("applicants_affected") if enrichment.user_impact else None,
        ),
        'patients_affected': _first_present(
            raw_get("patients_affected"),
            enrichment.user_impact.get("patients_affected") if enrichment.user_impact else None,
        ),
        'users_affected_exact': _first_present(
            raw_get("users_affected_exact"),
            raw_get("total_individuals_affected"),
            enrichment.user_impact.get("users_affected_exact") if enrichment.user_impact else None,
        ),
        'users_affected_min': _first_present(
            raw_get("users_affected_min"),
            enrichment.user_impact.get("users_affected_min") if enrichment.user_impact else None,
        ),
        'users_affected_max': _first_present(
            raw_get("users_affected_max"),
            enrichment.user_impact.get("users_affected_max") if enrichment.user_impact else None,
        ),

        # Financial impact - use raw JSON data
        # Schema uses *_usd suffixed names; legacy aliases kept for raw_get
        'ransom_amount_min': _first_present(
            raw_get("ransom_amount_min"),
            enrichment.financial_impact.get("ransom_amount_min") if enrichment.financial_impact else None,
        ),
        'ransom_amount_max': _first_present(
            raw_get("ransom_amount_max"),
            enrichment.financial_impact.get("ransom_amount_max") if enrichment.financial_impact else None,
        ),
        'recovery_costs_min': _first_present(
            raw_get("recovery_costs_min"),
            raw_get("recovery_cost_usd"),
            enrichment.financial_impact.get("recovery_costs_min") if enrichment.financial_impact else None,
        ),
        'recovery_costs_max': _first_present(
            raw_get("recovery_costs_max"),
            enrichment.financial_impact.get("recovery_costs_max") if enrichment.financial_impact else None,
        ),
        'legal_costs': _first_present(
            raw_get("legal_costs"),
            raw_get("legal_cost_usd"),
            enrichment.financial_impact.get("legal_costs") if enrichment.financial_impact else None,
        ),
        'insurance_claim': raw_get("insurance_claim") if raw_get("insurance_claim") is not None else (enrichment.financial_impact.get("insurance_claim") if enrichment.financial_impact else None),
        'insurance_claim_amount': _first_present(
            raw_get("insurance_claim_amount"),
            raw_get("insurance_payout_usd"),
            enrichment.financial_impact.get("insurance_claim_amount") if enrichment.financial_impact else None,
        ),
        'business_impact': _first_present(
            raw_get("business_impact"),
            enrichment.attack_dynamics.business_impact if enrichment.attack_dynamics else None,
        ),
        
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
        'notifications_sent': (
            raw_get("notification_sent")
            if raw_get("notification_sent") is not None
            else (
                raw_get("notifications_sent")
                if raw_get("notifications_sent") is not None
                else (enrichment.regulatory_impact.get("notifications_sent") if enrichment.regulatory_impact else None)
            )
        ),
        'notifications_sent_date': _first_present(
            raw_get("notification_sent_date"),
            raw_get("notifications_sent_date"),
            enrichment.regulatory_impact.get("notifications_sent_date") if enrichment.regulatory_impact else None,
        ),
        'dpa_notified': (raw_get("dpa_notified") if raw_get("dpa_notified") is not None
                         else (enrichment.regulatory_impact.get("dpa_notified") if enrichment.regulatory_impact else None)),
        'investigation_opened': (raw_get("investigation_opened") if raw_get("investigation_opened") is not None
                                 else (enrichment.regulatory_impact.get("investigation_opened") if enrichment.regulatory_impact else None)),
        'fine_imposed': raw_get("fine_imposed") if raw_get("fine_imposed") is not None else (enrichment.regulatory_impact.get("fine_imposed") if enrichment.regulatory_impact else None),
        # Schema uses "fine_amount_usd"
        'fine_amount': _first_present(
            raw_get("fine_amount_usd"),
            raw_get("fine_amount"),
            enrichment.regulatory_impact.get("fine_amount") if enrichment.regulatory_impact else None,
        ),
        'lawsuits_filed': raw_get("lawsuits_filed") if raw_get("lawsuits_filed") is not None else (enrichment.regulatory_impact.get("lawsuits_filed") if enrichment.regulatory_impact else None),
        # Schema uses "class_action_filed"
        'class_action': (raw_get("class_action_filed") if raw_get("class_action_filed") is not None
                         else raw_get("class_action") if raw_get("class_action") is not None
                         else (enrichment.regulatory_impact.get("class_action") if enrichment.regulatory_impact else None)),
        
        # Recovery - use raw JSON data for dates and metrics
        # Schema uses "recovery_duration_days"; legacy alias "recovery_timeframe_days"
        'recovery_timeframe_days': _first_present(
            raw_get("recovery_duration_days"),
            raw_get("recovery_timeframe_days"),
            enrichment.attack_dynamics.recovery_timeframe_days if enrichment.attack_dynamics else None,
            enrichment.recovery_metrics.get("recovery_timeframe_days") if enrichment.recovery_metrics else None,
        ),
        'recovery_started_date': _first_present(
            raw_get("recovery_started_date"),
            enrichment.recovery_metrics.get("recovery_started_date") if enrichment.recovery_metrics else None,
        ),
        'recovery_completed_date': _first_present(
            raw_get("recovery_completed_date"),
            raw_get("service_restoration_date"),
            enrichment.recovery_metrics.get("recovery_completed_date") if enrichment.recovery_metrics else None,
        ),
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
        'incident_response_firm': _first_present(
            raw_get("ir_firm_engaged"),
            raw_get("incident_response_firm"),
            enrichment.recovery_metrics.get("incident_response_firm") if enrichment.recovery_metrics else None,
        ),
        'forensics_firm': _first_present(
            raw_get("forensics_firm_engaged"),
            raw_get("forensics_firm"),
            enrichment.recovery_metrics.get("forensics_firm") if enrichment.recovery_metrics else None,
        ),
        
        # Transparency - use raw JSON data
        'public_disclosure': raw_get("public_disclosure") or raw_get("was_disclosed_publicly") if (raw_get("public_disclosure") is not None or raw_get("was_disclosed_publicly") is not None) else (enrichment.transparency_metrics.get("public_disclosure") if enrichment.transparency_metrics else None),
        'public_disclosure_date': raw_get("public_disclosure_date") or (enrichment.transparency_metrics.get("public_disclosure_date") if enrichment.transparency_metrics else None),
        'disclosure_delay_days': raw_get("disclosure_delay_days") or (enrichment.transparency_metrics.get("disclosure_delay_days") if enrichment.transparency_metrics else None),
        'transparency_level': raw_get("transparency_level") or (enrichment.transparency_metrics.get("transparency_level") if enrichment.transparency_metrics else None),
        
        # Timeline convenience: full data stored in incident_timeline junction table
        'timeline_events_count': (
            len(enrichment.timeline) if enrichment.timeline
            else len(raw_get("timeline") or [])
        ),

        # Vulnerability convenience columns: full data in incident_vulnerabilities junction table
        **_vuln_convenience(
            enrichment.vulnerabilities_exploited
            if isinstance(enrichment.vulnerabilities_exploited, list)
            else raw_get("vulnerabilities_exploited")
        ),

        # MITRE convenience: full data stored in incident_mitre_techniques junction table
        'primary_mitre_technique_id': _primary_mitre_id(
            enrichment.mitre_attack_techniques
            if isinstance(enrichment.mitre_attack_techniques, list)
            else raw_get("mitre_attack_techniques")
        ),

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
        'dwell_time_days': (enrichment.dwell_time_days
                            if isinstance(enrichment.dwell_time_days, (int, float))
                            else None) or raw_get("dwell_time_days"),
        'mttd_hours': raw_get("mttd_hours") or (enrichment.recovery_metrics.get("mttd_hours") if isinstance(enrichment.recovery_metrics, dict) else None),
        'mttr_hours': raw_get("mttr_hours") or (enrichment.recovery_metrics.get("mttr_hours") if isinstance(enrichment.recovery_metrics, dict) else None),

        # Financial (additional)
        'total_cost_estimate': (raw_get("currency_normalized_cost_usd") or raw_get("estimated_total_cost_usd")
                                or (enrichment.financial_impact.get("total_cost_estimate") if enrichment.financial_impact else None)),

        # Operational (additional)
        'clinical_operations_disrupted': raw_get("clinical_operations_disrupted") if raw_get("clinical_operations_disrupted") is not None else (enrichment.operational_impact_metrics.get("clinical_operations_disrupted") if enrichment.operational_impact_metrics else None),
        'graduation_delayed': raw_get("graduation_delayed") if raw_get("graduation_delayed") is not None else (enrichment.operational_impact_metrics.get("graduation_delayed") if enrichment.operational_impact_metrics else None),
        'online_learning_disrupted': raw_get("online_learning_disrupted") if raw_get("online_learning_disrupted") is not None else (enrichment.operational_impact_metrics.get("online_learning_disrupted") if enrichment.operational_impact_metrics else None),

        # Recovery (additional)
        'backup_status': _first_present(
            raw_get("backup_status"),
            enrichment.recovery_metrics.get("backup_status") if enrichment.recovery_metrics else None,
        ),
        'backup_age_days': _first_present(
            raw_get("backup_age_days"),
            enrichment.recovery_metrics.get("backup_age_days") if enrichment.recovery_metrics else None,
        ),
        'law_enforcement_involved': raw_get("law_enforcement_involved") if raw_get("law_enforcement_involved") is not None else (enrichment.recovery_metrics.get("law_enforcement_involved") if enrichment.recovery_metrics else None),
        'law_enforcement_agency': _first_present(
            ", ".join(v for v in raw_get("law_enforcement_agencies", []) if isinstance(v, str) and v.strip())
            if isinstance(raw_get("law_enforcement_agencies"), list) else None,
            raw_get("law_enforcement_agency"),
            enrichment.recovery_metrics.get("law_enforcement_agency") if enrichment.recovery_metrics else None,
        ),

        # Transparency (additional)
        'official_statement_url': (raw_get("official_statement_url")
                                   or (enrichment.transparency_metrics.get("official_statement_url") if enrichment.transparency_metrics else None)),

        # Research impact (new fields)
        'research_projects_affected': (
            raw_get("research_projects_affected")
            if raw_get("research_projects_affected") is not None
            else (enrichment.research_impact.get("research_projects_affected") if enrichment.research_impact else None)
        ),
        'research_data_compromised': (
            raw_get("research_data_compromised")
            if raw_get("research_data_compromised") is not None
            else (enrichment.research_impact.get("research_data_compromised") if enrichment.research_impact else None)
        ),
        'publications_delayed': (
            raw_get("publications_delayed")
            if raw_get("publications_delayed") is not None
            else (enrichment.research_impact.get("publications_delayed") if enrichment.research_impact else None)
        ),
        'grants_affected': (
            raw_get("grants_affected")
            if raw_get("grants_affected") is not None
            else (enrichment.research_impact.get("grants_affected") if enrichment.research_impact else None)
        ),
        'research_area': _first_present(
            raw_get("research_area"),
            enrichment.research_impact.get("research_area") if enrichment.research_impact else None,
        ),

        # Regulatory (additional)
        'regulatory_context': json.dumps(raw_get("applicable_regulations")) if raw_get("applicable_regulations") else (json.dumps(enrichment.regulatory_impact.get("regulatory_context")) if enrichment.regulatory_impact and enrichment.regulatory_impact.get("regulatory_context") else None),

        # Data (additional)
        'data_volume_gb': enrichment.data_volume_gb or raw_get("data_volume_gb"),

        # Attack classification (additional)
        'secondary_attack_categories': (
            json.dumps(raw_get("secondary_attack_categories"))
            if isinstance(raw_get("secondary_attack_categories"), list) and raw_get("secondary_attack_categories")
            else None
        ),
        'attack_chain': (
            json.dumps(enrichment.attack_chain)
            if isinstance(getattr(enrichment, 'attack_chain', None), list) and enrichment.attack_chain
            else (json.dumps(raw_get("attack_chain")) if isinstance(raw_get("attack_chain"), list) and raw_get("attack_chain") else None)
        ),
        'incident_date_precision': raw_get("incident_date_precision"),
        'encryption_extent': raw_get("encryption_extent") or (
            enrichment.system_impact.get("encryption_extent") if enrichment.system_impact else None
        ),
        'disclosure_source': raw_get("disclosure_source") or (
            enrichment.transparency_metrics.get("disclosure_source") if enrichment.transparency_metrics else None
        ),

        # Education-specific context
        'academic_period_affected': raw_get("academic_period_affected"),
        'dark_web_posting_confirmed': raw_get("dark_web_posting_confirmed"),
        'prior_breach_same_institution': raw_get("prior_breach_same_institution"),
        'notification_delay_days': raw_get("notification_delay_days"),

        # Summary
        'enriched_summary': enrichment.enriched_summary or raw_get("enriched_summary"),
        'extraction_notes': enrichment.extraction_notes or raw_get("extraction_notes"),
        'confidence': _first_present(
            raw_get("confidence_score"),
            raw_get("confidence"),
            _derive_extraction_confidence(enrichment, raw),
        ),
    }
    
    # Columns whose values are intentionally JSON-serialised lists (stored as TEXT).
    _JSON_TEXT_COLS = {
        "systems_affected_codes", "timeline_json", "mitre_techniques_json",
        "malware_families", "attacker_tools", "threat_actor_aliases",
        "cve_ids", "cvss_scores", "vulnerability_names", "affected_products",
        "data_categories", "regulatory_context",
        "secondary_attack_categories", "attack_chain",
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
    1. Canonical raw/final enrichment layers in incident_enrichments
    2. Flattened fields in incident_enrichments_flat table (for fast queries/CSV)
    
    Args:
        conn: Database connection
        incident_id: Incident ID to update
        enrichment_result: CTIEnrichmentResult to save
        force_replace: If True, replace existing enrichment regardless of confidence
        
    Returns:
        True if enrichment was saved, False if skipped
    """
    if not (_table_exists(conn, "incident_enrichments") and _table_exists(conn, "incident_enrichments_flat")):
        init_incident_enrichments_table(conn)

    # Check if we should upgrade existing enrichment
    if not force_replace:
        existing_enrichment = get_enrichment_result(conn, incident_id)
        if existing_enrichment:
            logger.info(
                f"Existing enrichment found for incident {incident_id}, replacing it."
            )

    storage_debug = _extract_storage_debug(raw_json_data)
    raw_json_data = _strip_storage_debug(raw_json_data)

    now = datetime.utcnow().isoformat()
    
    # Get incident data for flattened table (fallback values)
    cur = conn.execute(
        """
        SELECT institution_name, victim_raw_name, institution_type, country, region, city,
               title, subtitle, source_published_date, notes, all_urls, status
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
        # When LLM returned null, use the incidents table value (set at ingestion time)
        country_raw = country_fallback
    if not country_raw and notes_fallback:
        # Google News RSS and similar curated sources embed "lang=XX;country=YY;query=..."
        _nc = re.search(r"\bcountry=([A-Z]{2})\b", notes_fallback)
        if _nc:
            country_raw = _nc.group(1)  # ISO-2 code; normalize_country handles it
    if not country_raw:
        country_raw = _derive_country_from_context(conn, incident_id, incident_row)
        if isinstance(country_raw, tuple):
            country_raw = next((value for value in country_raw if value), None)
    country = normalize_country(country_raw) if country_raw else None
    country_code = get_country_code(country) if country else None

    region = _scalar(raw_json_data.get("region")) if raw_json_data else region_fallback
    if not region:
        region = region_fallback
    city = _scalar(raw_json_data.get("city")) if raw_json_data else city_fallback
    if not city:
        city = city_fallback
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
        # Secondary check: if cleaned result still looks like a news headline, fall
        # back to ingestion-time scoring.  This catches non-English headlines that
        # _ATTACK_TERM_RE in clean_institution_name() does not recognise.
        if is_headline_format(resolved_institution_name, incident_row["title"] if incident_row else None):
            logger.warning(
                "institution_name looks like a headline ('%s...'); falling back to ingestion-time names",
                resolved_institution_name[:60],
            )
            _recovered = choose_best_institution_name(
                institution_name_fallback,
                victim_name_fallback,
                incident_row["title"] if incident_row else None,
                incident_row["subtitle"] if incident_row else None,
            )
            if _recovered:
                resolved_institution_name = _recovered
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
    # Domains that are victim-listing sites, not news articles. Prefer any other URL.
    _LEAK_SITE_DOMAINS = {"ransomware.live", "ransomwatch.telemetry.ltd", "id.ransomware.live"}

    def _is_leak_site(url: Optional[str]) -> bool:
        if not url:
            return False
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc.lower().lstrip("www.") in _LEAK_SITE_DOMAINS
        except Exception:
            return False

    if _all_urls and _raw_primary_url and _raw_primary_url not in _all_urls:
        logger.warning(
            "LLM primary_url %s not in all_urls for %s; using %s instead",
            _raw_primary_url, incident_id, _all_urls[0],
        )
        primary_url = _all_urls[0]
    else:
        primary_url = _raw_primary_url

    # Prefer a real news article over a ransomware victim listing page.
    if _is_leak_site(primary_url) and _all_urls:
        news_url = next((u for u in _all_urls if not _is_leak_site(u)), None)
        if news_url:
            logger.debug("Replacing leak-site primary_url with news URL: %s", news_url)
            primary_url = news_url
    article_metadata = _get_primary_article_metadata(conn, incident_id, primary_url)
    article_publish_date = article_metadata.get("publish_date")

    publication_date = _scalar(raw_json_data.get("publication_date")) if raw_json_data else None
    if not publication_date:
        publication_date = article_publish_date

    # Extract SERP search window year from notes (e.g., "source=...;window=2025")
    _window_year = None
    if notes_fallback:
        _wm = re.search(r'\bwindow=(\d{4})\b', notes_fallback)
        if _wm:
            _window_year = int(_wm.group(1))

    # Guard LLM publication_date: discard if it looks like today's enrichment date but the
    # SERP search window was a prior year (LLM hallucinated current date instead of reading
    # the article), or if it's in the future.
    if publication_date:
        try:
            from datetime import date as _date
            _today = _date.fromisoformat(now[:10])
            _pub_dt = _date.fromisoformat(str(publication_date)[:10])
            if _pub_dt > _today:
                logger.warning(
                    "Discarding LLM publication_date %s (future date) for %s",
                    publication_date, incident_id,
                )
                publication_date = None
            elif (_today - _pub_dt).days <= 7 and _window_year and _window_year < _today.year:
                logger.warning(
                    "Discarding LLM publication_date %s (matches enrichment date; SERP window=%s) for %s",
                    publication_date, _window_year, incident_id,
                )
                publication_date = None
        except (ValueError, TypeError):
            pass

    # Update incident with enrichment data
    summary = enrichment_result.enriched_summary

    # Extract incident_date from raw JSON or timeline fallback
    llm_incident_date = None
    llm_date_precision = None
    llm_discovery_date = None
    if raw_json_data:
        llm_incident_date = _scalar(raw_json_data.get("incident_date"))
        llm_date_precision = _scalar(raw_json_data.get("incident_date_precision"))
        llm_discovery_date = _scalar(raw_json_data.get("discovery_date"))

    if not llm_incident_date and enrichment_result.timeline:
        dated_events = [e for e in enrichment_result.timeline if e.date]
        if dated_events:
            earliest_event = min(dated_events, key=lambda e: e.date)
            llm_incident_date = earliest_event.date
            llm_date_precision = earliest_event.date_precision or "approximate"

    # Structured data (timeline, MITRE, attack_dynamics) is now stored exclusively in
    # junction tables (incident_timeline, incident_mitre_techniques, incident_vulnerabilities).
    # llm_timeline / llm_mitre_attack / llm_attack_dynamics columns on incidents are legacy;
    # we no longer write to them so the authoritative copy stays in junction tables only.
    update_fields = """
        llm_enriched = 1,
        llm_enriched_at = ?,
        primary_url = ?,
        llm_summary = ?,
        last_updated_at = ?
    """

    update_params = [
        now,
        primary_url,
        summary,
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
    normalized_publication_date = str(publication_date)[:10] if publication_date else None
    normalized_source_published_date = (
        str(source_published_date_fallback)[:10]
        if source_published_date_fallback
        else None
    )
    if normalized_publication_date and normalized_publication_date != normalized_source_published_date:
        update_fields += ",\n        source_published_date = ?"
        update_params.append(normalized_publication_date)
    
    # Update incident_date if LLM extracted it — but guard against the LLM picking
    # a repost/mirror date that is far AFTER the article's source_published_date.
    # An incident cannot occur significantly after the article reporting it was written.
    if llm_incident_date:
        _apply_llm_date = True
        # Use pre-existing source_published_date; fall back to LLM's (now-validated) publication_date
        # so incidents with no prior date still get the year-mismatch check.
        _eff_src = source_published_date_fallback or publication_date
        if _eff_src:
            try:
                from datetime import date as _date
                _src_dt = _date.fromisoformat(str(_eff_src)[:10])
                _llm_dt = _date.fromisoformat(str(llm_incident_date)[:10])
                if (_llm_dt - _src_dt).days > 90:
                    logger.warning(
                        "Skipping LLM incident_date %s (>90 days after source_published_date %s) for %s",
                        llm_incident_date, _eff_src, incident_id,
                    )
                    _apply_llm_date = False
                # Year mismatch guard: catches LLM writing "2026" for a 2025 article.
                # Allow ±1 year to accommodate cross-year articles (e.g., Dec article about Jan incident).
                elif abs(_llm_dt.year - _src_dt.year) > 1:
                    logger.warning(
                        "Skipping LLM incident_date %s (year %s differs from source year %s by >1) for %s",
                        llm_incident_date, _llm_dt.year, _src_dt.year, incident_id,
                    )
                    _apply_llm_date = False
            except (ValueError, TypeError):
                pass
        # Also guard against the SERP search window year — catches the case where both
        # publication_date and incident_date were hallucinated to the current year.
        if _apply_llm_date and _window_year:
            try:
                from datetime import date as _date
                _llm_dt = _date.fromisoformat(str(llm_incident_date)[:10])
                if abs(_llm_dt.year - _window_year) > 1:
                    logger.warning(
                        "Skipping LLM incident_date %s (year %s differs from SERP window %s) for %s",
                        llm_incident_date, _llm_dt.year, _window_year, incident_id,
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
            return True  # Incident handled — deleted as post-enrich duplicate

    # Flatten and save to incident_enrichments_flat table
    # Pre-coerce raw_json_data: grammar-constrained LLM may return scalar fields as lists.
    # _flatten_enrichment_for_db also applies a post-pass, but doing it here means
    # the individual _scalar() calls above (country, region, etc.) share the same
    # normalised source without duplicating coercion logic.
    raw_extraction_payload = dict(raw_json_data) if raw_json_data else None
    _FLAT_ARRAY_KEYS = {
        "timeline", "mitre_attack_techniques", "systems_affected_codes",
        "data_categories_affected", "data_types", "operational_impacts",
        "security_improvements", "third_parties_involved", "other_edu_incidents",
        "iocs", "target_demographics", "attack_chain",
        "vulnerabilities_exploited", "malware_families", "attacker_tools",
        "threat_actor_aliases", "applicable_regulations", "institution_aliases",
        "law_enforcement_agencies", "regulators_notified", "investigating_agencies",
        "related_incidents", "key_quotes",
        "secondary_attack_categories",
    }
    raw_json_data_for_flat = raw_json_data
    if raw_json_data_for_flat:
        raw_json_data_for_flat = {
            k: (v[0] if isinstance(v, list) and v and k not in _FLAT_ARRAY_KEYS else
                (None if isinstance(v, list) and k not in _FLAT_ARRAY_KEYS else v))
            for k, v in raw_json_data_for_flat.items()
        }
    flat_data = _flatten_enrichment_for_db(enrichment_result, raw_json_data_for_flat)
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

    # Inject timeline into flat_data so _fill_timeline_dates() can operate on it.
    # Without this, flat_data["timeline_json"] is never set and the date-fill is a no-op.
    _pre_pp_timeline = (
        enrichment_result.timeline
        if isinstance(enrichment_result.timeline, list)
        else (raw_json_data or {}).get("timeline")
    )
    if _pre_pp_timeline:
        try:
            _tl_dicts = (
                [e.model_dump() if hasattr(e, "model_dump") else dict(e) for e in _pre_pp_timeline]
            )
            flat_data["timeline_json"] = json.dumps(_tl_dicts)
        except Exception:
            pass

    # Inject incident_date and source_published_date so _fill_timeline_dates() has anchors.
    # These live in the incidents table, not in the enrichments flat dict.
    if not flat_data.get("incident_date"):
        try:
            _row_inc_date = incident_row["incident_date"] if incident_row else None
        except (IndexError, KeyError, TypeError):
            _row_inc_date = None
        _inc_date = llm_incident_date or _row_inc_date
        if _inc_date:
            flat_data["incident_date"] = str(_inc_date)[:10]
    if normalized_publication_date:
        flat_data["source_published_date"] = normalized_publication_date
    elif not flat_data.get("source_published_date") and normalized_source_published_date:
        flat_data["source_published_date"] = normalized_source_published_date

    apply_post_processing(flat_data, incident_row, summary=summary)

    # Promote status to "confirmed" when enriched summary contains confirmation language.
    # infer_confirmed_status is deterministic — only promotes, never demotes.
    _incident_status = incident_row["status"] if incident_row else None
    if _incident_status == "suspected":
        _inc_title = incident_row["title"] if incident_row else None
        if infer_confirmed_status(summary, _inc_title):
            conn.execute(
                "UPDATE incidents SET status = 'confirmed' WHERE incident_id = ?",
                (incident_id,),
            )
            logger.debug("Promoted status to confirmed for %s based on enriched summary", incident_id)

    flat_data['created_at'] = now
    flat_data['updated_at'] = now

    raw_snapshot = _build_raw_extraction_snapshot(raw_extraction_payload)
    final_enrichment_json = _build_final_enrichment_record(
        enrichment_result=enrichment_result,
        raw_snapshot=raw_snapshot,
        flat_data=flat_data,
        storage_debug=storage_debug,
    )
    raw_llm_responses = storage_debug.get("raw_llm_responses")
    raw_response_payload = (
        json.dumps(raw_llm_responses, indent=2)
        if isinstance(raw_llm_responses, dict) and raw_llm_responses
        else None
    )
    llm_metadata = storage_debug.get("llm_metadata")
    storage_metadata = (
        json.dumps(llm_metadata, indent=2)
        if isinstance(llm_metadata, dict) and llm_metadata
        else None
    )
    enrichment_confidence = flat_data.get("confidence")

    # Save current/latest enrichment snapshots
    try:
        enrichment_columns = _table_columns(conn, "incident_enrichments")
        run_columns = _table_columns(conn, "incident_enrichment_runs")
        legacy_shadow_json = final_enrichment_json

        cur = conn.execute(
            "SELECT incident_id FROM incident_enrichments WHERE incident_id = ?",
            (incident_id,)
        )
        exists = cur.fetchone() is not None

        latest_values = (
            raw_response_payload,
            json.dumps(raw_snapshot, indent=2),
            final_enrichment_json,
            storage_metadata,
            _ENRICHMENT_STORAGE_VERSION,
            enrichment_confidence,
            _metadata_value(storage_debug, "provider"),
            _metadata_value(storage_debug, "model"),
            _metadata_value(storage_debug, "extraction_mode"),
            _metadata_value(storage_debug, "prompt_version"),
            _metadata_value(storage_debug, "schema_version"),
            _metadata_value(storage_debug, "mapper_version"),
            _metadata_value(storage_debug, "post_processing_version"),
        )

        if exists:
            update_fields = [
                "raw_response_payload = ?",
                "raw_extraction_json = ?",
                "final_enrichment_json = ?",
                "storage_metadata = ?",
                "enrichment_version = ?",
                "enrichment_confidence = ?",
                "llm_provider = ?",
                "llm_model = ?",
                "extraction_mode = ?",
                "prompt_version = ?",
                "schema_version = ?",
                "mapper_version = ?",
                "post_processing_version = ?",
            ]
            update_params = list(latest_values)
            if "enrichment_data" in enrichment_columns:
                update_fields.append("enrichment_data = ?")
                update_params.append(legacy_shadow_json)
            update_fields.append("updated_at = ?")
            update_params.extend([now, incident_id])
            conn.execute(
                f"""
                UPDATE incident_enrichments
                SET {', '.join(update_fields)}
                WHERE incident_id = ?
                """,
                tuple(update_params)
            )
        else:
            insert_columns = [
                "incident_id",
                "raw_response_payload",
                "raw_extraction_json",
                "final_enrichment_json",
                "storage_metadata",
                "enrichment_version",
                "enrichment_confidence",
                "llm_provider",
                "llm_model",
                "extraction_mode",
                "prompt_version",
                "schema_version",
                "mapper_version",
                "post_processing_version",
                "created_at",
                "updated_at",
            ]
            insert_values = [
                incident_id,
                *latest_values,
                now,
                now,
            ]
            if "enrichment_data" in enrichment_columns:
                insert_columns.insert(1, "enrichment_data")
                insert_values.insert(1, legacy_shadow_json)
            conn.execute(
                f"""
                INSERT INTO incident_enrichments (
                    {', '.join(insert_columns)}
                )
                VALUES ({', '.join(['?'] * len(insert_columns))})
                """,
                tuple(insert_values)
            )

        run_insert_columns = [
            "incident_id",
            "raw_response_payload",
            "raw_extraction_json",
            "final_enrichment_json",
            "storage_metadata",
            "enrichment_version",
            "enrichment_confidence",
            "llm_provider",
            "llm_model",
            "extraction_mode",
            "prompt_version",
            "schema_version",
            "mapper_version",
            "post_processing_version",
            "created_at",
        ]
        run_insert_values = [
            incident_id,
            raw_response_payload,
            json.dumps(raw_snapshot, indent=2),
            final_enrichment_json,
            storage_metadata,
            _ENRICHMENT_STORAGE_VERSION,
            enrichment_confidence,
            _metadata_value(storage_debug, "provider"),
            _metadata_value(storage_debug, "model"),
            _metadata_value(storage_debug, "extraction_mode"),
            _metadata_value(storage_debug, "prompt_version"),
            _metadata_value(storage_debug, "schema_version"),
            _metadata_value(storage_debug, "mapper_version"),
            _metadata_value(storage_debug, "post_processing_version"),
            now,
        ]
        if "enrichment_data" in run_columns:
            run_insert_columns.insert(1, "enrichment_data")
            run_insert_values.insert(1, legacy_shadow_json)
        conn.execute(
            f"""
            INSERT INTO incident_enrichment_runs (
                {', '.join(run_insert_columns)}
            )
            VALUES ({', '.join(['?'] * len(run_insert_columns))})
            """,
            tuple(run_insert_values)
        )
    except Exception:
        conn.execute("ROLLBACK")
        raise
    
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
        'country', 'country_code', 'region', 'city', 'attack_category', 'attack_vector', 'access_vector',
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
        'faculty_affected', 'alumni_affected', 'parents_affected', 'applicants_affected', 'patients_affected',
        'users_affected_exact', 'users_affected_min', 'users_affected_max',
        'ransom_amount_min', 'ransom_amount_max',
        'recovery_costs_min', 'recovery_costs_max', 'legal_costs', 'insurance_claim',
        'insurance_claim_amount', 'business_impact', 'gdpr_breach', 'hipaa_breach', 'ferpa_breach',
        'breach_notification_required', 'notifications_sent', 'notifications_sent_date',
        'dpa_notified', 'investigation_opened', 'fine_imposed', 'fine_amount',
        'lawsuits_filed', 'class_action', 'recovery_timeframe_days', 'recovery_started_date',
        'recovery_completed_date', 'from_backup', 'mfa_implemented', 'incident_response_firm',
        'forensics_firm', 'public_disclosure', 'public_disclosure_date', 'disclosure_delay_days',
        'transparency_level',
        # Convenience columns for junction-table data
        'timeline_events_count', 'primary_cve_id', 'max_cvss_score', 'primary_mitre_technique_id',
        # Threat intelligence
        'malware_families', 'attacker_tools', 'threat_actor_aliases', 'attack_campaign_name',
        'cloud_provider', 'dwell_time_days', 'mttd_hours', 'mttr_hours',
        # Financial (additional)
        'total_cost_estimate',
        # Operational (additional)
        'clinical_operations_disrupted', 'graduation_delayed', 'online_learning_disrupted',
        # Recovery (additional)
        'backup_status', 'backup_age_days', 'law_enforcement_involved', 'law_enforcement_agency',
        # Transparency (additional)
        'official_statement_url',
        # Research impact (new)
        'research_projects_affected', 'research_data_compromised', 'publications_delayed',
        'grants_affected', 'research_area',
        # Regulatory (additional)
        'regulatory_context',
        # Data (additional)
        'data_volume_gb',
        # Attack classification (additional)
        'secondary_attack_categories', 'attack_chain',
        'incident_date_precision', 'encryption_extent', 'disclosure_source',
        # Education-specific context
        'academic_period_affected', 'dark_web_posting_confirmed',
        'prior_breach_same_institution', 'notification_delay_days',
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

    # Save structured data to junction tables (outside the main transaction so
    # a junction-table failure never rolls back the flat record).
    try:
        vuln_source = (
            enrichment_result.vulnerabilities_exploited
            if isinstance(enrichment_result.vulnerabilities_exploited, list)
            else (raw_json_data or {}).get("vulnerabilities_exploited")
        )
        _save_incident_vulnerabilities(conn, incident_id, vuln_source)

        mitre_source = (
            enrichment_result.mitre_attack_techniques
            if isinstance(enrichment_result.mitre_attack_techniques, list)
            else (raw_json_data or {}).get("mitre_attack_techniques")
        )
        _save_incident_mitre_techniques(conn, incident_id, mitre_source)

        # Use post-processed timeline from flat_data if available (dates may have been
        # filled by _fill_timeline_dates); otherwise fall back to original enrichment data.
        _timeline_json_str = flat_data.get("timeline_json")
        if _timeline_json_str:
            try:
                timeline_source = json.loads(_timeline_json_str)
            except (json.JSONDecodeError, TypeError):
                timeline_source = (
                    enrichment_result.timeline
                    if isinstance(enrichment_result.timeline, list)
                    else (raw_json_data or {}).get("timeline")
                )
        else:
            timeline_source = (
                enrichment_result.timeline
                if isinstance(enrichment_result.timeline, list)
                else (raw_json_data or {}).get("timeline")
            )
        _save_incident_timeline(conn, incident_id, timeline_source)

        conn.commit()
    except Exception as exc:
        logger.warning("Junction-table save failed for %s (non-fatal): %s", incident_id, exc)

    logger.info(
        f"Saved enrichment result for incident {incident_id} (raw/final layers + flattened)"
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
    if not _table_exists(conn, "incident_enrichments"):
        return None

    cur = conn.execute(
        """
        SELECT final_enrichment_json FROM incident_enrichments
        WHERE incident_id = ?
        """,
        (incident_id,)
    )
    
    row = cur.fetchone()
    if not row:
        return None
    
    try:
        final_payload = json.loads(row["final_enrichment_json"])
        return CTIEnrichmentResult.model_validate(
            _extract_typed_enrichment_dict(final_payload)
        )
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
    if not _table_exists(conn, "incident_enrichments_flat"):
        return None

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
            "incident_enrichment_runs",
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
        "incident_enrichment_runs",
        "articles",
        "incident_sources",
        "source_events",
        "pipeline_checkpoint",
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
        conn.execute(
            "DELETE FROM incident_enrichment_runs WHERE incident_id = ?",
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
        
        # Preserve the full enrichment history table only when explicitly desired.
        # Revert semantics mean "wipe enrichment state", so clear runs too.
        conn.execute("DELETE FROM incident_enrichment_runs")
        
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
            f"DELETE FROM incident_enrichment_runs WHERE incident_id IN ({placeholders})",
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
        "incident_enrichment_runs",
        "articles",
        "incident_sources",
        "source_events",
        "pipeline_checkpoint",
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
            f"DELETE FROM incident_enrichment_runs WHERE incident_id IN ({placeholders})",
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
            "incident_enrichment_runs",
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
