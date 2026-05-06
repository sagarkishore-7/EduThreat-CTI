"""
Admin API endpoints for database management and export.

These endpoints require authentication and provide:
- Database export (full DB file download)
- CSV export of tables
- Scheduler status and control
"""

import os
import re
import csv
import io
import sqlite3
import secrets
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from pathlib import Path

from fastapi import APIRouter, HTTPException, Depends, Header, Response, UploadFile, File, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from src.edu_cti.core.config import DB_PATH, DATA_DIR
from src.edu_cti.api.database import get_api_connection, count_education_incidents
from src.edu_cti.api.cache import cache_invalidate
from src.edu_cti.core.metrics import get_metrics
from src.edu_cti.pipeline.phase2.utils.deduplication import deduplicate_by_institution

# Use DATA_DIR from config (auto-detects Railway)
PERSISTENT_DATA_DIR = DATA_DIR

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["Admin"])

# Admin authentication - use environment variables for credentials
ADMIN_USERNAME = os.getenv("EDUTHREAT_ADMIN_USERNAME", "admin")
ADMIN_PASSWORD_HASH = os.getenv("EDUTHREAT_ADMIN_PASSWORD_HASH")  # SHA256 hash

# API Key for simpler auth (alternative to username/password)
ADMIN_API_KEY = os.getenv("EDUTHREAT_ADMIN_API_KEY")

# Session tokens (in-memory, cleared on restart)
_active_sessions: Dict[str, datetime] = {}
SESSION_DURATION_HOURS = 24


def hash_password(password: str) -> str:
    """Hash password using SHA256."""
    return hashlib.sha256(password.encode()).hexdigest()


def verify_password(password: str) -> bool:
    """Verify password against stored hash."""
    if not ADMIN_PASSWORD_HASH:
        # Default for development only — set EDUTHREAT_ADMIN_PASSWORD_HASH in production
        logger.warning("No EDUTHREAT_ADMIN_PASSWORD_HASH set — using development default")
        default_hash = hash_password(os.getenv("EDUTHREAT_ADMIN_PASSWORD", "admin123"))
        return hash_password(password) == default_hash
    return hash_password(password) == ADMIN_PASSWORD_HASH


def verify_api_key(api_key: str) -> bool:
    """Verify API key."""
    if not ADMIN_API_KEY:
        return False
    return secrets.compare_digest(api_key, ADMIN_API_KEY)


def verify_session(session_token: str) -> bool:
    """Verify session token."""
    if session_token not in _active_sessions:
        return False
    
    expires = _active_sessions[session_token]
    if datetime.now() > expires:
        del _active_sessions[session_token]
        return False
    
    return True


def authenticate(
    authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None),
    x_session_token: Optional[str] = Header(None),
) -> bool:
    """
    Authenticate admin request.
    
    Supports:
    - Bearer token (session token)
    - X-API-Key header
    - X-Session-Token header
    """
    # Check session token
    if x_session_token and verify_session(x_session_token):
        return True
    
    # Check API key
    if x_api_key and verify_api_key(x_api_key):
        return True
    
    # Check Bearer token
    if authorization and authorization.startswith("Bearer "):
        token = authorization[7:]
        if verify_session(token):
            return True
    
    raise HTTPException(
        status_code=401,
        detail="Authentication required",
        headers={"WWW-Authenticate": "Bearer"},
    )


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    success: bool
    session_token: Optional[str] = None
    expires_at: Optional[str] = None
    message: str


class SchedulerStatus(BaseModel):
    running: bool
    last_rss_run: Optional[str]
    last_weekly_run: Optional[str]
    enrichment_enabled: bool
    next_jobs: List[str]


class ExportStats(BaseModel):
    total_incidents: int
    enriched_incidents: int
    education_related: int
    total_sources: int
    db_size_mb: float
    last_updated: str


@router.post("/login", response_model=LoginResponse)
async def admin_login(request: LoginRequest):
    """
    Admin login endpoint.
    
    Returns a session token valid for 24 hours.
    """
    if request.username != ADMIN_USERNAME:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    if not verify_password(request.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    # Generate session token
    session_token = secrets.token_urlsafe(32)
    expires_at = datetime.now() + timedelta(hours=SESSION_DURATION_HOURS)
    _active_sessions[session_token] = expires_at
    
    return LoginResponse(
        success=True,
        session_token=session_token,
        expires_at=expires_at.isoformat(),
        message="Login successful",
    )


@router.post("/logout")
async def admin_logout(
    x_session_token: Optional[str] = Header(None),
    _: bool = Depends(authenticate),
):
    """Invalidate current session."""
    if x_session_token and x_session_token in _active_sessions:
        del _active_sessions[x_session_token]
    
    return {"success": True, "message": "Logged out"}


@router.get("/export/stats", response_model=ExportStats)
async def get_export_stats(_: bool = Depends(authenticate)):
    """Get database statistics for export."""
    conn = get_api_connection()
    
    try:
        # Total incidents
        cur = conn.execute("SELECT COUNT(*) FROM incidents")
        total = cur.fetchone()[0]
        
        # Enriched incidents
        cur = conn.execute("SELECT COUNT(*) FROM incidents WHERE llm_enriched = 1")
        enriched = cur.fetchone()[0]
        
        # Education related
        education = count_education_incidents(conn)
        
        # Total distinct sources
        cur = conn.execute("SELECT COUNT(DISTINCT source) FROM incident_sources")
        sources = cur.fetchone()[0]
        
        # DB size — use SQLite page_count*page_size so WAL-mode writes are reflected
        try:
            page_count = conn.execute("PRAGMA page_count").fetchone()[0]
            page_size = conn.execute("PRAGMA page_size").fetchone()[0]
            db_size_mb = (page_count * page_size) / (1024 * 1024)
        except Exception:
            db_path = Path(DB_PATH)
            db_size_mb = db_path.stat().st_size / (1024 * 1024) if db_path.exists() else 0
        
        return ExportStats(
            total_incidents=total,
            enriched_incidents=enriched,
            education_related=education,
            total_sources=sources,
            db_size_mb=round(db_size_mb, 2),
            last_updated=datetime.now().isoformat(),
        )
    finally:
        conn.close()


@router.get("/export/database")
async def export_database(_: bool = Depends(authenticate)):
    """
    Download the full SQLite database file.
    
    Returns the database file as a download.
    """
    db_path = Path(DB_PATH)
    
    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database file not found")
    
    filename = f"eduthreat_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    
    return FileResponse(
        path=str(db_path),
        media_type="application/x-sqlite3",
        filename=filename,
    )


# IMPORTANT: Specific routes must come BEFORE parameterized routes
# Otherwise FastAPI will match /export/csv/full to /export/csv/{table_name}

@router.get("/export/csv/full")
async def export_full_csv(
    education_only: str = "false",
    _: bool = Depends(authenticate),
):
    """
    Export ALL incidents as CSV (enriched and unenriched).
    
    Includes all incidents from the database, whether they've been enriched or not.
    For enriched incidents, includes enrichment data. For unenriched incidents, only basic fields.
    """
    import traceback
    
    # Parse education_only string to boolean
    education_only_bool = education_only and education_only.lower() in ("true", "1", "yes", "on")
    
    # Log immediately to verify function is called
    logger.debug(f"Full CSV export called (education_only={education_only_bool})")
    
    try:
        from src.edu_cti.core.db import load_incident_by_id
        from src.edu_cti.pipeline.phase2.csv_export import load_enriched_incidents_from_db
        from src.edu_cti.core.deduplication import extract_urls_from_incident
    except ImportError as e:
        logger.error(f"Import error: {str(e)[:100]}")
        raise HTTPException(status_code=500, detail=f"Import error: {str(e)}")
    
    conn = None
    try:
        logger.info(f"Starting full CSV export (education_only={education_only_bool})")
        
        conn = get_api_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Failed to get database connection")
        
        # Get all incidents (check if broken_urls column exists)
        try:
            cur = conn.execute("PRAGMA table_info(incidents)")
            columns = [row[1] for row in cur.fetchall()]
            has_broken_urls = "broken_urls" in columns
        except sqlite3.Error:
            has_broken_urls = False
        
        # Build query - always select all columns from incidents
        query = """
            SELECT 
                i.*,
                GROUP_CONCAT(DISTINCT isrc.source) as sources
            FROM incidents i
            LEFT JOIN incident_sources isrc ON i.incident_id = isrc.incident_id
            GROUP BY i.incident_id
            ORDER BY i.ingested_at DESC
        """
        
        cur = conn.execute(query)
        
        # Get column names from the query result BEFORE fetching rows
        column_names = [description[0] for description in cur.description] if cur.description else []
        
        all_incidents = cur.fetchall()
        
        # Get enriched incidents with their enrichment data
        enriched_incidents_data = {}
        try:
            enriched_list = load_enriched_incidents_from_db(conn, use_flat_table=True)
            for inc in enriched_list:
                enriched_incidents_data[inc.get("incident_id")] = inc
        except Exception as e:
            logger.warning(f"Could not load enriched incidents: {e}")
        
        # Build combined dataset
        combined_incidents = []
        fieldnames_set = set()
        
        # Helper function to safely get row value (defined outside loop for efficiency)
        def safe_get(row, key, default=""):
            try:
                if key in column_names:
                    value = row[key]
                    return value if value is not None else default
                return default
            except (KeyError, IndexError, TypeError):
                return default
        
        for row in all_incidents:
            incident_id = safe_get(row, "incident_id", "")
            if not incident_id:
                continue  # Skip rows without incident_id
            
            llm_enriched_val = safe_get(row, "llm_enriched", 0)
            is_enriched = llm_enriched_val == 1 if llm_enriched_val else False
            
            # Start with basic incident data
            incident_dict = {
                "incident_id": incident_id,
                "sources": safe_get(row, "sources"),
                "institution_name": safe_get(row, "institution_name"),
                "victim_raw_name": safe_get(row, "victim_raw_name"),
                "institution_type": safe_get(row, "institution_type"),
                "country": safe_get(row, "country"),
                "region": safe_get(row, "region"),
                "city": safe_get(row, "city"),
                "incident_date": safe_get(row, "incident_date"),
                "date_precision": safe_get(row, "date_precision"),
                "source_published_date": safe_get(row, "source_published_date"),
                "ingested_at": safe_get(row, "ingested_at"),
                "title": safe_get(row, "title"),
                "subtitle": safe_get(row, "subtitle"),
                "primary_url": safe_get(row, "primary_url"),
                "all_urls": safe_get(row, "all_urls"),
                "broken_urls": safe_get(row, "broken_urls") if has_broken_urls else "",
                "attack_type_hint": safe_get(row, "attack_type_hint"),
                "status": safe_get(row, "status"),
                "source_confidence": safe_get(row, "source_confidence"),
                "notes": safe_get(row, "notes"),
                "llm_enriched": "Yes" if is_enriched else "No",
                "llm_enriched_at": safe_get(row, "llm_enriched_at"),
            }
            
            # Add enrichment data if available
            if is_enriched and incident_id in enriched_incidents_data:
                enriched_data = enriched_incidents_data[incident_id]
                # Add all enrichment fields
                for key, value in enriched_data.items():
                    if key not in incident_dict:  # Don't overwrite basic fields
                        incident_dict[key] = value
            
            # Apply education filter if requested
            if education_only_bool:
                is_education = incident_dict.get("is_education_related", False)
                if not is_education:
                    continue
            
            combined_incidents.append(incident_dict)
            fieldnames_set.update(incident_dict.keys())
        
        if not combined_incidents:
            raise HTTPException(status_code=404, detail="No incidents found")
        
        # Sort fieldnames: basic fields first, then enrichment fields
        basic_fields = [
            "incident_id", "sources", "institution_name", "victim_raw_name", "victim_raw_name_normalized",
            "institution_type", "country", "region", "city", "incident_date", "date_precision",
            "source_published_date", "ingested_at", "title", "subtitle", "primary_url", "all_urls",
            "broken_urls", "attack_type_hint", "status", "source_confidence", "notes",
            "llm_enriched", "llm_enriched_at"
        ]
        enrichment_fields = sorted([f for f in fieldnames_set if f not in basic_fields])
        fieldnames = [f for f in basic_fields if f in fieldnames_set] + enrichment_fields
        
        # Generate CSV
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(combined_incidents)
        
        csv_content = output.getvalue()
        
        filename = f"eduthreat_full_all_incidents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        logger.info(f"Generated CSV: {len(combined_incidents)} incidents, {len(fieldnames)} columns")
        
        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    except HTTPException as he:
        # Re-raise HTTP exceptions (like 404)
        logger.error(f"HTTPException: {str(he.detail)[:100]}")
        raise
    except Exception as e:
        error_msg = str(e)
        error_trace = traceback.format_exc()
        logger.error(f"Full CSV export failed: {error_msg[:200]}")
        raise HTTPException(
            status_code=500,
            detail=f"CSV export failed: {error_msg}"
        )
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


@router.get("/export/csv/enriched")
async def export_enriched_csv(
    education_only: str = "true",
    _: bool = Depends(authenticate),
):
    """
    Export enriched dataset as CSV (only incidents that have been enriched).
    
    Joins incidents with enrichments for complete data.
    Only includes incidents that have been processed by LLM enrichment.
    """
    from src.edu_cti.pipeline.phase2.csv_export import load_enriched_incidents_from_db
    
    # Parse education_only string to boolean
    education_only_bool = education_only and education_only.lower() in ("true", "1", "yes", "on")
    
    logger.debug(f"Enriched CSV export called (education_only={education_only_bool})")
    
    conn = None
    try:
        conn = get_api_connection()
        incidents = load_enriched_incidents_from_db(conn, use_flat_table=True)
        
        if education_only_bool:
            incidents = [i for i in incidents if i.get("is_education_related")]
        
        if not incidents:
            raise HTTPException(status_code=404, detail="No enriched incidents found")
        
        # Generate CSV
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=incidents[0].keys())
        writer.writeheader()
        writer.writerows(incidents)
        
        csv_content = output.getvalue()
        
        filename = f"eduthreat_enriched_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        logger.info(f"Generated enriched CSV: {len(incidents)} incidents")
        
        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Enriched CSV export failed: {str(e)[:200]}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"CSV export failed: {str(e)}"
        )
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _flatten_timeline(timeline_json_str: Optional[str]) -> dict:
    """
    Parse timeline JSON and return flat key→value pairs suitable for CSV columns.

    Extracts key event dates by event_type and computes durations (in days) between
    important milestones so analysts can answer questions like:
    "How long did it take from initial access to recovery?"
    """
    import json as _json
    from datetime import date as _date

    out: dict = {
        "timeline_event_types": "",         # ordered comma-sep list of all event types
        "timeline_initial_access_date": "",
        "timeline_discovery_date": "",
        "timeline_encryption_date": "",
        "timeline_data_exfiltration_date": "",
        "timeline_ransom_demand_date": "",
        "timeline_containment_date": "",
        "timeline_recovery_date": "",
        "timeline_disclosure_date": "",
        "timeline_notification_date": "",
        "days_initial_to_discovery": "",
        "days_initial_to_containment": "",
        "days_initial_to_recovery": "",
        "days_discovery_to_disclosure": "",
        "days_discovery_to_containment": "",
        "days_containment_to_recovery": "",
    }
    if not timeline_json_str:
        return out

    try:
        events = _json.loads(timeline_json_str)
    except (_json.JSONDecodeError, TypeError):
        return out

    if not isinstance(events, list):
        return out

    # Build a map: event_type → earliest date for that type
    type_to_date: dict[str, _date] = {}
    event_type_order: list[str] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        etype = ev.get("event_type") or "other"
        event_type_order.append(etype)
        raw_date = ev.get("date")
        if raw_date:
            try:
                dt = _date.fromisoformat(str(raw_date)[:10])
                if etype not in type_to_date or dt < type_to_date[etype]:
                    type_to_date[etype] = dt
            except (ValueError, TypeError):
                pass

    out["timeline_event_types"] = ", ".join(event_type_order)

    # Key milestone mappings
    milestones = {
        "timeline_initial_access_date":      ["initial_access", "exploitation", "reconnaissance"],
        "timeline_discovery_date":           ["discovery", "detection", "investigation"],
        "timeline_encryption_date":          ["encryption_started", "impact"],
        "timeline_data_exfiltration_date":   ["data_exfiltration", "exfiltration"],
        "timeline_ransom_demand_date":       ["ransom_demand"],
        "timeline_containment_date":         ["containment", "eradication"],
        "timeline_recovery_date":            ["recovery", "systems_restored", "remediation"],
        "timeline_disclosure_date":          ["disclosure", "public_statement"],
        "timeline_notification_date":        ["notification"],
    }
    resolved: dict[str, Optional[_date]] = {}
    for col, types in milestones.items():
        found = None
        for t in types:
            if t in type_to_date:
                d = type_to_date[t]
                if found is None or d < found:
                    found = d
        resolved[col] = found
        out[col] = found.isoformat() if found else ""

    def _days(a_key: str, b_key: str) -> str:
        a = resolved.get(a_key)
        b = resolved.get(b_key)
        if a and b and b >= a:
            return str((b - a).days)
        return ""

    out["days_initial_to_discovery"]    = _days("timeline_initial_access_date", "timeline_discovery_date")
    out["days_initial_to_containment"]  = _days("timeline_initial_access_date", "timeline_containment_date")
    out["days_initial_to_recovery"]     = _days("timeline_initial_access_date", "timeline_recovery_date")
    out["days_discovery_to_disclosure"] = _days("timeline_discovery_date",      "timeline_disclosure_date")
    out["days_discovery_to_containment"]= _days("timeline_discovery_date",      "timeline_containment_date")
    out["days_containment_to_recovery"] = _days("timeline_containment_date",    "timeline_recovery_date")
    return out


def _flatten_mitre(mitre_json_str: Optional[str]) -> dict:
    """
    Parse MITRE ATT&CK JSON and return flat columns with comma-separated values.

    Produces:
    - mitre_technique_ids     e.g. "T1486, T1078, T1190"
    - mitre_technique_names   e.g. "Data Encrypted for Impact, Valid Accounts, ..."
    - mitre_tactics           e.g. "Impact, Defense Evasion, Initial Access"  (deduplicated)
    - mitre_sub_techniques    e.g. "T1078.002, T1059.001"
    """
    import json as _json

    out = {
        "mitre_technique_ids": "",
        "mitre_technique_names": "",
        "mitre_tactics": "",
        "mitre_sub_techniques": "",
    }
    if not mitre_json_str:
        return out

    try:
        techniques = _json.loads(mitre_json_str)
    except (_json.JSONDecodeError, TypeError):
        return out

    if not isinstance(techniques, list):
        return out

    ids, names, tactics, subs = [], [], [], []
    seen_tactics: set[str] = set()
    _tid_re = re.compile(r"(T\d{4}(?:\.\d{3})?)")
    for t in techniques:
        if not isinstance(t, dict):
            continue
        raw_id = str(t.get("technique_id") or "").strip()
        extracted_id, extracted_name = "", ""
        if raw_id:
            # LLM sometimes writes "T1078.004: Valid Accounts: Cloud Accounts"
            m = _tid_re.match(raw_id)
            if m:
                extracted_id = m.group(1)
                # Remainder after "T####.###: " is the name
                remainder = raw_id[m.end():].lstrip(": ").strip()
                if remainder:
                    extracted_name = remainder
            else:
                extracted_id = raw_id.split(":")[0].strip()
        if extracted_id:
            ids.append(extracted_id)
        # Prefer explicit technique_name; fall back to name parsed from technique_id
        name = t.get("technique_name") or extracted_name
        if name:
            names.append(name)
        tactic = t.get("tactic")
        if tactic and tactic not in seen_tactics:
            tactics.append(tactic)
            seen_tactics.add(tactic)
        for sub in (t.get("sub_techniques") or []):
            if sub and sub not in subs:
                subs.append(sub)

    out["mitre_technique_ids"]   = ", ".join(ids)
    out["mitre_technique_names"] = ", ".join(names)
    out["mitre_tactics"]         = ", ".join(tactics)
    out["mitre_sub_techniques"]  = ", ".join(subs)
    return out


def _flatten_attack_dynamics(dynamics_json_str: Optional[str]) -> dict:
    """
    Parse attack_dynamics JSON (AttackDynamics schema) and return flat columns.

    Extracts attack_chain (ordered kill-chain/tactic stages) as a comma-separated
    string and encryption_impact as a scalar.
    """
    import json as _json

    out = {
        "attack_chain": "",
        "encryption_impact": "",
    }
    if not dynamics_json_str:
        return out

    try:
        obj = _json.loads(dynamics_json_str)
    except (_json.JSONDecodeError, TypeError):
        return out

    if not isinstance(obj, dict):
        return out

    chain = obj.get("attack_chain") or []
    if isinstance(chain, list):
        out["attack_chain"] = ", ".join(str(s) for s in chain if s)

    enc = obj.get("encryption_impact")
    if enc:
        out["encryption_impact"] = str(enc)

    return out


@router.get("/export/research-csv")
async def export_research_csv(
    _: bool = Depends(authenticate),
):
    """
    Research dataset export — every education-related enriched incident as one row.

    Joins incidents + incident_enrichments_flat + incident_sources into a single
    denormalised CSV.  Timeline, MITRE ATT&CK, and attack_dynamics JSON blobs are
    flattened into readable scalar / comma-separated columns:

    Timeline  → key event dates (initial_access_date, discovery_date, recovery_date …)
                + computed durations (days_initial_to_discovery, days_initial_to_recovery …)
    MITRE     → mitre_technique_ids, mitre_technique_names, mitre_tactics, mitre_sub_techniques
    Dynamics  → attack_chain (ordered kill-chain stages), encryption_impact

    Enriched field values take precedence over raw incident values where both exist.
    """
    JSON_BLOB_COLS = {
        "timeline_json", "mitre_techniques_json",
        "llm_timeline", "llm_mitre_attack", "llm_attack_dynamics",
    }

    conn = None
    try:
        conn = get_api_connection()

        # ── Column discovery ────────────────────────────────────────────────
        inc_cols = [
            r[1] for r in conn.execute("PRAGMA table_info(incidents)").fetchall()
            if r[1] not in JSON_BLOB_COLS
        ]
        flat_cols = [
            r[1] for r in conn.execute("PRAGMA table_info(incident_enrichments_flat)").fetchall()
            if r[1] not in JSON_BLOB_COLS and r[1] != "incident_id"
        ]

        # Also fetch the raw JSON blobs for post-processing
        inc_select = ", ".join(f"i.{c}" for c in inc_cols if c != "incident_id")
        flat_select_parts = []
        for c in flat_cols:
            if c in inc_cols:
                flat_select_parts.append(f"COALESCE(ef.{c}, i.{c}) AS {c}")
            else:
                flat_select_parts.append(f"ef.{c}")
        flat_select = ", ".join(flat_select_parts)

        query = f"""
            SELECT
                i.incident_id,
                GROUP_CONCAT(DISTINCT isrc.source) AS sources,
                {inc_select},
                {flat_select},
                ef.timeline_json,
                ef.mitre_techniques_json,
                i.llm_attack_dynamics
            FROM incidents i
            INNER JOIN incident_enrichments_flat ef
                ON i.incident_id = ef.incident_id
               AND ef.is_education_related = 1
            LEFT JOIN incident_sources isrc ON i.incident_id = isrc.incident_id
            GROUP BY i.incident_id
            ORDER BY i.incident_date DESC, i.ingested_at DESC
        """

        cur = conn.execute(query)
        raw_col_names = [d[0] for d in cur.description]
        raw_rows = cur.fetchall()

        if not raw_rows:
            raise HTTPException(status_code=404, detail="No education-related enriched incidents found.")

        # ── Flatten JSON blobs and build final rows ──────────────────────────
        # Remove the raw JSON columns from output; replace with flattened versions.
        json_col_set = {"timeline_json", "mitre_techniques_json", "llm_attack_dynamics"}
        base_col_names = [c for c in raw_col_names if c not in json_col_set]

        # Determine final column set (base + flattened additions)
        sample_timeline = _flatten_timeline(None)
        sample_mitre    = _flatten_mitre(None)
        sample_dynamics = _flatten_attack_dynamics(None)
        extra_cols = list(sample_timeline) + list(sample_mitre) + list(sample_dynamics)
        final_col_names = base_col_names + extra_cols

        timeline_idx  = raw_col_names.index("timeline_json")
        mitre_idx     = raw_col_names.index("mitre_techniques_json")
        dynamics_idx  = raw_col_names.index("llm_attack_dynamics")

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(final_col_names)

        for raw_row in raw_rows:
            base_values = [
                raw_row[i] if raw_row[i] is not None else ""
                for i, c in enumerate(raw_col_names)
                if c not in json_col_set
            ]
            flat_tl  = _flatten_timeline(raw_row[timeline_idx])
            flat_mi  = _flatten_mitre(raw_row[mitre_idx])
            flat_dyn = _flatten_attack_dynamics(raw_row[dynamics_idx])
            extra_values = (
                [flat_tl[c]  for c in sample_timeline] +
                [flat_mi[c]  for c in sample_mitre] +
                [flat_dyn[c] for c in sample_dynamics]
            )
            writer.writerow(base_values + extra_values)

        filename = f"eduthreat_research_edu_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        logger.info(
            "Research CSV export: %d edu incidents, %d columns (%d base + %d flattened)",
            len(raw_rows), len(final_col_names), len(base_col_names), len(extra_cols),
        )

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Cache-Control": "no-store",
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Research CSV export failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Export failed: {e}")
    finally:
        if conn:
            conn.close()


@router.get("/export/csv/{table_name}")
async def export_table_csv(
    table_name: str,
    education_only: bool = Query(True, description="Filter to education-related incidents only"),
    _: bool = Depends(authenticate),
):
    """
    Export a database table as CSV.
    
    Available tables:
    - incidents
    - incident_enrichments_flat
    - incident_sources
    
    Args:
        table_name: Name of table to export
        education_only: If True, only export education-related incidents
    """
    allowed_tables = ["incidents", "incident_enrichments_flat", "incident_sources"]
    
    if table_name not in allowed_tables:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid table. Allowed: {allowed_tables}"
        )
    
    conn = get_api_connection()
    
    try:
        # Build query with optional education filter
        if education_only and table_name in ["incidents", "incident_enrichments_flat"]:
            if table_name == "incidents":
                query = """
                    SELECT i.* FROM incidents i
                    JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
                    WHERE ef.is_education_related = 1
                """
            else:
                query = f"SELECT * FROM {table_name} WHERE is_education_related = 1"
        else:
            query = f"SELECT * FROM {table_name}"
        
        cur = conn.execute(query)
        rows = cur.fetchall()
        columns = [description[0] for description in cur.description]
        
        # Generate CSV
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)
        
        csv_content = output.getvalue()
        
        filename = f"eduthreat_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    finally:
        conn.close()


class SchedulerStartRequest(BaseModel):
    rss_interval_hours: int = 1
    api_interval_hours: int = 6
    daily_interval_hours: int = 24
    catch_up: bool = True


@router.get("/scheduler/status")
async def get_scheduler_status(_: bool = Depends(authenticate)):
    """Get real-time intelligence pipeline scheduler status."""
    from src.edu_cti.pipeline.manager import get_pipeline_manager

    manager = get_pipeline_manager()
    return manager.get_scheduler_status()


@router.post("/scheduler/start")
async def start_scheduler(
    request: SchedulerStartRequest = SchedulerStartRequest(),
    _: bool = Depends(authenticate),
):
    """
    Start the real-time intelligence pipeline scheduler.

    Runs recurring jobs:
    - RSS feeds: every rss_interval_hours (default 1h)
    - API sources: every api_interval_hours (default 6h)
    - Daily pipeline (all sources + enrich): every daily_interval_hours (default 24h)

    On first start, runs an immediate catch-up cycle to fetch recent incidents.
    """
    from src.edu_cti.pipeline.manager import get_pipeline_manager

    manager = get_pipeline_manager()
    result = manager.start_scheduler(
        rss_interval_hours=request.rss_interval_hours,
        api_interval_hours=request.api_interval_hours,
        daily_interval_hours=request.daily_interval_hours,
        catch_up=request.catch_up,
    )

    if result["status"] == "already_running":
        raise HTTPException(
            status_code=409,
            detail=f"Scheduler already running since {result['started_at']}",
        )

    logger.info(f"Scheduler started via admin API: {result}")
    return result


@router.post("/scheduler/stop")
async def stop_scheduler(_: bool = Depends(authenticate)):
    """Stop the real-time intelligence pipeline scheduler."""
    from src.edu_cti.pipeline.manager import get_pipeline_manager

    manager = get_pipeline_manager()
    result = manager.stop_scheduler()

    if result["status"] == "not_running":
        raise HTTPException(status_code=400, detail="Scheduler is not running")

    logger.info("Scheduler stopped via admin API")
    return result


class ReEnrichRequest(BaseModel):
    before_date: str  # ISO date string, e.g. "2026-03-15"


class DeduplicateRequest(BaseModel):
    date_window_days: int = 14   # merge same victim if dates are within this many days
    name_threshold: int = 85     # fuzzy name match threshold (0-100)
    dry_run: bool = False        # if True, return what would be merged without changing DB


@router.post("/re-enrich")
async def re_enrich_incidents(
    request: ReEnrichRequest,
    _: bool = Depends(authenticate),
):
    """
    Reset enrichment for all incidents enriched before a given date.

    This reverts their LLM enrichment data so they will be picked up
    by the next enrichment run with the updated extraction schema.

    Args:
        before_date: ISO date string (e.g. "2026-03-15"). All incidents
                     enriched before this date will be reset.
    """
    from src.edu_cti.pipeline.phase2.storage.db import revert_enrichment_before_date

    conn = get_api_connection(read_only=False)
    try:
        count = revert_enrichment_before_date(conn, request.before_date)
        cache_invalidate()
        logger.info(f"Re-enrich: reverted {count} incidents enriched before {request.before_date}")
        return {
            "success": True,
            "reverted_count": count,
            "before_date": request.before_date,
            "message": f"Reverted {count} incidents. Run enrichment to re-process them.",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Re-enrichment failed: {str(e)}")
    finally:
        conn.close()


@router.post("/deduplicate")
async def deduplicate_incidents_endpoint(
    request: DeduplicateRequest,
    _: bool = Depends(authenticate),
):
    """
    Merge duplicate incidents that refer to the same victim within a date window.

    Uses fuzzy name matching + temporal proximity (default 14 days) to find
    incidents across sources that describe the same attack event.  The incident
    with the most sources / richest data is kept; duplicates are removed and
    their source attributions are transferred to the surviving incident.
    """
    conn = get_api_connection(read_only=False)
    try:
        stats = deduplicate_by_institution(
            conn,
            window_days=request.date_window_days,
            name_threshold=request.name_threshold,
            dry_run=request.dry_run,
        )
        cache_invalidate()
        return stats
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Deduplication failed: {str(e)}")
    finally:
        conn.close()


@router.post("/cleanup-unknown-institutions")
async def cleanup_unknown_institutions_endpoint(
    dry_run: bool = True,
    action: str = "reset",
    _: bool = Depends(authenticate),
):
    """
    Handle enriched incidents where no specific institution was identified.

    These are sector-wide report/trend articles (e.g. "NYS school data incidents
    rose 72%", "Hackers increasingly target school districts") that the LLM
    enriched but could not attribute to a specific victim.

    Actions (set dry_run=false to apply):
    - action=reset  (default): Strip enrichment so the pipeline re-processes them.
                    Recommended first step — roundup articles will have secondary
                    stubs created for any named schools, then the parent will be
                    auto-deleted by the new post-enrichment logic.
    - action=delete: Hard-delete immediately.  Use only after a reset+re-enrichment
                    cycle has already run (or for incidents you're sure have no
                    specific victims).
    """
    _UNKNOWN_NAMES = {
        "", "unknown", "unknown institution", "unknown school",
        "unknown university", "unnamed", "unidentified", "undisclosed",
        "n/a", "none", "redacted",
    }

    conn = get_api_connection(read_only=dry_run)
    try:
        # Find enriched incidents whose effective institution name is Unknown:
        # ef.institution_name IS NULL means the LLM found no specific victim.
        # Also include incidents where ef.institution_name is a placeholder.
        cur = conn.execute(
            """
            SELECT i.incident_id, i.title,
                   ef.institution_name,
                   i.institution_name,
                   i.victim_raw_name
            FROM incidents i
            JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
            WHERE (
                ef.institution_name IS NULL
                OR LOWER(TRIM(ef.institution_name)) IN (
                    '', 'unknown', 'unknown institution', 'unknown school',
                    'unknown university', 'unnamed', 'unidentified',
                    'undisclosed', 'n/a', 'none', 'redacted'
                )
            )
            AND (
                i.institution_name IS NULL
                OR LOWER(TRIM(i.institution_name)) IN (
                    '', 'unknown', 'unknown institution', 'unknown school',
                    'unknown university', 'unnamed', 'unidentified',
                    'undisclosed', 'n/a', 'none', 'redacted'
                )
            )
            AND (
                i.victim_raw_name IS NULL
                OR LOWER(TRIM(i.victim_raw_name)) IN (
                    '', 'unknown', 'unknown institution', 'unknown school',
                    'unknown university', 'unnamed', 'unidentified',
                    'undisclosed', 'n/a', 'none', 'redacted'
                )
            )
            ORDER BY i.ingested_at DESC
            """
        )
        rows = cur.fetchall()

        candidates = [
            {
                "incident_id": r["incident_id"],
                "title": r["title"],
                "institution_name": r["institution_name"],
            }
            for r in rows
        ]

        if dry_run:
            return {
                "success": True,
                "dry_run": True,
                "action": action,
                "found": len(candidates),
                "sample": candidates[:20],
            }

        if action == "reset":
            # Strip enrichment — pipeline will re-enrich them.
            # The new post-enrichment logic will then auto-delete true sector
            # reports and create secondary stubs for roundup articles.
            reset_count = 0
            for row in rows:
                iid = row["incident_id"]
                conn.execute("DELETE FROM incident_enrichments WHERE incident_id = ?", (iid,))
                conn.execute("DELETE FROM incident_enrichment_runs WHERE incident_id = ?", (iid,))
                conn.execute("DELETE FROM incident_enrichments_flat WHERE incident_id = ?", (iid,))
                conn.execute(
                    "UPDATE incidents SET llm_enriched = 0, llm_enriched_at = NULL, "
                    "primary_url = NULL WHERE incident_id = ?",
                    (iid,)
                )
                reset_count += 1
            conn.commit()
            cache_invalidate()
            logger.info(f"Cleanup reset: {reset_count} Unknown-institution incidents queued for re-enrichment")
            return {
                "success": True,
                "dry_run": False,
                "action": "reset",
                "reset_count": reset_count,
                "message": f"Reset {reset_count} incidents for re-enrichment. Run the enrichment pipeline — sector reports will be auto-deleted, roundup articles will create stubs for named schools.",
            }

        # action == "delete" — hard delete
        deleted = 0
        for row in rows:
            iid = row["incident_id"]
            conn.execute("DELETE FROM incident_enrichments WHERE incident_id = ?", (iid,))
            conn.execute("DELETE FROM incident_enrichment_runs WHERE incident_id = ?", (iid,))
            conn.execute("DELETE FROM incident_enrichments_flat WHERE incident_id = ?", (iid,))
            conn.execute("DELETE FROM source_events WHERE incident_id = ?", (iid,))
            conn.execute("DELETE FROM incidents WHERE incident_id = ?", (iid,))
            deleted += 1

        conn.commit()
        cache_invalidate()
        logger.info(f"Cleanup deleted: {deleted} Unknown-institution incidents")
        return {
            "success": True,
            "dry_run": False,
            "action": "delete",
            "deleted": deleted,
        }
    except Exception as e:
        if not dry_run:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {str(e)}")
    finally:
        conn.close()


@router.post("/reset-phantom-enrichments")
async def reset_phantom_enrichments_endpoint(
    _: bool = Depends(authenticate),
):
    """
    Reset incidents marked as enriched but with no actual LLM data.

    These "phantom enriched" incidents were caused by fetch failures being
    incorrectly marked as enriched. Resetting them allows the pipeline to
    retry with improved fetching (Oxylabs fallback).
    """
    from src.edu_cti.pipeline.phase2.storage.db import reset_phantom_enrichments

    conn = get_api_connection(read_only=False)
    try:
        count = reset_phantom_enrichments(conn)
        cache_invalidate()
        return {
            "success": True,
            "reset_count": count,
            "message": f"Reset {count} phantom enriched incidents. Run enrichment to re-process them.",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Reset failed: {str(e)}")
    finally:
        conn.close()


@router.get("/purge-non-education/preview")
async def purge_non_education_preview(
    _: bool = Depends(authenticate),
):
    """Diagnostic: show exact DB counts before purging."""
    conn = get_api_connection()
    try:
        counts = {}
        counts["incidents_total"] = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
        counts["incidents_enriched"] = conn.execute("SELECT COUNT(*) FROM incidents WHERE llm_enriched = 1 AND (llm_excluded IS NULL OR llm_excluded = 0)").fetchone()[0]
        counts["incidents_unenriched"] = conn.execute("SELECT COUNT(*) FROM incidents WHERE llm_enriched = 0 AND (llm_excluded IS NULL OR llm_excluded = 0)").fetchone()[0]
        counts["incidents_soft_deleted"] = conn.execute("SELECT COUNT(*) FROM incidents WHERE llm_excluded = 1").fetchone()[0]
        counts["incidents_with_llm_summary"] = conn.execute("SELECT COUNT(*) FROM incidents WHERE llm_summary IS NOT NULL AND length(llm_summary) > 10").fetchone()[0]
        counts["incidents_without_llm_summary"] = conn.execute("SELECT COUNT(*) FROM incidents WHERE llm_enriched = 1 AND (llm_summary IS NULL OR length(llm_summary) <= 10)").fetchone()[0]

        counts["enrichments_flat_total"] = conn.execute("SELECT COUNT(*) FROM incident_enrichments_flat").fetchone()[0]
        counts["enrichments_flat_edu_1"] = conn.execute("SELECT COUNT(*) FROM incident_enrichments_flat WHERE is_education_related = 1").fetchone()[0]
        counts["enrichments_flat_edu_0"] = conn.execute("SELECT COUNT(*) FROM incident_enrichments_flat WHERE is_education_related = 0").fetchone()[0]
        counts["enrichments_flat_edu_null"] = conn.execute("SELECT COUNT(*) FROM incident_enrichments_flat WHERE is_education_related IS NULL").fetchone()[0]
        counts["enrichments_latest_total"] = conn.execute("SELECT COUNT(*) FROM incident_enrichments").fetchone()[0]
        counts["enrichment_runs_total"] = conn.execute("SELECT COUNT(*) FROM incident_enrichment_runs").fetchone()[0]

        counts["orphan_enriched"] = conn.execute("""
            SELECT COUNT(*) FROM incidents i
            WHERE i.llm_enriched = 1
              AND NOT EXISTS (SELECT 1 FROM incident_enrichments_flat ef WHERE ef.incident_id = i.incident_id)
        """).fetchone()[0]

        counts["articles_total"] = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        counts["articles_successful"] = conn.execute("SELECT COUNT(*) FROM articles WHERE fetch_successful = 1").fetchone()[0]
        counts["articles_failed"] = conn.execute("SELECT COUNT(*) FROM articles WHERE fetch_successful = 0").fetchone()[0]

        # Sample 5 orphan IDs if any
        orphan_sample = conn.execute("""
            SELECT i.incident_id, i.title, i.llm_enriched,
                   CASE WHEN i.llm_summary IS NOT NULL THEN length(i.llm_summary) ELSE 0 END as summary_len
            FROM incidents i
            WHERE i.llm_enriched = 1
              AND NOT EXISTS (SELECT 1 FROM incident_enrichments_flat ef WHERE ef.incident_id = i.incident_id)
            LIMIT 5
        """).fetchall()
        counts["orphan_samples"] = [
            {"id": r[0], "title": r[1][:60] if r[1] else None, "summary_len": r[3]}
            for r in orphan_sample
        ]

        # Sample 5 non-edu enrichments_flat
        non_edu_sample = conn.execute("""
            SELECT ef.incident_id, ef.is_education_related, ef.enriched_summary
            FROM incident_enrichments_flat ef
            WHERE ef.is_education_related = 0 OR ef.is_education_related IS NULL
            LIMIT 5
        """).fetchall()
        counts["non_edu_samples"] = [
            {"id": r[0], "is_edu": r[1], "summary_len": len(r[2]) if r[2] else 0}
            for r in non_edu_sample
        ]

        return counts
    finally:
        conn.close()


@router.post("/purge-non-education")
async def purge_non_education_endpoint(
    _: bool = Depends(authenticate),
):
    """
    Delete all incidents classified as not education-related by the LLM.

    These are incidents that were scraped from broad news searches and the LLM
    determined they are not about cyberattacks on educational institutions.
    Removes them from all tables (incidents, enrichments, articles, sources).
    """
    from src.edu_cti.pipeline.phase2.storage.db import purge_non_education_incidents

    conn = get_api_connection(read_only=False)
    try:
        result = purge_non_education_incidents(conn)
        cache_invalidate()
        total = result["total_purged"]
        return {
            "success": True,
            **result,
            "message": f"Purged {total} non-education incidents ({result['non_education_purged']} non-edu, {result['orphan_purged']} orphans).",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Purge failed: {str(e)}")
    finally:
        conn.close()


@router.post("/upload-database")
async def upload_database(
    file: UploadFile = File(...),
    _: bool = Depends(authenticate),
):
    """
    Upload a database file to replace the current database.

    This allows you to upload your local database file to persistent storage.
    The uploaded file will replace the existing database.
    
    WARNING: This will replace the existing database. Make a backup first!
    """
    from pathlib import Path
    import shutil
    
    if not file.filename.endswith('.db'):
        raise HTTPException(
            status_code=400,
            detail="File must be a .db file (SQLite database)"
        )
    
    # Use DATA_DIR from config (auto-detects Railway vs local)
    dest_dir = PERSISTENT_DATA_DIR
    dest_db = DB_PATH
    backup_db = dest_dir / f"eduthreat.db.backup.{int(datetime.now().timestamp())}"
    
    try:
        # Create destination directory
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        # Backup existing database if it exists
        if dest_db.exists():
            logger.info(f"Backing up existing database to {backup_db}")
            shutil.copy2(dest_db, backup_db)
        
        # Save uploaded file
        logger.info(f"Uploading database file: {file.filename}")
        
        with open(dest_db, "wb") as f:
            content = await file.read()
            f.write(content)
        
        # Verify the uploaded database
        conn = get_api_connection()
        try:
            cur = conn.execute("SELECT COUNT(*) FROM incidents")
            incident_count = cur.fetchone()[0]
            
            cur = conn.execute("SELECT COUNT(*) FROM incident_enrichments_flat WHERE is_education_related = 1")
            enriched_count = cur.fetchone()[0]
            
            db_size = dest_db.stat().st_size / (1024 * 1024)  # MB
            conn.close()
            
            logger.info(f"Database uploaded: {incident_count} incidents, {enriched_count} enriched, {db_size:.2f} MB")
            
            return {
                "success": True,
                "message": "Database uploaded successfully",
                "incident_count": incident_count,
                "enriched_count": enriched_count,
                "db_size_mb": round(db_size, 2),
                "backup_location": str(backup_db) if dest_db.exists() and backup_db.exists() else None,
                "destination": str(dest_db),
            }
        except Exception as e:
            # Restore backup if verification failed
            if backup_db.exists():
                logger.warning(f"Uploaded database verification failed, restoring backup: {e}")
                shutil.copy2(backup_db, dest_db)
            
            conn.close()
            raise HTTPException(
                status_code=400,
                detail=f"Uploaded file is not a valid database: {str(e)}"
            )
            
    except Exception as e:
        logger.error(f"Database upload failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {str(e)}"
        )


@router.post("/scheduler/trigger/{job_type}")
async def trigger_scheduler_job(
    job_type: str,
    _: bool = Depends(authenticate),
):
    """
    Manually trigger a scheduler job (runs in background via PipelineManager).

    Job types:
    - rss: Run RSS feed ingestion
    - weekly: Run weekly full ingestion (curated + news)
    - enrich: Run LLM enrichment

    Returns immediately with a run_id. Poll /admin/pipeline/status or
    connect to /admin/pipeline/logs/stream for progress.
    """
    from src.edu_cti.pipeline.manager import get_pipeline_manager

    job_to_phase = {
        "rss": "rss",
        "weekly": "weekly",
        "enrich": "enrich",
    }

    if job_type not in job_to_phase:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid job type. Allowed: {list(job_to_phase.keys())}",
        )

    manager = get_pipeline_manager()

    if manager.is_running:
        current = manager.current_run
        raise HTTPException(
            status_code=409,
            detail=f"Pipeline already running: {current.phase} (run_id={current.run_id})",
        )

    phase = job_to_phase[job_type]
    params = {}
    if job_type == "enrich":
        params["limit"] = None  # No limit for manual trigger

    try:
        run = manager.start_phase(phase, params)
        logger.info(f"Scheduler job triggered: {job_type} -> run_id={run.run_id}")
        return {
            "success": True,
            "job_type": job_type,
            "run_id": run.run_id,
            "message": f"Job {job_type} started in background. Poll /admin/pipeline/status for progress.",
        }
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))


# ============================================================
# Pipeline Control Endpoints
# ============================================================

VALID_PHASES = ["ingest", "enrich", "historical", "daily", "ingest_source", "rss", "weekly"]


class PipelineStartRequest(BaseModel):
    phase: str
    params: Optional[Dict[str, Any]] = None


class PipelineRunResponse(BaseModel):
    run_id: str
    phase: str
    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    progress: Dict[str, Any] = {}
    result: Dict[str, Any] = {}
    error: Optional[str] = None
    params: Dict[str, Any] = {}


@router.post("/pipeline/start", response_model=PipelineRunResponse)
async def start_pipeline(
    request: PipelineStartRequest,
    _: bool = Depends(authenticate),
):
    """
    Start a pipeline phase in the background.

    Phases:
    - **ingest**: Run Phase 1 ingestion (all source groups)
    - **enrich**: Run Phase 2 LLM enrichment
    - **historical**: Full historical collection (2019+) then enrich
    - **daily**: Incremental ingestion + enrichment
    - **ingest_source**: Ingest a specific source group (pass group in params)
    - **rss**: RSS feed ingestion only
    - **weekly**: Weekly full ingestion (curated + news)

    Params (optional, varies by phase):
    - full_historical: bool - Full scrape vs incremental
    - groups: list[str] - Source groups to ingest
    - sources: list[str] - Specific sources within group
    - max_pages: int - Max pages per source
    - limit: int - Max incidents to enrich
    - skip_enrich: bool - Skip enrichment in historical/daily
    - group: str - For ingest_source phase
    """
    from src.edu_cti.pipeline.manager import get_pipeline_manager

    if request.phase not in VALID_PHASES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid phase '{request.phase}'. Valid: {VALID_PHASES}",
        )

    manager = get_pipeline_manager()

    if manager.is_running:
        current = manager.current_run
        raise HTTPException(
            status_code=409,
            detail={
                "message": f"Pipeline already running: {current.phase}",
                "run_id": current.run_id,
                "phase": current.phase,
                "started_at": current.started_at,
            },
        )

    try:
        run = manager.start_phase(request.phase, request.params or {})
        cache_invalidate()  # Clear cached dashboard/analytics data
        logger.info(f"Pipeline started: phase={request.phase}, run_id={run.run_id}")
        return PipelineRunResponse(
            run_id=run.run_id,
            phase=run.phase,
            status=run.status.value,
            started_at=run.started_at,
            params=run.params,
            progress=run.progress,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))


@router.get("/pipeline/status")
async def get_pipeline_status(_: bool = Depends(authenticate)):
    """
    Get current pipeline execution status.

    Returns the currently running pipeline (if any) and its progress.
    """
    from src.edu_cti.pipeline.manager import get_pipeline_manager

    manager = get_pipeline_manager()
    current = manager.current_run

    if current is None:
        return {
            "running": False,
            "current_run": None,
        }

    return {
        "running": manager.is_running,
        "current_run": current.to_dict(),
    }


@router.post("/pipeline/stop")
async def stop_pipeline(_: bool = Depends(authenticate)):
    """
    Request cancellation of the current pipeline run.

    The pipeline will stop at the next safe checkpoint (between source groups
    or enrichment batches). It will not abort mid-operation.
    """
    from src.edu_cti.pipeline.manager import get_pipeline_manager

    manager = get_pipeline_manager()

    if not manager.is_running:
        raise HTTPException(status_code=400, detail="No pipeline is currently running")

    cancelled = manager.request_cancel()
    if cancelled:
        return {
            "success": True,
            "message": "Cancel requested. Pipeline will stop at next checkpoint.",
            "run_id": manager.current_run.run_id,
        }
    raise HTTPException(status_code=400, detail="Failed to cancel pipeline")


@router.get("/pipeline/history")
async def get_pipeline_history(
    limit: int = Query(20, ge=1, le=50),
    _: bool = Depends(authenticate),
):
    """
    Get pipeline run history.

    Returns the most recent pipeline runs (up to limit).
    """
    from src.edu_cti.pipeline.manager import get_pipeline_manager

    manager = get_pipeline_manager()
    history = manager.get_history(limit=limit)
    return {"runs": history, "total": len(history)}


@router.get("/pipeline/runs/{run_id}")
async def get_pipeline_run(
    run_id: str,
    _: bool = Depends(authenticate),
):
    """Get details for a specific pipeline run, including logs."""
    from src.edu_cti.pipeline.manager import get_pipeline_manager

    manager = get_pipeline_manager()
    run = manager.get_run(run_id)

    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    return run.to_dict(include_logs=True)


@router.get("/pipeline/logs")
async def get_pipeline_logs(
    run_id: Optional[str] = Query(None, description="Specific run ID (default: current)"),
    offset: int = Query(0, ge=0, description="Log line offset"),
    limit: int = Query(200, ge=1, le=2000, description="Max lines to return"),
    _: bool = Depends(authenticate),
):
    """
    Get pipeline logs with pagination.

    If no run_id, returns logs for the current/most recent run.
    Use offset for polling: pass the last offset+count to get only new lines.
    """
    from src.edu_cti.pipeline.manager import get_pipeline_manager

    manager = get_pipeline_manager()

    if run_id:
        run = manager.get_run(run_id)
    else:
        run = manager.current_run
        if run is None and manager._history:
            run = manager._history[-1]

    if run is None:
        return {"logs": [], "total": 0, "offset": 0, "has_more": False}

    all_logs = list(run.logs)
    total = len(all_logs)
    page = all_logs[offset : offset + limit]

    return {
        "run_id": run.run_id,
        "status": run.status.value,
        "logs": page,
        "total": total,
        "offset": offset,
        "has_more": (offset + limit) < total,
    }


@router.get("/pipeline/logs/stream")
async def stream_pipeline_logs(
    run_id: Optional[str] = Query(None),
    _: bool = Depends(authenticate),
):
    """
    Server-Sent Events (SSE) endpoint for real-time pipeline log streaming.

    Connect from the frontend with EventSource:
    ```js
    const es = new EventSource('/api/admin/pipeline/logs/stream?run_id=xxx',
      { headers: { 'X-Session-Token': token } });
    es.onmessage = (e) => console.log(JSON.parse(e.data));
    ```

    Events:
    - `log`: New log line
    - `progress`: Progress update
    - `status`: Status change (completed/failed/cancelled)
    - `done`: Stream ended
    """
    import asyncio
    from starlette.responses import StreamingResponse as StarletteStreamingResponse
    from src.edu_cti.pipeline.manager import get_pipeline_manager

    manager = get_pipeline_manager()

    if run_id:
        run = manager.get_run(run_id)
    else:
        run = manager.current_run

    if run is None:
        raise HTTPException(status_code=404, detail="No active pipeline run")

    async def event_generator():
        import json

        last_log_idx = 0
        last_status = run.status.value
        last_progress = dict(run.progress)

        # Send initial state
        yield f"data: {json.dumps({'type': 'status', 'status': run.status.value, 'phase': run.phase, 'run_id': run.run_id})}\n\n"

        while True:
            # Send new log lines
            current_logs = list(run.logs)
            if len(current_logs) > last_log_idx:
                for line in current_logs[last_log_idx:]:
                    yield f"data: {json.dumps({'type': 'log', 'line': line})}\n\n"
                last_log_idx = len(current_logs)

            # Send progress updates
            current_progress = dict(run.progress)
            if current_progress != last_progress:
                yield f"data: {json.dumps({'type': 'progress', **current_progress})}\n\n"
                last_progress = current_progress

            # Send status changes
            current_status = run.status.value
            if current_status != last_status:
                yield f"data: {json.dumps({'type': 'status', 'status': current_status})}\n\n"
                last_status = current_status

            # End stream when run is done
            if run.status.value in ("completed", "failed", "cancelled"):
                yield f"data: {json.dumps({'type': 'done', 'status': current_status, 'duration': run.duration_seconds, 'result': run.result, 'error': run.error})}\n\n"
                break

            await asyncio.sleep(0.5)

    return StarletteStreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ============================================================
# Incident Management Endpoints
# ============================================================

class IncidentBrief(BaseModel):
    incident_id: str
    institution_name: Optional[str] = None
    country: Optional[str] = None
    incident_date: Optional[str] = None
    attack_type_hint: Optional[str] = None
    title: Optional[str] = None
    sources: Optional[str] = None
    ingested_at: Optional[str] = None
    llm_enriched: bool = False


@router.get("/incidents/unenriched")
async def list_unenriched_incidents(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    search: Optional[str] = Query(None),
    _: bool = Depends(authenticate),
):
    """List unenriched incidents with pagination."""
    conn = get_api_connection()
    try:
        where = "WHERE i.llm_enriched = 0 OR i.llm_enriched IS NULL"
        params: list = []
        if search:
            where += " AND (i.institution_name LIKE ? OR i.title LIKE ? OR i.country LIKE ?)"
            params.extend([f"%{search}%"] * 3)

        count_q = f"SELECT COUNT(*) FROM incidents i {where}"
        cur = conn.execute(count_q, params)
        total = cur.fetchone()[0]

        query = f"""
            SELECT i.incident_id, i.institution_name, i.country, i.incident_date,
                   i.attack_type_hint, i.title, i.ingested_at, i.llm_enriched,
                   GROUP_CONCAT(DISTINCT isrc.source) as sources
            FROM incidents i
            LEFT JOIN incident_sources isrc ON i.incident_id = isrc.incident_id
            {where}
            GROUP BY i.incident_id
            ORDER BY i.ingested_at DESC
            LIMIT ? OFFSET ?
        """
        params.extend([per_page, (page - 1) * per_page])
        cur = conn.execute(query, params)
        rows = cur.fetchall()

        incidents = [
            {
                "incident_id": r["incident_id"],
                "institution_name": r["institution_name"],
                "country": r["country"],
                "incident_date": r["incident_date"],
                "attack_type_hint": r["attack_type_hint"],
                "title": r["title"],
                "sources": r["sources"],
                "ingested_at": r["ingested_at"],
                "llm_enriched": bool(r["llm_enriched"]),
            }
            for r in rows
        ]

        return {
            "incidents": incidents,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": (total + per_page - 1) // per_page,
        }
    finally:
        conn.close()


@router.get("/incidents/enriched")
async def list_enriched_incidents(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    search: Optional[str] = Query(None),
    _: bool = Depends(authenticate),
):
    """List enriched incidents with pagination."""
    conn = get_api_connection()
    try:
        where = "WHERE i.llm_enriched = 1"
        params: list = []
        if search:
            where += " AND (i.institution_name LIKE ? OR i.title LIKE ? OR i.country LIKE ?)"
            params.extend([f"%{search}%"] * 3)

        count_q = f"SELECT COUNT(*) FROM incidents i {where}"
        cur = conn.execute(count_q, params)
        total = cur.fetchone()[0]

        query = f"""
            SELECT i.incident_id,
                   COALESCE(ef.institution_name, i.institution_name) AS institution_name,
                   COALESCE(ef.country, i.country) AS country,
                   i.incident_date,
                   i.attack_type_hint, i.title, i.ingested_at, i.llm_enriched,
                   ef.attack_category, ef.ransomware_family, ef.threat_actor_name,
                   ef.is_education_related,
                   GROUP_CONCAT(DISTINCT isrc.source) as sources
            FROM incidents i
            LEFT JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
            LEFT JOIN incident_sources isrc ON i.incident_id = isrc.incident_id
            {where}
            GROUP BY i.incident_id
            ORDER BY i.ingested_at DESC
            LIMIT ? OFFSET ?
        """
        params.extend([per_page, (page - 1) * per_page])
        cur = conn.execute(query, params)
        rows = cur.fetchall()

        incidents = [
            {
                "incident_id": r["incident_id"],
                "institution_name": r["institution_name"],
                "country": r["country"],
                "incident_date": r["incident_date"],
                "attack_type_hint": r["attack_type_hint"],
                "title": r["title"],
                "sources": r["sources"],
                "ingested_at": r["ingested_at"],
                "llm_enriched": True,
                "attack_category": r["attack_category"],
                "ransomware_family": r["ransomware_family"],
                "threat_actor_name": r["threat_actor_name"],
                "is_education_related": bool(r["is_education_related"]) if r["is_education_related"] is not None else None,
            }
            for r in rows
        ]

        return {
            "incidents": incidents,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": (total + per_page - 1) // per_page,
        }
    finally:
        conn.close()


class DeleteIncidentsRequest(BaseModel):
    incident_ids: List[str]


@router.post("/incidents/delete")
async def delete_incidents(
    request: DeleteIncidentsRequest,
    _: bool = Depends(authenticate),
):
    """
    Delete specific incidents by ID.

    Removes the incident and all related data (sources, enrichments, articles).
    """
    if not request.incident_ids:
        raise HTTPException(status_code=400, detail="No incident IDs provided")

    conn = get_api_connection(read_only=False)
    try:
        placeholders = ",".join(["?"] * len(request.incident_ids))
        ids = request.incident_ids

        # Delete from all related tables
        tables = [
            "incident_enrichments_flat",
            "incident_enrichments",
            "incident_enrichment_runs",
            "incident_sources",
            "source_events",
            "articles",
        ]
        deleted_counts = {}
        for table in tables:
            try:
                cur = conn.execute(
                    f"DELETE FROM {table} WHERE incident_id IN ({placeholders})", ids
                )
                deleted_counts[table] = cur.rowcount
            except Exception:
                deleted_counts[table] = 0

        # Delete incidents themselves
        cur = conn.execute(
            f"DELETE FROM incidents WHERE incident_id IN ({placeholders})", ids
        )
        deleted_counts["incidents"] = cur.rowcount

        conn.commit()

        logger.info(f"Deleted {deleted_counts['incidents']} incidents: {ids[:5]}{'...' if len(ids) > 5 else ''}")
        cache_invalidate()  # Clear cached dashboard/analytics data

        return {
            "success": True,
            "deleted": deleted_counts["incidents"],
            "details": deleted_counts,
            "message": f"Deleted {deleted_counts['incidents']} incident(s) and related data",
        }
    except Exception as e:
        logger.error(f"Delete incidents failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")
    finally:
        conn.close()


@router.get("/incidents/soft-deleted")
async def list_soft_deleted_incidents(
    limit: int = 200,
    _: bool = Depends(authenticate),
):
    """
    List all soft-deleted (llm_excluded=1) incidents for review.
    These were classified as not-education-related by the LLM but kept in
    the DB rather than hard-deleted, so they can be reviewed and restored.
    """
    conn = get_api_connection()
    try:
        rows = conn.execute(
            """
            SELECT incident_id, institution_name, victim_raw_name, incident_date,
                   llm_excluded_reason, llm_enriched_at, ingested_at
            FROM incidents
            WHERE llm_excluded = 1
            ORDER BY llm_enriched_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return {
            "count": len(rows),
            "incidents": [dict(r) for r in rows],
        }
    finally:
        conn.close()


@router.post("/incidents/restore")
async def restore_soft_deleted_incidents(
    request: DeleteIncidentsRequest,
    _: bool = Depends(authenticate),
):
    """
    Restore soft-deleted incidents: clear llm_excluded flag and reset
    llm_enriched=0 so Phase 2 will re-enrich them with fresh articles.

    Pass incident_ids=[] to restore ALL soft-deleted incidents.
    """
    conn = get_api_connection(read_only=False)
    try:
        if request.incident_ids:
            placeholders = ",".join(["?"] * len(request.incident_ids))
            rows = conn.execute(
                f"SELECT incident_id FROM incidents WHERE llm_excluded = 1 AND incident_id IN ({placeholders})",
                request.incident_ids,
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT incident_id FROM incidents WHERE llm_excluded = 1"
            ).fetchall()

        ids = [r["incident_id"] for r in rows]
        if not ids:
            return {"success": True, "restored": 0, "message": "No soft-deleted incidents found"}

        placeholders = ",".join(["?"] * len(ids))
        conn.execute(
            f"""
            UPDATE incidents
            SET llm_excluded = 0,
                llm_excluded_reason = NULL,
                llm_enriched = 0,
                llm_enriched_at = NULL
            WHERE incident_id IN ({placeholders})
            """,
            ids,
        )
        conn.commit()
        cache_invalidate()
        logger.info(f"Restored {len(ids)} soft-deleted incidents for re-enrichment")
        return {
            "success": True,
            "restored": len(ids),
            "message": f"Restored {len(ids)} incident(s). Phase 2 will re-enrich them with fresh articles on the next run.",
        }
    except Exception as e:
        logger.error(f"Restore incidents failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Restore failed: {str(e)}")
    finally:
        conn.close()


@router.post("/incidents/clear-all")
async def clear_all_incidents(
    _: bool = Depends(authenticate),
):
    """
    Delete ALL incidents and related data. Resets the database to empty state.

    WARNING: This is irreversible. Make a backup first!
    """
    conn = get_api_connection(read_only=False)
    try:
        tables = [
            # Enrichment / article data
            "incident_enrichments_flat",
            "incident_enrichments",
            "incident_enrichment_runs",
            "pipeline_checkpoint",  # resume-tracking — must clear for fresh run
            "incident_sources",
            "source_events",
            "articles",
            "incidents",
            # Pipeline / source bookkeeping
            "source_state",      # last_pubdate per RSS source — CRITICAL for re-ingestion
            "pipeline_runs",
            # Metrics — cleared alongside incidents so counters reflect the new dataset
            "pipeline_metrics",
        ]
        deleted_counts = {}
        for table in tables:
            try:
                cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                conn.execute(f"DELETE FROM {table}")
                deleted_counts[table] = count
            except Exception:
                deleted_counts[table] = 0

        conn.commit()

        # Vacuum to reclaim space
        conn.execute("VACUUM")

        # Reset in-memory metrics and the dedicated metrics store so /metrics
        # reflects the clean slate immediately and after restarts.
        metrics = get_metrics()
        metrics.reset()
        metrics.clear_persistence()

        total = sum(deleted_counts.values())
        cache_invalidate()  # Clear cached dashboard/analytics data
        logger.warning(f"CLEARED ALL DATA: {deleted_counts}")

        return {
            "success": True,
            "total_deleted": total,
            "details": deleted_counts,
            "message": f"Cleared all data from database. {deleted_counts.get('incidents', 0)} incidents removed.",
        }
    except Exception as e:
        logger.error(f"Clear all failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Clear all failed: {str(e)}")
    finally:
        conn.close()

@router.post("/repair-date-corruption")
async def repair_date_corruption_endpoint(
    dry_run: bool = True,
    re_enrich: bool = False,
    forward_threshold_days: int = 90,
    _: bool = Depends(authenticate),
):
    """
    Find and fix incidents where the LLM incorrectly overrode incident_date with a date
    significantly later than source_published_date (e.g. a Chinese mirror/repost date).

    Also fixes primary_url when it points to a SERP-discovered URL not in all_urls,
    replacing it with the first real source URL from all_urls.

    Parameters:
    - dry_run: if true (default), only report what would be changed.
    - re_enrich: if true, mark date-fixed incidents llm_enriched=0 so they re-process.
    - forward_threshold_days: days gap that flags a date as corrupted (default 90).
    """
    import json as _json
    from datetime import date as _date

    conn = get_api_connection(read_only=False)
    try:
        # --- Find date-corrupted incidents ---
        date_rows = conn.execute(
            """
            SELECT incident_id, incident_date, source_published_date, primary_url, all_urls
            FROM incidents
            WHERE incident_date IS NOT NULL
              AND source_published_date IS NOT NULL
              AND julianday(substr(incident_date, 1, 10))
                  - julianday(substr(source_published_date, 1, 10)) > ?
            ORDER BY (
                julianday(substr(incident_date, 1, 10))
                - julianday(substr(source_published_date, 1, 10))
            ) DESC
            """,
            (forward_threshold_days,),
        ).fetchall()

        date_fixes = []
        for row in date_rows:
            iid = row["incident_id"]
            old_date = row["incident_date"]
            new_date = row["source_published_date"]
            try:
                gap = (
                    _date.fromisoformat(str(old_date)[:10])
                    - _date.fromisoformat(str(new_date)[:10])
                ).days
            except (ValueError, TypeError):
                gap = None
            date_fixes.append({
                "incident_id": iid,
                "old_incident_date": old_date,
                "new_incident_date": new_date,
                "gap_days": gap,
            })
            if not dry_run:
                fields = "incident_date = ?, date_precision = 'approximate'"
                params: list = [new_date]
                if re_enrich:
                    fields += ", llm_enriched = 0, llm_enriched_at = NULL"
                conn.execute(
                    f"UPDATE incidents SET {fields} WHERE incident_id = ?",
                    (*params, iid),
                )

        # --- Find wrong primary_url incidents ---
        url_rows = conn.execute(
            """
            SELECT incident_id, primary_url, all_urls
            FROM incidents
            WHERE primary_url IS NOT NULL
              AND all_urls IS NOT NULL
              AND all_urls != '[]'
              AND all_urls != ''
            """
        ).fetchall()

        url_fixes = []
        for row in url_rows:
            iid = row["incident_id"]
            try:
                urls = _json.loads(row["all_urls"])
            except (_json.JSONDecodeError, TypeError):
                continue
            if urls and row["primary_url"] not in urls:
                correct_url = urls[0]
                url_fixes.append({
                    "incident_id": iid,
                    "old_primary_url": row["primary_url"],
                    "new_primary_url": correct_url,
                })
                if not dry_run:
                    conn.execute(
                        "UPDATE incidents SET primary_url = ? WHERE incident_id = ?",
                        (correct_url, iid),
                    )

        if not dry_run:
            conn.commit()
            cache_invalidate()

        logger.info(
            "repair-date-corruption: dry_run=%s date_fixes=%d url_fixes=%d re_enrich=%s",
            dry_run, len(date_fixes), len(url_fixes), re_enrich,
        )
        return {
            "dry_run": dry_run,
            "forward_threshold_days": forward_threshold_days,
            "date_corrupted_count": len(date_fixes),
            "url_corrupted_count": len(url_fixes),
            "date_fixes": date_fixes,
            "url_fixes": url_fixes,
            "re_enrich_applied": re_enrich and not dry_run,
            "message": (
                f"Would fix {len(date_fixes)} date(s) and {len(url_fixes)} primary_url(s)."
                if dry_run else
                f"Fixed {len(date_fixes)} date(s) and {len(url_fixes)} primary_url(s)."
            ),
        }
    except Exception as e:
        if not dry_run:
            conn.rollback()
        logger.error(f"repair-date-corruption failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# =============================================================================
# DATA QUALITY: manual review queue + inline edit
# =============================================================================

import json as _json


class IncidentEditRequest(BaseModel):
    """Fields the admin can override. Any field set to a non-None value here
    becomes the new value; any field present in this payload is added to
    manually_edited_fields so re-enrichment will not overwrite it."""
    institution_name: Optional[str] = None
    incident_date: Optional[str] = None
    source_published_date: Optional[str] = None
    discovery_date: Optional[str] = None
    country: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None


@router.get("/manual-review-queue")
async def manual_review_queue(
    limit: int = Query(100, ge=1, le=500),
    _: bool = Depends(authenticate),
):
    """
    List incidents flagged for manual review (data-quality sweeper exhausted
    the re-enrich retries). Returns the fields most commonly wrong so the
    admin can edit inline.
    """
    conn = get_api_connection()
    try:
        cur = conn.execute(
            """
            SELECT
                incident_id,
                institution_name,
                title,
                incident_date,
                source_published_date,
                discovery_date,
                country,
                region,
                city,
                primary_url,
                manual_review_reason,
                re_enrich_attempts,
                manually_edited,
                manually_edited_fields
            FROM incidents
            WHERE manual_review_required = 1
              AND COALESCE(llm_excluded, 0) = 0
            ORDER BY ingested_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            try:
                r["manually_edited_fields"] = (
                    _json.loads(r["manually_edited_fields"])
                    if r.get("manually_edited_fields") else []
                )
            except (TypeError, _json.JSONDecodeError):
                r["manually_edited_fields"] = []
        return {"incidents": rows, "total": len(rows)}
    finally:
        conn.close()


@router.patch("/incidents/{incident_id}")
async def edit_incident(
    incident_id: str,
    payload: IncidentEditRequest,
    _: bool = Depends(authenticate),
):
    """
    Manually edit an incident. Fields in the payload (non-None) are written
    and added to manually_edited_fields so subsequent re-enrichment runs
    will not overwrite them. Clears manual_review_required when applied.
    """
    edits = {k: v for k, v in payload.model_dump().items() if v is not None}
    if not edits:
        raise HTTPException(status_code=400, detail="No fields provided")

    # Date sanity-check anything the admin types in.
    from src.edu_cti.pipeline.data_quality import is_safe_date as _safe_date
    for date_field in ("incident_date", "source_published_date", "discovery_date"):
        if date_field in edits and not _safe_date(edits[date_field]):
            raise HTTPException(
                status_code=400,
                detail=f"{date_field}={edits[date_field]!r} is not a valid YYYY-MM-DD in [1990, today+3d]",
            )

    conn = get_api_connection()
    try:
        cur = conn.execute(
            "SELECT manually_edited_fields FROM incidents WHERE incident_id = ?",
            (incident_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Incident not found")

        existing_locked: List[str] = []
        if row["manually_edited_fields"]:
            try:
                parsed = _json.loads(row["manually_edited_fields"])
                if isinstance(parsed, list):
                    existing_locked = [str(f) for f in parsed]
            except (TypeError, _json.JSONDecodeError):
                pass
        new_locked = sorted(set(existing_locked) | set(edits.keys()))

        set_clauses = ", ".join(f"{k} = ?" for k in edits.keys())
        params = list(edits.values()) + [
            _json.dumps(new_locked),
            incident_id,
        ]
        conn.execute(
            f"""
            UPDATE incidents
            SET {set_clauses},
                manually_edited = 1,
                manually_edited_fields = ?,
                manual_review_required = 0,
                manual_review_reason = NULL
            WHERE incident_id = ?
            """,
            params,
        )
        conn.commit()
        cache_invalidate()
        logger.info(
            "Manual edit on %s: fields=%s (now locked: %s)",
            incident_id, list(edits.keys()), new_locked,
        )
        return {
            "incident_id": incident_id,
            "edited_fields": list(edits.keys()),
            "locked_fields": new_locked,
        }
    finally:
        conn.close()


@router.post("/data-quality/sweep-now")
async def trigger_data_quality_sweep(_: bool = Depends(authenticate)):
    """Run the data-quality sweep on demand (don't wait for the 6h cron)."""
    from src.edu_cti.pipeline.data_quality import sweep_invalid_data
    conn = get_api_connection()
    try:
        result = sweep_invalid_data(conn)
        return result
    finally:
        conn.close()
