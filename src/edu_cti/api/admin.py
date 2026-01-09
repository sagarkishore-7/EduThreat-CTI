"""
Admin API endpoints for database management and export.

These endpoints require authentication and provide:
- Database export (full DB file download)
- CSV export of tables
- Scheduler status and control
"""

import os
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
from src.edu_cti.api.database import get_api_connection

# Use DATA_DIR from config (auto-detects Railway)
PERSISTENT_DATA_DIR = DATA_DIR

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])

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
        # If no hash configured, use default for development
        default_hash = hash_password("admin123")  # Change in production!
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
        cur = conn.execute("SELECT COUNT(*) FROM incident_enrichments_flat WHERE is_education_related = 1")
        education = cur.fetchone()[0]
        
        # Total sources
        cur = conn.execute("SELECT COUNT(*) FROM incident_sources")
        sources = cur.fetchone()[0]
        
        # DB size
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
    logger.debug(f"Full CSV export called (education_only={education_only_bool})")
    
    try:
        from src.edu_cti.core.db import load_incident_by_id
        from src.edu_cti.pipeline.phase2.csv_export import load_enriched_incidents_from_db
        from src.edu_cti.core.deduplication import extract_urls_from_incident
    except ImportError as e:
        logger.error(f"Import error: {str(e)[:100]}")
        logger.error(f"Import error: {str(e)[:100]}")
        raise HTTPException(status_code=500, detail=f"Import error: {str(e)}")
    
    conn = None
    try:
        logger.info(f"Starting full CSV export (education_only={education_only_bool})")
        logger.info(f"Starting full CSV export (education_only={education_only_bool})")
        
        conn = get_api_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Failed to get database connection")
        
        # Get all incidents (check if broken_urls column exists)
        try:
            cur = conn.execute("PRAGMA table_info(incidents)")
            columns = [row[1] for row in cur.fetchall()]
            has_broken_urls = "broken_urls" in columns
        except:
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
                "university_name": safe_get(row, "university_name"),
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
            "incident_id", "sources", "university_name", "victim_raw_name", "victim_raw_name_normalized",
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
            except:
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
            except:
                pass


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
            except:
                pass


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
    logger.debug(f"Full CSV export called (education_only={education_only_bool})")
    
    try:
        from src.edu_cti.core.db import load_incident_by_id
        from src.edu_cti.pipeline.phase2.csv_export import load_enriched_incidents_from_db
        from src.edu_cti.core.deduplication import extract_urls_from_incident
    except ImportError as e:
        logger.error(f"Import error: {str(e)[:100]}")
        logger.error(f"Import error: {str(e)[:100]}")
        raise HTTPException(status_code=500, detail=f"Import error: {str(e)}")
    
    conn = None
    try:
        logger.info(f"Starting full CSV export (education_only={education_only_bool})")
        logger.info(f"Starting full CSV export (education_only={education_only_bool})")
        
        conn = get_api_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Failed to get database connection")
        if not conn:
            raise HTTPException(status_code=500, detail="Failed to get database connection")
        
        # Get all incidents (check if broken_urls column exists)
        try:
            cur = conn.execute("PRAGMA table_info(incidents)")
            columns = [row[1] for row in cur.fetchall()]
            has_broken_urls = "broken_urls" in columns
        except:
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
                "university_name": safe_get(row, "university_name"),
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
            "incident_id", "sources", "university_name", "victim_raw_name", "victim_raw_name_normalized",
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
            except:
                pass


@router.get("/scheduler/status")
async def get_scheduler_status(_: bool = Depends(authenticate)):
    """Get scheduler status if running."""
    try:
        from src.edu_cti.scheduler import IngestionScheduler
        # This would need to access the running scheduler instance
        # For now, return a placeholder
        return {
            "status": "available",
            "message": "Scheduler module available. Use CLI to start: python -m src.edu_cti.scheduler",
        }
    except ImportError:
        return {
            "status": "unavailable",
            "message": "Scheduler module not available",
        }


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
            logger.info("Backing up existing database")
            shutil.copy2(dest_db, backup_db)
        
        # Save uploaded file
        logger.info(f"Uploading database file: {file.filename}")
        logger.info(f"Uploading database: {file.filename}")
        
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
            
            logger.info(f"Database uploaded successfully: {incident_count} incidents")
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
                logger.error("Database verification failed, restoring backup")
                shutil.copy2(backup_db, dest_db)
            
            conn.close()
            raise HTTPException(
                status_code=400,
                detail=f"Uploaded file is not a valid database: {str(e)}"
            )
            
    except Exception as e:
        logger.error(f"Database upload failed: {e}", exc_info=True)
        logger.error(f"Upload failed: {str(e)[:200]}")
        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {str(e)}"
        )


@router.post("/migrate-db")
async def migrate_database_endpoint(_: bool = Depends(authenticate)):
    """
    Migrate database from repo to persistent storage.
    
    This copies data/eduthreat.db from repository to the configured data directory.
    If source doesn't exist, initializes a fresh database.
    On Railway, uses /app/data (persistent volume).
    On local, uses ./data directory.
    """
    import shutil
    from pathlib import Path
    from src.edu_cti.core.db import init_db
    from src.edu_cti.pipeline.phase2.storage.db import init_incident_enrichments_table
    
    # Possible source locations (in order of preference)
    # Check if there's a database in the repo (for migration)
    possible_sources = [
        Path("data/eduthreat.db"),  # Relative path (local repo)
        Path("../data/eduthreat.db"),  # If running from different directory
        DB_PATH,  # Current database location (may be same as dest)
    ]
    
    # Use DATA_DIR from config (auto-detects Railway vs local)
    dest_dir = PERSISTENT_DATA_DIR
    dest_db = DB_PATH
    
    try:
        # Create destination directory
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        # Find source database
        source_db = None
        for path in possible_sources:
            if path.exists():
                source_db = path.resolve()  # Resolve to absolute path
                break
        
        # Check if source and destination are the same file (already migrated)
        if source_db and dest_db.exists():
            try:
                if source_db.samefile(dest_db):
                    # Already in the right place!
                    logger.info(f"Database already at destination: {dest_db}")
                    logger.info(f"Database already at destination: {dest_db}")
                    
                    # Verify integrity
                    conn = get_api_connection()
                    cur = conn.execute("SELECT COUNT(*) FROM incidents")
                    incident_count = cur.fetchone()[0]
                    conn.close()
                    
                    db_size = dest_db.stat().st_size / (1024 * 1024)  # MB
                    
                    return {
                        "success": True,
                        "message": "Database already in persistent storage",
                        "incident_count": incident_count,
                        "db_size_mb": round(db_size, 2),
                        "destination": str(dest_db),
                        "note": "Database is already in the correct location. No migration needed.",
                    }
            except (OSError, ValueError):
                # samefile() can fail if files don't exist or are on different filesystems
                pass
        
        # If source exists and is different from destination, copy it
        if source_db and source_db != dest_db:
            logger.info(f"Migrating database from {source_db} to {dest_db}")
            logger.info(f"Copying database from {source_db} to {dest_db}")
            
            # Get source size
            source_size = source_db.stat().st_size / (1024 * 1024)  # MB
            
            # Copy database
            shutil.copy2(source_db, dest_db)
            
            # Verify
            if not dest_db.exists():
                return {
                    "success": False,
                    "message": "Database copy failed - destination not found",
                }
            
            dest_size = dest_db.stat().st_size / (1024 * 1024)  # MB
            
            # Verify integrity
            conn = get_api_connection()
            cur = conn.execute("SELECT COUNT(*) FROM incidents")
            incident_count = cur.fetchone()[0]
            conn.close()
            
            return {
                "success": True,
                "message": "Database migrated successfully",
                "source_size_mb": round(source_size, 2),
                "dest_size_mb": round(dest_size, 2),
                "incident_count": incident_count,
                "destination": str(dest_db),
            }
        else:
            # No source found - check if destination already has data
            if dest_db.exists():
                logger.info(f"Database already exists at {dest_db}")
                logger.info(f"Database already exists at {dest_db}")
                
                conn = get_api_connection()
                cur = conn.execute("SELECT COUNT(*) FROM incidents")
                incident_count = cur.fetchone()[0]
                conn.close()
                
                db_size = dest_db.stat().st_size / (1024 * 1024)  # MB
                
                return {
                    "success": True,
                    "message": "Database already exists in persistent storage",
                    "incident_count": incident_count,
                    "db_size_mb": round(db_size, 2),
                    "destination": str(dest_db),
                    "note": "Database is already initialized. Run ingestion to populate data.",
                }
            else:
                # Initialize fresh database
                logger.info(f"Initializing fresh database at {dest_db}")
                logger.info(f"Initializing fresh database at {dest_db}")
                
                # Initialize fresh database
                conn = get_api_connection()
                init_db(conn)
                init_incident_enrichments_table(conn)
                conn.commit()
                
                cur = conn.execute("SELECT COUNT(*) FROM incidents")
                incident_count = cur.fetchone()[0]
                conn.close()
                
                return {
                    "success": True,
                    "message": "Fresh database initialized successfully",
                    "incident_count": incident_count,
                    "destination": str(dest_db),
                    "note": "New empty database created. Run historical ingestion to populate data.",
                }
    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        logger.error(f"Migration failed: {str(e)[:200]}")
        return {
            "success": False,
            "message": f"Migration failed: {str(e)}",
        }


@router.post("/scheduler/trigger/{job_type}")
async def trigger_scheduler_job(
    job_type: str,
    _: bool = Depends(authenticate),
):
    """
    Manually trigger a scheduler job.
    
    Job types:
    - rss: Run RSS feed ingestion
    - weekly: Run weekly full ingestion
    - enrich: Run LLM enrichment
    """
    import logging
    import sys
    from io import StringIO
    
    logger = logging.getLogger(__name__)
    
    allowed_jobs = ["rss", "weekly", "enrich"]
    
    if job_type not in allowed_jobs:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid job type. Allowed: {allowed_jobs}"
        )
    
    # Capture logs
    log_capture = StringIO()
    handler = logging.StreamHandler(log_capture)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    handler.setFormatter(formatter)
    
    # Add handler to root logger temporarily
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    
    try:
        from src.edu_cti.scheduler.scheduler import IngestionScheduler
        from src.edu_cti.core.metrics import get_metrics, start_timer, stop_timer
        
        metrics = get_metrics()
        start_timer(f"scheduler_job_{job_type}")
        
        logger.info(f"Triggering scheduler job: {job_type}")
        
        scheduler = IngestionScheduler(enable_enrichment=True)
        
        if job_type == "rss":
            logger.info("Starting RSS ingestion")
            scheduler._run_rss_ingestion()
        elif job_type == "weekly":
            logger.info("Starting weekly ingestion")
            scheduler._run_weekly_ingestion()
        elif job_type == "enrich":
            logger.info("Starting LLM enrichment")
            # Process all unenriched incidents when triggered manually (no limit)
            scheduler._run_enrichment(limit=None, manual_trigger=True)
        
        duration = stop_timer(f"scheduler_job_{job_type}")
        metrics.increment(f"scheduler_job_{job_type}_total", labels={"status": "success"})
        
        # Get captured logs
        log_output = log_capture.getvalue()
        
        logger.info(f"Job {job_type} completed in {duration:.2f}s")
        
        # Log metrics summary
        metrics.log_summary()
        
        return {
            "success": True,
            "job_type": job_type,
            "duration_seconds": duration,
            "message": f"Job {job_type} completed",
            "logs": log_output.split("\n")[-50:],  # Last 50 lines
        }
    except Exception as e:
        from src.edu_cti.core.metrics import get_metrics
        metrics = get_metrics()
        metrics.increment(f"scheduler_job_{job_type}_total", labels={"status": "error"})
        
        logger.error(f"Job {job_type} failed: {str(e)[:200]}")
        
        log_output = log_capture.getvalue()
        
        raise HTTPException(
            status_code=500,
            detail=f"Job failed: {str(e)}",
        )
    finally:
        root_logger.removeHandler(handler)


@router.post("/fix-incident-dates")
async def fix_incident_dates_endpoint(
    apply: bool = False,
    _: bool = Depends(authenticate),
):
    """
    Fix incident dates from timeline data.
    
    This updates incident_date for enriched incidents by extracting
    the earliest date from their timeline events.
    
    Args:
        apply: If True, actually apply changes. If False, dry-run only.
    """
    import json
    from datetime import datetime
    
    logger.info(f"Fixing incident dates (apply={apply})")
    logger.info(f"Fixing incident dates (apply={apply})")
    
    conn = get_api_connection(read_only=False)  # Need write access
    
    try:
        # Get all enriched incidents with timeline data
        cur = conn.execute("""
            SELECT 
                i.incident_id,
                i.incident_date,
                i.date_precision,
                i.source_published_date,
                e.enrichment_data
            FROM incidents i
            JOIN incident_enrichments e ON i.incident_id = e.incident_id
            WHERE i.llm_enriched = 1
            AND e.enrichment_data IS NOT NULL
        """)
        
        incidents = cur.fetchall()
        logger.info(f"Found {len(incidents)} enriched incidents to check")
        
        fixed_count = 0
        skipped_count = 0
        fixed_incidents = []
        
        def parse_date(date_str: str):
            """Parse date string to datetime."""
            try:
                return datetime.strptime(date_str, "%Y-%m-%d")
            except:
                return None
        
        for row in incidents:
            incident_id = row["incident_id"]
            current_date = row["incident_date"]
            source_pub_date = row["source_published_date"]
            enrichment_data = json.loads(row["enrichment_data"])
            
            # Get timeline from enrichment
            timeline = enrichment_data.get("timeline", [])
            if not timeline:
                skipped_count += 1
                continue
            
            # Find earliest date in timeline
            dated_events = [e for e in timeline if e.get("date")]
            if not dated_events:
                skipped_count += 1
                continue
            
            earliest_event = min(dated_events, key=lambda e: e["date"])
            timeline_date = earliest_event["date"]
            timeline_precision = earliest_event.get("date_precision") or "approximate"
            
            # Check if current date is likely a published date or if timeline date is earlier
            current_dt = parse_date(current_date) if current_date else None
            timeline_dt = parse_date(timeline_date) if timeline_date else None
            source_dt = parse_date(source_pub_date) if source_pub_date else None
            
            should_update = False
            reason = ""
            
            if current_dt and timeline_dt:
                # Update if timeline date is earlier than current date
                # OR if current date matches source published date (likely wrong)
                if timeline_dt < current_dt:
                    should_update = True
                    reason = f"Timeline date ({timeline_date}) is earlier than current date ({current_date})"
                elif current_date == source_pub_date:
                    should_update = True
                    reason = f"Current date ({current_date}) matches source published date, using timeline date ({timeline_date})"
            
            if should_update:
                if apply:
                    conn.execute("""
                        UPDATE incidents
                        SET incident_date = ?,
                            date_precision = ?
                        WHERE incident_id = ?
                    """, (timeline_date, timeline_precision, incident_id))
                    conn.commit()
                    logger.info(f"Updated {incident_id}: {current_date} -> {timeline_date}")
                else:
                    logger.debug(f"[DRY RUN] Would update {incident_id}: {current_date} -> {timeline_date}")
                
                fixed_incidents.append({
                    "incident_id": incident_id,
                    "old_date": current_date,
                    "new_date": timeline_date,
                    "reason": reason
                })
                fixed_count += 1
        
        summary = {
            "success": True,
            "apply": apply,
            "fixed": fixed_count,
            "skipped": skipped_count,
            "total_checked": len(incidents),
            "message": f"{'Fixed' if apply else 'Would fix'} {fixed_count} incidents" if fixed_count > 0 else "No incidents need fixing",
        }
        
        if fixed_count > 0 and fixed_count <= 20:  # Only include details if not too many
            summary["fixed_incidents"] = fixed_incidents
        
        logger.info(f"Date fix complete: fixed={fixed_count}, skipped={skipped_count}")
        
        return summary
        
    except Exception as e:
        logger.error(f"Date fix failed: {str(e)[:200]}")
        raise HTTPException(
            status_code=500,
            detail=f"Date fix failed: {str(e)}",
        )
    finally:
        conn.close()