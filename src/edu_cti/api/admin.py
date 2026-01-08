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
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from pathlib import Path

from fastapi import APIRouter, HTTPException, Depends, Header, Response
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from src.edu_cti.core.config import DB_PATH, DATA_DIR
from src.edu_cti.api.database import get_api_connection

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


@router.get("/export/csv/{table_name}")
async def export_table_csv(
    table_name: str,
    education_only: bool = True,
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


@router.get("/export/csv/full")
async def export_full_csv(
    education_only: bool = True,
    _: bool = Depends(authenticate),
):
    """
    Export full enriched dataset as CSV.
    
    Joins incidents with enrichments for complete data.
    """
    from src.edu_cti.pipeline.phase2.csv_export import load_enriched_incidents_from_db
    
    conn = get_api_connection()
    
    try:
        incidents = load_enriched_incidents_from_db(conn, use_flat_table=True)
        
        if education_only:
            incidents = [i for i in incidents if i.get("is_education_related")]
        
        if not incidents:
            raise HTTPException(status_code=404, detail="No incidents found")
        
        # Generate CSV
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=incidents[0].keys())
        writer.writeheader()
        writer.writerows(incidents)
        
        csv_content = output.getvalue()
        
        filename = f"eduthreat_full_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    finally:
        conn.close()


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
    allowed_jobs = ["rss", "weekly", "enrich"]
    
    if job_type not in allowed_jobs:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid job type. Allowed: {allowed_jobs}"
        )
    
    try:
        from src.edu_cti.scheduler.scheduler import IngestionScheduler
        
        scheduler = IngestionScheduler(enable_enrichment=True)
        
        if job_type == "rss":
            scheduler._run_rss_ingestion()
        elif job_type == "weekly":
            scheduler._run_weekly_ingestion()
        elif job_type == "enrich":
            scheduler._run_enrichment()
        
        return {
            "success": True,
            "job_type": job_type,
            "message": f"Job {job_type} completed",
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Job failed: {str(e)}"
        )
