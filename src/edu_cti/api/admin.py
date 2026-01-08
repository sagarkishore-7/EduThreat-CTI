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

from fastapi import APIRouter, HTTPException, Depends, Header, Response, UploadFile, File
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from src.edu_cti.core.config import DB_PATH, DATA_DIR
from src.edu_cti.api.database import get_api_connection

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


@router.post("/upload-database")
async def upload_database(
    file: UploadFile = File(...),
    _: bool = Depends(authenticate),
):
    """
    Upload a database file to replace the current database.
    
    This allows you to upload your local database file to Railway persistent storage.
    The uploaded file will replace the existing database at /app/data/eduthreat.db
    
    WARNING: This will replace the existing database. Make a backup first!
    """
    from pathlib import Path
    import shutil
    
    if not file.filename.endswith('.db'):
        raise HTTPException(
            status_code=400,
            detail="File must be a .db file (SQLite database)"
        )
    
    dest_dir = Path("/app/data")
    dest_db = dest_dir / "eduthreat.db"
    backup_db = dest_dir / f"eduthreat.db.backup.{int(datetime.now().timestamp())}"
    
    try:
        # Create destination directory
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        # Backup existing database if it exists
        if dest_db.exists():
            logger.info(f"Backing up existing database to {backup_db}")
            print(f"[UPLOAD] Backing up existing database...", flush=True)
            shutil.copy2(dest_db, backup_db)
        
        # Save uploaded file
        logger.info(f"Uploading database file: {file.filename}")
        print(f"[UPLOAD] Uploading database file: {file.filename}", flush=True)
        
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
            print(f"[UPLOAD] ✓ Database uploaded successfully", flush=True)
            print(f"[UPLOAD]   Incidents: {incident_count}", flush=True)
            print(f"[UPLOAD]   Enriched (education): {enriched_count}", flush=True)
            print(f"[UPLOAD]   Size: {db_size:.2f} MB", flush=True)
            
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
                print(f"[UPLOAD] ✗ Database verification failed, restoring backup", flush=True)
                shutil.copy2(backup_db, dest_db)
            
            conn.close()
            raise HTTPException(
                status_code=400,
                detail=f"Uploaded file is not a valid database: {str(e)}"
            )
            
    except Exception as e:
        logger.error(f"Database upload failed: {e}", exc_info=True)
        print(f"[UPLOAD] ✗ Failed: {e}", flush=True)
        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {str(e)}"
        )


@router.post("/migrate-db")
async def migrate_database_endpoint(_: bool = Depends(authenticate)):
    """
    Migrate database from repo to Railway persistent storage.
    
    This copies data/eduthreat.db to /app/data/eduthreat.db if it exists.
    If source doesn't exist, initializes a fresh database.
    On Railway, if volume is mounted at /app/data, data/eduthreat.db may resolve to the same location.
    """
    import shutil
    from pathlib import Path
    from src.edu_cti.core.db import init_db
    from src.edu_cti.pipeline.phase2.storage.db import init_incident_enrichments_table
    
    # Possible source locations (in order of preference)
    possible_sources = [
        Path("data/eduthreat.db"),  # Relative path
        Path("/app/data/eduthreat.db"),  # Absolute path (Railway volume)
        Path("../data/eduthreat.db"),  # If running from different directory
    ]
    
    dest_dir = Path("/app/data")
    dest_db = dest_dir / "eduthreat.db"
    
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
                    print(f"[MIGRATION] ✓ Database already at destination: {dest_db}", flush=True)
                    
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
            print(f"[MIGRATION] Copying database from {source_db} to {dest_db}", flush=True)
            
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
                print(f"[MIGRATION] Database already exists at {dest_db}", flush=True)
                
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
                print(f"[MIGRATION] Initializing fresh database at {dest_db}", flush=True)
                
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
        print(f"[MIGRATION] ✗ Failed: {e}", flush=True)
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
        
        logger.info(f"[ADMIN] Triggering scheduler job: {job_type}")
        print(f"[ADMIN] Triggering scheduler job: {job_type}", flush=True)
        
        scheduler = IngestionScheduler(enable_enrichment=True)
        
        if job_type == "rss":
            logger.info("[ADMIN] Starting RSS ingestion...")
            print("[ADMIN] Starting RSS ingestion...", flush=True)
            scheduler._run_rss_ingestion()
        elif job_type == "weekly":
            logger.info("[ADMIN] Starting weekly ingestion...")
            print("[ADMIN] Starting weekly ingestion...", flush=True)
            scheduler._run_weekly_ingestion()
        elif job_type == "enrich":
            logger.info("[ADMIN] Starting LLM enrichment...")
            print("[ADMIN] Starting LLM enrichment...", flush=True)
            scheduler._run_enrichment()
        
        duration = stop_timer(f"scheduler_job_{job_type}")
        metrics.increment(f"scheduler_job_{job_type}_total", labels={"status": "success"})
        
        # Get captured logs
        log_output = log_capture.getvalue()
        
        logger.info(f"[ADMIN] Job {job_type} completed in {duration:.2f}s")
        print(f"[ADMIN] Job {job_type} completed in {duration:.2f}s", flush=True)
        
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
        
        logger.error(f"[ADMIN] Job {job_type} failed: {e}", exc_info=True)
        print(f"[ADMIN] Job {job_type} failed: {e}", flush=True)
        
        log_output = log_capture.getvalue()
        
        raise HTTPException(
            status_code=500,
            detail=f"Job failed: {str(e)}",
        )
    finally:
        root_logger.removeHandler(handler)