# src/edu_cti/db.py
from pathlib import Path
from typing import List, Optional, Set, Tuple
from datetime import datetime
from contextlib import contextmanager

import sqlite3

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.core.config import DB_PATH
from src.edu_cti.core.deduplication import (
    extract_urls_from_incident,
    normalize_url,
    merge_incidents,
)


def get_connection(
    db_path: Path = DB_PATH,
    timeout: float = 30.0,
    read_only: bool = False,
) -> sqlite3.Connection:
    """
    Get a database connection with proper configuration for concurrent access.
    
    Args:
        db_path: Path to database file
        timeout: Connection timeout in seconds (default: 30s for writes, 5s for reads)
        read_only: If True, opens connection in read-only mode (faster, no locks)
    
    Returns:
        SQLite connection with WAL mode enabled and proper timeouts
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # For read-only connections, use URI mode with ?mode=ro
    if read_only:
        db_uri = f"file:{db_path}?mode=ro"
        conn = sqlite3.connect(db_uri, uri=True, timeout=5.0, check_same_thread=False)
    else:
        # Write connections: longer timeout for write operations
        conn = sqlite3.connect(str(db_path), timeout=timeout, check_same_thread=False)
    
    conn.row_factory = sqlite3.Row
    
    # Enable WAL (Write-Ahead Logging) mode for better concurrency
    # WAL allows multiple readers while a writer is active
    if not read_only:
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            # Optimize for concurrent access
            conn.execute("PRAGMA synchronous=NORMAL")  # Balance between safety and speed
            conn.execute("PRAGMA busy_timeout=30000")  # 30 second timeout for busy database
            # Increase cache size for better performance
            conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys=ON")
        except sqlite3.Error as e:
            # If WAL mode fails (e.g., on read-only filesystem), continue with default mode
            # This can happen on some network filesystems
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Could not enable WAL mode: {e}. Continuing with default journal mode.")
    
    return conn


@contextmanager
def db_transaction(conn: sqlite3.Connection, commit: bool = True):
    """
    Context manager for database transactions.
    
    Ensures transactions are committed or rolled back properly,
    and helps keep transactions short for better concurrency.
    
    Usage:
        with db_transaction(conn):
            conn.execute("INSERT INTO ...")
            # Automatically commits on exit
    """
    try:
        yield conn
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db(conn: sqlite3.Connection) -> None:
    """
    Create tables for deduplicated incidents + source attribution + per-source dedup + state.
    
    Schema design:
    - incidents: Deduplicated incidents (cross-source dedup applied)
    - incident_sources: Tracks which sources contributed to each incident (many-to-many)
    - source_events: Tracks per-source ingestion (prevents re-ingesting same source event)
    - source_state: Tracks source ingestion state
    
    Also enables WAL mode for better concurrent access.
    """
    # Enable WAL mode first (before creating tables)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
        conn.execute("PRAGMA foreign_keys=ON")
    except sqlite3.Error:
        # If WAL mode fails, continue with default (e.g., read-only filesystem)
        pass
    
    conn.executescript(
        """
        -- Main incidents table: Deduplicated incidents only
        CREATE TABLE IF NOT EXISTS incidents (
            incident_id          TEXT PRIMARY KEY,
            -- Note: 'source' field removed - use incident_sources table instead

            university_name      TEXT,
            victim_raw_name      TEXT,
            institution_type     TEXT,
            country              TEXT,
            region               TEXT,
            city                 TEXT,

            incident_date        TEXT,
            date_precision       TEXT,
            source_published_date TEXT,
            ingested_at          TEXT,
            last_updated_at       TEXT,  -- When last merged/updated

            title                TEXT,
            subtitle             TEXT,

            primary_url          TEXT,  -- Phase 2: LLM-selected best URL
            all_urls             TEXT,  -- Semicolon-separated URLs

            leak_site_url        TEXT,
            source_detail_url     TEXT,
            screenshot_url        TEXT,

            attack_type_hint     TEXT,
            status               TEXT,
            source_confidence    TEXT,  -- Highest confidence from sources

            notes                TEXT,
            
            -- Phase 2 fields (for LLM enrichment - reserved for future)
            llm_enriched         INTEGER DEFAULT 0,
            llm_enriched_at      TEXT,
            llm_summary          TEXT,
            llm_timeline         TEXT,
            llm_mitre_attack     TEXT,
            llm_attack_dynamics   TEXT
        );

        -- Track which sources contributed to each incident (many-to-many)
        CREATE TABLE IF NOT EXISTS incident_sources (
            incident_id      TEXT NOT NULL,
            source           TEXT NOT NULL,
            source_event_id  TEXT,
            first_seen_at    TEXT NOT NULL,
            confidence       TEXT,
            PRIMARY KEY (incident_id, source, source_event_id),
            FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE
        );

        -- Track per-source ingestion (prevents re-ingesting same source event)
        CREATE TABLE IF NOT EXISTS source_events (
            source           TEXT NOT NULL,
            source_event_id  TEXT NOT NULL,
            incident_id      TEXT NOT NULL,
            first_seen_at    TEXT NOT NULL,
            PRIMARY KEY (source, source_event_id),
            FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE
        );

        -- Source state tracking
        CREATE TABLE IF NOT EXISTS source_state (
            source       TEXT PRIMARY KEY,
            last_pubdate TEXT
        );
        
        -- Indexes for performance
        CREATE INDEX IF NOT EXISTS idx_incidents_country ON incidents(country);
        CREATE INDEX IF NOT EXISTS idx_incidents_date ON incidents(incident_date);
        CREATE INDEX IF NOT EXISTS idx_incident_sources_incident ON incident_sources(incident_id);
        CREATE INDEX IF NOT EXISTS idx_incident_sources_source ON incident_sources(source);
        """
    )
    conn.commit()


def source_event_exists(
    conn: sqlite3.Connection, source: str, source_event_id: str
) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM source_events WHERE source = ? AND source_event_id = ?",
        (source, source_event_id),
    )
    return cur.fetchone() is not None


def register_source_event(
    conn: sqlite3.Connection,
    source: str,
    source_event_id: str,
    incident_id: str,
    first_seen_at: str,
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO source_events (source, source_event_id, incident_id, first_seen_at)
        VALUES (?, ?, ?, ?)
        """,
        (source, source_event_id, incident_id, first_seen_at),
    )


def insert_incident(conn: sqlite3.Connection, incident: BaseIncident, preserve_enrichment: bool = True) -> str:
    """
    Insert a deduplicated BaseIncident into the incidents table.
    Note: 'source' field is no longer stored here - use incident_sources table.
    
    Args:
        conn: Database connection
        incident: BaseIncident to insert/update
        preserve_enrichment: If True and incident exists, preserve enrichment data from existing record
    
    Returns:
        incident_id of inserted/updated incident
    """
    data = incident.to_dict()
    now = datetime.utcnow().isoformat()
    
    # Check if incident already exists and has enrichment data
    enrichment_fields = None
    if preserve_enrichment:
        cur = conn.execute(
            "SELECT llm_enriched, llm_enriched_at, llm_summary, llm_timeline, llm_mitre_attack, llm_attack_dynamics, primary_url FROM incidents WHERE incident_id = ?",
            (incident.incident_id,)
        )
        existing = cur.fetchone()
        if existing and existing["llm_enriched"] == 1:
            # Preserve enrichment data
            enrichment_fields = {
                "llm_enriched": existing["llm_enriched"],
                "llm_enriched_at": existing["llm_enriched_at"],
                "llm_summary": existing["llm_summary"],
                "llm_timeline": existing["llm_timeline"],
                "llm_mitre_attack": existing["llm_mitre_attack"],
                "llm_attack_dynamics": existing["llm_attack_dynamics"],
                # Preserve primary_url if it was set by enrichment and new incident doesn't have it
                "primary_url": existing["primary_url"] if existing["primary_url"] and not incident.primary_url else incident.primary_url,
            }
    
    # Ensure all keys exist (even if None)
    fields = [
        "incident_id",
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
        "last_updated_at",
        "title",
        "subtitle",
        "primary_url",
        "all_urls",
        "leak_site_url",
        "source_detail_url",
        "screenshot_url",
        "attack_type_hint",
        "status",
        "source_confidence",
        "notes",
        # Enrichment fields
        "llm_enriched",
        "llm_enriched_at",
        "llm_summary",
        "llm_timeline",
        "llm_mitre_attack",
        "llm_attack_dynamics",
    ]
    
    values = [data.get(f) for f in fields[:23]]  # Base fields
    
    # Handle enrichment fields (append to values list)
    if enrichment_fields:
        # Preserve existing enrichment
        values.extend([
            enrichment_fields.get("llm_enriched", 0),
            enrichment_fields.get("llm_enriched_at"),
            enrichment_fields.get("llm_summary"),
            enrichment_fields.get("llm_timeline"),
            enrichment_fields.get("llm_mitre_attack"),
            enrichment_fields.get("llm_attack_dynamics"),
        ])
        # Update primary_url with preserved value if applicable
        primary_url_idx = fields.index("primary_url")
        if enrichment_fields.get("primary_url"):
            values[primary_url_idx] = enrichment_fields.get("primary_url")
    else:
        # No enrichment to preserve - use defaults
        values.extend([0, None, None, None, None, None])
    
    # Set last_updated_at
    values[fields.index("last_updated_at")] = now

    placeholders = ",".join("?" for _ in fields)
    field_list = ",".join(fields)

    conn.execute(
        f"""
        INSERT OR REPLACE INTO incidents ({field_list})
        VALUES ({placeholders})
        """,
        values,
    )
    return incident.incident_id


def add_incident_source(
    conn: sqlite3.Connection,
    incident_id: str,
    source: str,
    source_event_id: Optional[str],
    first_seen_at: str,
    confidence: Optional[str] = None,
) -> None:
    """
    Add a source attribution to an incident.
    This tracks which sources contributed to finding this incident.
    """
    conn.execute(
        """
        INSERT OR IGNORE INTO incident_sources 
        (incident_id, source, source_event_id, first_seen_at, confidence)
        VALUES (?, ?, ?, ?, ?)
        """,
        (incident_id, source, source_event_id, first_seen_at, confidence),
    )


def find_duplicate_incident_by_urls(
    conn: sqlite3.Connection, incident: BaseIncident
) -> Optional[Tuple[str, bool, bool]]:
    """
    Find an existing incident in the database that shares URLs with the given incident.
    This is used for cross-source deduplication.
    
    Enhanced to handle enriched incidents:
    - If enriched incident exists and all new URLs are duplicates, return (incident_id, True, False)
      where True means "is_enriched" and False means "should_drop"
    - If enriched incident exists and new URLs contain additional URLs, return (incident_id, True, True)
      where True means "should_upgrade"
    - If non-enriched incident exists, return (incident_id, False, False)
    
    Args:
        conn: Database connection
        incident: Incident to check for duplicates
        
    Returns:
        Tuple of (incident_id, is_enriched, should_upgrade_or_drop) if duplicate found,
        None otherwise. 
        - is_enriched: Whether the existing incident is enriched
        - should_upgrade_or_drop: If is_enriched=True, True means upgrade needed, False means drop.
                                  If is_enriched=False, False means normal merge.
    """
    # Extract and normalize URLs from the new incident
    new_urls = extract_urls_from_incident(incident)
    if not new_urls:
        return None
    
    new_normalized = {normalize_url(url) for url in new_urls}
    
    # Get all incidents from database with enrichment status
    cur = conn.execute("SELECT incident_id, all_urls, llm_enriched FROM incidents")
    rows = cur.fetchall()
    
    for row in rows:
        existing_incident_id = row["incident_id"]
        existing_urls_str = row["all_urls"] or ""
        is_enriched = row["llm_enriched"] == 1
        
        # Parse existing URLs
        existing_urls = [
            url.strip() for url in existing_urls_str.split(";") if url.strip()
        ]
        
        # Normalize and check for overlap
        existing_normalized = {normalize_url(url) for url in existing_urls}
        overlap = existing_normalized & new_normalized
        
        # If any URL matches, it's a potential duplicate
        if overlap:
            if is_enriched:
                # Check if all new URLs are duplicates
                if new_normalized.issubset(existing_normalized):
                    # All URLs are duplicates - should drop new incident
                    return (existing_incident_id, True, False)
                else:
                    # New URLs contain additional URLs - should upgrade
                    return (existing_incident_id, True, True)
            else:
                # Not enriched - normal merge
                return (existing_incident_id, False, False)
    
    return None


def load_incident_by_id(
    conn: sqlite3.Connection, incident_id: str
) -> Optional[BaseIncident]:
    """
    Load a single incident by ID from the database.
    """
    cur = conn.execute("SELECT * FROM incidents WHERE incident_id = ?", (incident_id,))
    row = cur.fetchone()
    
    if not row:
        return None
    
    return _row_to_incident(row)


def _row_to_incident(row) -> BaseIncident:
    """
    Convert a database row to BaseIncident object.
    """
    # Parse all_urls from semicolon-separated string
    all_urls_str = row["all_urls"] or ""
    all_urls = [url.strip() for url in all_urls_str.split(";") if url.strip()]
    
    # Handle required fields that might be None in database
    university_name = row["university_name"]
    if not university_name:
        university_name = row["victim_raw_name"] or "Unknown"
    
    # Get primary source from incident_sources (BaseIncident requires source field)
    # Source is not stored in incidents table - use incident_sources table for attribution
    source = "merged"  # Will be set properly when loading with sources
    
    incident = BaseIncident(
        incident_id=row["incident_id"],
        source=source,  # Placeholder - will be set when loading with sources
        source_event_id=None,  # Not stored in incidents table anymore
        university_name=university_name,
        victim_raw_name=row["victim_raw_name"],
        institution_type=row["institution_type"],
        country=row["country"],
        region=row["region"],
        city=row["city"],
        incident_date=row["incident_date"],
        date_precision=row["date_precision"] or "unknown",
        source_published_date=row["source_published_date"],
        ingested_at=row["ingested_at"],
        title=row["title"],
        subtitle=row["subtitle"],
        primary_url=row["primary_url"],
        all_urls=all_urls,
        leak_site_url=row["leak_site_url"],
        source_detail_url=row["source_detail_url"],
        screenshot_url=row["screenshot_url"],
        attack_type_hint=row["attack_type_hint"],
        status=row["status"] or "suspected",
        source_confidence=row["source_confidence"] or "medium",
        notes=row["notes"],
    )
    return incident


def get_last_pubdate(conn: sqlite3.Connection, source: str) -> Optional[str]:
    cur = conn.execute(
        "SELECT last_pubdate FROM source_state WHERE source = ?", (source,)
    )
    row = cur.fetchone()
    return row["last_pubdate"] if row and row["last_pubdate"] else None


def set_last_pubdate(conn: sqlite3.Connection, source: str, last_pubdate: str) -> None:
    conn.execute(
        """
        INSERT INTO source_state (source, last_pubdate)
        VALUES (?, ?)
        ON CONFLICT(source) DO UPDATE SET last_pubdate=excluded.last_pubdate
        """,
        (source, last_pubdate),
    )


def load_all_incidents_from_db(conn: sqlite3.Connection) -> List[BaseIncident]:
    """
    Load all deduplicated incidents from the database.
    This is used for building CSV from database instead of re-scraping.
    
    Args:
        conn: SQLite database connection
        
    Returns:
        List of BaseIncident objects loaded from database (already deduplicated)
    """
    cur = conn.execute("SELECT * FROM incidents ORDER BY ingested_at DESC")
    rows = cur.fetchall()
    
    incidents: List[BaseIncident] = []
    for row in rows:
        incident = _row_to_incident(row)
        # Get primary source from incident_sources for display
        source_cur = conn.execute(
            """
            SELECT source FROM incident_sources 
            WHERE incident_id = ? 
            ORDER BY first_seen_at ASC 
            LIMIT 1
            """,
            (incident.incident_id,),
        )
        source_row = source_cur.fetchone()
        if source_row:
            incident.source = source_row["source"]
        incidents.append(incident)
    
    return incidents


def get_incident_sources(
    conn: sqlite3.Connection, incident_id: str
) -> List[dict]:
    """
    Get all sources that contributed to an incident.
    
    Returns:
        List of dicts with keys: source, source_event_id, first_seen_at, confidence
    """
    cur = conn.execute(
        """
        SELECT source, source_event_id, first_seen_at, confidence
        FROM incident_sources
        WHERE incident_id = ?
        ORDER BY first_seen_at ASC
        """,
        (incident_id,),
    )
    return [dict(row) for row in cur.fetchall()]
