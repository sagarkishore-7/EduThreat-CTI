# src/edu_cti/db.py
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import datetime
from contextlib import contextmanager
import json

import sqlite3

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.core.config import DB_PATH
from src.edu_cti.core.deduplication import (
    extract_urls_from_incident,
    normalize_url,
    merge_incidents,
)

import logging
_db_logger = logging.getLogger(__name__)


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
        conn.row_factory = sqlite3.Row
        # Optimize read-only connections
        try:
            conn.execute("PRAGMA cache_size=-8000")  # 8MB cache
            conn.execute("PRAGMA mmap_size=268435456")  # 256MB memory-mapped I/O
            conn.execute("PRAGMA query_only=ON")
        except sqlite3.Error:
            pass
        return conn
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
            conn.execute("PRAGMA cache_size=-16000")  # 16MB cache
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
        conn.execute("PRAGMA cache_size=-16000")  # 16MB cache
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

            institution_name      TEXT,
            victim_raw_name      TEXT,
            institution_type     TEXT,
            country              TEXT,  -- Full country name (normalized)
            country_code         TEXT,  -- ISO 3166-1 alpha-2 code (e.g., "US", "GB")
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
            broken_urls          TEXT,  -- Semicolon-separated URLs that failed to fetch

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
            llm_attack_dynamics   TEXT,

            -- SERP discovery tracking (Oxylabs Google News search)
            -- Prevents re-spending credits on the same failed search each restart.
            -- After SERP_MAX_ATTEMPTS consecutive failures the incident is deleted.
            serp_attempt_count   INTEGER DEFAULT 0
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
        CREATE INDEX IF NOT EXISTS idx_incidents_enriched ON incidents(llm_enriched);
        CREATE INDEX IF NOT EXISTS idx_incidents_attack_type ON incidents(attack_type_hint);
        CREATE INDEX IF NOT EXISTS idx_incidents_ingested ON incidents(ingested_at);
        CREATE INDEX IF NOT EXISTS idx_incident_sources_incident ON incident_sources(incident_id);
        CREATE INDEX IF NOT EXISTS idx_incident_sources_source ON incident_sources(source);

        -- ===== NEW v2.0 TABLES =====

        -- IOC (Indicators of Compromise) table
        -- Stores IOCs extracted from articles via regex (pre-enrichment)
        -- and from LLM enrichment. Many-to-many with incidents.
        CREATE TABLE IF NOT EXISTS iocs (
            ioc_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ioc_type        TEXT NOT NULL,  -- ipv4, ipv6, domain, url, md5, sha1, sha256, cve, email, btc, mitre_technique
            ioc_value       TEXT NOT NULL,
            first_seen_at   TEXT NOT NULL,
            last_seen_at    TEXT NOT NULL,
            source          TEXT,           -- 'regex', 'llm', 'cisa_kev', 'otx', etc.
            confidence      TEXT DEFAULT 'medium',  -- low, medium, high
            context         TEXT,           -- brief context of where/how found
            UNIQUE(ioc_type, ioc_value)
        );

        -- Many-to-many: incidents <-> IOCs
        CREATE TABLE IF NOT EXISTS incident_iocs (
            incident_id     TEXT NOT NULL,
            ioc_id          INTEGER NOT NULL,
            extraction_method TEXT DEFAULT 'regex',  -- regex, llm, api
            extracted_at    TEXT NOT NULL,
            PRIMARY KEY (incident_id, ioc_id),
            FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE,
            FOREIGN KEY (ioc_id) REFERENCES iocs(ioc_id) ON DELETE CASCADE
        );

        -- IOC enrichment from external sources (VirusTotal, OTX, etc.)
        CREATE TABLE IF NOT EXISTS ioc_enrichments (
            ioc_id          INTEGER NOT NULL,
            source          TEXT NOT NULL,  -- 'otx', 'virustotal', 'abuseipdb', 'shodan'
            enrichment_data TEXT,           -- JSON blob from external API
            reputation_score REAL,          -- normalized 0-100 (higher = more malicious)
            tags            TEXT,           -- comma-separated tags
            enriched_at     TEXT NOT NULL,
            PRIMARY KEY (ioc_id, source),
            FOREIGN KEY (ioc_id) REFERENCES iocs(ioc_id) ON DELETE CASCADE
        );

        -- Threat actor knowledge base
        CREATE TABLE IF NOT EXISTS threat_actors (
            actor_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT NOT NULL UNIQUE,
            aliases         TEXT,           -- semicolon-separated aliases
            description     TEXT,
            motivation      TEXT,           -- financial, espionage, hacktivism, unknown
            country_origin  TEXT,           -- attributed country
            first_seen      TEXT,           -- first observed activity date
            last_seen       TEXT,
            mitre_group_id  TEXT,           -- e.g., G0016 for APT29
            reference_urls  TEXT,           -- semicolon-separated reference URLs
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        );

        -- Many-to-many: incidents <-> threat actors
        CREATE TABLE IF NOT EXISTS incident_threat_actors (
            incident_id     TEXT NOT NULL,
            actor_id        INTEGER NOT NULL,
            attribution_confidence TEXT DEFAULT 'medium',  -- low, medium, high
            attribution_source TEXT,         -- 'llm', 'manual', 'cisa', etc.
            attributed_at   TEXT NOT NULL,
            PRIMARY KEY (incident_id, actor_id),
            FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE,
            FOREIGN KEY (actor_id) REFERENCES threat_actors(actor_id) ON DELETE CASCADE
        );

        -- Translation cache for multi-language support
        CREATE TABLE IF NOT EXISTS translations (
            translation_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            original_text   TEXT NOT NULL,
            original_language TEXT NOT NULL,  -- ISO 639-1 code (e.g., 'zh', 'ja', 'de')
            translated_text TEXT NOT NULL,
            translation_engine TEXT DEFAULT 'google',  -- google, deepl, local
            translated_at   TEXT NOT NULL,
            content_hash    TEXT UNIQUE      -- hash of original_text for dedup
        );

        -- Source health tracking (for monitoring scraper reliability)
        CREATE TABLE IF NOT EXISTS source_health (
            source          TEXT NOT NULL,
            check_time      TEXT NOT NULL,
            status          TEXT NOT NULL,   -- 'ok', 'error', 'timeout', 'blocked'
            response_code   INTEGER,
            response_time_ms REAL,
            error_message   TEXT,
            incidents_found INTEGER DEFAULT 0,
            PRIMARY KEY (source, check_time)
        );

        -- Pipeline run tracking (persists across container restarts)
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id          TEXT PRIMARY KEY,
            phase           TEXT NOT NULL,
            status          TEXT NOT NULL DEFAULT 'running',  -- running, completed, failed, cancelled, interrupted
            params          TEXT,           -- JSON blob
            started_at      TEXT,
            finished_at     TEXT,
            duration_seconds REAL,
            result          TEXT,           -- JSON blob
            error           TEXT,
            progress_step   TEXT,
            progress_detail TEXT,
            progress_percent INTEGER DEFAULT 0
        );

        -- Indexes for new tables
        CREATE INDEX IF NOT EXISTS idx_iocs_type ON iocs(ioc_type);
        CREATE INDEX IF NOT EXISTS idx_iocs_value ON iocs(ioc_value);
        CREATE INDEX IF NOT EXISTS idx_incident_iocs_incident ON incident_iocs(incident_id);
        CREATE INDEX IF NOT EXISTS idx_incident_iocs_ioc ON incident_iocs(ioc_id);
        CREATE INDEX IF NOT EXISTS idx_threat_actors_name ON threat_actors(name);
        CREATE INDEX IF NOT EXISTS idx_incident_threat_actors_incident ON incident_threat_actors(incident_id);
        CREATE INDEX IF NOT EXISTS idx_source_health_source ON source_health(source);
        CREATE INDEX IF NOT EXISTS idx_translations_hash ON translations(content_hash);
        CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);
        """
    )
    
    # Migration: Rename university_name → institution_name (SQLite 3.25+)
    try:
        cur = conn.execute("PRAGMA table_info(incidents)")
        columns = [row[1] for row in cur.fetchall()]
        if "university_name" in columns and "institution_name" not in columns:
            conn.execute("ALTER TABLE incidents RENAME COLUMN university_name TO institution_name")
            conn.commit()
            import logging
            logging.getLogger(__name__).info("Renamed incidents.university_name → institution_name (migration)")
    except sqlite3.Error as e:
        import logging
        logging.getLogger(__name__).debug(f"Migration rename university_name: {e}")

    # Migration: Add broken_urls column if it doesn't exist (for existing databases)
    try:
        cur = conn.execute("PRAGMA table_info(incidents)")
        columns = [row[1] for row in cur.fetchall()]
        if "broken_urls" not in columns:
            conn.execute("ALTER TABLE incidents ADD COLUMN broken_urls TEXT")
            conn.commit()
            import logging
            logger = logging.getLogger(__name__)
            logger.info("Added broken_urls column to incidents table (migration)")
    except sqlite3.Error as e:
        # If migration fails, log but don't fail (column might already exist)
        import logging
        logger = logging.getLogger(__name__)
        logger.debug(f"Migration check for broken_urls column: {e}")
    
    # Migration: Add country_code column if it doesn't exist (for existing databases)
    try:
        cur = conn.execute("PRAGMA table_info(incidents)")
        columns = [row[1] for row in cur.fetchall()]
        if "country_code" not in columns:
            conn.execute("ALTER TABLE incidents ADD COLUMN country_code TEXT")
            conn.commit()
            import logging
            logger = logging.getLogger(__name__)
            logger.info("Added country_code column to incidents table (migration)")
    except sqlite3.Error as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.debug(f"Migration check for country_code column: {e}")

    # Migration: Add serp_attempt_count column if it doesn't exist
    try:
        cur = conn.execute("PRAGMA table_info(incidents)")
        columns = [row[1] for row in cur.fetchall()]
        if "serp_attempt_count" not in columns:
            conn.execute(
                "ALTER TABLE incidents ADD COLUMN serp_attempt_count INTEGER DEFAULT 0"
            )
            conn.commit()
            import logging
            logger = logging.getLogger(__name__)
            logger.info("Added serp_attempt_count column to incidents table (migration)")
    except sqlite3.Error as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.debug(f"Migration check for serp_attempt_count column: {e}")

    # Migration: Add llm_excluded columns for soft-delete support.
    # Previously, incidents classified as "not education-related" were hard-deleted.
    # Now we keep them as soft-deleted rows so they can be reviewed / re-enriched
    # with corrected articles without losing the original ingestion metadata.
    try:
        cur = conn.execute("PRAGMA table_info(incidents)")
        columns = [row[1] for row in cur.fetchall()]
        if "llm_excluded" not in columns:
            conn.execute(
                "ALTER TABLE incidents ADD COLUMN llm_excluded INTEGER DEFAULT 0"
            )
            conn.execute(
                "ALTER TABLE incidents ADD COLUMN llm_excluded_reason TEXT"
            )
            conn.commit()
            import logging
            logger = logging.getLogger(__name__)
            logger.info("Added llm_excluded columns to incidents table (migration)")
    except sqlite3.Error as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.debug(f"Migration check for llm_excluded column: {e}")

    # Migration: Add discovery_date column (LLM-extracted date incident was discovered).
    try:
        cur = conn.execute("PRAGMA table_info(incidents)")
        columns = [row[1] for row in cur.fetchall()]
        if "discovery_date" not in columns:
            conn.execute("ALTER TABLE incidents ADD COLUMN discovery_date TEXT")
            conn.commit()
            import logging
            logging.getLogger(__name__).info("Added discovery_date column to incidents table (migration)")
    except Exception:
        pass

    # Migration: Backfill data_breached=1 for enriched incidents where the LLM omitted
    # the boolean but attack_category or data signals clearly imply a breach.
    # Safe to run on every startup — only touches rows where data_breached IS NULL.
    try:
        _breach_cats = (
            "'data_breach_external'", "'data_breach_internal'",
            "'data_exposure_misconfiguration'", "'data_leak_accidental'",
            "'ransomware_double_extortion'", "'ransomware_triple_extortion'",
            "'ransomware_data_leak_only'",
        )
        _breach_cats_sql = ", ".join(_breach_cats)
        # Check if incident_enrichments_flat table exists before running
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='incident_enrichments_flat'"
        )
        if cur.fetchone():
            conn.execute(f"""
                UPDATE incident_enrichments_flat
                SET data_breached = 1
                WHERE data_breached IS NULL
                  AND is_education_related = 1
                  AND (
                    attack_category IN ({_breach_cats_sql})
                    OR data_exfiltrated = 1
                    OR data_categories IS NOT NULL
                    OR records_affected_exact IS NOT NULL
                    OR records_affected_min IS NOT NULL
                  )
            """)
            conn.commit()
    except Exception:
        pass

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
        "institution_name",
        "victim_raw_name",
        "institution_type",
        "country",
        "country_code",
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

    values = [data.get(f) for f in fields[:24]]  # Base fields (including country_code)
    
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


def find_duplicate_by_name_and_date(
    conn: sqlite3.Connection,
    incident: "BaseIncident",
    date_window_days: int = 14,
    name_threshold: int = 85,
) -> Optional[str]:
    """
    Find an existing incident that matches by victim name + date window.

    Used as a fallback when URL-based dedup finds no match (e.g. Comparitech
    incidents have no URLs, ransomware.live incidents link to different articles
    than kompriefing/konbriefing for the same event).

    Rules:
    - Names must score >= name_threshold (fuzzy token_sort_ratio after normalization)
    - If both incidents have dates, they must be within date_window_days of each other
    - If one or both dates are missing, only merge if names are an exact (normalized) match

    Returns the existing incident_id if a duplicate is found, else None.
    """
    from src.edu_cti.sources.future_work.fuzzy_dedup import are_likely_same_incident

    candidate_name = incident.institution_name or incident.victim_raw_name or ""
    candidate_date = incident.incident_date
    if not candidate_name:
        return None

    # Only scan the same rough year to avoid cross-year false positives
    # (a school attacked in 2019 and again in 2023 are different incidents)
    year_filter = ""
    year_params: list = []
    if candidate_date and len(candidate_date) >= 4:
        try:
            yr = int(candidate_date[:4])
            year_filter = "AND (incident_date IS NULL OR incident_date LIKE ? OR incident_date LIKE ? OR incident_date LIKE ?)"
            year_params = [f"{yr}%", f"{yr-1}%", f"{yr+1}%"]
        except ValueError:
            pass

    query = f"""
        SELECT incident_id, institution_name, victim_raw_name, incident_date
        FROM incidents
        WHERE (institution_name IS NOT NULL AND institution_name != '')
          OR  (victim_raw_name  IS NOT NULL AND victim_raw_name  != '')
        {year_filter}
    """
    rows = conn.execute(query, year_params).fetchall()

    for row in rows:
        existing_name = row["institution_name"] or row["victim_raw_name"] or ""
        existing_date = row["incident_date"]
        if not existing_name:
            continue
        if are_likely_same_incident(
            candidate_name, existing_name,
            candidate_date, existing_date,
            name_threshold=name_threshold,
            date_window_days=date_window_days,
        ):
            return row["incident_id"]

    return None


def mark_urls_as_broken(conn: sqlite3.Connection, incident_id: str, broken_urls: List[str]) -> None:
    """
    Mark URLs as broken for an incident.
    
    Args:
        conn: Database connection
        incident_id: Incident ID
        broken_urls: List of URLs that failed to fetch
    """
    if not broken_urls:
        return
    
    # Get existing broken URLs
    cur = conn.execute("SELECT broken_urls FROM incidents WHERE incident_id = ?", (incident_id,))
    row = cur.fetchone()
    existing_broken = set()
    if row and row["broken_urls"]:
        existing_broken = {url.strip() for url in row["broken_urls"].split(";") if url.strip()}
    
    # Add new broken URLs (normalize for consistency)
    new_broken = {normalize_url(url) for url in broken_urls}
    all_broken = existing_broken | new_broken
    
    # Update database
    broken_urls_str = ";".join(sorted(all_broken))
    conn.execute(
        "UPDATE incidents SET broken_urls = ? WHERE incident_id = ?",
        (broken_urls_str, incident_id)
    )


def get_broken_urls(conn: sqlite3.Connection, incident_id: str) -> Set[str]:
    """
    Get set of broken URLs for an incident.
    
    Args:
        conn: Database connection
        incident_id: Incident ID
        
    Returns:
        Set of normalized broken URLs
    """
    cur = conn.execute("SELECT broken_urls FROM incidents WHERE incident_id = ?", (incident_id,))
    row = cur.fetchone()
    if row and row["broken_urls"]:
        return {normalize_url(url.strip()) for url in row["broken_urls"].split(";") if url.strip()}
    return set()


def has_broken_urls(conn: sqlite3.Connection, incident_id: str) -> bool:
    """
    Check if an incident has any broken URLs.
    
    Args:
        conn: Database connection
        incident_id: Incident ID
        
    Returns:
        True if incident has broken URLs, False otherwise
    """
    broken = get_broken_urls(conn, incident_id)
    return len(broken) > 0


def clear_broken_urls(conn: sqlite3.Connection, incident_id: str, urls: List[str]) -> None:
    """
    Clear URLs from broken_urls list (e.g., when they're successfully fetched).
    
    Args:
        conn: Database connection
        incident_id: Incident ID
        urls: List of URLs to remove from broken list
    """
    broken = get_broken_urls(conn, incident_id)
    if not broken:
        return
    
    urls_to_remove = {normalize_url(url) for url in urls}
    remaining_broken = broken - urls_to_remove
    
    if remaining_broken:
        broken_urls_str = ";".join(sorted(remaining_broken))
        conn.execute(
            "UPDATE incidents SET broken_urls = ? WHERE incident_id = ?",
            (broken_urls_str, incident_id)
        )
    else:
        # No broken URLs left
        conn.execute(
            "UPDATE incidents SET broken_urls = NULL WHERE incident_id = ?",
            (incident_id,)
        )


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
    institution_name = row["institution_name"]
    if not institution_name:
        institution_name = row["victim_raw_name"] or "Unknown"
    
    # Get primary source from incident_sources (BaseIncident requires source field)
    # Source is not stored in incidents table - use incident_sources table for attribution
    source = "merged"  # Will be set properly when loading with sources
    
    incident = BaseIncident(
        incident_id=row["incident_id"],
        source=source,  # Placeholder - will be set when loading with sources
        source_event_id=None,  # Not stored in incidents table anymore
        institution_name=institution_name,
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


# ===== IOC Database Operations =====

def upsert_ioc(
    conn: sqlite3.Connection,
    ioc_type: str,
    ioc_value: str,
    source: str = "regex",
    confidence: str = "medium",
    context: Optional[str] = None,
) -> int:
    """
    Insert or update an IOC, returning the ioc_id.

    If the IOC already exists, updates last_seen_at.
    """
    now = datetime.utcnow().isoformat()

    cur = conn.execute(
        "SELECT ioc_id FROM iocs WHERE ioc_type = ? AND ioc_value = ?",
        (ioc_type, ioc_value),
    )
    row = cur.fetchone()

    if row:
        ioc_id = row["ioc_id"]
        conn.execute(
            "UPDATE iocs SET last_seen_at = ?, source = COALESCE(?, source) WHERE ioc_id = ?",
            (now, source, ioc_id),
        )
        return ioc_id
    else:
        cur = conn.execute(
            """
            INSERT INTO iocs (ioc_type, ioc_value, first_seen_at, last_seen_at, source, confidence, context)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (ioc_type, ioc_value, now, now, source, confidence, context),
        )
        return cur.lastrowid


def link_ioc_to_incident(
    conn: sqlite3.Connection,
    incident_id: str,
    ioc_id: int,
    extraction_method: str = "regex",
) -> None:
    """Link an IOC to an incident (many-to-many)."""
    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT OR IGNORE INTO incident_iocs (incident_id, ioc_id, extraction_method, extracted_at)
        VALUES (?, ?, ?, ?)
        """,
        (incident_id, ioc_id, extraction_method, now),
    )


def save_iocs_for_incident(
    conn: sqlite3.Connection,
    incident_id: str,
    ioc_list: List[Dict[str, str]],
    extraction_method: str = "regex",
) -> int:
    """
    Save a list of IOCs and link them to an incident.

    Args:
        conn: Database connection
        incident_id: Incident to link IOCs to
        ioc_list: List of {"type": "...", "value": "..."} dicts
        extraction_method: How IOCs were extracted

    Returns:
        Number of IOCs saved
    """
    # Verify incident exists before linking (FK constraint)
    cur = conn.execute("SELECT 1 FROM incidents WHERE incident_id = ?", (incident_id,))
    if cur.fetchone() is None:
        _db_logger.debug(f"Incident {incident_id} not in DB, skipping IOC save")
        return 0

    count = 0
    for ioc in ioc_list:
        ioc_id = upsert_ioc(
            conn,
            ioc_type=ioc["type"],
            ioc_value=ioc["value"],
            source=extraction_method,
        )
        link_ioc_to_incident(conn, incident_id, ioc_id, extraction_method)
        count += 1
    if count > 0:
        conn.commit()
    return count


def get_iocs_for_incident(
    conn: sqlite3.Connection,
    incident_id: str,
) -> List[Dict[str, Any]]:
    """Get all IOCs linked to an incident."""
    cur = conn.execute(
        """
        SELECT i.ioc_id, i.ioc_type, i.ioc_value, i.confidence, i.source,
               ii.extraction_method, ii.extracted_at
        FROM iocs i
        JOIN incident_iocs ii ON i.ioc_id = ii.ioc_id
        WHERE ii.incident_id = ?
        ORDER BY i.ioc_type, i.ioc_value
        """,
        (incident_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def get_incidents_sharing_ioc(
    conn: sqlite3.Connection,
    ioc_id: int,
) -> List[str]:
    """Get all incident IDs that share a specific IOC (for correlation)."""
    cur = conn.execute(
        "SELECT incident_id FROM incident_iocs WHERE ioc_id = ?",
        (ioc_id,),
    )
    return [row["incident_id"] for row in cur.fetchall()]


# ===== Threat Actor Database Operations =====

def upsert_threat_actor(
    conn: sqlite3.Connection,
    name: str,
    aliases: Optional[str] = None,
    description: Optional[str] = None,
    motivation: Optional[str] = None,
    country_origin: Optional[str] = None,
    mitre_group_id: Optional[str] = None,
) -> int:
    """Insert or update a threat actor, returning the actor_id."""
    now = datetime.utcnow().isoformat()

    cur = conn.execute("SELECT actor_id FROM threat_actors WHERE name = ?", (name,))
    row = cur.fetchone()

    if row:
        actor_id = row["actor_id"]
        conn.execute(
            """
            UPDATE threat_actors
            SET aliases = COALESCE(?, aliases),
                description = COALESCE(?, description),
                motivation = COALESCE(?, motivation),
                country_origin = COALESCE(?, country_origin),
                mitre_group_id = COALESCE(?, mitre_group_id),
                last_seen = ?,
                updated_at = ?
            WHERE actor_id = ?
            """,
            (aliases, description, motivation, country_origin, mitre_group_id, now, now, actor_id),
        )
        return actor_id
    else:
        cur = conn.execute(
            """
            INSERT INTO threat_actors
            (name, aliases, description, motivation, country_origin, first_seen, last_seen,
             mitre_group_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (name, aliases, description, motivation, country_origin, now, now, mitre_group_id, now, now),
        )
        return cur.lastrowid


def link_threat_actor_to_incident(
    conn: sqlite3.Connection,
    incident_id: str,
    actor_id: int,
    confidence: str = "medium",
    source: str = "llm",
) -> None:
    """Link a threat actor to an incident."""
    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT OR IGNORE INTO incident_threat_actors
        (incident_id, actor_id, attribution_confidence, attribution_source, attributed_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (incident_id, actor_id, confidence, source, now),
    )


# ===== Source Health Tracking =====

def record_source_health(
    conn: sqlite3.Connection,
    source: str,
    status: str,
    response_code: Optional[int] = None,
    response_time_ms: Optional[float] = None,
    error_message: Optional[str] = None,
    incidents_found: int = 0,
) -> None:
    """Record a source health check result."""
    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT INTO source_health
        (source, check_time, status, response_code, response_time_ms, error_message, incidents_found)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (source, now, status, response_code, response_time_ms, error_message, incidents_found),
    )
    conn.commit()


# ===== Translation Cache =====

def get_cached_translation(
    conn: sqlite3.Connection,
    content_hash: str,
) -> Optional[str]:
    """Get a cached translation by content hash."""
    cur = conn.execute(
        "SELECT translated_text FROM translations WHERE content_hash = ?",
        (content_hash,),
    )
    row = cur.fetchone()
    return row["translated_text"] if row else None


def cache_translation(
    conn: sqlite3.Connection,
    original_text: str,
    original_language: str,
    translated_text: str,
    content_hash: str,
    engine: str = "google",
) -> None:
    """Cache a translation result."""
    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT OR REPLACE INTO translations
        (original_text, original_language, translated_text, translation_engine, translated_at, content_hash)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (original_text, original_language, translated_text, engine, now, content_hash),
    )
    conn.commit()
