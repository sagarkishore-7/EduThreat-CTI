# Database Schema Documentation

**Version**: 1.6.0  
**Last Updated**: 2026-01-08

## Overview

`eduthreat.db` is a SQLite database that serves as the **persistent storage layer** for the EduThreat-CTI pipeline. It stores **deduplicated incidents** with cross-source deduplication applied during ingestion, and comprehensive LLM enrichment data.

## Database Features

### WAL Mode (Write-Ahead Logging)

The database uses WAL mode for concurrent access:
- **Multiple readers** can access the database while a writer is active
- **Readers don't block writers**, and writers don't block readers
- Essential for production with API + background processes
- Automatically enabled on database initialization

### Connection Management

- **Read-only connections**: API endpoints use read-only mode (faster, no locks)
- **Write connections**: Background processes use write mode with longer timeout (30s)
- **Connection timeouts**: 5s for reads, 30s for writes
- **Transaction management**: Short transactions with immediate commits

See [DATABASE_CONCURRENCY.md](DATABASE_CONCURRENCY.md) for detailed concurrency documentation.

## Database Schema

### 1. `incidents` Table

**Purpose**: Stores deduplicated cyber incident data

**Schema**:
```sql
CREATE TABLE incidents (
    incident_id          TEXT PRIMARY KEY,    -- Unique identifier (hash-based)
    
    -- Victim Information
    university_name      TEXT,                -- Normalized institution name
    victim_raw_name      TEXT,                -- Original name from source
    institution_type     TEXT,                -- "University" | "School" | "Research Institute" | "Unknown"
    country              TEXT,                -- Full country name (normalized)
    country_code         TEXT,                -- ISO 3166-1 alpha-2 code (e.g., "US", "GB")
    region               TEXT,
    city                 TEXT,
    
    -- Dates
    incident_date        TEXT,                -- YYYY-MM-DD format (from LLM timeline if enriched)
    date_precision       TEXT,                -- "day" | "month" | "year" | "unknown"
    source_published_date TEXT,               -- When source published
    ingested_at          TEXT,                -- UTC timestamp when ingested
    last_updated_at       TEXT,                -- When last merged/updated
    
    -- Content
    title                TEXT,
    subtitle             TEXT,
    
    -- URLs
    primary_url          TEXT,                -- Best URL selected by LLM (Phase 2)
    all_urls             TEXT,                -- Semicolon-separated URLs
    broken_urls          TEXT,                -- Semicolon-separated URLs that failed to fetch
    
    -- CTI URLs
    leak_site_url        TEXT,                -- Ransomware leak site URL
    source_detail_url     TEXT,                -- Source detail page
    screenshot_url        TEXT,                -- Screenshot/image URL
    
    -- Classification
    attack_type_hint     TEXT,                -- e.g., "ransomware"
    status               TEXT,                -- "suspected" | "confirmed"
    source_confidence    TEXT,                -- "low" | "medium" | "high" (highest from sources)
    
    notes                TEXT,                -- Additional metadata
    
    -- Phase 2 fields (LLM enrichment)
    llm_enriched         INTEGER DEFAULT 0,   -- 1 if enriched, 0 if not
    llm_enriched_at      TEXT,                -- UTC timestamp of enrichment
    llm_summary          TEXT,                -- Comprehensive incident summary
    llm_timeline         TEXT,                -- JSON timeline of events
    llm_mitre_attack     TEXT,                -- JSON array of MITRE ATT&CK techniques
    llm_attack_dynamics   TEXT                -- JSON attack dynamics
);
```

**Indexes**:
- `idx_incidents_country` on `country`
- `idx_incidents_date` on `incident_date`

### 2. `incident_sources` Table

**Purpose**: Tracks which sources contributed to each incident (many-to-many relationship)

**Schema**:
```sql
CREATE TABLE incident_sources (
    incident_id      TEXT NOT NULL,          -- Reference to incidents table
    source           TEXT NOT NULL,          -- Source name (e.g., "konbriefing", "darkreading")
    source_event_id  TEXT,                   -- Source-native event ID
    first_seen_at    TEXT NOT NULL,          -- When first seen from this source
    confidence       TEXT,                   -- Source's confidence level
    PRIMARY KEY (incident_id, source, source_event_id),
    FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE
);
```

**How it works**:
- Each incident can have multiple sources (same incident found from different sources)
- Tracks when each source first reported the incident
- Enables source attribution and provenance tracking
- Used to determine highest `source_confidence` for incidents table

**Indexes**:
- `idx_incident_sources_incident` on `incident_id`
- `idx_incident_sources_source` on `source`

### 3. `source_events` Table

**Purpose**: Tracks which events have been ingested from each source (per-source deduplication)

**Schema**:
```sql
CREATE TABLE source_events (
    source           TEXT NOT NULL,          -- Source name
    source_event_id  TEXT NOT NULL,          -- Source-native event identifier
    incident_id      TEXT NOT NULL,          -- Reference to incidents table
    first_seen_at    TEXT NOT NULL,          -- When first ingested
    PRIMARY KEY (source, source_event_id)    -- Ensures uniqueness per source
);
```

**How it works**:
- Before inserting an incident, the pipeline checks if `(source, source_event_id)` exists
- If it exists, the incident is skipped (already ingested from this source)
- If not, the pipeline checks for cross-source duplicates (URL matching)
- If duplicate found: Merges with existing incident, adds to `incident_sources`
- If new: Inserts new incident, adds to `incident_sources`
- This enables **incremental updates** - only new incidents are processed

### 4. `source_state` Table

**Purpose**: Tracks ingestion state per source (for incremental collection)

**Schema**:
```sql
CREATE TABLE source_state (
    source       TEXT PRIMARY KEY,          -- Source name
    last_pubdate TEXT                       -- Last publication date processed
);
```

**How it works**:
- Tracks the last processed publication date per source
- Enables efficient incremental collection (only fetch new incidents)
- Updated after each successful ingestion run

### 5. `incident_enrichments` Table

**Purpose**: Stores full LLM enrichment data as JSON

**Schema**:
```sql
CREATE TABLE incident_enrichments (
    incident_id          TEXT PRIMARY KEY,
    enrichment_data      TEXT NOT NULL,      -- Full JSON enrichment data
    enrichment_version   TEXT DEFAULT '2.0',
    enrichment_confidence REAL,              -- LLM confidence score
    created_at           TEXT NOT NULL,
    updated_at            TEXT NOT NULL,
    FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE
);
```

**How it works**:
- Stores complete enrichment data as JSON for flexibility
- Allows schema evolution without migration
- Used for detailed incident views and CTI reports

### 6. `incident_enrichments_flat` Table

**Purpose**: Stores flattened enrichment fields for fast queries and CSV export

**Schema**:
```sql
CREATE TABLE incident_enrichments_flat (
    incident_id TEXT PRIMARY KEY,
    
    -- Education & Institution
    is_education_related INTEGER,
    institution_name TEXT,
    institution_type TEXT,
    country TEXT,              -- Full country name (normalized)
    country_code TEXT,          -- ISO 3166-1 alpha-2 code
    region TEXT,
    city TEXT,
    
    -- Attack Details (88+ fields)
    attack_category TEXT,
    attack_vector TEXT,
    initial_access_vector TEXT,
    ransomware_family TEXT,
    threat_actor_name TEXT,
    -- ... (see full schema in phase2/storage/db.py)
    
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
    -- ... (many more fields)
    
    -- Operational Impact
    teaching_impacted INTEGER,
    research_impacted INTEGER,
    classes_cancelled INTEGER,
    downtime_days REAL,
    -- ... (many more fields)
    
    -- User Impact
    students_affected INTEGER,
    staff_affected INTEGER,
    faculty_affected INTEGER,
    -- ... (many more fields)
    
    -- Financial Impact
    recovery_costs_min REAL,
    recovery_costs_max REAL,
    -- ... (many more fields)
    
    -- Regulatory Impact
    gdpr_breach INTEGER,
    hipaa_breach INTEGER,
    ferpa_breach INTEGER,
    fine_amount REAL,
    -- ... (many more fields)
    
    -- Recovery & Transparency
    recovery_timeframe_days REAL,
    public_disclosure INTEGER,
    -- ... (many more fields)
    
    FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE
);
```

**How it works**:
- Flattened version of enrichment data for fast queries
- Used for CSV export and dashboard analytics
- 88+ fields covering all aspects of CTI analysis
- Automatically populated when enrichment is saved

## Database Workflow

### Initialization

1. Database is created automatically when pipeline runs
2. Tables are created if they don't exist (`CREATE TABLE IF NOT EXISTS`)
3. WAL mode is enabled automatically
4. Located at `data/eduthreat.db` (configurable via `EDU_CTI_DATA_DIR`)

### Phase 1: Ingestion Process

1. **Collect incidents** from sources (via `pipeline/phase1/`)
2. **Check per-source deduplication**: For each incident, check if `(source, source_event_id)` exists in `source_events`
3. **Check cross-source deduplication**: If new from this source, check for duplicates by URL matching
4. **Merge or insert**: 
   - If duplicate found: Merge with existing incident, update `incident_sources`
   - If new: Insert into `incidents` table, add to `incident_sources`
5. **Register event**: Add entry to `source_events` table
6. **Update source state**: Update `source_state` with last processed date
7. **Commit**: Save changes to database

### Phase 2: Enrichment Process

1. **Select unenriched incidents** from `incidents` table (`llm_enriched = 0`)
2. **Fetch articles** using multi-fallback strategy
3. **LLM extraction**: Extract structured CTI data
4. **Save enrichment**:
   - Update `incidents` table with enrichment flags and summary fields
   - Insert/update `incident_enrichments` with full JSON
   - Insert/update `incident_enrichments_flat` with flattened fields
5. **Update incident_date**: Use earliest date from LLM timeline if available
6. **Commit**: Save changes to database

## Querying the Database

### Basic Queries

```sql
-- Count incidents by country
SELECT country, COUNT(*) as count 
FROM incidents 
WHERE country IS NOT NULL
GROUP BY country 
ORDER BY count DESC;

-- Find recent incidents
SELECT incident_id, title, incident_date, country
FROM incidents 
WHERE incident_date >= '2024-01-01' 
ORDER BY incident_date DESC 
LIMIT 10;

-- Find enriched incidents
SELECT COUNT(*) 
FROM incidents 
WHERE llm_enriched = 1;

-- Find incidents by source
SELECT s.source, COUNT(*) as count
FROM incident_sources s
GROUP BY s.source
ORDER BY count DESC;
```

### Advanced Queries

```sql
-- Find incidents with specific ransomware family
SELECT i.incident_id, i.title, e.ransomware_family, e.ransom_amount
FROM incidents i
JOIN incident_enrichments_flat e ON i.incident_id = e.incident_id
WHERE e.ransomware_family IS NOT NULL
ORDER BY i.incident_date DESC;

-- Find incidents with data breaches
SELECT i.incident_id, i.title, e.records_affected_exact, e.pii_records_leaked
FROM incidents i
JOIN incident_enrichments_flat e ON i.incident_id = e.incident_id
WHERE e.data_breached = 1
ORDER BY e.records_affected_exact DESC;

-- Find incidents by MITRE ATT&CK technique
SELECT i.incident_id, i.title, i.llm_mitre_attack
FROM incidents i
WHERE i.llm_enriched = 1
  AND i.llm_mitre_attack LIKE '%T1566%'  -- Phishing technique
LIMIT 10;
```

## Country Normalization

The database stores both:
- **`country`**: Full country name (normalized, e.g., "United States")
- **`country_code`**: ISO 3166-1 alpha-2 code (e.g., "US")

This enables:
- Human-readable display in dashboard
- Machine-readable codes for CTI reports
- Flag emoji generation
- Consistent filtering and analytics

## Data Integrity

### Foreign Keys

- `incident_sources.incident_id` → `incidents.incident_id` (CASCADE DELETE)
- `source_events.incident_id` → `incidents.incident_id` (CASCADE DELETE)
- `incident_enrichments.incident_id` → `incidents.incident_id` (CASCADE DELETE)
- `incident_enrichments_flat.incident_id` → `incidents.incident_id` (CASCADE DELETE)

### Constraints

- `incident_id` is PRIMARY KEY in all tables
- `(source, source_event_id)` is UNIQUE in `source_events`
- `(incident_id, source, source_event_id)` is UNIQUE in `incident_sources`

## Maintenance

### Backup

- Database is in `.gitignore` (not versioned)
- Regular backups recommended for production
- Railway persistent volumes provide automatic backups

### Size Management

- Database grows with collected incidents
- Can be cleaned up by removing old incidents
- Vacuum operation can reclaim space: `VACUUM;`

### Migration

- Schema migrations are handled automatically
- New columns are added via migration checks in `init_db()`
- See `src/edu_cti/core/db.py` for migration logic

## Production Considerations

### Railway Deployment

- Database stored in persistent volume at `/app/data/eduthreat.db`
- WAL mode works with Railway's persistent volumes
- Read-only connections work with mounted volumes

### Performance

- Indexes on frequently queried columns
- WAL mode for concurrent access
- Connection pooling not needed (SQLite handles this internally)
- For very high traffic, consider PostgreSQL migration

## References

- [SQLite Documentation](https://www.sqlite.org/docs.html)
- [SQLite WAL Mode](https://www.sqlite.org/wal.html)
- [Database Concurrency Guide](DATABASE_CONCURRENCY.md)
