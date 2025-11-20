# Database Documentation: eduthreat.db

## Overview

`eduthreat.db` is a SQLite database that serves as the **persistent storage layer** for the EduThreat-CTI pipeline. It stores **deduplicated incidents** with cross-source deduplication applied during ingestion.

## Why is it Generated?

The database is generated for several important reasons:

### 1. **Persistent Storage**
- Stores all incidents collected from various sources
- Survives pipeline restarts and updates
- Provides a single source of truth for all collected data

### 2. **Deduplication**
- **Per-Source Deduplication**: Prevents re-ingesting the same incident from the same source
- **Cross-Source Deduplication**: Merges incidents with same URLs from different sources during ingestion
- Tracks which incidents have already been processed
- Enables incremental updates (only fetch new incidents)

### 3. **State Management**
- Tracks ingestion state per source
- Records when incidents were first seen
- Enables efficient incremental collection

### 4. **Query Capabilities**
- SQL queries for analysis and filtering
- Can be used for reporting and analytics
- Foundation for Phase 2+ features

## Database Schema

The database contains **4 main tables**:

### 1. `incidents` Table

**Purpose**: Stores deduplicated cyber incident data

**Schema**:
```sql
CREATE TABLE incidents (
    incident_id          TEXT PRIMARY KEY,    -- Unique identifier (hash-based)
    -- Note: 'source' field removed - use incident_sources table for source attribution
    
    -- Victim Information
    university_name      TEXT,               -- Normalized institution name
    victim_raw_name      TEXT,               -- Original name from source
    institution_type     TEXT,               -- "University" | "School" | "Research Institute"
    country              TEXT,                -- ISO-2 country code
    region               TEXT,
    city                 TEXT,
    
    -- Dates
    incident_date        TEXT,                -- YYYY-MM-DD format
    date_precision       TEXT,                -- "day" | "month" | "year" | "unknown"
    source_published_date TEXT,               -- When source published
    ingested_at          TEXT,                -- UTC timestamp when ingested
    last_updated_at       TEXT,                -- When last merged/updated
    
    -- Content
    title                TEXT,
    subtitle             TEXT,
    
    -- URLs (Phase 1: primary_url=None, all URLs in all_urls)
    primary_url          TEXT,               -- None in Phase 1
    all_urls             TEXT,               -- Semicolon-separated URLs
    
    -- CTI URLs
    leak_site_url        TEXT,               -- Ransomware leak site URL
    source_detail_url    TEXT,               -- Source detail page
    screenshot_url        TEXT,               -- Screenshot/image URL
    
    -- Classification
    attack_type_hint     TEXT,               -- e.g., "ransomware"
    status               TEXT,               -- "suspected" | "confirmed"
    source_confidence    TEXT,               -- "low" | "medium" | "high"
    
    notes                TEXT,                -- Additional metadata
    
    -- Phase 2 fields (reserved for LLM enrichment)
    llm_enriched         INTEGER DEFAULT 0,
    llm_enriched_at      TEXT,
    llm_summary          TEXT,
    llm_timeline         TEXT,
    llm_mitre_attack     TEXT,
    llm_attack_dynamics   TEXT
);
```

### 2. `incident_sources` Table

**Purpose**: Tracks which sources contributed to each incident (many-to-many relationship)

**Schema**:
```sql
CREATE TABLE incident_sources (
    incident_id      TEXT NOT NULL,          -- Reference to incidents table
    source           TEXT NOT NULL,          -- Source name
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

### 3. `source_events` Table

**Purpose**: Tracks which events have been ingested from each source (deduplication)

**Schema**:
```sql
CREATE TABLE source_events (
    source           TEXT NOT NULL,         -- Source name
    source_event_id  TEXT NOT NULL,         -- Source-native event identifier
    incident_id      TEXT NOT NULL,          -- Reference to incidents table
    first_seen_at    TEXT NOT NULL,          -- When first ingested
    PRIMARY KEY (source, source_event_id)   -- Ensures uniqueness per source
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

**Purpose**: Tracks ingestion state per source (for future incremental collection)

**Schema**:
```sql
CREATE TABLE source_state (
    source       TEXT PRIMARY KEY,          -- Source name
    last_pubdate TEXT                       -- Last publication date processed
);
```

**Future use**: Can be used to track the last processed date per source for efficient incremental collection.

## Database Workflow

### Initialization
1. Database is created automatically when pipeline runs
2. Tables are created if they don't exist (`CREATE TABLE IF NOT EXISTS`)
3. Located at `data/eduthreat.db` (configurable via `EDU_CTI_DB_PATH`)

### Ingestion Process
1. **Collect incidents** from sources (via `cli/ingestion.py`)
2. **Check per-source deduplication**: For each incident, check if `(source, source_event_id)` exists in `source_events`
3. **Check cross-source deduplication**: If new from this source, check for duplicates by URL matching
4. **Merge or insert**: 
   - If duplicate found: Merge with existing incident, update `incident_sources`
   - If new: Insert into `incidents` table, add to `incident_sources`
5. **Register event**: Add entry to `source_events` table
6. **Commit**: Save changes to database

### Querying
The database can be queried using standard SQL:

```sql
-- Count incidents by source
SELECT source, COUNT(*) FROM incidents GROUP BY source;

-- Find incidents by country
SELECT * FROM incidents WHERE country = 'US';

-- Find recent incidents
SELECT * FROM incidents WHERE incident_date >= '2024-01-01' ORDER BY incident_date DESC;

-- Find incidents with specific attack type
SELECT * FROM incidents WHERE attack_type_hint = 'ransomware';
```

## Relationship to CSV Output

The database and CSV serve different purposes:

| Feature | Database (`eduthreat.db`) | CSV (`base_dataset.csv`) |
|---------|---------------------------|--------------------------|
| **Purpose** | Persistent storage, deduplication | Snapshot export, analysis |
| **Format** | SQLite (structured, queryable) | CSV (portable, human-readable) |
| **Deduplication** | Per-source + Cross-source (applied at ingestion) | Already deduplicated (from database) |
| **Updates** | Incremental (only new incidents) | Full snapshot (all incidents) |
| **Use Case** | Pipeline state, incremental updates | Data export, analysis, sharing |

## Re-Running the Pipeline

### What Happens When You Re-Run?

When you run `python -m src.edu_cti.cli.pipeline --news-max-pages all` multiple times:

**Database Ingestion (Step 1)**:
- ✅ **Checks `source_events` table** for each incident (per-source deduplication)
- ✅ **Checks for cross-source duplicates** by URL matching
- ✅ **Merges if duplicate found**, or inserts if new
- ✅ **Tracks source attribution** in `incident_sources` table
- ✅ **Efficient**: Only processes new incidents, prevents duplicate inserts

**CSV Building (Step 2)**:
- ✅ **Loads from database** (already deduplicated)
- ✅ **No re-scraping needed** (production-efficient)
- ✅ **Fast**: Simple database query

### Example Output

```bash
# First run
[*] Step 1: Running ingestion pipeline...
    konbriefing: 323 incidents (323 new)
    thehackernews: 150 incidents (150 new)
[✓] Ingestion pipeline completed. Newly inserted incidents: 473

# Second run (same command)
[*] Step 1: Running ingestion pipeline...
    konbriefing: 323 incidents (0 new)      # ← All skipped (already in DB)
    thehackernews: 150 incidents (0 new)   # ← All skipped (already in DB)
[✓] Ingestion pipeline completed. Newly inserted incidents: 0

[*] Step 2: Building unified base dataset...
[*] Loading incidents from database...
[*] Loaded 650 incidents from database (already deduplicated)
[✓] Base dataset written: 650 incidents
```

The database **prevents re-ingesting the same incidents** and **automatically deduplicates across sources**, making re-runs efficient and the database clean!

## When is it Generated?

The database is generated/updated when you run:

1. **Main Pipeline** (`cli/pipeline.py`):
   ```bash
   python -m src.edu_cti.cli.pipeline
   ```
   - Initializes database
   - Ingests incidents
   - Builds CSV snapshot

2. **Ingestion Only** (`cli/ingestion.py`):
   ```bash
   python -m src.edu_cti.cli.ingestion
   ```
   - Only updates database (no CSV)
   - Useful for scheduled incremental updates

## Configuration

Database location can be configured via environment variable:

```bash
# In .env file or environment
EDU_CTI_DB_PATH=data/eduthreat.db
EDU_CTI_DATA_DIR=data
```

## Benefits

1. **Efficiency**: Only processes new incidents (incremental updates)
2. **Reliability**: Persistent storage survives restarts
3. **Queryability**: SQL queries for analysis
4. **State Management**: Tracks what's been processed
5. **Scalability**: Can handle large datasets efficiently

## Future Enhancements

The database structure supports:
- **Phase 2**: LLM enrichment results can be stored in additional tables
- **Phase 3**: CTI outputs can query the database
- **Analytics**: Complex queries for threat intelligence analysis
- **API**: Database can serve as backend for web APIs

## Maintenance

- **Backup**: Database is in `.gitignore` (not versioned)
- **Size**: Grows with collected incidents
- **Cleanup**: Can be deleted and regenerated (will re-ingest all sources)
- **Extensibility**: Schema can be extended for Phase 2+ features

