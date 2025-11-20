# Deduplication Strategy

## Overview

EduThreat-CTI uses a **two-level deduplication strategy**:

1. **Per-Source Deduplication** (Database level) - Prevents re-ingesting same incident from same source
2. **Cross-Source Deduplication** (CSV building level) - Merges incidents with same URLs from different sources

## Per-Source Deduplication (Database)

### How It Works

When you run the pipeline, the database checks if an incident has already been ingested:

1. **Check**: Before inserting, checks `source_events` table for `(source, source_event_id)`
2. **Skip if exists**: If found, incident is skipped (already ingested)
3. **Insert if new**: If not found, incident is inserted and registered

### Example

```python
# First run: Ingests incident
source = "thehackernews"
source_event_id = "https://thehackernews.com/article123"
# → Not in database → INSERTED

# Second run (same command): Same incident found
source = "thehackernews"
source_event_id = "https://thehackernews.com/article123"
# → Already in source_events → SKIPPED
```

### Benefits

✅ **Efficient**: Only processes new incidents  
✅ **Incremental**: Can run pipeline multiple times safely  
✅ **Fast**: Database lookup is very quick  

### What It Prevents

- Re-ingesting the same article from The Hacker News
- Re-ingesting the same incident from KonBriefing
- Re-processing incidents already in the database

### What It Does NOT Prevent

- Same incident appearing in multiple sources (e.g., same article in The Hacker News AND SecurityWeek)
- This is handled by cross-source deduplication (see below)

## Cross-Source Deduplication (CSV Building)

### How It Works

When building the unified CSV, incidents are deduplicated based on URL matching:

1. **Collect all incidents** from all sources (fresh collection or from database)
2. **Normalize URLs** (remove trailing slashes, www., fragments)
3. **Group by shared URLs** (incidents sharing at least one URL are grouped)
4. **Merge groups** (keep highest confidence, merge all URLs and metadata)
5. **Output deduplicated list**

### Example

```python
# Incident 1: The Hacker News
source = "thehackernews"
all_urls = ["https://example.com/article"]

# Incident 2: SecurityWeek (same article)
source = "securityweek"
all_urls = ["https://example.com/article"]

# Cross-source deduplication:
# → Detects shared URL
# → Merges into single incident
# → Keeps highest confidence source
# → Combines metadata
```

## Current Behavior When Re-Running Pipeline

### Scenario: Re-run `python -m src.edu_cti.cli.pipeline --news-max-pages all`

**Step 1: Database Ingestion**
- ✅ **Per-source deduplication**: Database checks `source_events` table
- ✅ **Cross-source deduplication**: Checks for duplicates by URL matching
- ✅ **Merges or inserts**: Updates existing incidents or inserts new ones
- ✅ **Efficient**: Only processes new incidents, prevents duplicates

**Step 2: CSV Building**
- ✅ **Loads from database**: Reads deduplicated incidents from database
- ✅ **No re-scraping**: Fast and efficient
- ✅ **Already deduplicated**: No additional deduplication needed

### Design Benefits

1. **Database**: Stores deduplicated incidents (cross-source dedup at ingestion)
2. **CSV**: Simple export from database (already deduplicated)
3. **Efficiency**: No re-scraping needed for CSV building
4. **Consistency**: Database and CSV stay in sync

## Summary

| Deduplication Type | Where | When | What It Prevents |
|---------------------|-------|------|------------------|
| **Per-Source** | Database (`source_events` table) | Ingestion step | Re-ingesting same incident from same source |
| **Cross-Source** | CSV building (`deduplication.py`) | Dataset building step | Multiple incidents with same URLs from different sources |

### Answer to Your Question

**Yes, the database handles both per-source and cross-source deduplication!**

When you re-run:
```bash
python -m src.edu_cti.cli.pipeline --news-max-pages all
```

The database will:
1. ✅ **Check per-source deduplication** in `source_events` table
2. ✅ **Check cross-source deduplication** by URL matching
3. ✅ **Merge if duplicate found**, or insert if new
4. ✅ **Track source attribution** in `incident_sources` table
5. ✅ **Show you the count**: "Newly inserted incidents: X" (only new ones)

The CSV building step loads from the database (already deduplicated), making re-runs fast and efficient.

