# Raw Directory Usage

## Overview

The `data/raw/` directory is used to store **per-source CSV snapshots** before aggregation and deduplication. However, in the current workflow, it's only populated in specific scenarios.

## Current Workflow

### Main Pipeline (`pipeline.py`)
1. **Ingestion Phase**: Collects incidents directly into database (no raw files written)
   - Uses `collect_curated_incidents()` and `collect_news_incidents()`
   - These functions don't write to raw directory
   - Data goes directly to `data/eduthreat.db`

2. **Dataset Building Phase**: Loads from database (default)
   - Uses `build_dataset(from_database=True)` by default
   - Reads from `data/eduthreat.db`
   - Writes unified CSV to `data/processed/base_dataset.csv`
   - **No raw files written**

### When Raw Directory IS Written

Raw files are only written when using `build_dataset.py` directly with `from_database=False` (fresh collection):

```bash
# This writes to raw/ directory
python -m src.edu_cti.cli.build_dataset --groups news --fresh-collection

# Or when build_dataset() is called with from_database=False
build_dataset(groups, from_database=False)
```

In this case:
- `run_curated_pipeline()` writes to `data/raw/curated/{source}_base.csv`
- `run_news_pipeline()` writes to `data/raw/news/{source}_base.csv`

## Directory Structure

```
data/
├── raw/                    # Per-source snapshots (optional, only for debugging)
│   ├── curated/
│   │   ├── konbriefing_base.csv
│   │   ├── ransomwarelive_base.csv
│   │   └── databreach_base.csv
│   └── news/
│       ├── krebsonsecurity_base.csv
│       ├── thehackernews_base.csv
│       ├── therecord_base.csv
│       ├── securityweek_base.csv
│       └── darkreading_base.csv
├── processed/
│   └── base_dataset.csv    # Unified dataset (always written)
└── eduthreat.db            # SQLite database (primary storage)
```

## Use Cases for Raw Directory

### 1. **Debugging & Development**
When testing individual sources or debugging ingestion issues:
```bash
python -m src.edu_cti.cli.build_dataset --groups news --news-sources krebsonsecurity --fresh-collection
# This writes data/raw/news/krebsonsecurity_base.csv
```

### 2. **Incremental Analysis**
To see what each source collected before deduplication:
```bash
# Check individual source outputs
cat data/raw/news/krebsonsecurity_base.csv
cat data/raw/curated/ransomwarelive_base.csv
```

### 3. **Source Validation**
To verify source-specific data quality before aggregation.

## Should We Keep It?

### Option 1: Remove Raw Directory (Simpler)
**Pros:**
- Simpler codebase
- Less disk usage
- Fewer files to manage

**Cons:**
- Harder to debug individual sources
- No intermediate outputs for inspection

### Option 2: Keep for Debugging (Current)
**Pros:**
- Useful for debugging individual sources
- Allows inspection of pre-aggregation data
- Helps with source-specific issues

**Cons:**
- Adds complexity
- Not used in main workflow
- May confuse users about its purpose

### Option 3: Always Write Raw Files
**Pros:**
- Consistent outputs
- Always available for debugging
- Better traceability

**Cons:**
- More I/O operations
- More disk usage
- Slower pipeline

## Recommendation

**Keep the raw directory but make it optional/development-only:**
1. Only write to raw when explicitly requested (debugging mode)
2. Document it's for debugging/development purposes
3. Add a flag like `--write-raw-snapshots` to enable it
4. Default to NOT writing raw files in production pipeline

This way:
- Production pipeline is faster and simpler
- Developers can enable raw files when needed for debugging
- Documentation clarifies its purpose

## Current Implementation ✅

The raw directory is now **optional** and disabled by default (as of latest update).

### Usage

**Default behavior (production - current):**
- Raw files are NOT written (faster, less disk usage)
- Data goes directly to database, then to unified CSV
- Main pipeline: `write_raw=False` (default)

**Enable raw files for debugging:**
```bash
# Write raw files when building from sources
python -m src.edu_cti.cli.build_dataset --groups news --write-raw

# Or when building with fresh collection
python -m src.edu_cti.cli.build_dataset --groups news --fresh-collection --write-raw
```

### Implementation Details

- `run_curated_pipeline(write_raw=False)` - Default: no raw files
- `run_news_pipeline(write_raw=False)` - Default: no raw files  
- `build_dataset(write_raw=False)` - Default: no raw files
- CLI: `--write-raw` flag enables raw file writing

### When to Use Raw Directory

**Use `--write-raw` when:**
1. Debugging individual source ingestion
2. Inspecting pre-deduplication data
3. Validating source-specific outputs
4. Development/testing purposes

**Don't use `--write-raw` for:**
1. Production pipelines (slower, uses more disk)
2. Automated runs (unnecessary I/O)
3. When you only need the unified CSV

## Summary

The raw directory is **kept but made optional**:
- ✅ **Default**: Not written (production efficiency)
- ✅ **Optional**: Enable with `--write-raw` flag (debugging/development)
- ✅ **Purpose**: Per-source snapshots for debugging individual sources
- ✅ **Location**: `data/raw/curated/` and `data/raw/news/`

This provides the best of both worlds:
- Fast production pipelines without unnecessary I/O
- Optional debugging output when needed
- Clear documentation of its purpose
