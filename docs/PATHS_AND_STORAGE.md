# Data Paths and Storage Configuration

## Overview

The EduThreat-CTI project automatically detects the environment (Railway vs local) and uses the appropriate data directory paths.

## Automatic Path Detection

### Railway (Production)
- **Data Directory**: `/app/data` (Railway persistent volume)
- **Database**: `/app/data/eduthreat.db`
- **Detection**: 
  - Checks `RAILWAY_ENVIRONMENT`, `RAILWAY_PROJECT_ID`, or `RAILWAY_SERVICE_ID` env vars
  - Or checks if `/app/data` exists and is writable
  - Or uses `EDU_CTI_DATA_DIR=/app/data` (set in Dockerfile)

### Local Development
- **Data Directory**: `./data` (relative to project root)
- **Database**: `./data/eduthreat.db`
- **Detection**: Default when Railway is not detected

## Configuration Priority

1. **Explicit Override** (highest priority): `EDU_CTI_DATA_DIR` environment variable
2. **Railway Auto-Detection**: Detects Railway environment automatically
3. **Local Default**: Falls back to `./data` for local development

## How It Works

The configuration is centralized in `src/edu_cti/core/config.py`:

```python
# Auto-detects environment
DATA_DIR = _get_data_dir()  # Returns /app/data on Railway, ./data locally
DB_PATH = DATA_DIR / "eduthreat.db"
```

All modules import from this config:
- `src/edu_cti/pipeline/phase1/base_io.py` - Uses `DATA_DIR` for raw/processed files
- `src/edu_cti/core/db.py` - Uses `DB_PATH` for database
- `src/edu_cti/api/admin.py` - Uses `DATA_DIR` and `DB_PATH` for operations

## Railway Setup

The Dockerfile sets:
```dockerfile
ENV EDU_CTI_DATA_DIR=/app/data
ENV EDU_CTI_DB_PATH=eduthreat.db
```

This ensures Railway always uses the persistent volume at `/app/data`.

## Local Development

No configuration needed - automatically uses `./data` directory.

## Verification

Check which paths are being used:

```python
from src.edu_cti.core.config import DATA_DIR, DB_PATH
print(f"Data directory: {DATA_DIR.absolute()}")
print(f"Database path: {DB_PATH.absolute()}")
```

## All Data Operations

All data operations automatically use the correct paths:
- ✅ Database reads/writes → `DB_PATH`
- ✅ CSV exports → `DATA_DIR/processed/`
- ✅ Raw data storage → `DATA_DIR/raw/`
- ✅ Logs → `logs/` (not in persistent storage)

No code changes needed - everything uses the centralized config!
