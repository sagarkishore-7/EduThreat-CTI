# Database Concurrency & Production Readiness

## Problem

When background processes (ingestion, enrichment) are writing to the SQLite database, the dashboard API was unable to read data, causing the dashboard to appear empty or fail to load.

## Solution

We've implemented **WAL (Write-Ahead Logging) mode** and optimized connection handling to allow concurrent reads and writes.

## Changes Made

### 1. WAL Mode (Write-Ahead Logging)

**What it does:**
- Allows multiple readers while a writer is active
- Readers don't block writers, and writers don't block readers
- Essential for production SQLite databases with concurrent access

**Implementation:**
- Enabled automatically on database initialization
- Configured in `src/edu_cti/core/db.py` via `PRAGMA journal_mode=WAL`

### 2. Read-Only Connections for API

**What it does:**
- API endpoints use read-only connections by default
- Read-only connections never acquire write locks
- Faster and safer for concurrent access

**Implementation:**
- `get_api_connection()` defaults to `read_only=True`
- Read-only connections use SQLite URI mode: `file:path?mode=ro`
- Shorter timeout (5s) for read operations

### 3. Connection Timeouts

**What it does:**
- Prevents indefinite blocking when database is busy
- Write operations: 30 second timeout
- Read operations: 5 second timeout

**Implementation:**
- Configured via `timeout` parameter in `get_connection()`
- Uses SQLite's `PRAGMA busy_timeout` for automatic retries

### 4. Optimized Database Settings

**Performance optimizations:**
- `PRAGMA synchronous=NORMAL` - Balance between safety and speed
- `PRAGMA cache_size=-64000` - 64MB cache for better performance
- `PRAGMA foreign_keys=ON` - Ensure referential integrity

### 5. Transaction Context Manager

**What it does:**
- Ensures transactions are committed or rolled back properly
- Helps keep transactions short for better concurrency

**Usage:**
```python
from src.edu_cti.core.db import db_transaction

with db_transaction(conn):
    conn.execute("INSERT INTO ...")
    # Automatically commits on exit
```

## How It Works

### Before (Without WAL)
```
Writer: [LOCKED] Writing to database...
Reader: [BLOCKED] Waiting for writer to finish...
Reader: [BLOCKED] Still waiting...
```

### After (With WAL)
```
Writer: [Writing] Updating database...
Reader: [Reading] Querying data... ✓ (no blocking)
Reader: [Reading] Querying data... ✓ (no blocking)
```

## API Connection Usage

### Read Operations (Default)
```python
from src.edu_cti.api.database import get_api_connection

# Read-only connection (default)
conn = get_api_connection()  # read_only=True by default
try:
    # Query data
    cur = conn.execute("SELECT ...")
    results = cur.fetchall()
finally:
    conn.close()
```

### Write Operations (Admin Only)
```python
from src.edu_cti.api.database import get_api_connection

# Write connection (admin endpoints only)
conn = get_api_connection(read_only=False)
try:
    # Write data
    conn.execute("INSERT INTO ...")
    conn.commit()
finally:
    conn.close()
```

## Pipeline Connection Usage

### Background Processes
```python
from src.edu_cti.core.db import get_connection

# Write connection with longer timeout
conn = get_connection(timeout=30.0, read_only=False)
try:
    # Long-running write operations
    with db_transaction(conn):
        # Multiple operations
        conn.execute("INSERT INTO ...")
        conn.execute("UPDATE ...")
        # Automatically commits
finally:
    conn.close()
```

## Monitoring

### Check WAL Mode Status
```sql
PRAGMA journal_mode;
-- Should return: wal
```

### Check Connection Count
```sql
PRAGMA compile_options;
```

### Monitor Database Locks
- Check Railway logs for connection timeout errors
- Monitor API response times during background operations

## Best Practices

1. **Always close connections**: Use try/finally or context managers
2. **Keep transactions short**: Commit frequently, don't hold locks for long
3. **Use read-only when possible**: API reads should use read-only connections
4. **Handle timeouts gracefully**: Retry on timeout errors
5. **Monitor performance**: Watch for slow queries during concurrent operations

## Troubleshooting

### Dashboard still shows no data during enrichment

1. **Check WAL mode is enabled:**
   ```python
   conn = get_connection()
   cur = conn.execute("PRAGMA journal_mode")
   print(cur.fetchone())  # Should be ('wal',)
   ```

2. **Check connection timeouts:**
   - API connections should have 5s timeout
   - Write connections should have 30s timeout

3. **Check for long-running transactions:**
   - Look for transactions that don't commit quickly
   - Use `db_transaction` context manager

### Database locked errors

1. **Increase timeout:**
   ```python
   conn = get_connection(timeout=60.0)  # 60 seconds
   ```

2. **Check for unclosed connections:**
   - Ensure all connections are closed in finally blocks
   - Use context managers where possible

3. **Check WAL mode:**
   - WAL mode should prevent most locking issues
   - If WAL mode fails (e.g., read-only filesystem), fallback to default mode

## Production Considerations

### Railway Deployment

- WAL mode works with Railway's persistent volumes
- Read-only connections work with mounted volumes
- Connection pooling not needed (SQLite handles this internally)

### Scaling

- SQLite with WAL mode handles moderate concurrent access well
- For very high traffic, consider:
  - Read replicas (copy database for reads)
  - PostgreSQL migration (for very high concurrency)
  - Caching layer (Redis) for frequently accessed data

## References

- [SQLite WAL Mode Documentation](https://www.sqlite.org/wal.html)
- [SQLite Concurrency](https://www.sqlite.org/faq.html#q5)
- [SQLite Performance Tuning](https://www.sqlite.org/performance.html)
