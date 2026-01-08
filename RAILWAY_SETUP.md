# Railway Deployment Setup Guide

## Persistent Storage Setup

### 1. Create Volume in Railway

1. Go to your Railway project dashboard
2. Click **"New"** → **"Volume"**
3. Name it `data` (or any name you prefer)
4. Mount path: `/app/data`
5. Click **"Add"**

### 2. Link Volume to Service

1. Go to your service settings
2. Under **"Volumes"**, click **"Add Volume"**
3. Select the volume you created
4. Mount path: `/app/data`
5. Click **"Add"**

### 3. Environment Variables

Set these in Railway → Variables:

```bash
# Database (uses persistent volume)
EDU_CTI_DATA_DIR=/app/data
EDU_CTI_DB_PATH=eduthreat.db

# Ollama Cloud API
OLLAMA_API_KEY=<your-ollama-api-key>
OLLAMA_HOST=https://ollama.com
OLLAMA_MODEL=deepseek-v3.1:671b-cloud

# Admin Panel (optional - change defaults!)
EDUTHREAT_ADMIN_USERNAME=admin
EDUTHREAT_ADMIN_PASSWORD_HASH=<sha256-hash-of-password>
EDUTHREAT_ADMIN_API_KEY=<secure-random-key>

# VPN (optional)
EDUTHREAT_USE_VPN=false
EDUTHREAT_VPN_AUTO_ROTATE=false

# Logging
EDU_CTI_LOG_LEVEL=INFO
```

### 4. Migrate Existing Database

If you have an existing database in the repo:

**Option A: Via Railway CLI (Recommended)**
```bash
# Connect to Railway
railway link

# Copy local DB to Railway volume
railway run python scripts/migrate_db_to_railway.py
```

**Option B: Manual Copy**
```bash
# SSH into Railway container
railway shell

# Copy DB (if you have it locally)
# Upload via Railway dashboard → Volumes → Upload files
```

**Option C: Let it create fresh**
- If no DB exists, it will be created automatically on first run
- Run historical ingestion to populate: `python -m src.edu_cti.scheduler.scheduler --mode historical`

### 5. Verify Setup

1. Check logs: Railway → Deployments → View Logs
2. Check health: `curl https://your-app.railway.app/api/health`
3. Check metrics: `curl https://your-app.railway.app/metrics`
4. Check admin panel: `https://your-app.railway.app/admin` (via dashboard)

## Monitoring

### Prometheus Metrics

Metrics are available at `/metrics` endpoint:
- `rss_ingestion_incidents` - Counter of incidents from RSS
- `weekly_ingestion_incidents` - Counter of incidents from weekly runs
- `scheduler_job_*_duration_seconds` - Histogram of job durations
- `scheduler_job_*_total` - Counter of job runs (success/error)

### Logs

All scheduler jobs log to stdout/stderr, visible in Railway logs:
- `[SCHEDULER]` - Scheduler job logs
- `[METRIC]` - Metrics updates
- `[ADMIN]` - Admin-triggered jobs

## Running Scheduler

### Option 1: Via Admin Panel (Recommended)
1. Login at `/admin`
2. Click "Run RSS Ingestion" or "Run Weekly Ingestion"
3. View logs in Railway dashboard

### Option 2: Via Railway CLI
```bash
railway run python -m src.edu_cti.scheduler.scheduler --mode rss-once
railway run python -m src.edu_cti.scheduler.scheduler --mode weekly-once
```

### Option 3: Continuous Scheduler (Background)
```bash
# Start continuous scheduler (RSS every 2hrs, weekly on Sunday 2 AM)
railway run python -m src.edu_cti.scheduler.scheduler
```

## Troubleshooting

### Database not found
- Check volume is mounted at `/app/data`
- Check `EDU_CTI_DATA_DIR=/app/data` is set
- Check logs for database initialization errors

### Scheduler not working
- Check logs for errors
- Verify Ollama API key is set
- Check metrics endpoint for job status

### Chrome/Selenium issues
- Railway automatically uses headless mode (no display)
- If sites block headless, they may need different scraping approach
- Check logs for Selenium errors

### Low disk space
- Railway volumes have size limits
- Monitor via Railway dashboard
- Consider archiving old data periodically
