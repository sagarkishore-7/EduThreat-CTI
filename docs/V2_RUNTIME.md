# Postgres V2 Runtime

This document describes the operational entrypoints for the fresh Postgres-backed `v2` stack.

For the full Railway service-by-service deployment guide, see
[RAILWAY_V2_DEPLOY.md](RAILWAY_V2_DEPLOY.md).

## Service Split

The `v2` rollout is designed around two long-running services:

- `API service`
  - serves `/api/v2/*` and `/api/admin/v2/*`
  - does not run ingestion or enrichment jobs
- `Worker service`
  - runs recurring plan scheduling
  - drains `pipeline_tasks`
  - performs resolve, fetch, enrich, canonicalize, and analytics refresh work

## CLI Entrypoints

These commands are installed from `pyproject.toml`:

- `eduthreat-v2-api`
- `eduthreat-v2-worker`
- `eduthreat-v2-scheduler`
- `eduthreat-v2-runtime`
- `eduthreat-v2-migrate`
- `eduthreat-v2-preflight`

## Recommended Production Commands

### 1. Run schema migrations

```bash
eduthreat-v2-migrate upgrade head
```

Optional preflight after deploy:

- `eduthreat-v2-preflight --require-ready`
- `GET /api/admin/v2/preflight`
- `POST /api/admin/v2/login`

### 2. Start the dedicated v2 API

```bash
eduthreat-v2-api --host 0.0.0.0
```

### 3. Start the unified worker runtime

```bash
eduthreat-v2-runtime --workers 2
```

This starts:

- the recurring scheduler
- a pool of long-running worker threads

If you need worker-only execution without recurring scheduling:

```bash
eduthreat-v2-runtime --workers 2 --no-scheduler
```

## Environment Variables

### Database

- `EDU_CTI_V2_DATABASE_URL`
- `ALEMBIC_DATABASE_URL`
- `EDU_CTI_V2_DB_POOL_SIZE`
- `EDU_CTI_V2_DB_MAX_OVERFLOW`
- `EDU_CTI_V2_DB_POOL_TIMEOUT`
- `EDU_CTI_V2_DB_POOL_RECYCLE`
- `EDU_CTI_V2_DB_STATEMENT_TIMEOUT_MS`
- `EDU_CTI_V2_TASK_LEASE_SECONDS`

### Runtime

- `EDU_CTI_V2_WORKER_COUNT`
- `EDU_CTI_V2_ENABLE_SCHEDULER`
- `LOG_LEVEL`
- existing source/API credentials used by collection and enrichment

### Fetch And Discovery Budget Controls

- `EDU_CTI_FETCH_TIER_PROFILE=scrapling_first` is the default low-cost article chain: Scrapling -> newspaper3k rescue -> Oxylabs -> archive.org.
- `EDU_CTI_FETCH_ENABLE_NEWSPAPER=0` or `EDU_CTI_FETCH_DISABLE_NEWSPAPER=1` disables newspaper3k rescue if it causes source-specific issues.
- `EDU_CTI_FETCH_ENABLE_SCRAPLING_BROWSER=1` enables the optional browser-backed Scrapling rescue tier. It is disabled by default because it launches Chromium.
- `EDU_CTI_SCRAPLING_BROWSER_MODE=dynamic|stealthy` selects the browser rescue posture. Start with `dynamic`; when set to `stealthy`, the normal early browser rescue still uses DynamicFetcher and StealthyFetcher is reserved as the last fallback after archive.org fails.
- `EDU_CTI_FETCH_ENABLE_SCRAPLING_STEALTH_LAST=1` explicitly enables last-resort StealthyFetcher after archive.org. This defaults to enabled when `EDU_CTI_SCRAPLING_BROWSER_MODE=stealthy`.
- `EDU_CTI_SCRAPLING_BROWSER_TRIGGER_REASONS=403,empty_content,soft_404` controls which static-Scrapling failures are allowed to launch the browser rescue tier.
- `EDU_CTI_SCRAPLING_BROWSER_MAX_CONCURRENCY=1` caps concurrent browser-backed Scrapling fetches per worker process.
- `EDU_CTI_SCRAPLING_PROXY_URL` or `EDU_CTI_SCRAPLING_PROXY_POOL` can provide known-good proxies for browser Scrapling. The runtime does not auto-discover free proxies because they are noisy, unsafe, and often blocked.
- `EDU_CTI_FETCH_ENABLE_LEGACY_TIERS=1` is the rollback switch that allows the heavier HttpClient/curl_cffi/Playwright article tier again.
- `EDU_CTI_OXYLABS_ENABLED=0` disables all Oxylabs article fetch and paid SERP calls, even if credentials are present.
- `EDU_CTI_ENABLE_OXYLABS_SERP=0` keeps paid SERP disabled. URL discovery still runs through free Google News RSS via Scrapling.
- `EDU_CTI_ENABLE_YAHOO_NEWS_DISCOVERY=1` enables the optional Yahoo News HTML fallback; keep disabled unless consent pages are no longer observed.
- `EDU_CTI_NEWS_DISCOVERY_MAX_RESULTS=5` caps discovered article URLs per URL-less incident.
- `EDU_CTI_FETCH_DISABLE_OXYLABS=1` disables Oxylabs only inside the article fetch chain.
- `EDU_CTI_DATABREACHES_ARCHIVE_ENABLED=1` re-enables the DataBreaches education archive crawler. It is disabled by default because the category archive currently returns anti-bot/challenge pages and the free RSS feed remains available.
- `EDU_CTI_DATABREACHES_OXYLABS_FALLBACK=1` allows the DataBreaches archive crawler to try Oxylabs on failed category pages.

## Railway Notes

For the first live `v2` bring-up:

1. provision fresh Postgres
2. run `eduthreat-v2-migrate upgrade head`
3. deploy one API service with `eduthreat-v2-api --host 0.0.0.0`
4. deploy one worker service with `eduthreat-v2-runtime --workers 2`
5. trigger a first named plan from `/api/admin/v2/run-plan`

The Docker image now includes:

- `alembic/`
- `alembic.ini`

so the migration command works inside deployed containers.
