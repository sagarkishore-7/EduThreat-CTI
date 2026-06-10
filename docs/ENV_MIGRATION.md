# Environment variable migration (prefix removal)

The project is dropping the long `EDU_CTI_V2_` / `EDU_CTI_` prefixes in favour of
short, unprefixed names. Every read now goes through `src/edu_cti_v2/env.py`
`get_env()`, which tries the **new name first**, then the **legacy prefixed name**,
then the default. **Nothing breaks during migration:** the old Railway env names
keep working until you switch them over.

## How to migrate on Railway
1. For each row below, add the **new** variable (same value as the legacy one).
2. Verify the services boot and behave (both names work simultaneously).
3. Once confirmed, delete the **legacy** variable.

Set the new names on **both** `v2-api` and `v2-worker` where applicable.

## Batch 1 — v2 worker / database / runtime (done)

| New name | Legacy name | Notes |
|---|---|---|
| `DB_URL` | `EDU_CTI_V2_DATABASE_URL` | also falls back to standard `DATABASE_URL` |
| `DB_USER` | `EDU_CTI_V2_DB_USER` | |
| `DB_PASSWORD` | `EDU_CTI_V2_DB_PASSWORD` | |
| `DB_HOST` | `EDU_CTI_V2_DB_HOST` | |
| `DB_PORT` | `EDU_CTI_V2_DB_PORT` | |
| `DB_NAME` | `EDU_CTI_V2_DB_NAME` | |
| `DB_DRIVER` | `EDU_CTI_V2_DB_DRIVER` | |
| `DB_ECHO` | `EDU_CTI_V2_DB_ECHO` | |
| `DB_POOL_SIZE` | `EDU_CTI_V2_DB_POOL_SIZE` | |
| `DB_MAX_OVERFLOW` | `EDU_CTI_V2_DB_MAX_OVERFLOW` | |
| `DB_POOL_TIMEOUT` | `EDU_CTI_V2_DB_POOL_TIMEOUT` | |
| `DB_POOL_RECYCLE` | `EDU_CTI_V2_DB_POOL_RECYCLE` | |
| `DB_STATEMENT_TIMEOUT_MS` | `EDU_CTI_V2_DB_STATEMENT_TIMEOUT_MS` | |
| `DB_APP_NAME` | `EDU_CTI_V2_DB_APP_NAME` | |
| `DB_SCHEMA` | `EDU_CTI_V2_DB_SCHEMA` | |
| `TASK_LEASE_SECONDS` | `EDU_CTI_V2_TASK_LEASE_SECONDS` | |
| `WORKER_COUNT` | `EDU_CTI_V2_WORKER_COUNT` | |
| `FETCH_WORKER_COUNT` | `EDU_CTI_V2_FETCH_WORKER_COUNT` | |
| `RESOLVE_WORKER_COUNT` | `EDU_CTI_V2_RESOLVE_WORKER_COUNT` | |
| `CANONICALIZE_WORKER_COUNT` | `EDU_CTI_V2_CANONICALIZE_WORKER_COUNT` | |
| `MAX_ACTIVE_ENRICH_TASKS` | `EDU_CTI_V2_MAX_ACTIVE_ENRICH_TASKS` | |
| `ALLOW_HIGH_ENRICH_CONCURRENCY` | `EDU_CTI_V2_ALLOW_HIGH_ENRICH_CONCURRENCY` | |
| `MAX_FETCH_BACKLOG` | `EDU_CTI_V2_MAX_FETCH_BACKLOG` | |
| `MAX_WORKERS` | `EDU_CTI_V2_MAX_WORKERS` | |
| `MODEL_FLOOR_MB` | `EDU_CTI_V2_MODEL_FLOOR_MB` | |
| `PER_WORKER_MB` | `EDU_CTI_V2_PER_WORKER_MB` | |
| `MAX_RSS_MB` | `EDU_CTI_V2_MAX_RSS_MB` | |
| `ENABLE_LOCAL_ML` | `EDU_CTI_V2_ENABLE_LOCAL_ML` | |
| `PREWARM_MODELS` | `EDU_CTI_V2_PREWARM_MODELS` | |
| `IDLE_RESOURCE_RELEASE_SECONDS` | `EDU_CTI_V2_IDLE_RESOURCE_RELEASE_SECONDS` | |
| `ENABLE_SCHEDULER` | `EDU_CTI_V2_ENABLE_SCHEDULER` | |
| `COLLECTOR_VERSION` | `EDU_CTI_V2_COLLECTOR_VERSION` | |
| `PHASE1_DUAL_WRITE` | `EDU_CTI_V2_PHASE1_DUAL_WRITE` | |
| `FETCH_BACKLOG_LIMIT` | `EDU_CTI_V2_FETCH_BACKLOG_LIMIT` | |
| `RESOLVE_BACKLOG_LIMIT` | `EDU_CTI_V2_RESOLVE_BACKLOG_LIMIT` | |
| `FETCH_BACKLOG_RESUME_RATIO` | `EDU_CTI_V2_FETCH_BACKLOG_RESUME_RATIO` | |
| `RESOLVE_BACKLOG_RESUME_RATIO` | `EDU_CTI_V2_RESOLVE_BACKLOG_RESUME_RATIO` | |
| `BACKLOG_POLL_SECONDS` | `EDU_CTI_V2_BACKLOG_POLL_SECONDS` | |
| `OXYLABS_ENABLED` | `EDU_CTI_OXYLABS_ENABLED` | |
| `NEWS_MAX_PAGES` | `EDU_CTI_NEWS_MAX_PAGES` | default raised 50→100 |
| `NEWS_EXACT_PHRASE_MAX_PAGES` | `EDU_CTI_NEWS_EXACT_PHRASE_MAX_PAGES` | |
| `GOOGLE_NEWS_RSS_HISTORICAL_WINDOW_DAYS` | `EDU_CTI_GOOGLE_NEWS_RSS_HISTORICAL_WINDOW_DAYS` | default 31→21 |
| `COLLECT_MAX_PAGES` | — (new) | bounds historical news page-walk; unset = all |
| `RSS_MAX_AGE_DAYS` | — (new) | historical RSS look-back; default 3650 |

## Removed (dead) variables
These legacy aliases are no longer read (they all duplicated `OXYLABS_ENABLED`):
`EDU_CTI_INCLUDE_PAID_RSS`, `EDU_CTI_INCLUDE_OXYLABS_NEWS_SOURCE`,
`EDU_CTI_OXYLABS_NEWS_ENABLED`. Delete them from Railway.

## Batch 2 — `edu_cti` phase-2 fetch / discovery / extraction (pending)
The remaining single-prefix `EDU_CTI_*` vars (fetch tiers, Scrapling/browser,
discovery, timeouts, logging) are migrated in a follow-up; their legacy names keep
working until then. Unprefixed standard names (`PORT`, `LOG_LEVEL`, `OLLAMA_*`,
`OXYLABS_USERNAME/PASSWORD`, `EDUTHREAT_ADMIN_*`, `DATABASE_URL`) are unchanged.
