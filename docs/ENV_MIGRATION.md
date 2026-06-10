# Environment variable migration (prefix removal)

The project is dropping the long `EDU_CTI_V2_` / `EDU_CTI_` prefixes in favour of
short, unprefixed names. Every read now goes through `src/edu_cti_v2/env.py`
`get_env()`, which tries the **new name first**, then the **legacy prefixed name**,
then the default. **Nothing breaks during migration:** the old Railway env names
keep working until you switch them over.

## Status: migration applied on Railway (2026-06-10)
The new unprefixed names are set on **both** `v2-api` and `v2-worker`, and all legacy
`EDU_CTI*` variables have been **deleted** — with ONE deliberate exception:

> **`EDU_CTI_V2_DATABASE_URL` is kept.** It is the database connection string and no
> replacement (`DB_URL` / standard `DATABASE_URL`) is configured on the services. The
> code reads `get_env("DB_URL", "EDU_CTI_V2_DATABASE_URL", "DATABASE_URL")`, so it still
> resolves. To finish the rename: in the Railway dashboard set `DB_URL` to the Postgres
> connection (e.g. reference `${{Postgres.DATABASE_URL}}`), confirm the service boots,
> then delete `EDU_CTI_V2_DATABASE_URL`. (Left to the dashboard to avoid handling the
> secret via CLI.)

The new env applies on the next deploy; running containers keep their in-memory env until
then. The original old→new reference table is retained below for completeness.

## (Historical) How to migrate on Railway
1. For each row below, add the **new** variable (same value as the legacy one).
2. Verify the services boot and behave (both names work simultaneously).
3. Once confirmed, delete the **legacy** variable.

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

## Batch 2 — `edu_cti` phase-2 fetch / discovery / extraction (done)
Every remaining `EDU_CTI_*` read now goes through `get_env` with the unprefixed
name first and the legacy `EDU_CTI_*` fallback. **A full-repo sweep confirms no
bare `EDU_CTI_` env reads remain.** New names (drop the `EDU_CTI_` prefix):

| New name | Legacy name |
|---|---|
| `NEWS_DISCOVERY_MAX_RESULTS` | `EDU_CTI_NEWS_DISCOVERY_MAX_RESULTS` |
| `NEWS_DISCOVERY_DECODE_LIMIT` | `EDU_CTI_NEWS_DISCOVERY_DECODE_LIMIT` |
| `GOOGLE_NEWS_DECODE_TIMEOUT_SECONDS` | `EDU_CTI_GOOGLE_NEWS_DECODE_TIMEOUT_SECONDS` |
| `GOOGLE_NEWS_RSS_REQUEST_DELAY_SECONDS` | `EDU_CTI_GOOGLE_NEWS_RSS_REQUEST_DELAY_SECONDS` |
| `SOURCE_TIMEOUT_SECONDS` | `EDU_CTI_SOURCE_TIMEOUT_SECONDS` |
| `UNIFY_LISTING_FETCH` | `EDU_CTI_UNIFY_LISTING_FETCH` |
| `KEYWORDS_PATH` | `EDU_CTI_KEYWORDS_PATH` |
| `DATA_DIR` | `EDU_CTI_DATA_DIR` |
| `DB_PATH` | `EDU_CTI_DB_PATH` |
| `METRICS_DB_PATH` | `EDU_CTI_METRICS_DB_PATH` |
| `LOG_LEVEL` | `EDU_CTI_LOG_LEVEL` (also the standard `LOG_LEVEL`) |
| `LOG_FILE` | `EDU_CTI_LOG_FILE` |
| `FETCH_TIER_PROFILE` | `EDU_CTI_FETCH_TIER_PROFILE` |
| `FETCH_ENABLE_*` / `FETCH_DISABLE_*` | `EDU_CTI_FETCH_ENABLE_*` / `EDU_CTI_FETCH_DISABLE_*` |
| `ARTICLE_MIN_CONTENT_CHARS` | `EDU_CTI_ARTICLE_MIN_CONTENT_CHARS` |
| `DATABREACHES_ARTICLE_MIN_CONTENT_CHARS` | `EDU_CTI_DATABREACHES_ARTICLE_MIN_CONTENT_CHARS` |
| `SCRAPLING_*` (browser mode, proxy, impersonate, cdp, trigger reasons, …) | `EDU_CTI_SCRAPLING_*` |
| `DYNAMIC_BLOCK_FAILURE_THRESHOLD` | `EDU_CTI_DYNAMIC_BLOCK_FAILURE_THRESHOLD` |

In `article_fetcher.py` the tier-enable flags route through prefix-aware helpers,
so every `EDU_CTI_FETCH_*` / `EDU_CTI_SCRAPLING_*` flag responds to its unprefixed
name automatically.

Unchanged (already standard/unprefixed): `PORT`, `OLLAMA_*`, `OXYLABS_USERNAME/PASSWORD`,
`EDUTHREAT_ADMIN_*`, `DATABASE_URL`, `PHASE2_*`, `RAILWAY_*`, `DISABLE_ML_FEATURES`.
