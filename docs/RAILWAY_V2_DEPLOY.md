# Railway V2 Deployment

This guide explains how to deploy the fresh Postgres-backed `v2` stack on Railway.

## Target Topology

Create `3` Railway services in the same project:

1. `Postgres`
2. `v2-api`
3. `v2-worker`

Use the same repository and Docker image for both `v2-api` and `v2-worker`. Only the start command changes.

## What Changes Compared to the Old Stack

- no SQLite volume is required for the new `v2` runtime
- the API and worker are separate services
- canonical incidents and source lineage live in Postgres
- recurring scheduler jobs run in the worker service, not the API

## Start Commands

### v2-api

```bash
eduthreat-v2-api --host 0.0.0.0
```

### v2-worker

Normal recurring mode:

```bash
eduthreat-v2-runtime --workers 2
```

Initial backfill mode without recurring scheduler jobs:

```bash
eduthreat-v2-runtime --workers 2 --no-scheduler
```

You can also control that with:

```bash
EDU_CTI_V2_ENABLE_SCHEDULER=0
```

## One-Time Migration Command

Run this once after Postgres is provisioned and the service image is deployed:

```bash
eduthreat-v2-migrate upgrade head
```

## Required Environment Variables

Set these on both `v2-api` and `v2-worker` unless noted otherwise.

### Postgres

- `EDU_CTI_V2_DATABASE_URL`
- `ALEMBIC_DATABASE_URL`

Recommended value on Railway:

- point both at the Railway Postgres `DATABASE_URL`

### Admin auth

Set on `v2-api`:

- `EDUTHREAT_ADMIN_API_KEY`

Recommended:

- use the API key path first
- optionally also set `EDUTHREAT_ADMIN_USERNAME`
- optionally also set `EDUTHREAT_ADMIN_PASSWORD_HASH`

### LLM enrichment

Set on `v2-worker`, and mirror to `v2-api` if you want preflight to reflect the real worker config:

- `OLLAMA_API_KEY`
- `OLLAMA_HOST`
- `OLLAMA_MODEL`

Recommended starting value:

- `OLLAMA_MODEL=deepseek-v3.1:671b-cloud`

### Oxylabs and paid-search collection

Set on `v2-worker`, and mirror to `v2-api` if you want preflight visibility:

- `OXYLABS_USERNAME`
- `OXYLABS_PASSWORD`
- `ENABLE_OXYLABS_NEWS_HISTORICAL`
- `ENABLE_OXYLABS_NEWS_DAILY`

Recommended starting values:

- `ENABLE_OXYLABS_NEWS_HISTORICAL=1`
- `ENABLE_OXYLABS_NEWS_DAILY=0`

### Worker/runtime tuning

Set on `v2-worker`:

- `EDU_CTI_V2_WORKER_COUNT=2`
- `EDU_CTI_V2_ENABLE_SCHEDULER=0` for the initial historical backfill
- `LOG_LEVEL=INFO`

Optional DB tuning:

- `EDU_CTI_V2_DB_POOL_SIZE=10`
- `EDU_CTI_V2_DB_MAX_OVERFLOW=20`
- `EDU_CTI_V2_DB_POOL_TIMEOUT=30`
- `EDU_CTI_V2_DB_POOL_RECYCLE=1800`
- `EDU_CTI_V2_DB_STATEMENT_TIMEOUT_MS=30000`
- `EDU_CTI_V2_TASK_LEASE_SECONDS=300`

## Recommended Bring-Up Sequence

### 1. Create the Postgres service

- add a fresh Railway Postgres database
- do not reuse the old SQLite-backed volume

### 2. Create the `v2-api` service

- same repo
- same Dockerfile
- start command: `eduthreat-v2-api --host 0.0.0.0`

### 3. Create the `v2-worker` service

- same repo
- same Dockerfile
- start command: `eduthreat-v2-runtime --workers 2 --no-scheduler`

Start with scheduler disabled for the first historical run so the worker does not also fire recurring incremental plans during the backfill.

### 4. Set environment variables

Set all required secrets and URLs before the first migration.

### 5. Run migrations

From a Railway shell or one-off command:

```bash
eduthreat-v2-migrate upgrade head
```

### 6. Run preflight

From a Railway shell:

```bash
eduthreat-v2-preflight --require-ready
```

Or via the API after login:

- `POST /api/admin/v2/login`
- `GET /api/admin/v2/preflight`

### 7. Start the first fresh historical run

Login:

```bash
curl -X POST "$API_BASE/api/admin/v2/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"YOUR_PASSWORD"}'
```

Run a plan:

```bash
curl -X POST "$API_BASE/api/admin/v2/run-plan?plan_name=historical_full"
```

Or for maximum paid coverage:

```bash
curl -X POST "$API_BASE/api/admin/v2/run-plan?plan_name=historical_max_coverage"
```

### 8. After historical backfill finishes

Change the worker service to enable recurring scheduling:

- either update the start command to `eduthreat-v2-runtime --workers 2`
- or keep the same start command and set `EDU_CTI_V2_ENABLE_SCHEDULER=1`

Then redeploy/restart the worker.

## What to Monitor

### Health

- `/health`
- `/api/health`
- `/api/v2/health`

### Admin/runtime

- `/api/admin/v2/preflight`
- `/api/admin/v2/status`
- `/api/admin/v2/tasks`
- `/api/admin/v2/runs`
- `/api/admin/v2/scheduler/status`

### Logs

Watch the worker logs for:

- collection progress
- fetch failures
- enrichment failures
- canonicalization progress
- analytics refresh completions

## First Historical Run Tips

- start with `historical_full`
- use `historical_max_coverage` only when Oxylabs credentials and cost expectations are confirmed
- keep scheduler disabled during initial backfill
- do not cut the frontend over to `/api/v2` until the first end-to-end dataset looks healthy

## Minimum Ready Checklist

Before you start the first historical run, all of these should be true:

- Postgres exists and is reachable
- `eduthreat-v2-migrate upgrade head` has completed
- `eduthreat-v2-preflight --require-ready` exits successfully
- API service health endpoint is green
- worker service is running
- admin auth works on `/api/admin/v2/login`

