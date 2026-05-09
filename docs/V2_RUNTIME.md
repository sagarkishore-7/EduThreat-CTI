# Postgres V2 Runtime

This document describes the operational entrypoints for the fresh Postgres-backed `v2` stack.

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

## Recommended Production Commands

### 1. Run schema migrations

```bash
eduthreat-v2-migrate upgrade head
```

Optional preflight after deploy:

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
- `LOG_LEVEL`
- existing source/API credentials used by collection and enrichment

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
