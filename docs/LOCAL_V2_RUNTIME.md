# Local V2 Runtime

This runbook starts a Railway-like v2 stack locally for multi-hour pipeline
soak tests before pushing changes to production.

The local Postgres database runs in Docker with a named volume, so data survives
when the worker/API stop:

- container: `eduthreat-v2-postgres-local`
- volume: `eduthreat_v2_postgres_data`
- local port: `55433`

## One-Time Setup

```bash
scripts/local_v2_stack.sh init-env
scripts/local_v2_stack.sh install-deps
scripts/local_v2_stack.sh db-up
scripts/local_v2_stack.sh migrate
scripts/local_v2_stack.sh preflight
```

The helper loads `.env` first and `.env.local` second. Keep secrets such as
`OLLAMA_API_KEY` in `.env`; keep local Postgres/runtime overrides in
`.env.local`.

## Run The Local Stack

Use separate terminal tabs:

```bash
scripts/local_v2_stack.sh api
```

```bash
scripts/local_v2_stack.sh worker
```

Then enqueue a plan without synchronously draining tasks:

```bash
scripts/local_v2_stack.sh run-plan rss_fast_refresh
```

For a larger local coverage run:

```bash
scripts/local_v2_stack.sh run-plan incremental_refresh
```

Only use `historical_full` when you intentionally want a long backfill.

## Dashboard

The helper creates `../EduThreat-CTI-Dashboard/.env.local` when that dashboard
repo exists next to this repo:

```bash
NEXT_PUBLIC_API_URL=http://localhost:8000
```

Start the dashboard from the dashboard repo:

```bash
cd ../EduThreat-CTI-Dashboard
npm run dev
```

Open:

- API docs: `http://127.0.0.1:8000/docs`
- API health: `http://127.0.0.1:8000/api/health`
- Dashboard: `http://localhost:3000`

Local admin defaults are in `.env.local`:

- username: `admin`
- password: `admin123`
- API key: `local-dev-admin-key`

## Tuning Local Speed

Edit `.env.local`:

```bash
EDU_CTI_V2_WORKER_COUNT=4
EDU_CTI_V2_FETCH_WORKER_COUNT=2
EDU_CTI_V2_RESOLVE_WORKER_COUNT=1
EDU_CTI_V2_CANONICALIZE_WORKER_COUNT=1
```

This is intentionally close to the Railway worker shape while keeping browser
fetches bounded:

```bash
EDU_CTI_FETCH_ENABLE_SCRAPLING_BROWSER=1
EDU_CTI_SCRAPLING_BROWSER_MODE=dynamic
EDU_CTI_SCRAPLING_BROWSER_MAX_CONCURRENCY=1
EDU_CTI_ARTICLE_MIN_CONTENT_CHARS=500
```

Free URL discovery can use Google News RSS, but local EU runs often get blocked
while decoding Google wrapper links. For local soak tests, keep Google disabled
and use Bing News RSS first because it exposes direct article URLs. Oxylabs SERP
stays off unless you explicitly enable it:

```bash
EDU_CTI_ENABLE_GOOGLE_NEWS_DISCOVERY=0
EDU_CTI_ENABLE_BING_NEWS_DISCOVERY=1
EDU_CTI_ENABLE_OXYLABS_SERP=0
EDU_CTI_GOOGLE_NEWS_DECODE_TIMEOUT_SECONDS=4
EDU_CTI_NEWS_DISCOVERY_DECODE_LIMIT=20
```

To test last-resort stealth locally:

```bash
EDU_CTI_SCRAPLING_BROWSER_MODE=stealthy
EDU_CTI_FETCH_ENABLE_SCRAPLING_STEALTH_LAST=1
```

## Stop Without Losing Data

Stop API/worker with `Ctrl+C`.

Stop Postgres without deleting data:

```bash
scripts/local_v2_stack.sh db-down
```

Start it again later:

```bash
scripts/local_v2_stack.sh db-up
```

Do not delete the Docker volume unless you intentionally want a clean reset.
