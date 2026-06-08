# Logging

EduThreat-CTI uses a single structured-logging setup built on
[`structlog`](https://www.structlog.org/) wrapping the standard library. One call
to `setup_logging()` (in `src/edu_cti/core/logging_utils.py`) configures the whole
process — every entry point (v2 worker, runtime, scheduler, API, and the legacy
phase1/phase2 CLIs) routes through it, and all existing
`logging.getLogger(__name__)` call sites are picked up automatically.

## Output formats

Controlled by the `LOG_FORMAT` env var:

| `LOG_FORMAT` | Output | Use |
|---|---|---|
| `json` | one JSON object per line | production (Railway) — filterable, aggregator-ready |
| `console` | pretty, coloured, aligned | local development |

Default: `json` when `RAILWAY_ENVIRONMENT` is set, otherwise `console`. The
Railway worker/API images bake `LOG_FORMAT=json`; flip a service to
`LOG_FORMAT=console` anytime to read it directly.

A JSON line looks like:

```json
{"timestamp":"2026-06-08T10:05:20.412Z","level":"info","logger":"edu_cti_v2.services.task_runtime",
 "event":"task_completed","task_id":"d1f2…","run_id":"87d9…","task_type":"enrich_source",
 "source_incident_id":"a3b1…","elapsed_ms":412}
```

## Context propagation — trace one task or incident

Identifiers are bound to a `contextvar` and merged into **every** downstream log
line, so you never have to thread them by hand:

- The worker binds `task_id`, `task_type`, `run_id`, `worker_id` for the duration
  of each task (`process_leased_task`), and `source_incident_id` for per-incident
  task types (fetch/resolve/enrich/canonicalize).
- The collectors bind `source` + `source_group` for the duration of each source.

Helpers (in `logging_utils.py`):

```python
from src.edu_cti.core.logging_utils import bind_log_context, unbind_log_context, clear_log_context

bind_log_context(task_id="…", run_id="…")   # None values are dropped
unbind_log_context("source")                  # remove specific keys
clear_log_context()                            # clear all (done at task boundary)
```

For new structured events, either use stdlib with `extra=` (rendered via
`ExtraAdder`) or a native structlog logger:

```python
logger.info("source_completed", extra={"incidents": 612, "elapsed_ms": 412})
# or
from src.edu_cti.core.logging_utils import get_structlog
get_structlog(__name__).info("source_completed", incidents=612, elapsed_ms=412)
```

## Filtering production logs

Pipe Railway JSON logs through `jq`:

```bash
# every line for one task across all stages
railway logs --service v2-worker | jq -c 'select(.task_id=="d1f2…")'

# everything that happened to one incident
railway logs --service v2-worker | jq -c 'select(.source_incident_id=="a3b1…")'

# only failures
railway logs --service v2-worker | jq -c 'select(.level=="error" or .event|test("failed"))'

# per-source collection summary
railway logs --service v2-worker | jq -c 'select(.event=="source_completed") | {source, incidents, elapsed_ms}'
```

## Event taxonomy (hot path)

| Event | Where | Key fields |
|---|---|---|
| `source_started` / `source_completed` / `source_failed` | collectors | `source`, `source_group`, `incidents`, `elapsed_ms`, `error` |
| `task_completed` / `task_failed` / `task_dead_lettered` | worker | `task_id`, `task_type`, `run_id`, `elapsed_ms`, `error` |

Other modules keep their existing log messages; they automatically gain clean
formatting plus whatever context is bound at the time.

## Levels & noise control

- `LOG_LEVEL` (default `INFO`) sets the root level.
- Per-logger overrides: `LOG_LEVEL_<logger>=WARNING` (e.g.
  `LOG_LEVEL_scrapling=ERROR`).
- Third-party noise is suppressed centrally: `httpx`, `httpcore`, `urllib3`,
  `playwright`, `curl_cffi`, `transformers`, `sentence_transformers`, `gliner`,
  `huggingface_hub` → WARNING. `scrapling` is additionally set to not propagate
  (it otherwise double-prints every fetch). HuggingFace progress bars and the
  gliner truncation `UserWarning` are silenced (in code and via the Dockerfile
  env `TRANSFORMERS_VERBOSITY=error`, `HF_HUB_DISABLE_PROGRESS_BARS=1`,
  `PYTHONWARNINGS`).
- Oversized messages (e.g. a degenerate raw LLM response) are truncated to 2000
  chars so a single line can't flood the logs.

## Optional: error aggregation

For alerting, a Sentry SDK init gated behind `SENTRY_DSN` can be added to
`setup_logging` later — not currently enabled.
