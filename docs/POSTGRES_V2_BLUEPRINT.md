# Postgres V2 Blueprint

**Status**: Approved target architecture  
**Scope**: Greenfield replacement for the current SQLite-backed runtime  
**Updated**: 2026-05-09

## Decisions Locked

- Database: `Postgres`
- Deployment: split into separate `API` and `Worker` services
- Migration style: `fresh start`, no legacy SQLite data migration
- Article retention: `extracted text + metadata`, not raw HTML
- Deduplication: `canonical incidents + lineage retained`
- Manual admin merge/split: `later phase`, after automatic canonicalization is stable

This document describes the implementation target for `v2`. It is intentionally
greenfield: we do not carry forward compatibility shims for the overloaded
SQLite schema.

## Goals

1. Preserve every collected source record as its own first-class entity.
2. Make deduplication reversible, explainable, and non-destructive.
3. Separate source-level enrichment from canonical incident enrichment.
4. Move long-running collection/enrichment work out of the API process.
5. Make dashboard and analytics reads fast and stable under load.
6. Use Postgres-native concurrency primitives instead of in-process locking hacks.

## Service Boundaries

### API Service

Responsibilities:

- Serve public incident and analytics endpoints
- Serve admin read/reporting endpoints
- Trigger worker jobs by writing `pipeline_runs` / `pipeline_tasks`
- Read canonical incidents, source lineage, and analytics summaries

Must not:

- run schedulers
- fetch articles
- call the LLM
- perform long-running deduplication or re-enrichment loops

### Worker Service

Responsibilities:

- Run the scheduler
- Collect source incidents
- Resolve URLs and fetch articles
- Enrich source incidents
- Canonicalize and deduplicate source incidents
- Refresh analytics rollups/materialized views
- Run re-enrichment and data-quality sweeps

Must not:

- serve end-user REST traffic

## Data Model Overview

The model is split into five layers:

1. `Source observation layer`
2. `Article fetch layer`
3. `Canonical incident layer`
4. `Enrichment layer`
5. `Pipeline execution layer`

### 1. Source Observation Layer

#### `source_incidents`

Purpose:

- one row per collected item from a source
- immutable-ish ingest record
- raw provenance anchor for lineage, replay, and re-enrichment

Important fields:

- source identity: `source_name`, `source_group`, `source_event_key`
- raw victim/title/date/location fields
- raw source payload in `jsonb`
- `ingest_hash` for idempotency and integrity checks

#### `source_incident_urls`

Purpose:

- retain all URLs seen for a source incident
- distinguish original wrapper URLs from resolved article URLs
- support URL-based dedup candidate generation without polluting canonical data

Important fields:

- `url`
- `normalized_url`
- `resolved_url`
- `url_kind`
- `is_wrapper`
- `is_resolved_primary`

#### `source_state`

Purpose:

- source-level incremental collection checkpoints
- stores query/window/cursor state without abusing the incident tables

Examples:

- Google News RSS query window progress
- RSS last published date
- paginated source cursor/page token

### 2. Article Fetch Layer

#### `article_documents`

Purpose:

- extracted article text and metadata only
- one row per fetched article document
- can be reused across retries and re-enrichment

Important fields:

- `source_incident_id`
- `source_incident_url_id`
- `title`, `author`, `publish_date`
- `content_text`
- `content_hash`
- `metadata jsonb`

#### `article_fetch_attempts`

Purpose:

- audit every fetch attempt by tier
- drive retry policy and source health analysis

Important fields:

- `fetch_tier`
- `latency_ms`
- `success`
- `http_status`
- `error_code`
- `error_message`

### 3. Canonical Incident Layer

#### `canonical_incidents`

Purpose:

- deduplicated real-world incidents shown in the API and dashboard
- best merged representation of one attack event

Important fields:

- canonical institution/location/date/attack labels
- `primary_source_incident_id`
- `canonical_summary`
- `resolution_version`
- `resolution_metadata`

#### `canonical_memberships`

Purpose:

- lineage-preserving mapping from source incident to canonical incident
- stores why/how the match happened

Important fields:

- `match_type`
- `match_score`
- `survivor_score`
- `field_contribution jsonb`
- `is_primary_member`
- `matcher_version`

This replaces destructive dedup. We do not delete the losing source record.

### 4. Enrichment Layer

#### `source_enrichments`

Purpose:

- LLM output for an individual source incident
- preserves source-specific extraction artifacts

Important fields:

- `raw_response`
- `raw_extraction`
- `typed_enrichment`
- `enrichment_confidence`
- `article_document_id`
- `failed_reason`

#### `canonical_enrichments`

Purpose:

- merged structured CTI for a canonical incident
- API and analytics read from here, not from individual source enrichments

Important fields:

- `selected_source_enrichment_id`
- `merged_from_source_enrichment_ids`
- `canonical_projection`
- `analytics_projection`
- `field_provenance`
- `completeness_score`

#### `canonical_timeline_events`

Purpose:

- normalized timeline rows for canonical incidents
- avoids repeated JSON parsing for dashboard/API use

### 5. Pipeline Execution Layer

#### `pipeline_runs`

Purpose:

- top-level run records for collect/fetch/enrich/canonicalize/analytics jobs

#### `pipeline_tasks`

Purpose:

- durable unit-of-work queue for the worker service
- supports leasing, retries, cancellation, and visibility across restarts

Important fields:

- `task_type`
- `target_id`
- `priority`
- `status`
- `available_at`
- `lease_owner`
- `lease_token`
- `lease_expires_at`
- `attempt_count`
- `max_attempts`

#### `analytics_refresh_state`

Purpose:

- tracks which summary tables or materialized views need refresh

## Canonicalization Strategy

Deduplication should move from “delete duplicate rows” to “attach source rows to a canonical incident”.

### Candidate Generation

Perform SQL-first narrowing using:

- exact `normalized_url`
- exact `resolved_url`
- normalized institution/vendor/platform keys
- date window overlap
- country consistency
- trigram similarity on names

### Match Scoring

Scoring inputs:

- URL exact match
- URL resolved match
- normalized institution exact/near match
- vendor/platform exact match
- date agreement
- attack category agreement
- threat actor/ransomware agreement
- location conflict penalty
- “headline only / no concrete target” penalty

### Resolution

- if score >= threshold: attach to existing canonical incident
- else: create new canonical incident

### Primary Member Selection

The primary member should be chosen by score, not just source precedence.

Signals:

- concrete institution extracted
- valid resolved article
- better date precision
- richer structured field coverage
- stronger source confidence
- lower headline-ness

## Task Leasing Model

Worker task acquisition should use Postgres row leasing:

1. select candidate tasks with `status = 'queued'` and `available_at <= now()`
2. lock with `FOR UPDATE SKIP LOCKED`
3. set:
   - `status = 'leased'`
   - `lease_owner`
   - `lease_token`
   - `lease_expires_at`
4. commit
5. worker processes the task
6. worker updates task to `completed`, `failed`, or `queued` with backoff

This removes the need for in-process queue state as the source of truth.

## Repository Boundaries

The current codebase has too much SQL and orchestration logic spread across large
modules. `v2` should introduce explicit repository and service layers.

Recommended package layout:

```text
src/edu_cti_v2/
  db/
    connection.py
    migrations/
    types.py
  repositories/
    source_incidents.py
    source_urls.py
    source_state.py
    article_documents.py
    article_fetch_attempts.py
    source_enrichments.py
    canonical_incidents.py
    canonical_memberships.py
    canonical_enrichments.py
    timeline_events.py
    pipeline_runs.py
    pipeline_tasks.py
    analytics_refresh.py
  services/
    collection_service.py
    fetch_service.py
    enrichment_service.py
    canonicalization_service.py
    analytics_service.py
  workers/
    scheduler.py
    task_runner.py
    lease_manager.py
  api/
    public/
    admin/
```

Repository rules:

- repositories contain SQL and row mapping only
- services contain orchestration and business rules
- workers contain task execution loops only
- API layer only calls services/read repositories

## Initial Migration Set

These are the first migrations I would create.

### `0001_extensions`

- enable `pgcrypto`
- enable `pg_trgm`
- enable `unaccent`

### `0002_source_observation`

- create `source_incidents`
- create `source_incident_urls`
- create `source_state`

### `0003_article_fetch`

- create `article_documents`
- create `article_fetch_attempts`

### `0004_enrichment_source`

- create `source_enrichments`

### `0005_canonical_core`

- create `canonical_incidents`
- create `canonical_memberships`
- create `canonical_enrichments`
- create `canonical_timeline_events`

### `0006_pipeline_runtime`

- create `pipeline_runs`
- create `pipeline_tasks`
- create `analytics_refresh_state`

### `0007_indexes_and_search`

- add trigram and hot-path indexes
- add partial indexes for leased/queued task access

## Recommended Build Order

1. Create Postgres schema and migrations.
2. Stand up new `worker` service with `pipeline_runs` / `pipeline_tasks`.
3. Port Phase 1 to write `source_incidents` and `source_incident_urls`.
4. Add fetch and source-enrichment workers.
5. Add canonicalization worker.
6. Add canonical read model and API service.
7. Add analytics summary tables/materialized views.
8. Add manual admin merge/split operations later.

## Acceptance Criteria For V2 Cutover

Before the dashboard/API switches to `v2`, the following must be true:

- duplicate source records do not destroy lineage
- reset/re-enrich never loses source observations
- canonical incident detail can show source member lineage
- Google wrapper URLs never surface as canonical article URLs
- source-level and canonical-level enrichment are both queryable
- worker restarts do not lose leased tasks permanently
- API remains responsive while collection/enrichment runs in the worker

## Non-Goals For First Cut

- manual merge/split UI
- full old-data migration from SQLite
- raw HTML archival
- multi-tenant or multi-database support

Those can come later once automatic canonicalization is stable.
