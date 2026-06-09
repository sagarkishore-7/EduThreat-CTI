# REST API Documentation

**Version**: 2.8.0  
**Current Public Surface**: Postgres-backed `v2` API  
**Base URL**: `https://<your-v2-api-domain>`  
**Local URL**: `http://localhost:8000`

## Overview

EduThreat-CTI now serves the dashboard and operator workflows from the
canonical Postgres-backed `v2` API.

Use:

- public data: `/api/v2/*`
- operator controls: `/api/admin/v2/*`

The older SQLite-era `/api/*` surface is considered legacy and should not be
used for new integrations.

## Health

### `GET /health`

Basic service liveness.

### `GET /api/health`

Compatibility liveness endpoint for platform health checks.

### `GET /api/v2/health`

Returns the `v2` layer health explicitly.

## Public `v2` Endpoints

### `GET /api/v2/dashboard`

Returns the main dashboard payload:

- `stats`
- `incidents_by_country`
- `incidents_by_attack_type`
- `incidents_by_ransomware`
- `incidents_over_time`
- `recent_incidents`

### `GET /api/v2/stats`

Returns the dashboard statistics subset only.

### `GET /api/v2/incidents`

Canonical incident listing. Supports:

- `limit`
- `offset`
- `search`
- `country_code`
- `attack_category`
- `institution_type`
- `severity`
- `date_from`
- `date_to`
- `sort_by`
- `sort_order`

Compatibility mode:

- `format=legacy`

Use `format=legacy` if you need the old incident-list response shape.

### `GET /api/v2/incidents/{canonical_incident_id}`

Canonical incident detail.

Compatibility mode:

- `format=legacy`

This is what the migrated dashboard now uses for incident detail pages.

### `GET /api/v2/incidents/{canonical_incident_id}/report`

Downloads the CTI report for a canonical incident.

### `GET /api/v2/filters`

Returns:

- `countries`
- `attack_categories`
- `ransomware_families`
- `threat_actors`
- `institution_types`
- `years`

### `GET /api/v2/analytics/breakdowns`

Filtered analytics breakdowns with the same main facet families used by the
dashboard.

### `GET /api/v2/analytics/trend`

Filtered trend series.

Query parameters:

- `bucket=month|week|year`
- `limit`

### Compatibility Analytics Endpoints

These keep the dashboard cutover simple while still reading from the `v2`
canonical model:

- `GET /api/v2/analytics/countries`
- `GET /api/v2/analytics/attack-types`
- `GET /api/v2/analytics/ransomware`
- `GET /api/v2/analytics/timeline`
- `GET /api/v2/analytics/threat-actors`

## Admin `v2` Endpoints

### `POST /api/admin/v2/login`

Authenticate into the operator console.

### `POST /api/admin/v2/logout`

Invalidate the current session.

### `GET /api/admin/v2/preflight`

Checks:

- Postgres connectivity
- Alembic revision
- Ollama config
- Oxylabs config
- admin auth readiness

### `GET /api/admin/v2/status`

Returns runtime status including:

- canonical/source/article/enrichment counts
- queue health
- task summary
- recent tasks
- recent runs
- dashboard snapshot freshness

### `GET /api/admin/v2/plans`

Lists named orchestration plans.

### `POST /api/admin/v2/run-plan?plan_name=...`

Queues a named collection/enrichment plan.

### `POST /api/admin/v2/data-quality/sweep-now`

Queues a data-quality sweep over source enrichments.

### `POST /api/admin/v2/canonicalize/sweep-now`

Queues a bounded recanonicalization sweep.

### `GET /api/admin/v2/canonicalize/consistency-candidates`

Lists canonicals whose top-level fields appear to have drifted from their
authoritative projection.

### `POST /api/admin/v2/canonicalize/consistency-sweep-now`

Queues recanonicalization for consistency-drift candidates.

### `GET /api/admin/v2/manual-review-queue`

Lists enrichments that exhausted automatic cleanup and need operator review.

## Example Requests

### Dashboard

```bash
curl https://<your-v2-api-domain>/api/v2/dashboard
```

### Incident list

```bash
curl "https://<your-v2-api-domain>/api/v2/incidents?limit=25&offset=0&country_code=US&sort_by=incident_date&sort_order=desc"
```

### Legacy-compatible incident list

```bash
curl "https://<your-v2-api-domain>/api/v2/incidents?format=legacy&limit=25&offset=0"
```

### Incident detail

```bash
curl "https://<your-v2-api-domain>/api/v2/incidents/<canonical_incident_id>?format=legacy"
```

### Report download

```bash
curl -O "https://<your-v2-api-domain>/api/v2/incidents/<canonical_incident_id>/report"
```

### Login and run a plan

```bash
curl -X POST "https://<your-v2-api-domain>/api/admin/v2/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"<password>"}'
```

```bash
curl -X POST "https://<your-v2-api-domain>/api/admin/v2/run-plan?plan_name=historical" \
  -H "X-Session-Token: <session_token>"
```

## Notes

- The current dashboard repo is migrated to the `v2` API.
- New integrations should not target the legacy SQLite-backed `/api/*` routes.
- If you need deployment steps, see [V2_RUNTIME.md](V2_RUNTIME.md) and
  [RAILWAY_V2_DEPLOY.md](RAILWAY_V2_DEPLOY.md).
