# REST API Documentation

**Version**: 1.6.0  
**Base URL**: `https://eduthreat-cti-production.up.railway.app` (Production)  
**Local URL**: `http://localhost:8000` (Development)

## Overview

The EduThreat-CTI REST API provides programmatic access to cyber threat intelligence data for the education sector. Built with FastAPI, it offers comprehensive endpoints for querying incidents, analytics, and administrative operations.

## Authentication

### Public Endpoints

Most endpoints are public and require no authentication:
- Dashboard data
- Incident queries
- Analytics endpoints
- Filter options

### Admin Endpoints

Admin endpoints require authentication via token:
- Database exports
- Scheduler controls
- Data maintenance operations

**Authentication**: Token-based via `Authorization: Bearer <token>` header

## Base Endpoints

### Dashboard Data

#### `GET /api/dashboard`

Get complete dashboard data including statistics, recent incidents, and analytics.

**Response**:
```json
{
  "stats": {
    "total_incidents": 4500,
    "enriched_incidents": 3200,
    "ransomware_attacks": 1800,
    "data_breaches": 1200,
    "countries_affected": 45
  },
  "recent_incidents": [...],
  "analytics": {
    "by_country": [...],
    "by_attack_type": [...],
    "by_ransomware": [...]
  }
}
```

### Statistics

#### `GET /api/stats`

Get summary statistics.

**Response**:
```json
{
  "total_incidents": 4500,
  "enriched_incidents": 3200,
  "ransomware_attacks": 1800,
  "data_breaches": 1200,
  "countries_affected": 45,
  "threat_actors": 120
}
```

### Incidents

#### `GET /api/incidents`

Get paginated list of incidents with filtering and sorting.

**Query Parameters**:
- `page` (int, default: 1): Page number
- `page_size` (int, default: 20): Items per page
- `country` (string, optional): Filter by country (full name or code)
- `attack_type` (string, optional): Filter by attack type
- `ransomware` (string, optional): Filter by ransomware family
- `threat_actor` (string, optional): Filter by threat actor
- `year` (int, optional): Filter by year
- `enriched_only` (bool, default: false): Only enriched incidents
- `education_only` (bool, default: true): Only education-related incidents
- `search` (string, optional): Full-text search
- `sort_by` (string, default: "incident_date"): Sort field
- `sort_order` (string, default: "desc"): "asc" or "desc"

**Response**:
```json
{
  "incidents": [...],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total": 4500,
    "total_pages": 225
  }
}
```

#### `GET /api/incidents/{incident_id}`

Get detailed information for a specific incident.

**Response**:
```json
{
  "incident_id": "konbriefing_abc123",
  "title": "University Cyber Attack",
  "incident_date": "2024-03-15",
  "country": "United States",
  "country_code": "US",
  "enrichment": {
    "is_education_related": true,
    "attack_category": "ransomware",
    "ransomware_family": "LockBit",
    "timeline": [...],
    "mitre_attack": [...],
    "impact": {...}
  },
  "sources": [...]
}
```

#### `GET /api/incidents/{incident_id}/report`

Download comprehensive CTI report in Markdown format.

**Response**: Markdown text file

**Headers**:
- `Content-Type: text/markdown`
- `Content-Disposition: attachment; filename=cti-report-{incident_id}.md`

### Filters

#### `GET /api/filters`

Get available filter options.

**Response**:
```json
{
  "countries": [
    {"name": "United States", "code": "US", "flag_emoji": "🇺🇸", "count": 293},
    {"name": "United Kingdom", "code": "GB", "flag_emoji": "🇬🇧", "count": 45}
  ],
  "attack_types": [...],
  "ransomware_families": [...],
  "threat_actors": [...],
  "years": [2020, 2021, 2022, 2023, 2024]
}
```

### Analytics

#### `GET /api/analytics/countries`

Get country breakdown with incident counts.

**Response**:
```json
[
  {
    "country": "United States",
    "country_code": "US",
    "flag_emoji": "🇺🇸",
    "count": 293
  },
  ...
]
```

#### `GET /api/analytics/attack-types`

Get attack type breakdown.

**Response**:
```json
[
  {"attack_type": "ransomware", "count": 1800},
  {"attack_type": "data_breach", "count": 1200},
  ...
]
```

#### `GET /api/analytics/ransomware`

Get ransomware family statistics.

**Response**:
```json
[
  {"family": "LockBit", "count": 450},
  {"family": "BlackCat", "count": 320},
  ...
]
```

#### `GET /api/analytics/threat-actors`

Get threat actor statistics.

**Response**:
```json
[
  {"actor": "LockBit", "count": 450, "type": "ransomware_gang"},
  ...
]
```

## Admin Endpoints

All admin endpoints require authentication.

### Database Export

#### `GET /api/admin/export/database`

Download full database file.

**Authentication**: Required

**Response**: SQLite database file

#### `GET /api/admin/export/csv/enriched`

Export enriched incidents to CSV.

**Query Parameters**:
- `education_only` (bool, default: true): Only education-related incidents

**Authentication**: Required

**Response**: CSV file

#### `GET /api/admin/export/csv/full`

Export all incidents to CSV.

**Query Parameters**:
- `education_only` (bool, default: true): Only education-related incidents

**Authentication**: Required

**Response**: CSV file

### Scheduler Controls

#### `POST /api/admin/scheduler/trigger/{job_type}`

Trigger a scheduler job.

**Path Parameters**:
- `job_type`: "rss", "weekly", or "enrich"

**Authentication**: Required

**Response**:
```json
{
  "success": true,
  "job_type": "enrich",
  "message": "Job triggered successfully"
}
```

### Data Maintenance

#### `POST /api/admin/normalize-countries`

Normalize all country codes to full names in the database.

**Authentication**: Required

**Response**:
```json
{
  "success": true,
  "updated": 4500,
  "message": "Normalized 4500 country entries"
}
```

#### `POST /api/admin/fix-incident-dates`

Fix incident dates using LLM-extracted timeline data.

**Query Parameters**:
- `apply` (bool, default: false): If false, dry run only

**Authentication**: Required

**Response**:
```json
{
  "success": true,
  "apply": true,
  "fixed": 148,
  "skipped": 141,
  "total_checked": 313,
  "message": "Fixed 148 incidents"
}
```

## Error Responses

### 400 Bad Request

```json
{
  "detail": "Invalid query parameter"
}
```

### 404 Not Found

```json
{
  "detail": "Incident not found"
}
```

### 500 Internal Server Error

```json
{
  "detail": "Internal server error"
}
```

## Rate Limiting

- No rate limiting currently implemented
- Recommended: Implement rate limiting for production use
- Consider adding authentication for sensitive endpoints

## CORS

CORS is enabled for the dashboard domain:
- `Access-Control-Allow-Origin: *` (configurable)

## Interactive Documentation

### Swagger UI

Available at: `http://localhost:8000/docs` (development)

### ReDoc

Available at: `http://localhost:8000/redoc` (development)

## Example Usage

### Python

```python
import requests

# Get dashboard data
response = requests.get("https://eduthreat-cti-production.up.railway.app/api/dashboard")
data = response.json()

# Get incidents with filters
params = {
    "country": "United States",
    "attack_type": "ransomware",
    "page": 1,
    "page_size": 20
}
response = requests.get(
    "https://eduthreat-cti-production.up.railway.app/api/incidents",
    params=params
)
incidents = response.json()

# Get specific incident
response = requests.get(
    "https://eduthreat-cti-production.up.railway.app/api/incidents/konbriefing_abc123"
)
incident = response.json()
```

### JavaScript/TypeScript

```typescript
// Get dashboard data
const response = await fetch('https://eduthreat-cti-production.up.railway.app/api/dashboard');
const data = await response.json();

// Get incidents with filters
const params = new URLSearchParams({
  country: 'United States',
  attack_type: 'ransomware',
  page: '1',
  page_size: '20'
});
const incidentsResponse = await fetch(
  `https://eduthreat-cti-production.up.railway.app/api/incidents?${params}`
);
const incidents = await incidentsResponse.json();
```

### cURL

```bash
# Get dashboard data
curl https://eduthreat-cti-production.up.railway.app/api/dashboard

# Get incidents with filters
curl "https://eduthreat-cti-production.up.railway.app/api/incidents?country=United%20States&attack_type=ransomware"

# Get specific incident
curl https://eduthreat-cti-production.up.railway.app/api/incidents/konbriefing_abc123

# Download CTI report
curl -O https://eduthreat-cti-production.up.railway.app/api/incidents/konbriefing_abc123/report
```

## Versioning

API versioning is not currently implemented. All endpoints are under `/api/` prefix.

Future versions may use:
- `/api/v1/...` for version 1
- `/api/v2/...` for version 2

## Support

For API issues or questions:
- Open an issue on GitHub
- Check the [main documentation](../README.md)
- Review [ARCHITECTURE.md](ARCHITECTURE.md) for system design
