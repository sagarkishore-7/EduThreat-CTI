# CTI Analyst Guide

**Version**: 2.8.0  
**Last Updated**: 2026-05-10

## Overview

This guide is for CTI analysts, incident responders, and security researchers
using the EduThreat-CTI canonical `v2` dataset.

The dashboard and API now operate on the Postgres-backed canonical model:

- dashboard: `EduThreat-CTI-Dashboard`
- API: `/api/v2/*`
- operator controls: `/api/admin/v2/*`

## Access

### Dashboard

Use your deployed dashboard URL. The current dashboard is wired to the `v2`
canonical API surface.

### API

Base URL pattern:

```text
https://<your-v2-api-domain>/api/v2
```

Interactive docs:

```text
https://<your-v2-api-domain>/docs
```

## Common Workflows

### 1. Get dashboard summary

```python
import requests

base = "https://<your-v2-api-domain>"
dashboard = requests.get(f"{base}/api/v2/dashboard").json()

print(dashboard["stats"]["education_incidents"])
print(dashboard["stats"]["countries_affected"])
```

### 2. Search incidents

```python
import requests

base = "https://<your-v2-api-domain>"
response = requests.get(
    f"{base}/api/v2/incidents",
    params={
        "limit": 25,
        "offset": 0,
        "country_code": "US",
        "search": "powerschool",
        "sort_by": "incident_date",
        "sort_order": "desc",
    },
)
payload = response.json()

for item in payload["items"]:
    print(item["display_name"], item["incident_date"], item["attack_category"])
```

### 3. Use legacy-compatible incident payloads

The dashboard currently uses legacy-compatible shapes backed by the canonical
dataset. If you need that same shape:

```python
import requests

base = "https://<your-v2-api-domain>"
response = requests.get(
    f"{base}/api/v2/incidents",
    params={"format": "legacy", "limit": 20, "offset": 0},
)
payload = response.json()

for item in payload["incidents"]:
    print(item["institution_name"], item["incident_date"])
```

### 4. Get a full incident detail

```python
import requests

base = "https://<your-v2-api-domain>"
incident_id = "<canonical_incident_id>"
incident = requests.get(
    f"{base}/api/v2/incidents/{incident_id}",
    params={"format": "legacy"},
).json()

print("Institution:", incident["institution_name"])
print("Country:", incident["country"])
print("Threat actor:", incident.get("threat_actor_name"))
print("Ransomware family:", incident.get("attack_dynamics", {}).get("ransomware_family"))
```

### 5. Download a CTI report

```python
import requests

base = "https://<your-v2-api-domain>"
incident_id = "<canonical_incident_id>"
report = requests.get(f"{base}/api/v2/incidents/{incident_id}/report")

with open(f"cti-report-{incident_id}.md", "w") as handle:
    handle.write(report.text)
```

### 6. Threat actor view

```python
import requests

base = "https://<your-v2-api-domain>"
actors = requests.get(
    f"{base}/api/v2/analytics/threat-actors",
    params={"limit": 20},
).json()

for actor in actors["threat_actors"]:
    print(actor["name"], actor["incident_count"], actor["countries_targeted"])
```

### 7. Country and ransomware distributions

```python
import requests

base = "https://<your-v2-api-domain>"
countries = requests.get(f"{base}/api/v2/analytics/countries?limit=20").json()
ransomware = requests.get(f"{base}/api/v2/analytics/ransomware?limit=15").json()

print("Top countries:")
for row in countries["data"][:5]:
    print(row["category"], row["count"])

print("Top ransomware families:")
for row in ransomware["data"][:5]:
    print(row["category"], row["count"])
```

### 8. Trend analysis

```python
import requests

base = "https://<your-v2-api-domain>"
trend = requests.get(
    f"{base}/api/v2/analytics/trend",
    params={"bucket": "month", "limit": 24},
).json()

for point in trend["items"]:
    print(point["date"], point["count"])
```

## Analyst Notes

- Incident IDs are now canonical incident IDs in the `v2` dataset.
- Source lineage is preserved under canonical detail rather than being
  destructive-dedup SQLite survivors.
- The old SQLite-backed `/api/*` routes should be treated as legacy. Prefer the
  `v2` routes for all new analysis and automation.

## Related Docs

- [API.md](API.md)
- [V2_RUNTIME.md](V2_RUNTIME.md)
- [RAILWAY_V2_DEPLOY.md](RAILWAY_V2_DEPLOY.md)
