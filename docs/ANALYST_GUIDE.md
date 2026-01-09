# CTI Analyst Guide

**Version**: 1.6.0  
**Last Updated**: 2026-01-08

## Overview

This guide is for **cyber threat intelligence (CTI) analysts, security researchers, and incident responders** who want to use EduThreat-CTI for threat intelligence analysis, incident response, and security operations.

## Accessing the Data

### Dashboard

**URL**: https://eduthreat-cti-dashboard.vercel.app

**Features**:
- Real-time incident tracking
- Interactive visualizations
- Advanced filtering and search
- Detailed incident views
- CTI report downloads

### API

**Base URL**: `https://eduthreat-cti-production.up.railway.app/api`

**Interactive Docs**: `https://eduthreat-cti-production.up.railway.app/docs`

See [API.md](API.md) for complete API documentation.

## Key Use Cases

### 1. Threat Intelligence Analysis

#### Identify Threat Actors

```python
import requests

# Get threat actor statistics
response = requests.get(
    "https://eduthreat-cti-production.up.railway.app/api/analytics/threat-actors"
)
actors = response.json()

# Find incidents by specific threat actor
response = requests.get(
    "https://eduthreat-cti-production.up.railway.app/api/incidents",
    params={"threat_actor": "LockBit", "enriched_only": True}
)
incidents = response.json()["incidents"]
```

#### Track Ransomware Families

```python
# Get ransomware statistics
response = requests.get(
    "https://eduthreat-cti-production.up.railway.app/api/analytics/ransomware"
)
ransomware = response.json()

# Find recent LockBit attacks
response = requests.get(
    "https://eduthreat-cti-production.up.railway.app/api/incidents",
    params={
        "ransomware": "LockBit",
        "enriched_only": True,
        "sort_by": "incident_date",
        "sort_order": "desc",
        "page_size": 50
    }
)
recent_lockbit = response.json()["incidents"]
```

### 2. Incident Response

#### Get Incident Details

```python
# Get full incident details
incident_id = "konbriefing_abc123"
response = requests.get(
    f"https://eduthreat-cti-production.up.railway.app/api/incidents/{incident_id}"
)
incident = response.json()

# Extract key information
print(f"Title: {incident['title']}")
print(f"Date: {incident['incident_date']}")
print(f"Institution: {incident['enrichment']['institution_name']}")
print(f"Attack Category: {incident['enrichment']['attack_category']}")
print(f"Ransomware Family: {incident['enrichment']['ransomware_family']}")
print(f"Threat Actor: {incident['enrichment']['threat_actor_name']}")
```

#### Download CTI Report

```python
# Download comprehensive CTI report
incident_id = "konbriefing_abc123"
response = requests.get(
    f"https://eduthreat-cti-production.up.railway.app/api/incidents/{incident_id}/report"
)

# Save report
with open(f"cti-report-{incident_id}.md", "w") as f:
    f.write(response.text)
```

### 3. MITRE ATT&CK Mapping

#### Analyze Techniques

```python
import json
from collections import Counter

# Get enriched incidents
response = requests.get(
    "https://eduthreat-cti-production.up.railway.app/api/incidents",
    params={"enriched_only": True, "page_size": 1000}
)
incidents = response.json()["incidents"]

# Extract MITRE ATT&CK techniques
all_techniques = []
for incident in incidents:
    if incident.get("llm_mitre_attack"):
        techniques = json.loads(incident["llm_mitre_attack"])
        for tech in techniques:
            all_techniques.append({
                "technique_id": tech.get("technique_id"),
                "technique_name": tech.get("technique_name"),
                "tactic": tech.get("tactic")
            })

# Count techniques
technique_counts = Counter([t["technique_id"] for t in all_techniques])
print("Most common techniques:")
for tech_id, count in technique_counts.most_common(10):
    print(f"  {tech_id}: {count}")
```

#### Map to Tactics

```python
from collections import defaultdict

# Group by tactic
by_tactic = defaultdict(int)
for tech in all_techniques:
    by_tactic[tech["tactic"]] += 1

print("Techniques by MITRE ATT&CK Tactic:")
for tactic, count in sorted(by_tactic.items(), key=lambda x: x[1], reverse=True):
    print(f"  {tactic}: {count}")
```

### 4. IOCs (Indicators of Compromise)

#### Extract IOCs from Timeline

```python
import json
import re

# Get incident with timeline
incident_id = "konbriefing_abc123"
response = requests.get(
    f"https://eduthreat-cti-production.up.railway.app/api/incidents/{incident_id}"
)
incident = response.json()

# Extract IOCs from timeline
timeline = json.loads(incident.get("llm_timeline", "[]"))
iocs = []

for event in timeline:
    if event.get("iocs"):
        iocs.extend(event["iocs"])

# Extract IPs, domains, hashes
ips = [ioc for ioc in iocs if re.match(r'^\d+\.\d+\.\d+\.\d+$', ioc)]
domains = [ioc for ioc in iocs if '.' in ioc and not re.match(r'^\d+\.\d+\.\d+\.\d+$', ioc)]
hashes = [ioc for ioc in iocs if len(ioc) in [32, 40, 64] and all(c in '0123456789abcdef' for c in ioc.lower())]

print(f"IPs: {ips}")
print(f"Domains: {domains}")
print(f"Hashes: {hashes}")
```

### 5. Attack Pattern Analysis

#### Analyze Attack Vectors

```python
# Get attack vector statistics
response = requests.get(
    "https://eduthreat-cti-production.up.railway.app/api/incidents",
    params={"enriched_only": True, "page_size": 1000}
)
incidents = response.json()["incidents"]

# Count attack vectors
from collections import Counter
vectors = Counter([
    inc["enrichment"]["initial_access_vector"]
    for inc in incidents
    if inc.get("enrichment", {}).get("initial_access_vector")
])

print("Most common initial access vectors:")
for vector, count in vectors.most_common(10):
    print(f"  {vector}: {count}")
```

#### Analyze Attack Chains

```python
# Extract attack chains
chains = []
for incident in incidents:
    enrichment = incident.get("enrichment", {})
    if enrichment.get("attack_chain"):
        chains.append(enrichment["attack_chain"])

# Analyze common patterns
from collections import Counter
chain_counts = Counter(chains)
print("Most common attack chains:")
for chain, count in chain_counts.most_common(10):
    print(f"  {chain}: {count}")
```

### 6. Impact Assessment

#### Data Breach Analysis

```python
# Get data breach incidents
response = requests.get(
    "https://eduthreat-cti-production.up.railway.app/api/incidents",
    params={
        "attack_type": "data_breach",
        "enriched_only": True,
        "page_size": 1000
    }
)
breaches = response.json()["incidents"]

# Calculate statistics
total_records = sum([
    inc["enrichment"]["records_affected_exact"]
    for inc in breaches
    if inc.get("enrichment", {}).get("records_affected_exact")
])

pii_records = sum([
    inc["enrichment"]["pii_records_leaked"]
    for inc in breaches
    if inc.get("enrichment", {}).get("pii_records_leaked")
])

print(f"Total records affected: {total_records:,}")
print(f"PII records leaked: {pii_records:,}")
```

#### Operational Impact Analysis

```python
# Analyze operational disruption
response = requests.get(
    "https://eduthreat-cti-production.up.railway.app/api/incidents",
    params={"enriched_only": True, "page_size": 1000}
)
incidents = response.json()["incidents"]

# Count operational impacts
teaching_disrupted = sum([
    1 for inc in incidents
    if inc.get("enrichment", {}).get("teaching_disrupted") == 1
])

research_disrupted = sum([
    1 for inc in incidents
    if inc.get("enrichment", {}).get("research_disrupted") == 1
])

classes_cancelled = sum([
    1 for inc in incidents
    if inc.get("enrichment", {}).get("classes_cancelled") == 1
])

print(f"Teaching disrupted: {teaching_disrupted}")
print(f"Research disrupted: {research_disrupted}")
print(f"Classes cancelled: {classes_cancelled}")
```

### 7. Geographic Threat Analysis

#### Country-Specific Analysis

```python
# Get incidents by country
response = requests.get(
    "https://eduthreat-cti-production.up.railway.app/api/incidents",
    params={
        "country": "United States",
        "enriched_only": True,
        "page_size": 1000
    }
)
us_incidents = response.json()["incidents"]

# Analyze US-specific threats
ransomware_families = Counter([
    inc["enrichment"]["ransomware_family"]
    for inc in us_incidents
    if inc.get("enrichment", {}).get("ransomware_family")
])

print("Top ransomware families targeting US education:")
for family, count in ransomware_families.most_common(5):
    print(f"  {family}: {count}")
```

### 8. Threat Actor Attribution

#### Track Threat Actor Activity

```python
# Get incidents by threat actor
response = requests.get(
    "https://eduthreat-cti-production.up.railway.app/api/incidents",
    params={
        "threat_actor": "LockBit",
        "enriched_only": True,
        "sort_by": "incident_date",
        "sort_order": "desc"
    }
)
lockbit_incidents = response.json()["incidents"]

# Analyze LockBit activity
print(f"Total LockBit incidents: {len(lockbit_incidents)}")

# Group by country
countries = Counter([
    inc["country"]
    for inc in lockbit_incidents
    if inc.get("country")
])

print("LockBit targets by country:")
for country, count in countries.most_common(10):
    print(f"  {country}: {count}")
```

## CTI Report Analysis

### Download and Analyze Reports

```python
import requests
import re

# Download CTI report
incident_id = "konbriefing_abc123"
response = requests.get(
    f"https://eduthreat-cti-production.up.railway.app/api/incidents/{incident_id}/report"
)
report = response.text

# Extract key sections
sections = {
    "Executive Summary": re.search(r'## Executive Summary(.*?)##', report, re.DOTALL),
    "MITRE ATT&CK": re.search(r'## MITRE ATT&CK Mapping(.*?)##', report, re.DOTALL),
    "IOCs": re.search(r'## Indicators of Compromise(.*?)##', report, re.DOTALL),
}

for section_name, match in sections.items():
    if match:
        print(f"\n{section_name}:")
        print(match.group(1)[:500])  # First 500 chars
```

## Integration with SIEM/SOAR

### Export to SIEM

```python
import requests
import json

# Get all enriched incidents
all_incidents = []
page = 1
while True:
    response = requests.get(
        "https://eduthreat-cti-production.up.railway.app/api/incidents",
        params={"page": page, "page_size": 100, "enriched_only": True}
    )
    data = response.json()
    all_incidents.extend(data["incidents"])
    
    if page >= data["pagination"]["total_pages"]:
        break
    page += 1

# Format for SIEM (example: Splunk)
siem_events = []
for incident in all_incidents:
    siem_event = {
        "timestamp": incident["incident_date"],
        "source": "EduThreat-CTI",
        "event_type": incident.get("enrichment", {}).get("attack_category"),
        "threat_actor": incident.get("enrichment", {}).get("threat_actor_name"),
        "ransomware_family": incident.get("enrichment", {}).get("ransomware_family"),
        "country": incident["country"],
        "institution": incident["university_name"],
        "incident_id": incident["incident_id"]
    }
    siem_events.append(siem_event)

# Export to JSON
with open("eduthreat_cti_events.json", "w") as f:
    json.dump(siem_events, f, indent=2)
```

## Best Practices

### 1. Data Freshness

- Check `llm_enriched_at` timestamp for enrichment freshness
- Use `enriched_only=True` for analysis requiring complete data
- Monitor for new incidents regularly

### 2. Data Validation

- Verify incident dates are reasonable
- Check for missing critical fields
- Validate IOCs before using in security tools

### 3. Attribution

- Use `threat_actor_name` and `ransomware_family` for attribution
- Cross-reference with other threat intelligence sources
- Consider source confidence levels

### 4. IOCs

- Extract IOCs from timeline data
- Validate IOCs before adding to blocklists
- Track IOC age and relevance

### 5. Reporting

- Use CTI reports for comprehensive analysis
- Include incident IDs in reports for traceability
- Reference source URLs for verification

## Automation Examples

### Daily Threat Brief

```python
import requests
from datetime import datetime, timedelta

# Get incidents from last 24 hours
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
response = requests.get(
    "https://eduthreat-cti-production.up.railway.app/api/incidents",
    params={
        "incident_date": yesterday,
        "enriched_only": True
    }
)
recent = response.json()["incidents"]

# Generate brief
brief = f"""
Daily Threat Brief - {datetime.now().strftime("%Y-%m-%d")}

New Incidents: {len(recent)}
Ransomware: {sum(1 for i in recent if i.get('enrichment', {}).get('attack_category') == 'ransomware')}
Data Breaches: {sum(1 for i in recent if i.get('enrichment', {}).get('data_breached') == 1)}

Top Threat Actors:
{chr(10).join([f"  - {i['enrichment']['threat_actor_name']}" for i in recent[:5] if i.get('enrichment', {}).get('threat_actor_name')])}
"""

print(brief)
```

### IOC Extraction Script

```python
import requests
import json
import re

def extract_iocs(incident_id):
    """Extract IOCs from an incident."""
    response = requests.get(
        f"https://eduthreat-cti-production.up.railway.app/api/incidents/{incident_id}"
    )
    incident = response.json()
    
    timeline = json.loads(incident.get("llm_timeline", "[]"))
    iocs = {
        "ips": [],
        "domains": [],
        "hashes": [],
        "urls": []
    }
    
    for event in timeline:
        if event.get("iocs"):
            for ioc in event["iocs"]:
                if re.match(r'^\d+\.\d+\.\d+\.\d+$', ioc):
                    iocs["ips"].append(ioc)
                elif ioc.startswith("http"):
                    iocs["urls"].append(ioc)
                elif len(ioc) in [32, 40, 64] and all(c in '0123456789abcdef' for c in ioc.lower()):
                    iocs["hashes"].append(ioc)
                elif '.' in ioc:
                    iocs["domains"].append(ioc)
    
    return iocs

# Example usage
iocs = extract_iocs("konbriefing_abc123")
print(json.dumps(iocs, indent=2))
```

## Support

For CTI analysis questions:
- Review [API.md](API.md) for API details
- Check [RESEARCHER_GUIDE.md](RESEARCHER_GUIDE.md) for data analysis examples
- Open an issue on GitHub for feature requests
