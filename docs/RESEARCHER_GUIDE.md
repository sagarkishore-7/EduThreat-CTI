# Researcher Guide

**Version**: 1.6.0  
**Last Updated**: 2026-01-08

## Overview

This guide is for **academic researchers, data scientists, and analysts** who want to use EduThreat-CTI data for research, analysis, and publication.

## Getting the Data

### Option 1: Download from Dashboard

1. **Access the Dashboard**: https://eduthreat-cti-dashboard.vercel.app
2. **Admin Panel**: Login to admin panel
3. **Export CSV**: Use "Export Full CSV" or "Export Enriched CSV" buttons
4. **Download Database**: Use "Export Database" for complete SQLite database

### Option 2: Use the API

```python
import requests
import pandas as pd

# Get all incidents (paginated)
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

# Convert to DataFrame
df = pd.DataFrame(all_incidents)
```

### Option 3: Clone and Run Locally

```bash
# Clone repository
git clone https://github.com/sagarkishore-7/EduThreat-CTI.git
cd EduThreat-CTI

# Install dependencies
pip install -r requirements.txt

# Run Phase 1 ingestion
python -m src.edu_cti.pipeline.phase1 --full-historical

# Run Phase 2 enrichment (requires OLLAMA_API_KEY)
export OLLAMA_API_KEY=your_key
python -m src.edu_cti.pipeline.phase2

# Export to CSV
python -m src.edu_cti.pipeline.phase2.csv_export
```

## Data Schema

### Base Incident Fields

| Field | Type | Description |
|-------|------|-------------|
| `incident_id` | string | Unique identifier (hash-based) |
| `title` | string | Incident title |
| `incident_date` | string | YYYY-MM-DD format |
| `country` | string | Full country name (normalized) |
| `country_code` | string | ISO 3166-1 alpha-2 code |
| `university_name` | string | Normalized institution name |
| `attack_type_hint` | string | Basic classification (e.g., "ransomware") |
| `primary_url` | string | Best URL for article |
| `all_urls` | array | All URLs related to incident |

### Enrichment Fields (192+ fields)

See [DATABASE.md](DATABASE.md) for complete schema.

**Key Categories**:
- **Attack Details**: Category, vector, ransomware family, threat actor
- **Data Impact**: Records affected, PII exposure, data types
- **System Impact**: Systems affected, infrastructure context
- **User Impact**: Students, faculty, staff affected
- **Operational Impact**: Teaching, research, admissions disruption
- **Financial Impact**: Ransom amounts, recovery costs, insurance
- **Regulatory Impact**: GDPR, HIPAA, FERPA, fines, lawsuits
- **Recovery**: Timeline, phases, security improvements
- **Transparency**: Disclosure timing, updates

## Research Use Cases

### 1. Trend Analysis

**Question**: How have ransomware attacks on universities changed over time?

```python
import pandas as pd
import matplotlib.pyplot as plt

# Load data
df = pd.read_csv("enriched_incidents.csv")

# Filter ransomware incidents
ransomware = df[df["attack_category"] == "ransomware"]

# Group by year
yearly = ransomware.groupby(df["incident_date"].str[:4]).size()

# Plot
yearly.plot(kind="line", title="Ransomware Attacks on Universities Over Time")
plt.show()
```

### 2. Geographic Analysis

**Question**: Which countries are most affected by education sector cyber attacks?

```python
# Group by country
by_country = df.groupby("country").size().sort_values(ascending=False)

# Top 10 countries
print(by_country.head(10))
```

### 3. Ransomware Family Analysis

**Question**: Which ransomware families target education most frequently?

```python
# Filter ransomware incidents
ransomware = df[df["ransomware_family"].notna()]

# Group by family
by_family = ransomware.groupby("ransomware_family").size().sort_values(ascending=False)

print(by_family)
```

### 4. Impact Analysis

**Question**: What is the average number of records affected in data breaches?

```python
# Filter data breaches
breaches = df[df["data_breached"] == 1]

# Calculate statistics
print(f"Mean records affected: {breaches['records_affected_exact'].mean():,.0f}")
print(f"Median records affected: {breaches['records_affected_exact'].median():,.0f}")
print(f"Total records affected: {breaches['records_affected_exact'].sum():,.0f}")
```

### 5. Financial Impact Analysis

**Question**: What is the total financial impact of ransomware attacks?

```python
# Filter ransomware with ransom demands
ransomware = df[
    (df["attack_category"] == "ransomware") &
    (df["was_ransom_demanded"] == 1) &
    (df["ransom_amount"].notna())
]

# Calculate total ransom demanded
total_ransom = ransomware["ransom_amount"].sum()
print(f"Total ransom demanded: ${total_ransom:,.0f}")

# Calculate recovery costs
recovery = df[df["recovery_costs_min"].notna()]
total_recovery = recovery["recovery_costs_min"].sum()
print(f"Total recovery costs (min): ${total_recovery:,.0f}")
```

### 6. MITRE ATT&CK Analysis

**Question**: What are the most common MITRE ATT&CK techniques used?

```python
import json
from collections import Counter

# Load incidents with MITRE data
incidents = df[df["llm_mitre_attack"].notna()]

# Extract techniques
all_techniques = []
for mitre_json in incidents["llm_mitre_attack"]:
    techniques = json.loads(mitre_json)
    for tech in techniques:
        all_techniques.append(tech.get("technique_id"))

# Count techniques
technique_counts = Counter(all_techniques)
print(technique_counts.most_common(10))
```

## Data Quality Considerations

### Limitations

1. **Source Reliability**: Data comes from OSINT sources with varying reliability
2. **Incomplete Data**: Not all incidents have complete enrichment data
3. **Temporal Bias**: Recent incidents may be over-represented
4. **Geographic Bias**: English-language sources may bias toward certain regions
5. **LLM Extraction**: Enrichment data is extracted by LLM and may contain errors

### Data Validation

```python
# Check data completeness
completeness = df.notna().sum() / len(df) * 100
print(completeness.sort_values(ascending=False))

# Check for outliers
print(df["records_affected_exact"].describe())

# Check date validity
df["incident_date"] = pd.to_datetime(df["incident_date"], errors="coerce")
invalid_dates = df[df["incident_date"].isna()]
print(f"Invalid dates: {len(invalid_dates)}")
```

## Citation

When using EduThreat-CTI data in publications, please cite:

```
EduThreat-CTI: Cyber Threat Intelligence for the Education Sector
https://github.com/sagarkishore-7/EduThreat-CTI
Version 1.6.0
```

## Ethical Considerations

1. **Privacy**: Data contains publicly disclosed information only
2. **No PII**: No personally identifiable information is stored
3. **Institution Names**: Institution names are from public sources
4. **Responsible Disclosure**: Follow responsible disclosure practices

## Getting Help

- **Documentation**: See [README.md](../README.md) and other docs
- **Issues**: Open an issue on GitHub for data quality questions
- **API**: Use interactive docs at `/docs` endpoint

## Example Research Questions

1. **Temporal Trends**: How have attack patterns changed over time?
2. **Geographic Distribution**: Which regions are most affected?
3. **Attack Vectors**: What are the most common initial access vectors?
4. **Ransomware Economics**: What are ransom amounts and payment rates?
5. **Recovery Times**: How long do institutions take to recover?
6. **Regulatory Impact**: What are the regulatory consequences?
7. **Threat Actor Analysis**: Which threat actors target education?
8. **System Impact**: Which systems are most commonly affected?
9. **Operational Disruption**: What is the impact on teaching and research?
10. **Transparency**: How quickly do institutions disclose incidents?

## Advanced Analysis

### Time Series Analysis

```python
import pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose

# Create time series
df["date"] = pd.to_datetime(df["incident_date"])
monthly = df.groupby(df["date"].dt.to_period("M")).size()

# Decompose
decomposition = seasonal_decompose(monthly, model="additive")
decomposition.plot()
```

### Network Analysis

```python
import networkx as nx

# Create network of threat actors and institutions
G = nx.Graph()

for _, row in df.iterrows():
    if row["threat_actor_name"] and row["university_name"]:
        G.add_edge(row["threat_actor_name"], row["university_name"])

# Analyze network
print(f"Nodes: {G.number_of_nodes()}")
print(f"Edges: {G.number_of_edges()}")
print(f"Connected components: {nx.number_connected_components(G)}")
```

### Statistical Analysis

```python
from scipy import stats

# Compare ransom amounts by ransomware family
lockbit = df[df["ransomware_family"] == "LockBit"]["ransom_amount"]
blackcat = df[df["ransomware_family"] == "BlackCat"]["ransom_amount"]

# T-test
t_stat, p_value = stats.ttest_ind(lockbit, blackcat)
print(f"T-statistic: {t_stat}, P-value: {p_value}")
```

## Contributing Research

If you publish research using EduThreat-CTI data, we'd love to hear about it! Please:
- Share your publications
- Cite the project
- Consider contributing improvements back to the project
