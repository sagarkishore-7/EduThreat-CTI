# EduThreat-CTI

[![Python](https://img.shields.io/badge/python-3.9+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![API](https://img.shields.io/badge/API-live-brightgreen.svg)](https://eduthreat-cti-production.up.railway.app/docs)
[![Dashboard](https://img.shields.io/badge/Dashboard-live-brightgreen.svg)](https://edu-threat-cti-dashboard.vercel.app)

**Open-source cyber threat intelligence pipeline for the global education sector**

> **Live Dashboard:** [edu-threat-cti-dashboard.vercel.app](https://edu-threat-cti-dashboard.vercel.app) — explore 3,000+ real-world education sector incidents with full CTI enrichment, analytics, and threat actor tracking.

EduThreat-CTI is a production-grade OSINT-to-CTI framework that continuously ingests, normalizes, and enriches cyber incidents affecting **universities, schools, K-12 districts, and research institutions worldwide**. It is the data backbone for ongoing academic research into the education sector's threat landscape.

---

## Overview

The education sector is among the most targeted by ransomware and data-theft actors, yet public visibility remains fragmented across dozens of sources. EduThreat-CTI solves this by building a **unified, analyst-grade incident dataset** from open sources — with LLM extraction of 192+ structured CTI fields per incident.

```
OSINT Sources → Phase 1 (Ingestion) → Phase 2 (LLM Enrichment) → REST API → Dashboard
```

**Current dataset:** 3,200+ incidents | 1,900+ LLM-enriched | 15+ sources | Updated continuously

---

## Features

- **Multi-source ingestion** — ransomware leak sites, cybersecurity news feeds, breach notification databases, curated education registers
- **LLM-powered enrichment** — DeepSeek V3 extracts 192+ structured CTI fields per incident: MITRE ATT&CK, attack dynamics, data/financial/regulatory impact, recovery timeline
- **Real-time pipeline** — scheduled ingestion + enrichment on Railway with automatic deduplication
- **REST API** — FastAPI with 40+ endpoints for filtering, analytics, and CTI report generation
- **Interactive dashboard** — Next.js frontend with charts, filtering, threat actor tracking, geographic analysis

---

## Live Platform

| Resource | URL |
|---|---|
| Dashboard | [edu-threat-cti-dashboard.vercel.app](https://edu-threat-cti-dashboard.vercel.app) |
| API (Swagger docs) | [eduthreat-cti-production.up.railway.app/docs](https://eduthreat-cti-production.up.railway.app/docs) |
| Dashboard repo | [github.com/sagarkishore-7/EduThreat-CTI-Dashboard](https://github.com/sagarkishore-7/EduThreat-CTI-Dashboard) |

---

## Architecture

```
src/edu_cti/
├── core/               # Shared config, DB connection, HTTP client, logging
├── sources/
│   ├── curated/        # Sources with dedicated education sections
│   │   ├── konbriefing.py         # University cyber attacks register
│   │   ├── comparitech.py         # Comparitech breach database
│   │   ├── ransomware_live.py     # Ransomware.live (education filter)
│   │   └── databreach.py          # DataBreaches.net education archive
│   ├── news/           # Keyword-based news scrapers
│   │   ├── therecord.py, securityweek.py, darkreading.py
│   │   └── krebsonsecurity.py, thehackernews.py
│   └── rss/            # Real-time RSS feeds with keyword filtering
│       ├── databreaches_rss.py
│       └── bleepingcomputer_rss.py
├── pipeline/
│   ├── phase1/         # Ingestion: scrape → normalize → deduplicate → SQLite
│   └── phase2/         # Enrichment: fetch articles → LLM extraction → store
├── scheduler/          # APScheduler jobs for continuous operation
└── api/                # FastAPI REST API (main.py + admin.py)
```

---

## Getting Started

### Prerequisites

- Python 3.9+
- An [Oxylabs](https://oxylabs.io) Web Scraper API account (for article fetching)
- A [DeepSeek](https://platform.deepseek.com) API key (for LLM enrichment)

### Installation

```bash
git clone https://github.com/sagarkishore-7/EduThreat-CTI.git
cd EduThreat-CTI
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Environment Variables

Copy `.env.example` and fill in your credentials:

```bash
cp .env.example .env
```

| Variable | Required | Description |
|---|---|---|
| `DEEPSEEK_API_KEY` | Yes | DeepSeek API key for LLM enrichment |
| `OXYLABS_USERNAME` | Yes | Oxylabs username for article fetching |
| `OXYLABS_PASSWORD` | Yes | Oxylabs password |
| `EDUTHREAT_ADMIN_PASSWORD_HASH` | For production | SHA-256 hash of admin password |

### Run the Pipeline

```bash
# Phase 1: Ingest all sources (first run — historical)
python -m src.edu_cti.pipeline.phase1 --full-historical

# Phase 1: Incremental update (daily)
python -m src.edu_cti.pipeline.phase1

# Phase 2: Enrich unenriched incidents with LLM
python -m src.edu_cti.pipeline.phase2

# Start the API server
python -m src.edu_cti.api
# → http://localhost:8000/docs
```

---

## Pipeline Phases

### Phase 1 — Ingestion

Scrapes 15+ sources, normalizes all incidents into a unified schema, and stores them in SQLite with cross-source deduplication.

| Mode | Command | Description |
|---|---|---|
| First run | `--full-historical` | Scrapes full archive (~2–3 hrs) |
| Daily | *(no flag)* | Only new incidents (~30 seconds) |
| Specific sources | `--groups curated --sources databreach` | Targeted refresh |

### Phase 2 — LLM Enrichment

For each incident, fetches the source article (via Oxylabs with JS rendering), then runs DeepSeek V3 to extract 192+ structured CTI fields:

- **Attack details** — vector, category, MITRE ATT&CK techniques, ransomware family, threat actor
- **Impact** — data categories exfiltrated, records affected, operational disruption, financial cost
- **Timeline** — initial access, exfiltration, discovery, disclosure, recovery dates
- **Regulatory** — applicable regulations (FERPA, GDPR, HIPAA), fines, lawsuits
- **Recovery** — containment method, duration, security improvements implemented

Fetch strategy: `newspaper3k → direct HTTP → Oxylabs (JS rendering) → archive.org`

### Phase 3 — API & Dashboard

FastAPI REST API with 40+ endpoints. Key endpoints:

| Endpoint | Description |
|---|---|
| `GET /api/incidents` | Paginated, filterable incident list |
| `GET /api/incidents/{id}` | Full incident with all enrichment data |
| `GET /api/dashboard` | Aggregate stats for the dashboard |
| `GET /api/analytics/*` | 20+ analytics endpoints (charts, heatmaps, Sankey flows) |
| `GET /api/incidents/{id}/report` | Download Markdown CTI report |
| `POST /api/admin/*` | Admin controls (auth required) |

---

## Data Sources

| Source | Type | Coverage |
|---|---|---|
| KonBriefing | Curated register | University attacks worldwide |
| Comparitech breach database | Curated | US K-12 + higher ed breaches |
| Ransomware.live | Leak site mirror | Active ransomware victims |
| RansomLook | Leak site aggregator | Multi-gang victim tracking |
| DataBreaches.net | News archive | Education sector breach reports |
| DataBreaches.net RSS | RSS | Real-time breach notifications |
| The Record (Recorded Future) | News | Cybersecurity journalism |
| BleepingComputer | RSS | Security news + edu keyword filter |
| SecurityWeek | News | Enterprise security coverage |
| Dark Reading | News | Enterprise security coverage |
| The Hacker News | News | Threat actor reporting |
| Krebs on Security | News | Investigative security journalism |

---

## Incident Schema

Each incident stores 192+ fields across these categories after LLM enrichment:

```
incident_id, university_name, victim_raw_name, institution_type
incident_date, country, region, city
attack_category, attack_vector, ransomware_family
threat_actor_name, threat_actor_category, threat_actor_motivation
data_categories, records_affected, pii_records_leaked
systems_affected, operational_impact, financial_impact_usd
applicable_regulations, breach_notification_sent, fines_imposed
recovery_duration_days, security_improvements
timeline[], mitre_attack_techniques[]
```

---

## Development

```bash
# Run tests
pytest tests/ -v

# Lint and format
black src/
flake8 src/

# Check database state
sqlite3 data/eduthreat.db "SELECT source, COUNT(*) FROM source_events GROUP BY source"
```

### Adding a New Source

1. Create `src/edu_cti/sources/<type>/<source_name>.py` with a `build_<source>_incidents()` function returning `list[BaseIncident]`
2. Register it in `src/edu_cti/core/sources.py`
3. Test: `pytest tests/phase1/test_source_contribution.py -v --source-name <name>`

See [CONTRIBUTING.md](CONTRIBUTING.md) and [docs/ADDING_SOURCES.md](docs/ADDING_SOURCES.md) for a step-by-step guide.

---

## Deployment

The pipeline and API run on [Railway](https://railway.app) with a persistent volume for the SQLite database.

```bash
# Deploy to Railway
railway up

# Environment variables are set in the Railway dashboard
# Required: DEEPSEEK_API_KEY, OXYLABS_USERNAME, OXYLABS_PASSWORD
# Required: EDUTHREAT_ADMIN_PASSWORD_HASH
```

See [RAILWAY_SETUP.md](RAILWAY_SETUP.md) for full deployment instructions.

---

## Documentation

| Document | Description |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System design and component overview |
| [docs/DATABASE.md](docs/DATABASE.md) | Full database schema reference |
| [docs/API.md](docs/API.md) | REST API endpoint reference |
| [docs/SOURCES.md](docs/SOURCES.md) | Source catalog and coverage notes |
| [docs/ADDING_SOURCES.md](docs/ADDING_SOURCES.md) | Guide for contributing new sources |
| [docs/DEDUPLICATION.md](docs/DEDUPLICATION.md) | Deduplication strategy |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Contribution guidelines |
| [CHANGELOG.md](CHANGELOG.md) | Version history |

---

## Research Context

This project supports ongoing research into the education sector's cyber threat landscape, targeting publication at **ACM CCS 2026**. The dataset and methodology are designed to be reproducible and citable.

If you use this dataset or pipeline in academic work, please cite the accompanying paper (forthcoming).

---

## Ethics & Legal

This project uses **public OSINT sources only**. No dark web scraping, no active scanning, no exploitation of any systems. All data is sourced from publicly available breach notifications, security journalism, and ransomware leak site mirrors that are indexed by search engines.

---

## License

[MIT License](LICENSE) — free to use, modify, and distribute with attribution.

---

## Contributing

Contributions are welcome. The most impactful contributions are:

- **New OSINT sources** — especially non-English sources and regional threat trackers
- **Schema improvements** — additional CTI fields or improved normalization
- **Analysis tools** — scripts for cross-incident correlation or campaign tracking

Open an issue or pull request on GitHub.
