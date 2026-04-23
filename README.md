# EduThreat-CTI

[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![API](https://img.shields.io/badge/API-live-brightgreen.svg)](https://eduthreat-cti-production.up.railway.app/docs)
[![Dashboard](https://img.shields.io/badge/Dashboard-live-brightgreen.svg)](https://edu-threat-cti-dashboard.vercel.app)
[![Version](https://img.shields.io/badge/version-2.7.1-blue.svg)](CHANGELOG.md)

**Production-grade open-source cyber threat intelligence pipeline for the global education sector**

> **Live Dashboard:** [edu-threat-cti-dashboard.vercel.app](https://edu-threat-cti-dashboard.vercel.app) — explore 3,200+ real-world education sector incidents with full CTI enrichment, interactive analytics, and threat actor tracking.

EduThreat-CTI is an end-to-end OSINT-to-CTI framework that continuously ingests, normalizes, and enriches cyber incidents affecting **universities, schools, K-12 districts, and research institutions worldwide**. It is the data backbone for ongoing academic research into the education sector's threat landscape.

---

## Table of Contents

- [Overview](#overview)
- [Live Platform](#live-platform)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [Pipeline Phases](#pipeline-phases)
- [Data Sources](#data-sources)
- [Incident Schema](#incident-schema)
- [REST API](#rest-api)
- [Configuration Reference](#configuration-reference)
- [Development](#development)
- [Deployment](#deployment)
- [Documentation](#documentation)
- [Research Context](#research-context)
- [Ethics & Legal](#ethics--legal)
- [License](#license)

---

## Overview

The education sector is among the most targeted by ransomware and data-theft actors, yet public visibility remains fragmented across dozens of sources. EduThreat-CTI solves this by building a **unified, analyst-grade incident dataset** from open sources — with LLM extraction of 192+ structured CTI fields per incident.

```
OSINT Sources (15+) → Phase 1: Ingestion → Phase 2: LLM Enrichment → REST API → Dashboard
```

**Current dataset:** 3,200+ incidents | 1,900+ LLM-enriched | 15+ sources | Updated continuously

### Key Capabilities

| Capability | Details |
|---|---|
| **Multi-source ingestion** | Ransomware leak sites, cybersecurity news, breach notification databases, curated education registers |
| **LLM-powered enrichment** | Ollama Cloud (deepseek-v3.1:671b) extracts 192+ structured CTI fields per incident |
| **Real-time pipeline** | Scheduled ingestion + enrichment on Railway with automatic deduplication |
| **REST API** | FastAPI with 40+ endpoints for filtering, analytics, and CTI report generation |
| **Interactive dashboard** | Next.js frontend with charts, filtering, threat actor tracking, and geographic analysis |
| **CTI reports** | One-click Markdown reports per incident (MITRE ATT&CK, NIST CSF, STIX 2.1 aligned) |

---

## Live Platform

| Resource | URL |
|---|---|
| Dashboard | [edu-threat-cti-dashboard.vercel.app](https://edu-threat-cti-dashboard.vercel.app) |
| API (Swagger UI) | [eduthreat-cti-production.up.railway.app/docs](https://eduthreat-cti-production.up.railway.app/docs) |
| Dashboard source | [github.com/sagarkishore-7/EduThreat-CTI-Dashboard](https://github.com/sagarkishore-7/EduThreat-CTI-Dashboard) |

---

## Architecture

EduThreat-CTI follows a **three-phase pipeline architecture** with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          OSINT Sources (15+)                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────┐│
│  │  Curated        │  │  News           │  │  RSS / API              ││
│  │  KonBriefing    │  │  SecurityWeek   │  │  DataBreaches RSS       ││
│  │  Comparitech    │  │  The Record     │  │  BleepingComputer RSS   ││
│  │  Ransomware.live│  │  Dark Reading   │  │  RansomLook API         ││
│  │  DataBreaches   │  │  Krebs / THN    │  │  OTX AlienVault API     ││
│  └─────────────────┘  └─────────────────┘  └─────────────────────────┘│
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        Phase 1: Ingestion                               │
│  Fetch & Parse → Normalize to BaseIncident → Cross-Source Dedup        │
│  → SQLite (WAL mode, concurrent access)                                 │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     Phase 2: LLM Enrichment                             │
│  Article Fetch (newspaper3k → curl_cffi → Playwright → archive.org)    │
│  → Ollama Cloud (deepseek-v3.1:671b)                                   │
│  → Extract 192+ CTI fields → Post-enrichment deduplication             │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Phase 3: REST API & Dashboard                        │
│  FastAPI (40+ endpoints) → Next.js Dashboard (Vercel)                  │
└─────────────────────────────────────────────────────────────────────────┘
```

### Repository Structure

```
src/edu_cti/
├── core/               # Shared config, DB connection, HTTP client, logging
├── sources/
│   ├── curated/        # Education-specific registers (KonBriefing, Comparitech, etc.)
│   ├── news/           # Keyword-based news scrapers (SecurityWeek, The Record, etc.)
│   ├── rss/            # Real-time RSS feeds with keyword filtering
│   └── api/            # Threat intel APIs (RansomLook, OTX, CISA KEV)
├── pipeline/
│   ├── phase1/         # Ingestion: scrape → normalize → deduplicate → SQLite
│   ├── phase2/         # Enrichment: article fetch → LLM extraction → store
│   │   ├── extraction/ # LLM schema, prompt, JSON-to-schema mapper
│   │   ├── storage/    # DB write, flat-table mapper, CSV export
│   │   └── utils/      # Article fetcher, deduplication, fetching strategy
│   └── orchestrator.py # Combined Phase 1 + Phase 2 runner
├── scheduler/          # APScheduler jobs for continuous operation
└── api/                # FastAPI REST API (main.py, admin.py, reports.py)

tests/
├── phase1/             # Ingestion unit + integration tests
└── phase2/             # Enrichment unit tests + regression suite (111 tests)

docs/                   # Full technical documentation (14 documents)
```

---

## Getting Started

### Prerequisites

- **Python 3.11+**
- [Oxylabs](https://oxylabs.io) Web Scraper API account (article fetching)
- [Ollama Cloud](https://ollama.com) API key (LLM enrichment)

### Installation

```bash
git clone https://github.com/sagarkishore-7/EduThreat-CTI.git
cd EduThreat-CTI
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env            # Edit with your credentials
```

### Quick Start

```bash
# 1. Ingest all sources (first run — scrapes full historical archive, ~2–3 hrs)
python -m src.edu_cti.pipeline.phase1 --full-historical

# 2. Enrich incidents with LLM (requires OLLAMA_API_KEY)
python -m src.edu_cti.pipeline.phase2

# 3. Start the API server
python -m src.edu_cti.api
# → Swagger UI: http://localhost:8000/docs
```

Or run both pipeline phases together:

```bash
python -m src.edu_cti.pipeline.orchestrator --full-historical
```

---

## Pipeline Phases

### Phase 1 — Ingestion

Scrapes 15+ OSINT sources, normalizes all incidents into a unified `BaseIncident` schema, and stores them in SQLite with two-level deduplication.

| Mode | Command | Runtime |
|---|---|---|
| First run (full historical) | `python -m src.edu_cti.pipeline.phase1 --full-historical` | ~2–3 hours |
| Daily incremental update | `python -m src.edu_cti.pipeline.phase1` | ~30 seconds |
| Specific source groups | `python -m src.edu_cti.pipeline.phase1 --groups curated news` | Varies |
| Single source with page cap | `python -m src.edu_cti.pipeline.phase1 --groups news --sources therecord --max-pages 10` | ~1 min |

**Deduplication strategy:**
- **Per-source:** `source_events` table prevents re-ingesting the same incident from the same source
- **Cross-source:** URL overlap + fuzzy name/date matching merges the same incident reported by multiple sources

### Phase 2 — LLM Enrichment

For each unenriched incident, fetches the source article via a multi-tier fallback chain, then uses Ollama Cloud (deepseek-v3.1:671b) to extract 192+ structured CTI fields.

**Article fetch strategy (in order):**
1. `newspaper3k` — fast, no JS
2. `curl_cffi` with TLS fingerprinting — bot detection bypass
3. Playwright headless browser — full JS rendering
4. `archive.org` Wayback Machine — dead link recovery
5. SERP fallback — Google search discovery for URL-less incidents

**Extracted CTI fields (192+ total):**

| Category | Fields |
|---|---|
| Education & Institution | `institution_name`, `institution_type`, `is_education_related` |
| Attack Details | `attack_category`, `attack_vector`, `ransomware_family`, MITRE ATT&CK techniques |
| Threat Actor | `threat_actor_name`, `threat_actor_category`, `threat_actor_motivation` |
| Data Impact | `records_affected_exact`, `pii_records_leaked`, `data_categories` |
| Financial Impact | `ransom_amount`, `ransom_paid`, `recovery_costs_min/max` |
| Timeline | `incident_date`, `discovery_date`, `disclosure_date`, `recovery_completed_date` |
| Regulatory | `ferpa_breach`, `gdpr_breach`, `hipaa_breach`, `fine_amount`, `lawsuits_filed` |
| Recovery | `recovery_duration_days`, `containment_method`, `security_improvements` |

```bash
# Basic enrichment
python -m src.edu_cti.pipeline.phase2

# With options
python -m src.edu_cti.pipeline.phase2 --limit 100 --workers 4 --export-csv

# Re-enrich incidents older than a specific date (via admin API)
curl -X POST http://localhost:8000/api/admin/re-enrich \
  -H "Authorization: Bearer <token>" \
  -d '{"before_date": "2026-01-01"}'
```

### Phase 3 — REST API

FastAPI REST API with 40+ endpoints, 300-second response caching, and admin controls.

```bash
python -m src.edu_cti.api --host 0.0.0.0 --port 8000
# → http://localhost:8000/docs
```

---

## Data Sources

| Source | Type | Coverage |
|---|---|---|
| [KonBriefing](https://konbriefing.com) | Curated register | University attacks worldwide |
| [Comparitech](https://www.comparitech.com/blog/vpn-privacy/us-education-data-breaches/) | Curated register | US K-12 + higher education breaches |
| [Ransomware.live](https://ransomware.live) | Leak site mirror | Active ransomware victims (education filter) |
| [RansomLook](https://www.ransomlook.io) | Leak site aggregator | Multi-gang victim tracking via API |
| [DataBreaches.net](https://databreaches.net) | News archive | Education sector breach reports |
| DataBreaches.net RSS | RSS | Real-time breach notifications |
| [The Record](https://therecord.media) | News | Recorded Future cybersecurity journalism |
| [BleepingComputer](https://bleepingcomputer.com) RSS | RSS | Security news with education keyword filter |
| [SecurityWeek](https://securityweek.com) | News | Enterprise security coverage |
| [Dark Reading](https://darkreading.com) | News | Enterprise security coverage |
| [The Hacker News](https://thehackernews.com) | News | Threat actor reporting |
| [Krebs on Security](https://krebsonsecurity.com) | News | Investigative security journalism |
| [OTX AlienVault](https://otx.alienvault.com) | Threat intel API | Threat intelligence pulses (API key required) |
| [CISA KEV](https://www.cisa.gov/known-exploited-vulnerabilities-catalog) | Advisory RSS | US cybersecurity advisories |
| Abuse.ch ThreatFox / URLhaus | Threat intel | Education-relevant IOCs and malicious URLs |

---

## Incident Schema

Each incident stores 192+ fields after LLM enrichment, organized across two database tables:

- **`incidents`** — Core identity, victim info, dates, source URLs (deduplicated)
- **`incident_enrichments_flat`** — All 88+ enrichment columns for fast analytics and CSV export

Key fields:

```
# Identity
incident_id, source, institution_name, victim_raw_name, institution_type

# Location & Time
country, country_code, region, city
incident_date, discovery_date, disclosure_date, recovery_completed_date

# Attack
attack_category, attack_vector, initial_access_vector, ransomware_family
mitre_attack_techniques[], threat_actor_name, threat_actor_category

# Impact
records_affected_exact, pii_records_leaked, data_categories[]
systems_affected[], was_ransom_demanded, ransom_amount, ransom_paid

# Regulatory
ferpa_breach, gdpr_breach, hipaa_breach, breach_notification_required
fine_amount, lawsuits_filed, notifications_sent

# Recovery
recovery_duration_days, downtime_days, from_backup
incident_response_firm, mfa_implemented

# Metadata
enriched_summary, llm_enriched, llm_enriched_at, primary_url, all_urls
```

---

## REST API

**Base URL (production):** `https://eduthreat-cti-production.up.railway.app`  
**Base URL (local):** `http://localhost:8000`  
**Swagger UI:** append `/docs`

### Public Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/health` | Health check |
| `GET` | `/api/stats` | Summary statistics |
| `GET` | `/api/dashboard` | Full dashboard data |
| `GET` | `/api/incidents` | Paginated, filterable incident list |
| `GET` | `/api/incidents/{id}` | Full incident detail with all enrichment |
| `GET` | `/api/incidents/{id}/report` | Markdown CTI report download |
| `GET` | `/api/filters` | Available filter options |

**Incident list query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `page` | int | Page number (default: 1) |
| `page_size` | int | Results per page (default: 20, max: 100) |
| `country` | string | Filter by country name or ISO code |
| `attack_type` | string | Filter by attack category |
| `year` | int | Filter by incident year |
| `enriched_only` | bool | Only enriched incidents |
| `sort_by` | string | Field to sort by (default: `incident_date`) |
| `sort_order` | string | `asc` or `desc` |

### Analytics Endpoints (20+)

All analytics endpoints are cached for 300 seconds.

```
GET /api/analytics/attack-trends
GET /api/analytics/attack-vectors
GET /api/analytics/mitre-tactics
GET /api/analytics/mitre-heatmap
GET /api/analytics/mitre-sunburst
GET /api/analytics/initial-access
GET /api/analytics/ransomware-timeline
GET /api/analytics/ransomware-families-detail
GET /api/analytics/ransom-economics
GET /api/analytics/ransom-payment-by-year
GET /api/analytics/ransomware-family-trend
GET /api/analytics/threat-actor-categories
GET /api/analytics/actor-institution-targeting
GET /api/analytics/actor-ttp-profile
GET /api/analytics/actor-network
GET /api/analytics/institution-risk-matrix
GET /api/analytics/breach-severity-timeline
GET /api/analytics/disclosure-timeline
GET /api/analytics/recovery-by-attack-type
GET /api/analytics/attack-flow
GET /api/analytics/ransom-flow
GET /api/analytics/country-attack-matrix
```

### Admin Endpoints (authentication required)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/admin/incidents/enriched` | Paginated enriched incidents |
| `GET` | `/api/admin/incidents/unenriched` | Paginated unenriched incidents |
| `GET` | `/api/admin/raw-incidents` | Raw DB inspection with filters |
| `POST` | `/api/admin/incidents/delete` | Delete incidents by IDs |
| `POST` | `/api/admin/incidents/clear-all` | Wipe entire database |
| `GET` | `/api/admin/export/csv/full` | Full CSV export |
| `GET` | `/api/admin/export/csv/enriched` | Enriched-only CSV export |
| `POST` | `/api/admin/scheduler/start` | Start cron pipeline |
| `POST` | `/api/admin/scheduler/stop` | Stop pipeline |
| `GET` | `/api/admin/scheduler/status` | Pipeline status |
| `POST` | `/api/admin/re-enrich` | Reset enrichment before date |
| `POST` | `/api/admin/normalize-countries` | Normalize country names |

**Authentication:** `Authorization: Bearer <token>` where token = `SHA-256(password)` or pre-generated API key.

---

## Configuration Reference

Copy `.env.example` to `.env` and fill in your credentials.

### Required for LLM Enrichment

| Variable | Description |
|---|---|
| `OLLAMA_API_KEY` | Ollama Cloud API key |
| `OXYLABS_USERNAME` | Oxylabs Web Scraper username |
| `OXYLABS_PASSWORD` | Oxylabs Web Scraper password |

### Optional — Enrichment Tuning

| Variable | Default | Description |
|---|---|---|
| `OLLAMA_MODEL` | `deepseek-v3.1:671b-cloud` | LLM model override |
| `OLLAMA_HOST` | `https://ollama.com` | Ollama API base URL |
| `ENRICHMENT_WORKERS` | `2` | Parallel enrichment threads (max 8) |
| `ENRICHMENT_BATCH_SIZE` | `10` | Incidents per processing batch |
| `ENRICHMENT_RATE_LIMIT_DELAY` | `2.0` | Seconds between LLM API calls |
| `ENRICHMENT_MAX_RETRIES` | `3` | Max retry attempts per incident |

### Optional — Storage & Logging

| Variable | Default | Description |
|---|---|---|
| `EDU_CTI_DB_PATH` | `data/eduthreat.db` | SQLite database path |
| `EDU_CTI_LOG_LEVEL` | `INFO` | Logging level |
| `EDU_CTI_LOG_FILE` | `logs/pipeline.log` | Log file path |

### Optional — API Server

| Variable | Default | Description |
|---|---|---|
| `EDUTHREAT_ADMIN_PASSWORD_HASH` | *(unset)* | SHA-256 hash of admin password |
| `EDUTHREAT_ADMIN_API_KEY` | *(unset)* | Pre-generated admin API key |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:3000` | Allowed CORS origins |

### Optional — Data Collection

| Variable | Default | Description |
|---|---|---|
| `OTX_API_KEY` | *(unset)* | OTX AlienVault API key (skips source if unset) |
| `SERP_MAX_ATTEMPTS` | `3` | Max SERP discovery retries |
| `HISTORICAL_START_YEAR` | `2000` | Earliest year for historical scrapes |

---

## Development

### Running Tests

```bash
# All tests
pytest tests/ -v

# Phase 1 tests only
pytest tests/phase1/ -v

# Phase 2 with mock LLM (no API key required)
python tests/phase2/test_comprehensive_llm_extraction.py --mock

# With actual LLM
export OLLAMA_API_KEY=<key>
python tests/phase2/test_comprehensive_llm_extraction.py

# Coverage report
pytest tests/ --cov=src.edu_cti --cov-report=html
open htmlcov/index.html
```

### Code Quality

```bash
black src/             # Format code
flake8 src/            # Lint
mypy src/              # Type check
```

### Checking Database State

```bash
sqlite3 data/eduthreat.db "SELECT source, COUNT(*) FROM source_events GROUP BY source ORDER BY 2 DESC"
sqlite3 data/eduthreat.db "SELECT COUNT(*) FROM incidents WHERE llm_enriched = 1"
```

### Adding a New OSINT Source

1. Create `src/edu_cti/sources/<type>/<source_name>.py` with a `build_<source>_incidents()` function returning `list[BaseIncident]`
2. Register it in `src/edu_cti/core/sources.py`
3. Run the contributor test: `pytest tests/phase1/test_source_contribution.py -v --source-name <name>`

See [docs/ADDING_SOURCES.md](docs/ADDING_SOURCES.md) for a detailed step-by-step guide.

---

## Deployment

### Docker

```bash
docker build -t eduthreat-cti .
docker run \
  -e OLLAMA_API_KEY=<key> \
  -e OXYLABS_USERNAME=<user> \
  -e OXYLABS_PASSWORD=<pass> \
  -e EDUTHREAT_ADMIN_PASSWORD_HASH=<sha256_hash> \
  -p 8000:8000 \
  eduthreat-cti
```

### Railway (Recommended)

The pipeline and API run on [Railway](https://railway.app) with a persistent volume for the SQLite database.

```bash
railway login
railway link <project_id>
railway up
```

Set environment variables in the Railway dashboard. The Dockerfile handles everything else.

See [RAILWAY_SETUP.md](RAILWAY_SETUP.md) for the full deployment guide.

---

## Documentation

| Document | Description |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System design, component overview, data flow |
| [docs/DATABASE.md](docs/DATABASE.md) | Full SQLite schema reference |
| [docs/DATABASE_CONCURRENCY.md](docs/DATABASE_CONCURRENCY.md) | WAL mode, concurrency model, connection pools |
| [docs/API.md](docs/API.md) | Complete REST API endpoint reference |
| [docs/SOURCES.md](docs/SOURCES.md) | Source catalog with coverage notes |
| [docs/ADDING_SOURCES.md](docs/ADDING_SOURCES.md) | Step-by-step guide for contributing new sources |
| [docs/DEDUPLICATION.md](docs/DEDUPLICATION.md) | Two-level deduplication strategy |
| [docs/RESEARCHER_GUIDE.md](docs/RESEARCHER_GUIDE.md) | Using the dataset for academic research |
| [docs/ANALYST_GUIDE.md](docs/ANALYST_GUIDE.md) | CTI analyst workflow |
| [docs/PATHS_AND_STORAGE.md](docs/PATHS_AND_STORAGE.md) | Directory structure and file organization |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Contribution guidelines |
| [CHANGELOG.md](CHANGELOG.md) | Full version history |

---

## Research Context

EduThreat-CTI supports ongoing research into the education sector's cyber threat landscape, targeting publication at **ACM CCS 2026**. The dataset and methodology are designed to be reproducible and citable.

If you use this dataset or pipeline in academic work, please cite the accompanying paper (forthcoming). See [docs/RESEARCHER_GUIDE.md](docs/RESEARCHER_GUIDE.md) for citation guidance and dataset access instructions.

---

## Ethics & Legal

This project uses **public OSINT sources only**. No dark web scraping, no active scanning, no exploitation of any systems. All data is sourced from publicly available breach notifications, security journalism, and ransomware leak site mirrors that are indexed by search engines.

Incident data is used solely for defensive security research and threat intelligence purposes.

---

## Contributing

Contributions are welcome. The most impactful contributions are:

- **New OSINT sources** — especially non-English sources and regional threat trackers
- **Schema improvements** — additional CTI fields or improved normalization
- **Analysis tools** — scripts for cross-incident correlation or campaign tracking

Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request.

---

## License

[MIT License](LICENSE) — free to use, modify, and distribute with attribution.
