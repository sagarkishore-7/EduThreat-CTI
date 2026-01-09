# System Architecture

**Version**: 1.6.0  
**Last Updated**: 2026-01-08

## Overview

EduThreat-CTI is a production-grade cyber threat intelligence pipeline designed for scalability, maintainability, and contributor-friendly extension. The system follows a three-phase architecture with clear separation of concerns.

## Design Principles

1. **Phase-Based Organization**: Clear separation of Phase 1 (Ingestion), Phase 2 (Enrichment), and Phase 3 (API/Dashboard)
2. **Source Modularity**: Easy to add new sources following established patterns
3. **Incremental Processing**: Efficient re-runs that only process new data
4. **Schema-Driven**: Structured data models for consistent processing
5. **Production-Ready**: WAL mode database, concurrent access, error recovery
6. **Open Source Ready**: Clear contribution guidelines and comprehensive documentation

## System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         OSINT Sources                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│  │  Curated     │  │    News       │  │     RSS       │        │
│  │  Sources     │  │   Sources     │  │   Sources    │        │
│  └──────────────┘  └──────────────┘  └──────────────┘        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Phase 1: Ingestion                           │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Source Builders (sources/)                               │  │
│  │  - Fetch & Parse                                         │  │
│  │  - Normalize to BaseIncident                              │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Deduplication Engine (core/deduplication.py)             │  │
│  │  - Per-source deduplication                                │  │
│  │  - Cross-source URL matching                               │  │
│  │  - Incident merging                                       │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Database Storage (core/db.py)                            │  │
│  │  - SQLite with WAL mode                                   │  │
│  │  - Concurrent read/write support                          │  │
│  │  - Source attribution tracking                            │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              SQLite Database (eduthreat.db)                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │  incidents   │  │incident_      │  │ source_      │       │
│  │              │  │sources        │  │events        │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Phase 2: LLM Enrichment                        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Article Fetching (phase2/storage/article_fetcher.py)   │  │
│  │  - newspaper3k → Selenium → archive.org                  │  │
│  │  - Multi-fallback strategy                               │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  LLM Client (phase2/llm_client.py)                       │  │
│  │  - Ollama Cloud API                                       │  │
│  │  - Rate limit handling                                   │  │
│  │  - Structured extraction                                 │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Enrichment Storage (phase2/storage/db.py)               │  │
│  │  - JSON storage (incident_enrichments)                     │  │
│  │  - Flattened table (incident_enrichments_flat)            │  │
│  │  - Country normalization                                  │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Phase 3: API & Dashboard                       │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  REST API (api/)                                         │  │
│  │  - FastAPI framework                                     │  │
│  │  - Read-only connections (WAL mode)                     │  │
│  │  - Admin endpoints                                       │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Dashboard (EduThreat-CTI-Dashboard)                    │  │
│  │  - Next.js frontend                                      │  │
│  │  - Real-time visualizations                             │  │
│  │  - CTI report downloads                                  │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Repository Structure

```
EduThreat-CTI/
├── README.md                    # Main project documentation
├── CHANGELOG.md                 # Detailed version history
├── VERSION_HISTORY.md           # Quick version reference
├── CONTRIBUTING.md              # Contributor guidelines
├── LICENSE                      # MIT License
├── .gitignore                   # Git ignore rules
├── .env.example                 # Environment variable template
├── requirements.txt             # Python dependencies
├── pyproject.toml               # Python project configuration
├── Dockerfile                   # Docker container definition
├── railway.json                 # Railway deployment config
│
├── src/                         # Source code directory
│   └── edu_cti/                 # Main package
│       ├── __init__.py
│       │
│       ├── core/                # Shared core functionality
│       │   ├── __init__.py
│       │   ├── models.py        # BaseIncident data model
│       │   ├── config.py        # Configuration & path detection
│       │   ├── db.py            # Database operations (WAL mode)
│       │   ├── deduplication.py # Cross-source deduplication
│       │   ├── http.py          # HTTP client with Selenium fallback
│       │   ├── utils.py         # General utilities
│       │   ├── logging_utils.py  # Logging configuration
│       │   ├── pagination.py    # Pagination helpers
│       │   ├── sources.py       # Source registry
│       │   ├── countries.py     # Country normalization
│       │   ├── metrics.py       # Prometheus metrics
│       │   └── vpn.py           # VPN integration (NordVPN)
│       │
│       ├── sources/              # Source implementations
│       │   ├── __init__.py
│       │   ├── curated/          # Curated sources (dedicated sections)
│       │   │   ├── __init__.py
│       │   │   ├── common.py
│       │   │   ├── konbriefing.py
│       │   │   ├── ransomware_live.py
│       │   │   └── databreach.py
│       │   ├── news/             # News sources (keyword search)
│       │   │   ├── __init__.py
│       │   │   ├── common.py
│       │   │   ├── darkreading.py
│       │   │   ├── krebsonsecurity.py
│       │   │   ├── securityweek.py
│       │   │   ├── thehackernews.py
│       │   │   └── therecord.py
│       │   └── rss/              # RSS feed sources
│       │       ├── __init__.py
│       │       ├── common.py
│       │       ├── bleepingcomputer_rss.py
│       │       └── databreaches_rss.py
│       │
│       ├── pipeline/             # Phase-based pipelines
│       │   ├── __init__.py
│       │   ├── phase1/            # Phase 1: Ingestion
│       │   │   ├── __init__.py
│       │   │   ├── __main__.py   # CLI entry point
│       │   │   ├── orchestrator.py # Full pipeline orchestrator
│       │   │   ├── build_dataset.py # Dataset building
│       │   │   ├── base_io.py    # I/O utilities
│       │   │   ├── curated.py    # Curated source orchestrator
│       │   │   ├── news.py       # News source orchestrator
│       │   │   ├── rss.py        # RSS source orchestrator
│       │   │   └── incremental_save.py # Incremental saving
│       │   └── phase2/            # Phase 2: Enrichment
│       │       ├── __init__.py
│       │       ├── __main__.py   # CLI entry point
│       │       ├── enrichment.py # Main enrichment orchestrator
│       │       ├── llm_client.py # Ollama LLM client
│       │       ├── schemas.py    # Pydantic schemas
│       │       ├── csv_export.py # CSV export utilities
│       │       ├── extraction/   # LLM extraction components
│       │       │   ├── extraction_prompt.py
│       │       │   ├── extraction_schema.py
│       │       │   └── json_to_schema_mapper.py
│       │       ├── storage/      # Article fetching & storage
│       │       │   ├── article_fetcher.py
│       │       │   ├── article_storage.py
│       │       │   └── db.py
│       │       └── utils/        # Enrichment utilities
│       │           ├── deduplication.py
│       │           ├── fetching_strategy.py
│       │           └── revert_enrichments.py
│       │
│       ├── api/                   # Phase 3: REST API
│       │   ├── __init__.py
│       │   ├── __main__.py       # API server entry point
│       │   ├── main.py           # FastAPI application
│       │   ├── database.py       # Database operations for API
│       │   ├── models.py         # Pydantic response models
│       │   ├── admin.py          # Admin endpoints
│       │   └── reports.py        # CTI report generation
│       │
│       └── scheduler/             # Job scheduler
│           ├── __init__.py
│           └── scheduler.py     # Ingestion & enrichment scheduler
│
├── tests/                        # Test suite
│   ├── __init__.py
│   ├── phase1/                   # Phase 1 tests
│   │   ├── test_models.py
│   │   ├── test_deduplication.py
│   │   ├── test_csv_output.py
│   │   ├── test_incremental_save.py
│   │   ├── test_pipeline.py
│   │   ├── test_rss_pipeline.py
│   │   └── test_source_contribution.py
│   └── phase2/                   # Phase 2 tests
│       ├── test_llm_client.py
│       ├── test_phase2_enrichment.py
│       ├── test_phase2_deduplication.py
│       ├── test_comprehensive_llm_extraction.py
│       └── test_llm_response_validation.py
│
├── docs/                         # Documentation
│   ├── ARCHITECTURE.md           # This file
│   ├── DATABASE.md               # Database schema documentation
│   ├── DEDUPLICATION.md          # Deduplication logic
│   ├── ADDING_SOURCES.md         # Guide for adding sources
│   ├── SOURCES.md                # Source documentation
│   ├── API.md                    # API documentation
│   ├── RESEARCHER_GUIDE.md       # Guide for researchers
│   └── ANALYST_GUIDE.md          # Guide for CTI analysts
│
├── scripts/                      # Utility scripts
│   ├── setup.py                  # Setup verification
│   ├── fix_incident_dates.py     # Date correction script
│   └── migrate_db_to_railway.py  # Database migration
│
├── data/                         # Data directory (git-ignored)
│   ├── eduthreat.db              # SQLite database
│   ├── raw/                      # Raw collected data
│   │   ├── curated/
│   │   ├── news/
│   │   └── rss/
│   └── processed/                # Processed datasets
│       └── base_dataset.csv
│
└── logs/                         # Log files (git-ignored)
    └── pipeline.log
```

## Module Organization

### Core Module (`core/`)

Shared functionality used across all phases:

- **models.py**: `BaseIncident` data model and related structures
- **config.py**: Configuration constants, environment variable support, path detection
- **db.py**: SQLite database operations with WAL mode, connection management
- **deduplication.py**: Cross-source URL-based deduplication logic
- **sources.py**: Centralized source registry for easy source addition
- **http.py**: HTTP client with retry, rate limiting, Selenium fallback, bot detection bypass
- **countries.py**: Country normalization, ISO code mapping, flag emoji generation
- **utils.py**: General utility functions
- **logging_utils.py**: Logging configuration with truncation
- **pagination.py**: Pagination helpers for web scraping
- **metrics.py**: Prometheus metrics for monitoring
- **vpn.py**: VPN integration for IP rotation (NordVPN)

### Phase 1: Ingestion (`pipeline/phase1/`)

Data collection and normalization:

- **orchestrator.py**: Main orchestrator for full Phase 1 pipeline
- **__main__.py**: CLI entry point for Phase 1
- **curated.py**: Orchestrates curated source collection
- **news.py**: Orchestrates news source collection
- **rss.py**: Orchestrates RSS feed collection
- **build_dataset.py**: Builds unified CSV dataset from database
- **base_io.py**: File I/O utilities
- **incremental_save.py**: Incremental saving logic

### Phase 2: LLM Enrichment (`pipeline/phase2/`)

LLM-based enrichment and analysis:

- **__main__.py**: CLI entry point for Phase 2
- **enrichment.py**: Main enrichment orchestrator with producer-consumer pattern
- **llm_client.py**: Ollama Cloud API client with rate limit handling
- **schemas.py**: Pydantic schemas for validation
- **extraction/**: LLM extraction components
  - **extraction_prompt.py**: LLM prompt templates
  - **extraction_schema.py**: JSON schema for structured extraction
  - **json_to_schema_mapper.py**: Maps LLM JSON to Pydantic models
- **storage/**: Article fetching and storage
  - **article_fetcher.py**: Multi-fallback article fetching
  - **article_storage.py**: Article content storage
  - **db.py**: Enrichment database operations
- **utils/**: Enrichment utilities
  - **deduplication.py**: Post-enrichment deduplication
  - **fetching_strategy.py**: Smart incident selection
  - **revert_enrichments.py**: Revert enrichment utilities

### Phase 3: API & Dashboard (`api/`)

REST API for serving CTI data:

- **main.py**: FastAPI application with CORS, error handling
- **database.py**: Database operations for API (read-only by default)
- **models.py**: Pydantic response models
- **admin.py**: Admin endpoints (authentication, exports, scheduler)
- **reports.py**: CTI report generation (Markdown format)

### Sources Module (`sources/`)

Source implementations organized by type:

- **curated/**: Sources with dedicated education sector sections
- **news/**: Keyword-based search sources
- **rss/**: RSS feed sources

## Data Flow

### Phase 1: Ingestion Flow

```
OSINT Sources
    │
    ▼
Source Builders (sources/)
    │
    ▼
BaseIncident Objects
    │
    ▼
Deduplication Engine
    ├─ Per-source deduplication (source_events table)
    └─ Cross-source deduplication (URL matching)
    │
    ▼
Database (incidents table)
    │
    ├─ incident_sources (source attribution)
    └─ source_events (deduplication tracking)
    │
    ▼
CSV Export (optional)
```

### Phase 2: Enrichment Flow

```
Unenriched Incidents (from database)
    │
    ▼
Article Fetching
    ├─ newspaper3k (first attempt)
    ├─ Selenium (fallback for bot-protected sites)
    └─ archive.org (final fallback)
    │
    ▼
LLM Extraction
    ├─ Education relevance check
    ├─ Structured CTI extraction (192+ fields)
    └─ Schema validation
    │
    ▼
Enrichment Storage
    ├─ incident_enrichments (JSON)
    └─ incident_enrichments_flat (flattened for analytics)
    │
    ▼
Update incidents table
    ├─ llm_enriched flag
    ├─ primary_url (best URL selected)
    └─ incident_date (from timeline)
```

### Phase 3: API Flow

```
API Request
    │
    ▼
FastAPI Router
    │
    ├─ Read-only DB connection (WAL mode)
    ├─ Query database
    └─ Format response (Pydantic models)
    │
    ▼
JSON Response
    │
    ▼
Dashboard (Next.js)
    ├─ Real-time visualizations
    ├─ Incident browsing
    └─ CTI report downloads
```

## Database Architecture

See [DATABASE.md](DATABASE.md) for detailed schema documentation.

**Key Tables:**
- `incidents`: Main incident records (deduplicated)
- `incident_sources`: Source attribution (many-to-many)
- `source_events`: Per-source deduplication tracking
- `source_state`: Source ingestion state
- `incident_enrichments`: Phase 2 enrichment data (JSON)
- `incident_enrichments_flat`: Flattened enrichment data (analytics)

## Concurrency & Performance

### WAL Mode (Write-Ahead Logging)

- Enables concurrent reads and writes
- Multiple readers don't block writers
- Writers don't block readers
- Essential for production with API + background processes

### Connection Management

- **Read-only connections**: API endpoints use read-only mode (faster, no locks)
- **Write connections**: Background processes use write mode with longer timeout
- **Connection timeouts**: 5s for reads, 30s for writes
- **Transaction management**: Short transactions with immediate commits

See [DATABASE_CONCURRENCY.md](DATABASE_CONCURRENCY.md) for details.

## Error Handling & Recovery

### Article Fetching

- Multi-fallback strategy: `newspaper3k → Selenium → archive.org`
- Bot detection bypass for protected sites
- Broken URL tracking and retry logic

### LLM Enrichment

- Rate limit retry with exponential backoff
- Failed enrichments retry on next run
- Dynamic timeouts scale with incident count
- Queue management prevents premature stopping

### Database Operations

- WAL mode prevents locking issues
- Transaction rollback on errors
- Connection retry logic
- Data validation before insertion

## Production Deployment

### Railway Deployment

- Docker containerization
- Persistent storage at `/app/data`
- Environment variable configuration
- Automatic deployments from GitHub

### Monitoring

- Prometheus metrics for ingestion/enrichment
- Logging optimized for Railway (500 logs/sec limit)
- Error tracking and alerting

## Adding New Sources

See [ADDING_SOURCES.md](ADDING_SOURCES.md) for detailed guide.

**Quick Summary:**
1. Create source builder in appropriate `sources/` subdirectory
2. Register in `core/sources.py`
3. Test with contributor test suite
4. Document in `docs/SOURCES.md`

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for contribution guidelines.

## License

MIT License - see [LICENSE](../LICENSE) file.
