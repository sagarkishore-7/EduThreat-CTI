# Architecture & Project Structure

## Overview

EduThreat-CTI follows a production-grade, modular architecture designed for scalability, maintainability, and contributor-friendly extension.

## Design Principles

1. **Phase-Based Organization**: Clear separation of Phase 1 (Ingestion), Phase 2 (Enrichment), and Phase 3 (Outputs)
2. **Source Modularity**: Easy to add new sources following established patterns
3. **Incremental Processing**: Efficient re-runs that only process new data
4. **Schema-Driven**: Structured data models for consistent processing
5. **Open Source Ready**: Clear contribution guidelines and documentation

## Repository Structure

```
EduThreat-CTI/
├── README.md                    # Main project documentation
├── CONTRIBUTING.md              # Contributor guidelines
├── LICENSE                      # License file (MIT)
├── CHANGELOG.md                 # Version history
├── .gitignore                   # Git ignore rules
├── .env.example                 # Environment variable template
├── requirements.txt             # Python dependencies
├── pyproject.toml               # Modern Python project configuration
├── setup.py                     # Package setup (legacy support)
│
├── src/                         # Source code directory
│   └── edu_cti/                 # Main package
│   ├── __init__.py
│   │
│   ├── __init__.py
│   ├── models.py                # Data models (BaseIncident, etc.)
│   ├── config.py                # Configuration constants
│   ├── db.py                    # Database operations
│   ├── deduplication.py         # Cross-source deduplication
│   ├── http.py                  # HTTP client utilities
│   ├── utils.py                 # General utilities
│   ├── logging_utils.py         # Logging configuration
│   ├── pagination.py            # Pagination helpers
│   ├── sources.py               # Source registry
│   │
│   ├── cli/                     # Command-line interfaces
│   │   ├── __init__.py
│   │   ├── pipeline.py          # Main pipeline orchestrator (Phase 1)
│   │   ├── ingestion.py         # Database ingestion (Phase 1)
│   │   ├── build_dataset.py     # Dataset building (Phase 1)
│   │   └── enrichment.py        # LLM enrichment pipeline (Phase 2)
│   │
│   ├── ingest/                  # Source implementations
│   │   ├── curated/             # Curated sources (dedicated sections)
│   │   │   ├── __init__.py
│   │   │   ├── konbriefing.py
│   │   │   ├── ransomware_live.py
│   │   │   └── databreach.py
│   │   ├── news/                # News sources (keyword search)
│   │   │   ├── __init__.py
│   │   │   ├── common.py        # Shared utilities
│   │   │   ├── darkreading.py
│   │   │   ├── krebsonsecurity.py
│   │   │   ├── securityweek.py
│   │   │   ├── thehackernews.py
│   │   │   └── therecord.py
│   │   └── rss/                 # RSS feed sources
│   │       ├── __init__.py
│   │       ├── common.py        # Shared utilities
│   │       └── databreaches_rss.py
│   │
│   ├── pipelines/               # Pipeline orchestration (Phase 1)
│   │   ├── __init__.py
│   │   ├── base_io.py           # File I/O utilities
│   │   ├── curated.py           # Curated sources pipeline
│   │   ├── news.py              # News sources pipeline
│   │   ├── rss.py               # RSS feed pipeline
│   │   └── incremental_save.py  # Incremental saving
│   │
│   └── enrichment/              # Phase 2: LLM Enrichment
│       ├── __init__.py
│       ├── enrichment.py        # Main enrichment orchestrator
│       ├── llm_client.py        # LLM API client
│       ├── article_fetcher.py   # Article fetching
│       ├── metadata_extractor.py  # Metadata coverage
│       ├── schemas.py           # Pydantic schemas
│       ├── schemas_extended.py  # Extended analytics schemas
│       └── db.py                # Enrichment database ops
│
├── tests/                       # Unit and integration tests
│   ├── __init__.py
│   ├── test_models.py
│   ├── test_deduplication.py
│   ├── test_csv_output.py
│   ├── test_incremental_save.py
│   ├── test_pipeline.py
│   └── test_rss_pipeline.py
│
├── docs/                        # Documentation
│   ├── ARCHITECTURE.md          # This file
│   ├── ADDING_SOURCES.md        # Guide for adding sources
│   ├── DATABASE.md              # Database schema and usage
│   ├── DEDUPLICATION.md         # Deduplication strategy
│   ├── SOURCES.md               # Source recommendations
│   ├── URL_SCHEMA.md            # URL schema documentation
│   └── RAW_DIRECTORY.md         # Raw data directory structure
│
├── data/                        # Data directories (gitignored)
│   ├── raw/                     # Raw source data
│   │   ├── curated/
│   │   ├── news/
│   │   └── rss/
│   └── processed/               # Processed datasets
│       └── base_dataset.csv
│
└── logs/                        # Log files (gitignored)
    └── pipeline.log
```

## Module Organization

### Core Module (`core/`)

Shared functionality used across all phases:

- **models.py**: BaseIncident data model and related structures
- **config.py**: Configuration constants and environment variable support
- **db.py**: SQLite database operations and schema management
- **deduplication.py**: Cross-source URL-based deduplication logic
- **sources.py**: Centralized source registry for easy source addition
- **http.py**: HTTP client with retry, rate limiting, and Selenium fallback
- **utils.py**: General utility functions
- **logging_utils.py**: Logging configuration
- **pagination.py**: Pagination helpers for web scraping

### Phase 1: Ingestion & Baseline (`phase1/`)

Data collection and normalization:

- **pipelines/**: Orchestrates collection from multiple source types
- **cli/**: Command-line interfaces for running Phase 1 operations

### Phase 2: LLM Enrichment (`phase2/`)

LLM-based enrichment and analysis:

- **enrichment/**: Core enrichment modules
- **cli/**: Command-line interface for enrichment pipeline

### Sources Module (`sources/`)

Source implementations organized by type:

- **curated/**: Sources with dedicated education sector sections
- **news/**: Keyword-based search sources
- **rss/**: RSS feed sources

## Data Flow

```
┌─────────────┐
│ OSINT       │
│ Sources     │
└──────┬──────┘
       │
       ▼
┌──────────────────┐
│ Phase 1:         │
│ - Fetch          │
│ - Parse          │
│ - Normalize      │
│ - Deduplicate    │
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│ Database         │
│ (SQLite)         │
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│ Base Dataset     │
│ (CSV)            │
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│ Phase 2:         │
│ - Fetch Articles │
│ - LLM Enrichment │
│ - Schema Extract │
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│ Enriched         │
│ Dataset          │
└──────────────────┘
```

## Adding New Sources

See [docs/ADDING_SOURCES.md](ADDING_SOURCES.md) for detailed guide.

**Quick Summary:**
1. Create source builder in appropriate `sources/` subdirectory
2. Register in `core/sources.py`
3. Test with CLI
4. Document in `docs/SOURCES.md`

## Database Schema

See [docs/DATABASE.md](DATABASE.md) for detailed schema.

**Key Tables:**
- `incidents`: Main incident records
- `incident_sources`: Source attribution (many-to-many)
- `source_events`: Per-source deduplication tracking
- `incident_enrichments`: Phase 2 enrichment data

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for contribution guidelines.

## License

MIT License - see [LICENSE](../LICENSE) file.
