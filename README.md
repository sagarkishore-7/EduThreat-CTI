# EduThreat-CTI

**Real-time cyber threat intelligence pipeline for the global education
sector**

EduThreat-CTI is an open-source cyber threat intelligence (CTI)
framework focused on **cyber incidents affecting universities, schools,
and research institutions worldwide**.\
Its mission is to make the education sector's threat landscape
**transparent, analyzable, and research-ready** by building a unified
dataset from diverse OSINT sources.

This project is inspired by large-scale cyber-incident measurement
studies (e.g., USENIX Security research) and extends that approach
**vertically** into the education domain.

------------------------------------------------------------------------

# 🎯 Project Goals

EduThreat-CTI aims to:

### ✔ Collect

Continuously ingest cyber-incident signals from a curated set of open
sources:

-   KonBriefing (cyber attacks on universities)
-   Ransomware leak site mirrors (RansomWatch, RansomFeed)
-   Cybersecurity news feeds (BleepingComputer, The Record)
-   University IT status pages and official disclosures
-   CERT advisories (CISA, NCSC, CERT-EU)
-   Other high-signal OSINT channels

### ✔ Normalize

Convert raw incidents into a **unified base schema**:

-   incident_id
-   source
-   university_name
-   country
-   incident_date
-   reference_urls
-   title / subtitle
-   attack_type_hint

### ✔ Prepare for Enrichment

Provide a clean dataset ready for:

-   LLM-based extraction\
-   MITRE ATT&CK mapping\
-   Timeline reconstruction\
-   Ransomware family classification\
-   Teaching/research/operations impact analysis\
-   Transparency & governance scoring\
-   STIX 2.1 / TAXII feeds

------------------------------------------------------------------------

# 🏗 Project Structure

    EduThreat-CTI/
    ├─ README.md
    ├─ LICENSE
    ├─ requirements.txt
    ├─ pyproject.toml
    ├─ src/
    │  └─ edu_cti/
    │     ├─ __init__.py
    │     ├─ core/                      # Shared core functionality
    │     │  ├─ __init__.py
    │     │  ├─ models.py               # Data models (BaseIncident)
    │     │  ├─ config.py               # Configuration (with env var support)
    │     │  ├─ db.py                   # Database operations
    │     │  ├─ deduplication.py        # Cross-source deduplication logic
    │     │  ├─ http.py                 # HTTP client with bot detection bypass
    │     │  ├─ utils.py                # Utility functions
    │     │  ├─ logging_utils.py        # Logging configuration
    │     │  ├─ pagination.py           # Pagination utilities
    │     │  └─ sources.py              # Source registry (for easy source addition)
    │     ├─ sources/                   # Source implementations
    │     │  ├─ __init__.py
    │     │  ├─ curated/                # Sources with dedicated education sections
    │     │  │  ├─ __init__.py
    │     │  │  ├─ common.py
    │     │  │  ├─ konbriefing.py
    │     │  │  ├─ ransomware_live.py
    │     │  │  └─ databreach.py
    │     │  ├─ news/                   # Keyword-based search sources
    │     │  │  ├─ __init__.py
    │     │  │  ├─ common.py
    │     │  │  ├─ krebsonsecurity.py
    │     │  │  ├─ thehackernews.py
    │     │  │  ├─ therecord.py
    │     │  │  ├─ securityweek.py
    │     │  │  └─ darkreading.py
    │     │  └─ rss/                    # RSS feed sources
    │     │     ├─ __init__.py
    │     │     ├─ common.py
    │     │     └─ databreaches_rss.py
    │     └─ pipeline/                  # Phase-based pipelines
    │        ├─ __init__.py
    │        ├─ phase1/                 # Phase 1: Ingestion
    │        │  ├─ __init__.py
    │        │  ├─ __main__.py          # Main CLI entry point
    │        │  ├─ orchestrator.py      # Full pipeline orchestrator
    │        │  ├─ build_dataset.py     # Dataset building
    │        │  ├─ base_io.py           # I/O utilities
    │        │  ├─ curated.py           # Curated source orchestrator
    │        │  ├─ news.py              # News source orchestrator
    │        │  ├─ rss.py               # RSS source orchestrator
    │        │  └─ incremental_save.py  # Incremental saving logic
    │        └─ phase2/                 # Phase 2: Enrichment
    │           ├─ __init__.py
    │           ├─ __main__.py          # Main CLI entry point
    │           ├─ enrichment.py        # Main enrichment orchestrator
    │           ├─ llm_client.py        # Ollama LLM client
    │           ├─ article_fetcher.py   # Article fetching
    │           ├─ metadata_extractor.py # Schema coverage analysis
    │           ├─ schemas.py           # Pydantic schemas
    │           ├─ schemas_extended.py  # Extended schemas for analytics
    │           ├─ db.py                # Enrichment database operations
    │           └─ deduplication.py     # Post-enrichment deduplication
    ├─ data/                          # Data directory (git-ignored)
    │  ├─ eduthreat.db                # SQLite database (git-ignored)
    │  ├─ raw/                        # Raw collected data (git-ignored)
    │  └─ processed/                  # Processed datasets (git-ignored)
    ├─ tests/                         # Test suite
    │  ├─ phase1/                     # Phase 1 tests
    │  └─ phase2/                     # Phase 2 tests
    ├─ docs/                          # Documentation
    │  ├─ ARCHITECTURE.md             # System architecture
    │  ├─ DATABASE.md                 # Database schema
    │  ├─ DEDUPLICATION.md            # Deduplication logic
    │  ├─ SOURCES.md                  # Source documentation
    │  ├─ ADDING_SOURCES.md           # Guide for adding sources
    │  ├─ RAW_DIRECTORY.md            # Raw data structure
    │  └─ URL_SCHEMA.md               # URL handling schema
    ├─ logs/                          # Log files (git-ignored)
    ├─ .gitignore                     # Git ignore rules
    ├─ .env.example                   # Environment variable template
    ├─ LICENSE                        # MIT License
    ├─ CONTRIBUTING.md                # Contribution guidelines
    ├─ CHANGELOG.md                   # Version history
    ├─ setup.py                       # Package setup
    ├─ pyproject.toml                 # Modern Python project config
    └─ requirements.txt               # Python dependencies

------------------------------------------------------------------------

# 🚀 Getting Started

``` bash
git clone https://github.com/sagarkishore-7/EduThreat-CTI.git
cd EduThreat-CTI
pip install -r requirements.txt

# Run the complete Phase 1 pipeline (recommended)
python -m src.edu_cti.pipeline.phase1.orchestrator
```

This will:
1. Initialize the SQLite database (`data/eduthreat.db`)
2. Ingest incidents from all sources into the database (with cross-source deduplication)
3. Build the unified base dataset CSV (`data/processed/base_dataset.csv`) from database


**Output files:**
- `data/eduthreat.db` - **SQLite database** with deduplicated incidents (cross-source deduplication applied at ingestion)
- `data/raw/curated/konbriefing_base.csv` - KonBriefing incidents snapshot
- `data/raw/curated/ransomwarelive_base.csv` - Ransomware.live incidents snapshot
- `data/raw/curated/databreach_base.csv` - DataBreaches.net education sector incidents snapshot
- `data/raw/news/*_base.csv` - Per-source news incident snapshots
- `data/processed/base_dataset.csv` - **Unified base dataset (Phase 1 output)** - Deduplicated CSV export

**Database Architecture:**
- **Deduplicated storage**: Database stores deduplicated incidents only (cross-source deduplication at ingestion)
- **Source attribution**: `incident_sources` table tracks which sources contributed to each incident
- **Incremental updates**: Efficient re-runs that only process new incidents
- **Ready for Phase 2**: Clean database structure ready for LLM enrichment

**Note:** In Phase 1, all URLs are collected in the `all_urls` field with `primary_url=None`. Phase 2 (LLM enrichment) will select the best URL from `all_urls` and set it as `primary_url`.

------------------------------------------------------------------------

# 🔍 Current Functionality (Phase 1)

✔ **Curated sources** (dedicated education sector sections):
  - KonBriefing (university cyber attacks database)
  - Ransomware.live (education sector filter)
  - DataBreaches.net (education sector archive)
✔ **News sources** (keyword-based search):
  - Krebs on Security, The Hacker News, The Record, SecurityWeek, Dark Reading
✔ Unified schema\
✔ Base dataset builder\
✔ **Cross-source URL-based deduplication** (at database ingestion level)\
✔ **Production-ready structure** (tests, config management, packaging)\
✔ **Database-driven architecture** (deduplicated storage, source attribution tracking)

## 📰 News & Search Scrapers

-   Dedicated ingestors for DataBreaches.net's Education archive (496+ pages as of Nov 2025) and per-source keyword searches (SecurityWeek, The Record, Dark Reading).
-   Each scraper walks the native pagination controls (e.g., `<ul class="page-numbers">` on DataBreaches.net, Algolia pagination on SecurityWeek/The Record) to discover the latest page dynamically.
-   Requests rotate User-Agents, inject randomized delays, and follow in-page "Next" links to better mimic human browsing patterns.
-   Normalized `BaseIncident` rows (with deterministic IDs) are written to `data/raw/news/<source>_base.csv` and merged into both the unified CSV snapshot and SQLite ingestion pipeline.

## 🔄 Deduplication

The pipeline automatically deduplicates incidents at multiple levels:

### Per-Source Deduplication
- Prevents re-ingesting the same incident from the same source
- Tracked via `source_events` table
- Enables efficient incremental updates

### Cross-Source Deduplication
- Merges incidents with same URLs from different sources during ingestion
- **URL Normalization**: URLs are normalized (removing trailing slashes, www. prefix, fragments) for accurate matching
- **Smart Merging**: When the same incident appears in multiple sources:
  - Keeps the incident with highest `source_confidence`
  - Merges all URLs from all sources into `all_urls`
  - Combines metadata, preferring non-empty values
  - Tracks all sources in `incident_sources` table (many-to-many relationship)
- **Database Structure**: Database stores deduplicated incidents only, with source attribution tracked separately

**Benefits:**
- ✅ Database is clean and ready for Phase 2 (LLM enrichment)
- ✅ Simple queries for Phase 3 (CTI website)
- ✅ Automatic deduplication when contributors add new sources

## ⚙️ Running Pipelines

### Main Pipeline Script (Recommended)

**`python -m src.edu_cti.pipeline.phase1.orchestrator`** - Run the complete Phase 1 pipeline:
- Initializes database
- Ingests incidents from all sources (with per-source deduplication)
- Builds unified base dataset CSV **from database** (production-efficient, no re-scraping)
- Applies cross-source deduplication when building CSV
- Ensures `primary_url=None` and all URLs in `all_urls`

**Production Mode (Default)**:
- Database prevents duplicate inserts (per-source deduplication)
- CSV is built from database (fast, no re-scraping)
- DB and CSV stay in sync automatically
- Re-runs only process new incidents

**Examples:**
```bash
# Run full pipeline (all sources, fetches all pages by default)
python -m src.edu_cti.pipeline.phase1.orchestrator

# Run only news sources
python -m src.edu_cti.pipeline.phase1.orchestrator --groups news

# Run only curated sources
python -m src.edu_cti.pipeline.phase1.orchestrator --groups curated

# Run specific news sources
python -m src.edu_cti.pipeline.phase1.orchestrator --groups news --news-sources darkreading krebsonsecurity

# Run with page limits (for testing)
python -m src.edu_cti.pipeline.phase1.orchestrator --news-max-pages 10

# Fetch all pages explicitly
python -m src.edu_cti.pipeline.phase1.orchestrator --news-max-pages all

# Skip ingestion (only build dataset)
python -m src.edu_cti.pipeline.phase1.orchestrator --skip-ingestion

# Skip dataset building (only run ingestion)
python -m src.edu_cti.pipeline.phase1.orchestrator --skip-dataset

# Disable cross-source deduplication
python -m src.edu_cti.pipeline.phase1.orchestrator --no-deduplication

# Re-scrape sources for CSV (instead of using database - for testing)
python -m src.edu_cti.pipeline.phase1.orchestrator --fresh-collection
```

### Phase 2: LLM Enrichment Pipeline

**`python -m src.edu_cti.pipeline.phase2`** - Run Phase 2 LLM enrichment:

After Phase 1 collects incidents, Phase 2 enriches them with LLM analysis:

1. **Article Fetching**: Fetches articles from all URLs in `all_urls`
2. **Education Relevance Check**: Uses LLM to verify incidents are education-related
3. **URL Confidence Scoring**: Scores and selects the best primary URL from `all_urls`
4. **Comprehensive Enrichment**: Extracts:
   - Detailed timeline of events
   - MITRE ATT&CK techniques and tactics
   - Attack dynamics (ransomware family, impact scope, recovery)
   - Business and operational impact analysis

**Requirements:**
- `OLLAMA_API_KEY` environment variable must be set
- Ollama Cloud API access

**Examples:**
```bash
# Set API key
export OLLAMA_API_KEY=your_api_key_here

# Run enrichment on all unenriched incidents
python -m src.edu_cti.pipeline.phase2

# Process only first 10 incidents (for testing)
python -m src.edu_cti.pipeline.phase2 --limit 10

# Process with custom batch size and rate limiting
python -m src.edu_cti.pipeline.phase2 --batch-size 5 --rate-limit-delay 3.0

# Process incidents even if not education-related (not recommended)
python -m src.edu_cti.pipeline.phase2 --keep-non-education
```

**Features:**
- ✅ **Incremental Processing**: Only processes unenriched incidents
- ✅ **Rate Limiting**: Configurable delays between API calls
- ✅ **Batch Processing**: Processes incidents in batches
- ✅ **Error Handling**: Gracefully handles failures and continues
- ✅ **Schema-Constrained Output**: Uses Pydantic models for structured extraction
- ✅ **Education Relevance Filtering**: Skips non-education incidents by default

**Model Selection:**
Phase 2 uses `deepseek-v3.1:671b-cloud` by default (configurable via `OLLAMA_MODEL` env var). This model was chosen for its superior performance on complex structured extraction tasks required for CTI analysis.

**Production Efficiency**:
- ✅ **Default behavior**: CSV built from database (no re-scraping)
- ✅ **Incremental updates**: Only new incidents are processed
- ✅ **DB and CSV sync**: CSV always reflects current database state
- ✅ **Cross-source dedup**: Applied when building CSV from database
- ✅ **Fast re-runs**: Database queries are much faster than re-scraping

### Individual Pipeline Scripts

-   `python -m src.edu_cti.pipeline.phase1.build_dataset --groups curated news`\
    Build snapshots for the requested source groups and emit `data/processed/base_dataset.csv`. Omit `--groups` to run both groups.
-   `python -m src.edu_cti.pipeline.phase1 --groups news`\
    Ingest incidents from the selected groups into `data/eduthreat.db` with per-source deduplication. Schedule `--groups news` for frequent real-time runs and `--groups curated` as a periodic refresh.

**Alternative:** After installation, you can use the CLI commands:
- `eduthreat-phase1` - Phase 1 ingestion pipeline
- `eduthreat-phase2` - Phase 2 enrichment pipeline
- `eduthreat-orchestrator` - Phase 1 full orchestrator (ingestion + dataset building)
- `eduthreat-build` - Dataset building only

------------------------------------------------------------------------

# 🧭 Roadmap

## Phase 1 --- Ingestion & Baseline

-   **Curated sources** (dedicated education sector sections):
    - KonBriefing (university cyber attacks database)
    - Ransomware.live (education sector filter)
    - DataBreaches.net (education sector archive)
-   **News sources** (keyword-based search):
    - Krebs on Security, The Hacker News, The Record, SecurityWeek, Dark Reading
-   Deduplication

## Phase 2 --- LLM Enrichment

-   ✅ Article fetching from URLs\
-   ✅ Education relevance checking\
-   ✅ URL confidence scoring and primary URL selection\
-   ✅ LLM-based structured extraction\
-   ✅ Timeline construction\
-   ✅ MITRE ATT&CK mapping\
-   ✅ Attack dynamics modeling\
-   ✅ Comprehensive CTI enrichment with Pydantic schemas

## Phase 3 --- CTI Outputs

-   Public dataset export\
-   STIX/TAXII\
-   Dashboard & analytics

------------------------------------------------------------------------

# 🧪 Technical Principles

-   Modularity\
-   Reproducibility\
-   Transparency\
-   Ethical OSINT only

------------------------------------------------------------------------

# 📊 BaseIncident Schema (Phase 1)

**Core Fields:**
-   `incident_id` - Unique identifier (deterministic hash)
-   `source` - Primary source name (for display; source attribution tracked in `incident_sources` table)
-   `source_event_id` - Source-native event ID (from primary source)
-   `university_name` - Normalized institution name
-   `victim_raw_name` - Original name from source
-   `institution_type` - "University" | "School" | "Research Institute" | "Unknown"
-   `country` - ISO-2 country code
-   `region`, `city` - Geographic details
-   `incident_date` - YYYY-MM-DD format
-   `date_precision` - "day" | "month" | "year" | "unknown"
-   `title`, `subtitle` - Article/incident text
-   `primary_url` - **None in Phase 1** (set by Phase 2 LLM enrichment)
-   `all_urls` - **List of all URLs** linked to the incident (Phase 1 collects all)
-   `attack_type_hint` - Basic classification (e.g., "ransomware")
-   `status` - "suspected" | "confirmed"
-   `source_confidence` - "low" | "medium" | "high"
-   `notes` - Additional metadata

**Phase 1 URL Handling:**
- All URLs are collected in `all_urls` field
- `primary_url` is set to `None` in Phase 1
- Phase 2 (LLM enrichment) selects the best URL from `all_urls` and sets it as `primary_url`

**Phase 2 LLM Enrichment Output:**
Phase 2 enriches incidents with structured CTI data stored in the database:

- **`llm_enriched`**: Boolean flag indicating enrichment status
- **`llm_enriched_at`**: Timestamp of enrichment
- **`primary_url`**: Best URL selected by LLM confidence scoring
- **`llm_summary`**: Comprehensive incident summary
- **`llm_timeline`**: JSON timeline of events with dates, IOCs, and actor attribution
- **`llm_mitre_attack`**: JSON array of MITRE ATT&CK techniques identified
- **`llm_attack_dynamics`**: JSON attack dynamics (vector, chain, impact, ransomware family, etc.)

Full enrichment data is stored in `incident_enrichments` table with complete structured output.

------------------------------------------------------------------------

# ⚠️ Ethics & Legal

Uses **public OSINT only**.\
No dark web, no scanning, no exploitation.

------------------------------------------------------------------------

# 📜 License

MIT License

------------------------------------------------------------------------

# 🤝 Contributions

Contributions welcome!\
Help by improving ingestion, adding OSINT sources, or refining schemas.

## Adding New Sources

1. Create a builder function in the appropriate ingest module (`ingest/curated/` or `ingest/news/`)
2. Register it in `sources.py` in the appropriate registry
3. The pipeline will automatically pick it up

See `docs/SOURCES.md` for a list of potential additional sources.

## Development Setup

```bash
# Install in development mode
pip install -e .

# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black src/

# Lint code
flake8 src/
```

------------------------------------------------------------------------

# 🌐 Why This Matters

The education sector is now a top target of cyberattacks, yet public
visibility is fragmented. EduThreat-CTI aims to build the **first open,
real-time CTI pipeline dedicated to academia**.
