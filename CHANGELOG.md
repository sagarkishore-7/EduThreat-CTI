# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.5.0] - 2025-11-27

### Enhanced Article Extraction & Cookie Consent Handling

This release significantly improves article extraction with dynamic global selectors and robust cookie consent handling for reliable content fetching from international news sites.

#### Added
- **Universal Content Extraction (80+ Selectors)**: Comprehensive CSS selectors covering global news sites, multiple CMS platforms, and semantic HTML patterns:
  - Site-specific: DarkReading, SecurityWeek, BleepingComputer, The Record, lesoir.be
  - CMS: WordPress, Drupal, Joomla, Ghost, Medium, Substack
  - Pattern-based: Dynamic class matching (`[class*="article-body"]`, etc.)
  - Semantic: HTML5 article/main elements, Schema.org microdata
  - Fallback: Progressive degradation with multiple fallback layers

- **Cookie Consent Auto-Handler**: Automatic detection and acceptance of cookie consent dialogs:
  - OneTrust, SourcePoint, Evidon consent frameworks
  - TechTarget/DarkReading-specific selectors
  - Generic button text matching (Accept, Agree, OK, etc.)
  - iframe support for embedded consent dialogs
  - Post-acceptance wait for dialog dismissal

- **Progress Tracking**: Real-time progress indicators for Phase 2 pipeline:
  - Fetching progress: `[5/20] (25%)`
  - Enrichment progress: `[3/20] (15%)`

- **`.gitignore` File**: Proper git ignore rules for Python, IDE, data files, and caches

#### Changed
- **Content Extraction Strategy**: Multi-fallback approach with priority ordering:
  1. Site-specific selectors (highest priority)
  2. CMS-specific patterns
  3. Semantic HTML5 elements
  4. Generic class/id patterns
  5. All paragraphs (fallback)
  6. Body text (last resort)

- **Unwanted Element Removal**: Expanded list of non-content elements removed before extraction:
  - Sidebars, comments, related articles, social buttons
  - Advertisements, newsletters, popups, modals
  - Navigation, breadcrumbs, tag clouds

- **Education Relevance**: LLM's `is_edu_cyber_incident` output is now the sole determinant (removed trusted sources override)

#### Fixed
- **DarkReading Article Fetching**: Cookie consent pop-ups no longer block content extraction
- **lesoir.be Extraction**: Added specific selectors for Belgian news site (`r-article--section`)
- **Selenium Cookie Handling**: Robust iframe switching for embedded consent dialogs
- **Multiple Indentation Errors**: Fixed indentation issues across pipeline files

#### Technical Improvements
- **Dynamic Selector Patterns**: Uses CSS attribute contains selectors (`[class*="..."]`) for flexibility
- **Parent Element Filtering**: Skips text inside nav/aside/footer elements
- **Deduplication**: Prevents duplicate content paragraphs
- **Character Limit**: Body text fallback limited to 10k characters

---

## [1.4.0] - 2025-11-26

### Incremental Ingestion for Phase 1

This release adds **incremental ingestion** support for all Phase 1 sources, enabling efficient daily updates without re-scraping entire archives.

#### Added
- **Incremental Mode (Default)**: All source builders now track `last_pubdate` in `source_state` table
  - `--full-historical` flag for first-time historical scrape
  - Default mode only fetches new incidents since last ingestion
  - Dramatically reduces ingestion time for regular updates (hours → seconds)
- **Source State Tracking**: New `source_state` table tracks last ingestion date per source
  - Automatically updated after each successful ingestion
  - Supports all curated, news, and RSS sources
- **ISO 8601 Date Parsing**: Added support for ISO 8601 datetime format (`2025-11-19T11:23:06-05:00`)
- **Brotli Compression Support**: Added `brotli` package for handling Brotli-compressed HTTP responses

#### Changed
- **DataBreaches.net (Curated)**: Stops pagination when reaching already-ingested dates
  - First run: All 490+ pages (~2-3 hours)
  - Daily incremental: 1-5 pages (~30 seconds)
- **KonBriefing**: Skips incidents older than `last_pubdate`
- **RSS Feeds**: All RSS sources skip articles older than `last_pubdate`
- **CLI Flags**:
  - `--full-historical`: Force full historical scrape
  - Default: Incremental mode

#### Fixed
- **Date Parsing**: ISO 8601 dates from DataBreaches.net `<time>` tags now parse correctly
- **Indentation Errors**: Fixed multiple indentation issues in source files

#### Usage

```bash
# First-time setup (full historical - takes hours)
python -m src.edu_cti.pipeline.phase1 --full-historical

# Daily updates (incremental - takes seconds)
python -m src.edu_cti.pipeline.phase1

# Run specific source
python -m src.edu_cti.pipeline.phase1 --groups curated --sources databreach

# Check source state
sqlite3 data/eduthreat.db "SELECT * FROM source_state"
```

---

## [1.3.0] - 2025-11-25

### Enhanced CTI Schema + BleepingComputer RSS Source

This release significantly enhances the extraction schema with extensive enum values for accurate and comprehensive threat intelligence extraction and cross-incident analysis. Also adds BleepingComputer as a new RSS data source.

#### Added
- **BleepingComputer RSS Source**: New RSS feed source (`bleepingcomputer`) that filters for Security category articles containing education keywords
- **Extended Education Keywords**: 70+ education-related keywords in `config.EDUCATION_KEYWORDS` for comprehensive filtering (institution types, education terms, levels, identifiers, research terms)
- **50+ Attack Categories**: Granular attack classification including ransomware variants (encryption, double/triple extortion), phishing types, data breach variants, malware types, and insider threat categories
- **60+ Attack Vectors**: Comprehensive initial access vectors including credential-based, vulnerability exploitation, exposed services, cloud-specific, and supply chain attacks
- **35+ Ransomware Families**: Complete ransomware family enumeration (LockBit, BlackCat/ALPHV, Cl0p, Akira, Play, 8Base, BianLian, Royal, Black Basta, Medusa, Rhysida, etc.)
- **Threat Actor Classification**: APT/nation-state, ransomware gangs, affiliates, hacktivists, with motivation tracking
- **30+ Data Categories**: Granular data type classification (student PII/SSN/grades, employee payroll, research IP, credentials)
- **35+ System Categories**: Detailed system impact tracking (LMS, SIS, ERP, research HPC, hospital systems)
- **25+ Operational Impacts**: Comprehensive operational disruption types (classes cancelled/moved online, exams postponed, research halted)
- **25+ Security Improvements**: Recovery action enumeration (MFA, EDR, network segmentation, zero trust)
- **Cross-Incident Analysis Fields**: Campaign tracking, related incidents, sector targeting patterns
- **MITRE ATT&CK Integration**: Full tactic enumeration with technique ID validation

#### Changed
- **Pydantic Schemas Updated**: All schema enums expanded to match extraction schema
- **Extraction Prompt Enhanced**: Detailed instructions for comprehensive CTI extraction
- **JSON-to-Schema Mapper**: Updated with new category mappings

---

## [1.2.0] - 2025-11-25

### Phase 2 LLM Enrichment Pipeline - Production Ready

This release marks the Phase 2 enrichment pipeline as **production-ready** with comprehensive bug fixes, improved error handling, and robust article fetching.

#### Added
- **Archive.org Fallback**: All failed article fetches now try archive.org/Wayback Machine as final fallback
- **Intelligent Error Classification**: Distinguishes between "enrichment failed" vs "not education-related" for proper retry logic
- **Ad Popup Handler**: Selenium now detects and closes common ad/newsletter popups during article extraction
- **Comprehensive CSV Export**: All victim name fields (`university_name`, `victim_raw_name`, `victim_raw_name_normalized`) now populate correctly
- **Contributor Test Suite**: New test structure for contributors adding sources

#### Changed
- **Article Fetching Strategy**: `newspaper3k → Selenium → archive.org → Skip` for all sites
- **Error Recovery**: Failed enrichments no longer mark incidents as "not education-related" - they will retry on next run
- **LLM Prompt Consolidation**: Single source of truth for LLM instructions in `extraction_prompt.py`
- **Database Storage**: All data flows to single `eduthreat.db` (removed separate test databases)

#### Fixed
- **CSV Victim Name Bug**: Fixed `victim_raw_name` and `university_name` being empty while `victim_raw_name_normalized` was filled
- **JSON Escape Handling**: Fixed `Invalid \escape` errors in LLM JSON output parsing
- **NoneType Iteration**: Fixed `TypeError` when `operational_impact` list is None
- **Path Object Handling**: Fixed `AttributeError` for string paths in CSV export
- **Cloudflare Detection**: Selenium bypasses now work consistently for protected sites
- **Konbriefing Classification**: Education-related incidents from curated sources no longer incorrectly skipped

#### Removed
- **Redundant Test Files**: Cleaned up duplicate test coverage files
- **Temporary Databases**: Removed `phase2_enrichments.db` and test artifacts from `data/processed/`
- **Duplicate LLM Prompts**: Removed inline prompts in `enrichment.py` that duplicated `extraction_prompt.py`

#### Documentation
- **Updated Tests README**: Comprehensive test documentation for contributors
- **Phase 2 README**: Complete pipeline documentation with data flow diagrams
- **CONTRIBUTING.md**: Updated with contributor test cases and verification steps

---

## [1.1.0] - 2025-11-24

### Phase 2 Enrichment Pipeline Improvements

#### Added
- **JSON Schema-based Extraction**: New extraction method using comprehensive JSON schema for structured LLM output
- **Comprehensive CTI Schema**: Extended extraction schema with 192+ fields covering all aspects of cyber threat intelligence
  - Education relevance and institution identification
  - Attack mechanics (vectors, chains, MITRE ATT&CK techniques)
  - Threat actor attribution and ransomware families
  - Data impact metrics (records, types, encryption, exfiltration)
  - System impact (affected systems, infrastructure context)
  - User impact (students, faculty, staff, alumni, etc.)
  - Operational impact (teaching, research, admissions, etc.)
  - Financial impact (ransom, recovery costs, insurance)
  - Regulatory impact (GDPR, HIPAA, FERPA, fines, lawsuits)
  - Recovery and remediation metrics
  - Transparency and disclosure tracking
  - Research impact assessment
- **Dual-table Storage Strategy**: Optimized database storage with both JSON and flattened tables
  - `incident_enrichments`: Full JSON storage for flexibility
  - `incident_enrichments_flat`: Flattened columns (88+ fields) for fast CSV export and analytics
- **Producer-Consumer Pattern**: Concurrent article fetching and enrichment processing
  - Articles fetched and pushed to queue immediately
  - Enrichment processes incidents as they arrive
  - Better resource utilization and faster processing
- **Article Selection Algorithm**: Multi-article handling with intelligent selection
  - Enriches all articles for each incident
  - Scores articles based on field coverage
  - Selects article with highest field coverage as primary
- **Comprehensive Coverage Test**: Test suite with prepared article covering all schema fields

#### Changed
- **Simplified Schema Structure**: Removed nested Pydantic models in favor of Dict structures for flexibility
- **Enhanced LLM Prompting**: Improved system prompts with explicit JSON output requirements and tag usage
- **Improved Error Handling**: Better fallback mechanisms and error recovery

#### Fixed
- **SQLite Thread Safety**: Added `check_same_thread=False` for multi-threaded operations
- **JSON Parsing Issues**: Fixed handling of escaped newlines and malformed JSON responses
- **CSV Export Bugs**: Fixed variable initialization and indentation errors
- **Return Value Consistency**: Fixed functions returning tuples instead of single values
- **Database Commit Issues**: Ensured proper transaction commits for data persistence

---

## [1.0.0] - 2025-01-20

### Initial Release

This is the first official release of EduThreat-CTI, a comprehensive cyber threat intelligence pipeline for the education sector.

### Added

#### Phase 1: Ingestion & Baseline
- **Multi-source data collection**: Collects cyber incidents from multiple OSINT sources
  - Curated sources (dedicated education sector sections): KonBriefing, Ransomware.live, DataBreaches.net
  - News sources (keyword-based search): The Hacker News, Krebs on Security, SecurityWeek, The Record, Dark Reading
  - RSS feeds: DataBreaches.net RSS feed
- **Unified data model**: `BaseIncident` schema for consistent incident representation across all sources
- **Database storage**: SQLite database with deduplicated incident storage
- **CSV export**: Unified base dataset export for analysis and sharing
- **Incremental processing**: Efficient re-runs that only process new incidents
- **Robust HTTP client**: Enhanced client with automatic Selenium fallback for bot-protected sites
- **Incremental saving**: Batch saving during collection prevents data loss on errors

#### Database Architecture
- **Deduplicated storage**: Database stores deduplicated incidents (cross-source deduplication at ingestion)
- **Source attribution**: `incident_sources` table tracks which sources contributed to each incident (many-to-many)
- **Per-source deduplication**: Prevents re-ingesting same incident from same source
- **Cross-source deduplication**: Automatically merges incidents with same URLs from different sources
- **Incremental updates**: Efficient re-runs that only process new incidents

#### Phase 2: LLM Enrichment
- **Article fetching**: Fetches and extracts article content from URLs
- **Education relevance checking**: LLM-based verification of education sector relevance
- **URL confidence scoring**: Hybrid scoring combining LLM assessment and metadata coverage
- **Comprehensive CTI extraction**: Timeline, MITRE ATT&CK mapping, attack dynamics
- **Extended analytics schema**: Detailed metrics for comprehensive analytics
- **Enrichment preservation**: Enrichment data preserved during Phase 1 merges

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## License

MIT License - see [LICENSE](LICENSE) file.
