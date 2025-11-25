# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2025-11-25

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
  - `incident_enrichments_flat`: Flattened columns (80+ fields) for fast CSV export and analytics
- **Producer-Consumer Pattern**: Concurrent article fetching and enrichment processing
  - Articles fetched and pushed to queue immediately
  - Enrichment processes incidents as they arrive
  - Better resource utilization and faster processing
- **Article Selection Algorithm**: Multi-article handling with intelligent selection
  - Enriches all articles for each incident
  - Scores articles based on field coverage
  - Selects article with highest field coverage as primary
- **Comprehensive Coverage Test**: Test suite with prepared article covering all schema fields
  - Located in `tests/phase2/test_enrichment_coverage.py`
  - Verifies extraction of all 192 schema fields
  - Generates coverage reports

#### Changed
- **Simplified Schema Structure**: Removed nested Pydantic models in favor of Dict structures for flexibility
- **Enhanced LLM Prompting**: Improved system prompts with explicit JSON output requirements and tag usage
- **Improved Error Handling**: Better fallback mechanisms and error recovery
  - JSON schema extraction with fallback to comprehensive method
  - Escaped newline handling in JSON parsing
  - Thread-safe database connections

#### Fixed
- **SQLite Thread Safety**: Added `check_same_thread=False` for multi-threaded operations
- **JSON Parsing Issues**: Fixed handling of escaped newlines and malformed JSON responses
- **CSV Export Bugs**: Fixed variable initialization and indentation errors
- **Return Value Consistency**: Fixed functions returning tuples instead of single values
- **Database Commit Issues**: Ensured proper transaction commits for data persistence

#### Technical Improvements
- **Code Organization**: Refactored Phase 2 directory structure
  - `extraction/`: JSON schema, prompts, and mappers
  - `storage/`: Database and article storage
  - `utils/`: Helper functions (deduplication, fetching strategy)
- **Logging Enhancements**: Added detailed logging for debugging and monitoring
- **Type Safety**: Improved type hints and error handling throughout

#### Documentation
- **Test Documentation**: Added README for Phase 2 test suite
- **Code Cleanup**: Removed temporary verification scripts and logs
- **Changelog Updates**: Comprehensive documentation of Phase 2 improvements

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

#### Deduplication System
- **URL-based matching**: Identifies duplicates by normalizing and comparing URLs
- **Smart merging**: Keeps highest confidence source, merges all URLs and metadata
- **URL normalization**: Removes trailing slashes, www. prefix, fragments for accurate matching
- **Statistics**: Provides detailed deduplication statistics

#### Source Registry
- **Centralized registry**: `core/sources.py` provides easy source management
- **Easy source addition**: New sources can be added by following established patterns
- **Source validation**: Built-in source name validation
- **Source documentation**: Comprehensive guide for adding sources

#### CLI Interface
- **Main orchestrator**: `python -m src.edu_cti.pipeline.phase1.orchestrator` - Complete Phase 1 workflow
- **Phase 1 pipeline**: `python -m src.edu_cti.pipeline.phase1` - Phase 1 ingestion
- **Phase 2 pipeline**: `python -m src.edu_cti.pipeline.phase2` - Phase 2 LLM enrichment
- **CLI commands**: `eduthreat-orchestrator`, `eduthreat-phase1`, `eduthreat-phase2`, `eduthreat-build`
- **Flexible options**: Source selection, page limits, group filtering, batch processing

#### Configuration
- **Environment variables**: Database path, log level, log file, Ollama API configurable via environment
- **Default values**: Sensible defaults in `core/config.py`
- **`.env` support**: Local development configuration via `.env` file

#### Documentation
- **Comprehensive README**: Complete project documentation with usage examples
- **Architecture docs**: Detailed architecture and design principles
- **Contributor guide**: `CONTRIBUTING.md` with contribution guidelines
- **Source addition guide**: `docs/ADDING_SOURCES.md` with step-by-step instructions
- **Database docs**: Complete database schema documentation
- **Deduplication guide**: Detailed deduplication strategy explanation
- **Source recommendations**: List of potential additional sources in `docs/SOURCES.md`

#### Development Tools
- **Package management**: `setup.py` and `pyproject.toml` for proper Python packaging
- **Testing framework**: Test structure with `pytest` support
- **Type hints**: Type annotations throughout codebase
- **Code quality**: Black formatting, flake8 linting support

### Technical Details

#### Database Schema
- `incidents`: Deduplicated incidents with enrichment fields
- `incident_sources`: Many-to-many relationship tracking source attribution
- `source_events`: Per-source deduplication tracking
- `source_state`: Source ingestion state tracking
- `incident_enrichments`: Phase 2 enrichment data storage

#### Project Structure
- **Phase-based organization**: Clear separation of Phase 1, Phase 2, Phase 3
- **Core module**: Shared functionality across all phases
- **Source modularity**: Easy-to-extend source system
- **Professional structure**: Contributor-friendly organization

### Future Enhancements

#### Phase 3 - CTI Outputs (Planned)
- Public dataset export formats
- STIX/TAXII feeds
- Dashboard & analytics
- API endpoints
- Data visualization

#### Additional Sources (Potential)
- BleepingComputer
- CISA alerts
- CERT advisories (NCSC, CERT-EU)
- University IT status pages
- Additional ransomware leak sites
- See `docs/SOURCES.md` for comprehensive list

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## License

MIT License - see [LICENSE](LICENSE) file.
