# Version History

Complete version history and release notes for EduThreat-CTI.

## Version 1.6.0 (2026-01-08)

**Focus**: LLM Enrichment Reliability & Production Improvements

### Key Features
- Rate limit retry logic with exponential backoff
- Dynamic consumer timeout scaling with incident count
- Enhanced queue empty detection
- Country normalization system with ISO codes
- CTI report generation (Markdown format)
- Comprehensive admin panel enhancements

### Breaking Changes
None

### Migration Notes
- Country normalization: Run `/api/admin/normalize-countries` endpoint to update existing data
- Database schema: Automatic migration adds `country_code` column

---

## Version 1.5.0 (2025-11-27)

**Focus**: Enhanced Article Extraction & Cookie Consent Handling

### Key Features
- 80+ dynamic CSS selectors for global news sites
- Automatic cookie consent handling
- Progress tracking for Phase 2 pipeline
- Dynamic enum normalization

---

## Version 1.4.0 (2025-11-26)

**Focus**: Incremental Ingestion

### Key Features
- Incremental ingestion for all Phase 1 sources
- Source state tracking (`source_state` table)
- ISO 8601 date parsing support
- Brotli compression support

---

## Version 1.3.0 (2025-11-25)

**Focus**: Enhanced CTI Schema + BleepingComputer RSS

### Key Features
- BleepingComputer RSS source
- Extended education keywords (70+)
- 50+ attack categories
- 60+ attack vectors
- 35+ ransomware families
- Comprehensive threat actor classification

---

## Version 1.2.0 (2025-11-25)

**Focus**: Phase 2 Production Ready

### Key Features
- Archive.org fallback for article fetching
- Intelligent error classification
- Ad popup handler for Selenium
- Comprehensive CSV export
- Contributor test suite

---

## Version 1.1.0 (2025-11-24)

**Focus**: Phase 2 Enrichment Pipeline Improvements

### Key Features
- JSON schema-based extraction
- Comprehensive CTI schema (192+ fields)
- Dual-table storage strategy
- Producer-consumer pattern
- Article selection algorithm

---

## Version 1.0.0 (2025-01-20)

**Focus**: Initial Release

### Key Features
- Multi-source data collection
- Unified data model
- Database storage with deduplication
- CSV export
- Incremental processing
- Robust HTTP client with Selenium fallback

---

For detailed changelog, see [CHANGELOG.md](CHANGELOG.md).
