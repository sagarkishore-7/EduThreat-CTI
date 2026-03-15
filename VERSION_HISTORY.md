# Version History

Complete version history and release notes for EduThreat-CTI.

## Version 2.3.0 (2026-03-15)

**Focus**: Real-Time Intelligence Pipeline, Re-Enrichment & Pipeline Cancel Fix

### Key Features
- One-click "Start Cron Job" in admin panel — continuous RSS (1h), API (6h), daily pipeline (24h) with auto-enrichment
- Re-Enrich by Date — reset old enrichments to re-process with updated extraction schema
- Pipeline cancel now actually works for Phase 2 enrichment (threading.Event propagation)
- Live progress tracking for all pipeline phases — enrichment, ingestion, and composite phases show real-time percent, step, and detail in admin dashboard
- Dashboard stat cards now filter to correct incidents when clicked
- ENRICHMENT_WORKERS env var respected by admin panel enrichment runs

### Breaking Changes
None

### Migration Notes
- `schedule` library required (already in requirements.txt)
- New admin endpoints: `POST /admin/scheduler/start`, `POST /admin/scheduler/stop`, `GET /admin/scheduler/status`, `POST /admin/re-enrich`

---

## Version 2.2.0 (2026-03-15)

**Focus**: Dashboard Redesign, Parallel Enrichment & Stats Overhaul

### Key Features
- Parallel LLM enrichment with `--workers N` flag (up to 8 threads)
- Analyst-focused dashboard stats (education_incidents, avg_recovery_days, financial_impact, MITRE coverage)
- Fixed duplicate metrics bug (enriched_incidents === total_incidents)
- Separated total/education/enriched/unenriched incident counts
- Fixed column name mismatch (recovery_costs → recovery_costs_max)

### Breaking Changes
- `DashboardStats` model has new required fields — frontend must be updated to match

### Migration Notes
- Dashboard API now returns `education_incidents` instead of relying on `enriched_incidents`
- Use `--workers 4` for 4x faster enrichment on multi-core machines

---

## Version 2.1.0 (2026-03-15)

**Focus**: New Intelligence Sources & Performance Optimization

### Key Features
- Abuse.ch ThreatFox integration (education-relevant IOCs)
- Abuse.ch URLhaus integration (malicious .edu URLs)
- In-memory TTL cache for API endpoints
- SQLite composite indexes for faster queries
- Optimized read PRAGMAs and reduced HTTP sleep times

### Breaking Changes
None

### Migration Notes
- No API keys needed for ThreatFox or URLhaus (free public exports)
- Run `python -m src.edu_cti.pipeline.phase1 --groups api --sources threatfox urlhaus` to ingest

---

## Version 2.0.0 (2026-03-15)

**Focus**: Production Deployment & Dashboard Integration

### Key Features
- Railway + Vercel deployment
- Admin incident management (CRUD)
- Pipeline manager with SSE log streaming
- Playwright bot evasion (replaces Selenium)
- OTX AlienVault, CISA RSS, international RSS feeds

### Breaking Changes
- OTX AlienVault now requires API key

### Migration Notes
- Set `OTX_API_KEY` environment variable for OTX source

---

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
