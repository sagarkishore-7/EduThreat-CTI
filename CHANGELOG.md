# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.10.0] - 2026-06-06

### Dashboard-supporting v2 analytics endpoints

Two fast, public, cached read endpoints added to power the dashboard's
"Operations Room" redesign (EduThreat-CTI-Dashboard v3.0.0). Both are derived
live from the canonical/source tables — no new storage.

#### Added
- **`GET /api/v2/analytics/kpi-trends`** — per-KPI monthly sparkline series with a
  period-over-period delta for the four dashboard KPI tiles (`incidents`,
  `ransomware`, `breaches`, `actors`). The delta compares the recent half of the
  window against the prior half, excluding the partial current month so the
  trend direction isn't skewed.
  - `CanonicalIncidentRepository.get_kpi_trend()` + `_kpi_metric_predicate()` —
    metric-scoped monthly `date_trunc` aggregates over `canonical_incidents`.
  - `V2CanonicalReadService.get_kpi_trends()` — assembles series, totals, and deltas.
- **`GET /api/v2/analytics/feeds`** — per-source ingestion health for the Intel
  Feeds page: lifetime + trailing-30d event volume, last-collected / last-published
  timestamps, and a freshness status (`healthy` / `stale` / `offline`) per source,
  plus a by-source-group rollup.
  - `V2CanonicalReadService.get_feed_health()` — aggregates the raw
    `source_incidents` collection layer grouped by `source_name` / `source_group`.

Both endpoints use the standard 30-second public read cache.

## [2.9.0] - 2026-05-01

### Grounded Intelligence Extraction — STIX Lookup, GLiNER NER, IntelEX RAG

#### Added

- **`src/edu_cti/pipeline/phase2/extraction/mitre_stix.py`** — MITRE ATT&CK STIX bundle lookup. Downloads and caches all 697 active Enterprise techniques (`DATA_DIR/mitre_attack_cache.json`, refreshed every 30 days). `get_technique_info(id)` returns `{name, tactic, description}` with subtechnique fallback. Phase-name normalisation map converts STIX internal names (e.g. `stealth`) to ATT&CK display names (`Defense Evasion`). Falls back to static dict on download failure.

- **`src/edu_cti/pipeline/phase2/extraction/ner_preprocessor.py`** — GLiNER zero-shot NER pre-pass. Loads `urchade/gliner_small-v2.1` once per process; detects educational institution, city, country, state/province, threat actor, ransomware family from the first 8 000 chars of each article. Formats results as a structured hint block injected into the LLM prompt. Model cached in `DATA_DIR/hf_cache`.

- **`src/edu_cti/pipeline/phase2/extraction/mitre_rag.py`** — IntelEX-style semantic MITRE retrieval. Encodes all 697 technique descriptions with `all-MiniLM-L6-v2` (384-dim, cached to `DATA_DIR/mitre_embeddings.npy`). `retrieve_similar_techniques(text, top_k=5)` returns semantically closest techniques via normalised dot-product cosine similarity in ~10ms. `build_mitre_rag_block()` formats a prompt context block with technique ID, name, tactic, and description snippet.

#### Changed

- **`post_processing.py` `_fill_mitre_technique_names()`** — STIX lookup now runs as primary source before the static dict fallback; also fills the `description` field (previously always null).

- **`enrichment.py` `_enrich_article()`** — appends NER hint block and MITRE RAG block to user prompt before the LLM call (~1 300 extra chars of grounding context).

- **`enrichment.py` `_enrich_article_split()`** — NER block appended to Part 1 prompt; RAG block appended to Part 2 prompt (MITRE-focused call) for targeted technique grounding.

- **`requirements.txt`** — added `stix2>=3.0.0`, `gliner>=0.2.0`, `sentence-transformers>=3.0.0`.

---

## [2.7.1] - 2026-04-23

### Exception Handling Hardening

#### Fixed

- **Bare `except:` clause in PRAGMA schema check** (`api/admin.py:292`) — replaced with `except sqlite3.Error:` so `KeyboardInterrupt` and `SystemExit` are no longer swallowed during the `PRAGMA table_info(incidents)` column-existence check.

- **Bare `except:` clauses in connection close** (`api/admin.py:437`, `api/admin.py:503`) — replaced with `except Exception:` in the `finally` blocks of `export_full_csv` and `export_enriched_csv`. System-level signals now propagate correctly on graceful shutdown.

- **Bare `except:` clauses in JSON parsing** (`api/reports.py:154`, `api/reports.py:203`, `api/reports.py:262`) — replaced with `except (json.JSONDecodeError, ValueError, TypeError):` for parsing `mitre_attack_techniques`, `systems_affected`, and `timeline` fields. Other exceptions (including `KeyboardInterrupt`) now propagate instead of being silently swallowed.

#### Changed

- Codebase-wide bare `except:` count reduced from 6 to 0 across API modules.

---

## [2.7.0] - 2026-04-17

### Phase 2 Extraction Pipeline — Critical Bug Fixes

Eight classes of silent data-loss bugs in the LLM extraction pipeline were diagnosed and fixed. These bugs caused MITRE ATT&CK data, breach records, regulatory fields, and discovery dates to never appear in enriched incidents, even when the LLM generated correct JSON.

#### Fixed

- **MITRE ATT&CK never extracted** — `technique_id` carried a regex `pattern` constraint (`^T\d{4}(\.\d{3})?$`) that Ollama's JSON-schema-to-GBNF compiler does not support. Ollama silently output an empty array for any field with a regex pattern. Removed the pattern; replaced with a plain description hint. MITRE techniques now populate correctly.

- **MITRE mapper discarded all fields except technique_id** — `json_to_schema_mapper.py` hardcoded `tactic`, `description`, and `sub_techniques` as `None` regardless of LLM output. Rewrote the mapper to read all five MITRE fields (`technique_id`, `technique_name`, `tactic`, `description`, `sub_techniques`) and handle both string and dict representations.

- **Twelve regex pattern constraints blocked GBNF generation** — The extraction schema applied `pattern` constraints to all date fields (`incident_date`, `discovery_date`, `publication_date`, `outage_start_date`, `outage_end_date`, `notification_sent_date`, `recovery_started_date`, `recovery_completed_date`, `public_disclosure_date`, `timeline[].date`), CVE IDs (`cve_id`), and country codes (`country_code`). All twelve patterns removed and replaced with description-level hints.

- **`enriched_summary` never generated** — Field was last in the ~1,000-line schema; `num_predict=16384` was exhausted before the LLM reached it. Increased `num_predict` to 24,576. Added `_build_summary()` fallback that constructs a factual sentence from structured intelligence fields when the LLM returns an empty summary (e.g. for headline-only articles).

- **`data_categories` vs `data_types` key mismatch** — Extraction schema emitted `data_categories` but the mapper read `data_types`. Data breach records were silently dropped for all incidents. Mapper updated to check both keys with fallback.

- **`ferpa_breach` stored under wrong key** — Regulatory impact dict used key `ferc_breach` (energy regulator) instead of `ferpa_breach`. Fixed key name in mapper.

- **`discovery_date` never persisted** — LLM correctly extracted `discovery_date` but `storage/db.py` never read or stored it. Added extraction and conditional `UPDATE` so the field now reaches the database and the dashboard timeline.

- **Five `event_type` enum values caused validation failures** — Timeline entries using `exploitation`, `impact`, `operational_impact`, `response_action`, or `security_improvement` were rejected by Pydantic because those values were missing from the extraction schema enum. All five values added.

#### Added

- **`_build_summary()` function** (`json_to_schema_mapper.py`) — generates a minimal factual summary from structured fields when `enriched_summary` is absent, ensuring every enriched incident has readable summary text.

- **Comprehensive test suite** (`tests/phase2/test_extraction_pipeline_fixes.py`) — 8 test classes covering all fixed bug classes:
  - `TestExtractionSchemaPatterns` — asserts zero `pattern` constraints in the schema
  - `TestTimelineEventTypeEnum` — verifies all required `event_type` values are present
  - `TestMITREMapper` — 9 tests for MITRE field extraction (dict, string, mixed, empty)
  - `TestDataCategoriesMapping` — 4 tests for `data_categories`/`data_types` fallback
  - `TestFerpaBreach` — 3 tests verifying correct `ferpa_breach` key
  - `TestBuildSummary` — 8 tests for fallback summary with and without LLM output
  - `TestDiscoveryDatePersistence` — 3 DB integration tests with isolated SQLite
  - `TestEndToEndPhishingIncident` — full round-trip test: phishing JSON → mapper → DB → verify

#### Changed

- **`num_predict` increased** from 16,384 to 24,576 tokens (`llm_client.py`) — schema JSON output reaches 8–12K tokens; 24K leaves room for the summary and trailing fields.

---

## [2.6.0] - 2026-03-19

### Interactive Nivo Visualization Endpoints

#### Added
- **5 new interactive analytics endpoints** for advanced Nivo visualizations:
  - `GET /api/analytics/attack-flow` — 3-column Sankey data: attack vector → category → impact outcome
  - `GET /api/analytics/mitre-sunburst` — hierarchical MITRE ATT&CK tree: tactic → technique for sunburst drill-down
  - `GET /api/analytics/actor-network` — force-directed network graph: actors linked by shared ransomware families
  - `GET /api/analytics/ransom-flow` — Sankey flow: institution type → ransomware family → payment outcome (count + amount)
  - `GET /api/analytics/country-attack-matrix` — chord diagram data: top countries × top attack categories
- **5 new Pydantic response models** (AttackFlowResponse, MitreSunburstResponse, ActorNetworkResponse, RansomFlowResponse, CountryAttackMatrixResponse)
- **5 new SQL query functions** for relationship-based and hierarchical data

---

## [2.5.0] - 2026-03-17

### Cross-Dimensional Intelligence Analytics

#### Added
- **10 new analytics endpoints** for cross-dimensional threat intelligence:
  - `GET /api/analytics/institution-risk-matrix` — institution type × attack category bubble matrix
  - `GET /api/analytics/recovery-by-attack-type` — avg recovery/downtime days per attack category
  - `GET /api/analytics/attack-vector-by-institution` — attack vector distribution per institution type
  - `GET /api/analytics/breach-severity-timeline` — monthly incident count + avg records breached over time
  - `GET /api/analytics/ransom-payment-by-year` — yearly demanded vs paid amounts with payment rate
  - `GET /api/analytics/ransomware-family-trend` — top ransomware families by month (stacked)
  - `GET /api/analytics/actor-institution-targeting` — threat actors × institution types heatmap
  - `GET /api/analytics/actor-ttp-profile` — threat actors × MITRE tactics (parsed from JSON)
  - `GET /api/analytics/disclosure-timeline` — disclosure delay scatter over time by country
  - `GET /api/analytics/breach-by-institution-type` — breach rate and avg records per institution type
- **10 new Pydantic response models** for structured API responses
- **10 new SQL query functions** with cross-dimensional GROUP BY analytics leveraging 90+ enriched columns
- All endpoints cached with 300s TTL for production performance

---

## [2.4.2] - 2026-03-17

### MITRE Heatmap Fix & Admin Raw Data Viewer

#### Fixed
- **MITRE ATT&CK heatmap showing all zeros**: Root cause was `tactic` field stored as `null` in technique JSON. Added a 140+ technique-ID-to-tactic lookup map (`_TECHNIQUE_TO_TACTIC`) that resolves tactics from technique IDs (e.g., T1566 → Initial Access, T1486 → Impact) when the tactic field is null
- **Initial access "unknown" vs "other" ambiguity**: Collapsed both values into "Unknown / Other" since the LLM cannot reliably distinguish between unreported and uncategorized access methods

#### Added
- **Raw Data Viewer** in admin panel: collapsible section with filters (incident ID, attack category, country, has MITRE, has enrichment JSON) and expandable rows showing all flat columns, MITRE JSON, and full enrichment JSON with copy buttons
- **`GET /api/admin/raw-incidents`** endpoint with filter params and pagination for raw DB inspection

---

## [2.4.1] - 2026-03-17

### Enrichment Pipeline Reliability Fix

#### Fixed
- **LLM request timeout**: Added 180s HTTP timeout (30s connect) to Ollama client — previously requests could hang indefinitely, blocking worker threads forever and causing enrichment to appear "stopped"
- **Premature consumer worker exit**: With multiple workers (e.g. 6), queue could temporarily empty between pushes, causing workers to exit after only 36s of waiting. Now uses exponential backoff (up to ~95s) and checks `queue.unfinished_tasks` to keep workers alive while other workers are still processing
- **Timeout/connection error recovery**: LLM timeouts and connection errors now re-queue the incident for retry instead of counting as permanent failures

---

## [2.4.0] - 2026-03-15

### Advanced Analytics API — 20+ New Endpoints

#### Added
- **20 new analytics SQL query functions** in `database.py` for deep CTI analytics across attack vectors, MITRE ATT&CK, ransomware economics, threat actor profiling, operational/financial/regulatory impact, recovery metrics, and transparency metrics
- **15 new Pydantic response models** in `models.py` for structured analytics responses
- **20 new cached FastAPI endpoints** in `main.py` under `/api/analytics/` — all with 300s cache TTL
- New endpoints include: `attack-trends`, `attack-vectors`, `mitre-tactics`, `initial-access`, `system-impact`, `ransomware-timeline`, `ransomware-families-detail`, `ransom-economics`, `ransomware-recovery`, `ransomware-geo`, `threat-actor-categories`, `threat-actor-motivations`, `threat-actor-timeline`, `actor-ransomware-matrix`, `actor-targeting`, `institution-types`, `operational-impact`, `financial-impact`, `data-impact`, `regulatory-impact`, `recovery-metrics`, `transparency-metrics`, `user-impact`

#### Notes
- All analytics queries use `incident_enrichments_flat WHERE is_education_related = 1` — only LLM-verified education incidents
- MITRE tactics parsed from `mitre_techniques_json` column; threat actor categories extracted from enrichment JSON
- Sparse data handled gracefully with empty arrays and zero defaults

---

## [2.3.0] - 2026-03-15

### Real-Time Intelligence Pipeline, Re-Enrichment & Pipeline Cancel Fix

#### Added
- **Real-Time Intelligence Pipeline (Cron Scheduler)**: One-click "Start Cron Job" button in admin panel activates continuous automated collection — RSS every 1h, API sources every 6h, full daily pipeline every 24h with auto-enrichment. Includes catch-up cycle on first start.
- **Re-Enrich by Date**: Admin panel section to reset enrichment for incidents processed before a specific date, allowing re-enrichment with updated extraction schemas. Uses `POST /admin/re-enrich` endpoint.
- **Scheduler API Endpoints**: `POST /admin/scheduler/start`, `POST /admin/scheduler/stop`, `GET /admin/scheduler/status` — real-time scheduler control with status reporting (jobs, next run times, last runs, total new incidents).
- **`revert_enrichment_before_date()`**: New DB function to bulk-reset enrichment for incidents enriched before a given date.

#### Fixed
- **Live Progress Tracking (All Phases)**: All pipeline phases now report live progress to the admin dashboard. Enrichment runs in a sub-thread with 2s polling of module-level `_progress` dict. Ingest updates after each source group completes. Composite phases (historical, daily) scale progress correctly — ingest 0-50%, enrichment 50-100% — instead of both overwriting 0-100%.
- **Pipeline Cancel (Phase 2)**: Stop button now actually stops enrichment. Previously `_cancel_requested` was set but Phase 2 had zero cancel checkpoints. Added `threading.Event` propagation from pipeline manager to phase2 fetch and enrich loops.
- **Dashboard Stat Filtering**: Clicking stat cards now correctly filters incidents (attack_category uses LIKE matching, data_breached uses dedicated boolean filter, URL params initialize filter state).

#### Changed
- **Pipeline Manager**: Integrated scheduler directly into `PipelineManager` singleton — shares execution engine, log capture, and cancel support with manual pipeline runs.
- **`ENRICHMENT_WORKERS` env var**: Admin panel enrichment now reads worker count from environment variable instead of hardcoded default of 1.

---

## [2.2.0] - 2026-03-15

### Dashboard Redesign, Parallel Enrichment & Stats Overhaul

#### Added
- **Parallel LLM Enrichment**: `--workers N` flag (max 8) for multi-threaded enrichment processing. Each worker gets its own LLM client and DB connection for thread safety.
- **Analyst-Focused Dashboard Stats**: New API metrics — `education_incidents`, `data_sources`, `avg_recovery_days`, `total_financial_impact`, `incidents_with_mitre` for research-oriented dashboard
- **DashboardStats Model Expansion**: Added `education_incidents`, `unenriched_incidents`, `data_sources`, `avg_recovery_days`, `total_financial_impact`, `incidents_with_mitre` fields

#### Changed
- **Dashboard Stats Fix**: `enriched_incidents` no longer hardcoded equal to `total_incidents` — now properly separated into `total_incidents` (all ingested), `education_incidents` (LLM-confirmed education), `enriched_incidents` (processed by LLM)
- **Pipeline Manager**: Accepts `workers` parameter and passes `--workers N` to phase2 enrichment

#### Fixed
- **Duplicate Dashboard Metrics**: Fixed `enriched_incidents` showing same value as `total_incidents` (was hardcoded on line 301 of database.py)
- **Column Name Mismatch**: Fixed `recovery_costs` → `recovery_costs_max` in stats queries (actual column name in DB)

---

## [2.1.0] - 2026-03-15

### New Intelligence Sources & Performance Optimization

#### Added
- **Abuse.ch ThreatFox**: IOC sharing platform — fetches education-relevant indicators of compromise (domains, IPs, URLs, hashes) from malware families targeting education sector. Supports both recent and full historical export (ZIP).
- **Abuse.ch URLhaus**: Malicious URL tracker — filters for URLs targeting `.edu` domains and education institutions, or using malware families known to target education.
- **In-Memory TTL Cache**: Dashboard and analytics endpoints cached for 5 minutes, reducing database load on repeated queries. Auto-invalidated on pipeline runs and admin deletions.
- **SQLite Composite Indexes**: Optimized JOIN + filter patterns for enrichment queries (`idx_enrichments_edu_incident`, `idx_enrichments_edu_country`, `idx_enrichments_edu_attack`)

#### Changed
- **SQLite Read PRAGMAs**: `cache_size=-8000` (8MB), `mmap_size=268435456` (256MB), `query_only=ON` for API read connections
- **HTTP Client**: Pre-request sleep reduced from (0.3-1.0s) to (0.1-0.5s) for faster scraping
- **Pipeline Manager**: Playwright browser cleanup in `_execute_run()` finally block to prevent resource leaks
- **API Source Registry**: Now includes 5 API sources (ransomlook, cisa_kev, otx_alienvault, threatfox, urlhaus)

---

## [2.0.0] - 2026-03-15

### Production Deployment & Dashboard Integration

Major release bringing production-ready deployment on Railway + Vercel, admin dashboard with full pipeline control, and comprehensive bug fixes for reliable operation.

#### Added
- **Railway Deployment**: Dockerfile with Python 3.12, Playwright browsers, persistent volume support, health checks
  - `railway.json` with Dockerfile builder, restart policy, health check at `/api/health`
  - `.railwayignore` to exclude data, logs, tests, docs from builds
  - Environment-based configuration (`EDU_CTI_DATA_DIR`, `EDU_CTI_DB_PATH`)
- **Vercel Deployment**: Next.js frontend auto-deploy with `vercel.json`, security headers, `.vercelignore`
  - Conditional standalone output (`DOCKER_BUILD=1` only)
  - Build-time `NEXT_PUBLIC_API_URL` for API connectivity
- **Admin Incident Management**: Full CRUD for incidents via dashboard
  - `GET /admin/incidents/unenriched` — paginated, searchable unenriched incidents
  - `GET /admin/incidents/enriched` — paginated with enrichment columns (attack_category, ransomware_family, etc.)
  - `POST /admin/incidents/delete` — delete by IDs with cascade to all related tables
  - `POST /admin/incidents/clear-all` — wipe entire DB with VACUUM for fresh start
- **Pipeline Manager**: Background execution engine with real-time log streaming
  - Phases: ingest, enrich, historical, daily, rss, weekly, ingest_source
  - SSE log streaming at `/admin/pipeline/logs/stream`
  - Run history, cancel support, progress tracking
- **Database Migration**: Auto-migration endpoint for Railway volume setup
- **Playwright Bot Evasion**: Replaced Selenium/Chrome with Playwright + stealth patches
  - curl_cffi TLS fingerprint impersonation (Tier 1)
  - Playwright headless with stealth (Tier 2)
  - Plain requests with retry (Tier 3)
- **International RSS Feeds**: heise_security (DE), cert_fr (FR), the_hindu_tech (IN), cert_br (BR), ncsc_uk (UK)
- **CISA RSS Feed**: US cybersecurity advisories filtered for education relevance
- **OTX AlienVault**: Threat intelligence pulse search (API key required)

#### Changed
- **HTTP Client Architecture**: Multi-tier fallback chain (curl_cffi → Playwright → requests) replaces Selenium
- **Playwright Thread Isolation**: All Playwright operations run in dedicated `ThreadPoolExecutor` thread to avoid asyncio event loop conflicts with FastAPI
- **OTX Source**: Now requires API key (was optional); skips entirely when key not set
- **International RSS**: Removed discontinued feeds (JPCERT, KrCERT, AusCERT — all return 404)
- **Timeouts**: OTX API timeout increased from 30s to 60s; international RSS from 30s to 45s
- **Package Config**: `pyproject.toml` readme inlined (no file reference) for Docker compatibility

#### Fixed
- **Playwright asyncio Conflict**: "Sync API inside asyncio loop" error when pipeline runs under FastAPI — solved via ThreadPoolExecutor isolation
- **CISA RSS TypeError**: `parse_rss_date()` returns datetime object, not string — fixed date comparison to use datetime directly instead of calling `fromisoformat()`
- **Docker Build Failures**:
  - Removed shell redirect syntax from Dockerfile COPY (`2>/dev/null || true`)
  - Fixed `setup.py` crash on missing README.md (excluded by `.railwayignore`)
  - Fixed editable install (`pip install -e .` → `pip install .`)
  - Fixed `setup.py`/`pyproject.toml` package discovery conflict (`src/src` path)
- **International RSS 404s**: Removed dead feeds, added explicit 404 handling with descriptive warnings

---

## [1.6.0] - 2026-01-08

### LLM Enrichment Reliability & Production Improvements

This release focuses on preventing premature stopping of LLM enrichment and improving production reliability for processing large datasets (4k+ incidents).

#### Added
- **Rate Limit Retry Logic**: Rate limit errors now retry with exponential backoff instead of stopping all enrichment
  - 60-second wait before retrying failed incidents
  - Automatic re-queuing of failed incidents
  - Prevents complete pipeline shutdown on temporary rate limits
- **Dynamic Consumer Timeout**: Consumer thread timeout now scales with incident count
  - Minimum 5 minutes, or 2 seconds per incident
  - Supports processing 4k+ incidents without premature timeout
- **Enhanced Queue Empty Detection**: Improved logic for detecting when queue is truly empty
  - Extended wait time from 10s to 30s
  - Multiple check attempts (3 attempts with 2s timeout each)
  - Prevents race conditions where items are still being added
- **Fallback Incident Selection**: Automatic retry with less strict filtering when domain filtering reduces selection
  - Ensures all available incidents are considered
  - Handles cases where many domains are blocked/rate-limited
- **Country Normalization System**: Comprehensive country code to name mapping
  - Full country names instead of codes in database
  - ISO 3166-1 alpha-2 code storage for CTI reports
  - Flag emoji generation for visualization
  - Automatic normalization of existing data
- **CTI Report Generation**: Comprehensive Markdown reports for each incident
  - Executive summary, incident overview, MITRE ATT&CK mapping
  - Threat actor analysis, impact assessment, timeline
  - IOCs, recovery & response, regulatory & compliance
  - Downloadable reports for researchers and analysts
- **Admin Panel Enhancements**: New admin endpoints for database management
  - Normalize countries endpoint
  - CSV export endpoints (enriched and full)
  - Improved error handling and logging

#### Changed
- **Fetching Strategy Buffer**: Increased from 3x to 5x for better domain diversity
- **Logging Optimization**: Removed emojis, truncated long messages, reduced verbosity
  - Optimized for Railway's 500 logs/sec limit
  - Compact console format, full file format
  - Progress logs every 10th item instead of every item
- **Country Data Storage**: Both `country` (full name) and `country_code` (ISO code) stored
  - Enables CTI-level standard reports
  - Supports both human-readable and machine-readable formats

#### Fixed
- **Premature Enrichment Stopping**: Fixed multiple issues causing enrichment to stop early
  - Rate limit errors no longer stop entire pipeline
  - Consumer timeout now scales with workload
  - Queue empty detection more robust
  - Incident selection ensures all incidents are processed
- **Database Concurrency**: Improved WAL mode and immediate commits prevent dashboard blocking
- **Incident Date Accuracy**: LLM-extracted timeline dates now correctly override source published dates
- **Broken URL Handling**: Improved tracking and updating of broken URLs
- **Article Fetching**: Enhanced handling for databreaches.net and wavy.com
  - Lower content threshold for databreaches.net
  - PerimeterX bot detection handler for wavy.com
- **Selenium on Railway**: Non-headless mode support using Xvfb virtual display
  - Enables bypassing advanced bot detection
  - Works with sites requiring visible browser

#### Technical Improvements
- **VPN Integration**: NordVPN support for IP rotation and bot detection evasion
- **Logging System**: Custom truncating formatter for optimal log visibility
- **Error Handling**: Comprehensive exception handling with detailed logging
- **Database Migrations**: Automatic migration for new columns (country_code, broken_urls)

---

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
- **Pydantic Validation Errors**: Dynamic enum normalization prevents validation failures from unexpected LLM outputs

#### Technical Improvements
- **Dynamic Enum Normalization**: Automatic mapping of LLM output variations to valid schema values:
  - `attack_vector`: Maps "email" → "phishing_email", "rdp" → "exposed_rdp", etc.
  - `attack_chain`: Maps "recon" → "reconnaissance", "c2" → "command_and_control", etc.
  - `event_type`: Maps "access" → "initial_access", "encrypt" → "encryption_started", etc.
  - `operational_impact`: Maps "classes_canceled" → "classes_cancelled", etc.
  - `business_impact`: Maps "high" → "severe", "low" → "limited", etc.
  - `encryption_impact`: Maps "complete" → "full", "some" → "partial", etc.
- **LLM Temperature Reduced**: Changed from 0.3 to 0.1 for more deterministic structured outputs
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
