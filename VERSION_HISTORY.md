# Version History

Complete version history and release notes for EduThreat-CTI.

## Version 2.9.0 (2026-05-01)

**Focus**: Grounded Intelligence Extraction — STIX Lookup, GLiNER NER Pre-pass, IntelEX RAG

Three complementary techniques that address the same root cause: the LLM extracting fields from parametric memory rather than article evidence. Together they reduce null rates for MITRE technique metadata, institution names, and geographic fields, while improving technique_id selection accuracy.

### Key Features

- **STIX-based MITRE technique lookup** (`src/edu_cti/pipeline/phase2/extraction/mitre_stix.py`) — downloads the full MITRE ATT&CK Enterprise STIX bundle (697 active techniques) and caches it to `DATA_DIR/mitre_attack_cache.json`. Provides `get_technique_info(id)` returning canonical `{name, tactic, description}` for any technique or subtechnique, with fallback to the base technique when subtechniques are absent. Normalises internal STIX phase names (e.g. `"stealth"` → `"Defense Evasion"`) to ATT&CK display names. Cache refreshes every 30 days; degrades gracefully to the existing static lookup table if the download fails.

- **Wired into `_fill_mitre_technique_names()`** (`post_processing.py`) — STIX lookup runs first as primary source; the hand-curated static dict (`_MITRE_TECHNIQUE_INFO`, ~100 entries) remains as secondary fallback. Now also fills the `description` field (first sentence of the ATT&CK technique description) which was previously always null.

- **GLiNER zero-shot NER pre-pass** (`src/edu_cti/pipeline/phase2/extraction/ner_preprocessor.py`) — runs `urchade/gliner_small-v2.1` (~150 MB) over the first 8 000 chars of each article before the main LLM call. Detects six entity types: educational institution, city, country, US state/Canadian province, threat actor group, ransomware family. Formats extracted entities as a `=== NER PRE-EXTRACTION HINTS ===` block injected into the LLM user prompt. Model cached in `DATA_DIR/hf_cache` via `HF_HOME` override so it survives Railway container restarts. Gracefully no-ops if GLiNER is not installed or the model fails to load.

- **IntelEX-style MITRE RAG** (`src/edu_cti/pipeline/phase2/extraction/mitre_rag.py`) — embeds all 697 ATT&CK technique descriptions using `all-MiniLM-L6-v2` (~90 MB, 384-dim vectors) and caches them to `DATA_DIR/mitre_embeddings.npy` + `mitre_embeddings_index.json`. At extraction time, encodes the first 2 000 chars of each article and retrieves the top-5 most semantically similar techniques by cosine similarity (~10ms on CPU). Formats results as a `=== MITRE ATT&CK CANDIDATE TECHNIQUES ===` block injected into the LLM prompt, giving the model grounded candidates instead of relying on training memory. Inspired by IntelEX (arxiv 2406.01560).

- **Enrichment path wiring** (`enrichment.py`) — both prompts in `_enrich_article()` and `_enrich_article_split()` now append the NER hint block and RAG block. In the split path, the RAG block is injected specifically into Part 2 (the MITRE-focused call) for targeted grounding. Total extra prompt context: ~1 300 chars per article, well within the 180 K char budget.

### Expected Impact

| Field | Before | After |
|-------|--------|-------|
| `technique_name` / `tactic` | Null for ~60% of MITRE entries | Filled by STIX for all 697 active techniques |
| `description` (per technique) | Always null | Filled by STIX (first sentence) |
| `institution_name` | 15–20% headline-copying errors | GLiNER surfaces correct name as hint |
| `city` / `region` | Null for ~40% of incidents | GLiNER extracts from article text |
| `ransomware_family` | Null when mentioned indirectly | GLiNER + RAG both surface candidates |
| `technique_id` accuracy | Hallucinated from memory | RAG retrieves top-5 evidence-grounded candidates |

### Dependencies Added

```
stix2>=3.0.0
gliner>=0.2.0
sentence-transformers>=3.0.0
```

### Breaking Changes

None. All three features degrade gracefully — if any dependency is missing or a network call fails, the pipeline continues without the enhancement.

---

## Version 2.8.0 (2026-04-29)

**Focus**: Instructor-Based LLM Self-Correction Layer

### Key Features

- **Instructor correction pass** (`src/edu_cti/pipeline/phase2/extraction/instructor_corrector.py`) — after the main GBNF-constrained extraction, a targeted second LLM call fires when ≥2 critical fields (attack_category, institution_type, attack_vector, country) are null/unknown. Uses the `instructor` library's Pydantic retry loop: if the LLM returns an invalid enum value, Pydantic raises a `ValueError` with the full list of valid options; Instructor sends that error back as a follow-up message and retries automatically (up to 3 times).

- **Pydantic validators on `CriticalFieldsCorrection`** — `@field_validator` on `attack_category`, `institution_type`, and `attack_vector` normalize the LLM response (lowercase, strip, replace spaces with underscores) and validate against frozenset enum tables. Error messages enumerate valid choices so the LLM self-corrects in the retry.

- **Cost model** — fires for ~20% of incidents; each correction call is ~3s on DeepSeek V3.1; net overhead ~0.6s per incident on average.

- **Graceful degradation on Python 3.9** — `instructor>=1.0.0` uses Python 3.10+ union syntax (`str | Path`) that raises `TypeError` during module evaluation on 3.9. The module catches both `ImportError` and `TypeError`, sets `INSTRUCTOR_AVAILABLE = False`, and makes the correction pass a no-op. Production (Railway, Python 3.11+) runs the full path.

- **`instructor_correction_applied_total` metric** — counter incremented in `enrichment.py` when corrections are applied; exposed in `metrics.research_summary()`.

- **Wired into both enrichment paths** — correction is called in `_enrich_article()` (single-chunk) and `_enrich_article_split()` (multi-chunk, after merge), before `json_to_cti_enrichment()`.

### Tests

- `tests/phase2/test_instructor_corrector.py` — 38 unit tests covering validators, null-field detection, trigger threshold, and the full `apply_instructor_corrections()` function.
- `tests/test_instructor_e2e.py` — 24 E2E tests covering: no-correction path, correction filling null fields, Pydantic validation retry flow, mapper/DB integration after correction, validator error messages, realistic incident payloads, and metric tracking.
- All 800 tests pass (18 skipped) on Python 3.9.

### Dependencies

```
instructor>=1.0.0; python_version >= "3.10"
```

### Breaking Changes

None.

---

## Version 2.7.1 (2026-04-23)

**Focus**: Exception Handling Hardening

### Key Fixes

- **6 bare `except:` clauses eliminated** across `api/admin.py` and `api/reports.py` — bare `except:` blocks were catching `KeyboardInterrupt` and `SystemExit`, preventing graceful shutdown and masking real failures.
  - `admin.py` PRAGMA check: `except:` → `except sqlite3.Error:`
  - `admin.py` connection close (×2): `except:` → `except Exception:`
  - `reports.py` JSON parsing (×3): `except:` → `except (json.JSONDecodeError, ValueError, TypeError):`

### Breaking Changes

None.

---

## Version 2.6.0 (2026-03-19)

**Focus**: Interactive Nivo Visualization Endpoints — Sankey, Sunburst, Network, Chord

### Key Features
- 5 new endpoints for advanced interactive visualizations (Sankey flows, sunburst drill-down, force-directed network, chord diagrams)
- Attack Flow Sankey: traces how attack vectors flow through categories to impact outcomes
- MITRE Sunburst: hierarchical tactic → technique tree built from enrichment JSON
- Actor Network: force-directed graph of actors connected by shared ransomware families
- Ransom Flow: institution → family → payment outcome with toggle between count and dollar amount
- Country-Attack Chord: geographic attack specialization patterns

---

## Version 2.5.0 (2026-03-17)

**Focus**: Cross-Dimensional Intelligence Analytics — 10 New Endpoints

### Key Features
- 10 new analytics endpoints that cross-reference rich dimensions (attack type × institution type, actor × MITRE tactic, ransomware family trends, breach severity over time, disclosure patterns)
- Institution Risk Matrix: shows which institution types face which threats
- Recovery by Attack Type: avg recovery/downtime days per attack category
- Attack Vector by Institution: are K-12 more vulnerable to phishing while universities face ransomware?
- Breach Severity Timeline: are breaches getting larger over time?
- Ransom Payment by Year: are institutions paying less ransom over time?
- Ransomware Family Trend: which families are rising/declining?
- Actor Institution Targeting: do specific actors specialize in targeting certain institution types?
- Actor TTP Profile: what MITRE tactics does each actor use?
- Disclosure Timeline: is disclosure getting faster? Which countries are slowest?
- Data Breach by Institution Type: which institution types lose the most data?

### Technical Details
- 10 new SQL query functions with cross-dimensional GROUP BY analytics
- 10 new Pydantic response models
- All endpoints cached with 300s TTL

### Breaking Changes
None

---

## Version 2.4.2 (2026-03-17)

**Focus**: MITRE Heatmap Fix & Admin Raw Data Viewer

### Key Fixes
- MITRE ATT&CK heatmap was showing all zeros because `tactic` field is null in stored technique JSON — added technique-ID-to-tactic lookup map (140+ techniques) to resolve tactics from IDs like T1566 → Initial Access
- Collapsed "unknown" and "other" initial access method categories into "Unknown / Other" to eliminate ambiguity

### Key Features
- Raw Data Viewer in admin panel for inspecting DB columns and enrichment JSON (filters: incident ID, attack category, country, has MITRE, has enrichment)
- New `/api/admin/raw-incidents` endpoint with filter params and pagination

### Breaking Changes
None

---

## Version 2.4.1 (2026-03-17)

**Focus**: Enrichment Pipeline Reliability Fix

### Key Fixes
- Added 180s HTTP timeout (30s connect) to Ollama LLM client — prevents worker threads from hanging indefinitely on stalled requests
- Fixed premature consumer worker exit with multiple workers — uses exponential backoff and `queue.unfinished_tasks` checks instead of exiting after 36s
- LLM timeouts and connection errors now re-queue incidents for retry instead of permanent failure

### Breaking Changes
None

### Migration Notes
- No config changes needed — timeout is automatic
- Existing `ENRICHMENT_WORKERS` setting continues to work, now more reliably with 2+ workers

---

## Version 2.4.0 (2026-03-15)

**Focus**: Advanced Analytics API — 20+ New Endpoints for CTI Dashboard

### Key Features
- 20 new SQL query functions in `database.py` powering deep CTI analytics
- 15 new Pydantic response models for structured analytics data
- 20 new cached FastAPI endpoints under `/api/analytics/`
- Attack intelligence: trends over time, vector distribution, MITRE ATT&CK tactic heatmap, initial access methods, system impact
- Ransomware intelligence: family activity timeline, detailed family stats with exfiltration rates, ransom economics (demand/payment/rates), recovery comparison (ransomware vs other), geographic targeting per family
- Threat actor intelligence: category/motivation distribution (parsed from enrichment JSON), monthly activity timeline, actor-ransomware matrix cross-tabulation, geographic targeting per actor
- Impact analytics: institution type distribution, operational impact radar (7 disruption metrics), financial impact by year (stacked costs), data breach metrics, regulatory compliance stats (GDPR/HIPAA/FERPA), recovery effectiveness, transparency/disclosure metrics, user impact totals

### Breaking Changes
None

### Migration Notes
- No database schema changes required — all queries use existing `incident_enrichments_flat` table
- New endpoints all follow existing cache pattern (300s TTL)

---

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
