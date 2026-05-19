# Future CTI Work

## Purpose

This document captures the next-stage CTI product and research work that is intentionally **not** shipped in the current production dashboard redesign. The guiding rule is simple:

- ship analytics only when the underlying data is already retained and reliable
- document richer CTI directions when they need schema, pipeline, or new endpoint work first

## What Is Already Backed In Production

The current `v2` platform already supports a meaningful education-sector CTI operating picture from retained canonical data:

- canonical incident register and detail views
- intelligence summary
- attack category breakdowns
- ransomware family analytics
- threat actor analytics
- country and geographic distribution
- trend / timeline analytics
- MITRE ATT&CK aggregate analytics
- Diamond-model aggregate analytics
- source disclosure provenance
- supporting source URLs per canonical
- canonical field provenance / source contribution lineage
- pipeline research metrics

These are safe to expose because the data is already stored in `v2` read models and can be tested end-to-end through the current API surface.

## Near-Term Backend Extensions

### 1. IOC persistence and aggregate IOC analytics

#### Why it is future work now

IOC-like structures exist in the extraction layer, but they are not yet retained in the `v2` canonical read model in a reliable, queryable form.

#### What to add

- canonical projection retention for:
  - IPs
  - domains
  - URLs
  - email indicators
  - hashes
  - file names / malware artifacts
- normalized IOC tables or materialized IOC blobs
- `GET /api/v2/analytics/iocs`
- incident detail IOC panel only after retention is live

#### Research value

- lets us quantify indicator disclosure in education-sector reporting
- supports comparative analysis between public narrative reporting and CTI-grade reporting
- enables IOC reuse / recurrence analysis across incidents

### 2. MITRE optimization and materialization

#### Current state

MITRE analytics are aggregated from JSON stored in canonical projections.

#### Future work

If the canonical corpus grows substantially, move from pure JSONB aggregation to one of:

- materialized MITRE summary tables
- derived per-incident MITRE relation rows
- scheduled analytics snapshot refreshes optimized for ATT&CK dashboards

#### Research value

- faster ATT&CK-heavy analytics
- easier trend analysis by tactic/technique over long time windows
- cleaner interoperability with future graph / STIX exports

### 3. Feed and ingestion observability analytics

#### What to add

- per-source collection yield
- per-source fetch success rate
- per-source enrichment yield
- source freshness / staleness
- source conflict / source disparity metrics
- feed reliability and article-selection success analytics

#### Potential endpoints

- `GET /api/v2/analytics/source-provenance`
- `GET /api/v2/analytics/ingestion-observability`

#### Research value

- demonstrates which public sources contribute the most usable education CTI
- supports methodology sections about source bias, quality, and coverage

### 4. Related-incident scoring and investigations workspace

#### What to add

- related incident suggestions by:
  - actor overlap
  - ransomware family overlap
  - MITRE overlap
  - vendor overlap
  - geography overlap
  - time-window proximity
- analyst-facing investigations page / workspace

#### Potential endpoint

- `GET /api/v2/incidents/{id}/related`

#### Research value

- supports campaign-level clustering
- enables longitudinal study of repeated targeting in education

## Pipeline / Schema Work Needed For Richer CTI Views

### IOC retention in `v2`

#### Pipeline

- preserve extracted IOCs during enrichment and canonicalization
- merge and deduplicate supporting-source IOCs at canonicalization time
- track source attribution per IOC

#### Schema

Options:

1. retain in canonical projection with strong normalization rules
2. add a dedicated canonical IOC table with:
   - `canonical_incident_id`
   - `indicator_type`
   - `indicator_value`
   - `normalized_value`
   - `confidence`
   - `source_enrichment_ids`

### Graph model

To support a true CTI graph surface, add explicit relationships between:

- incidents
- institutions
- vendors
- actors
- ransomware families
- countries
- MITRE techniques
- future IOCs

Possible approaches:

- relational edge table
- Postgres materialized graph-ish relations
- export-oriented STIX relation builder

### Richer campaign stitching

Potential future canonical/campaign layer:

- incident-level canonicalization remains the ground truth
- add a higher campaign layer for:
  - shared adversary
  - shared vendor/platform exposure
  - shared technique bundles
  - repeated extortion family targeting

## Future Frontend Surfaces

These were intentionally not added as production routes in the redesign because the data model is not complete enough yet.

### IOC workspace

- IOC summary cards
- IOC type distribution
- repeated IOC sightings
- IOC-to-incident tables

### Investigations / graph page

- incident relationship graph
- actor-family-victim pivots
- vendor-linked campaign spread

### Intel feeds / provenance page

- source performance
- yield by source family
- fetch/enrichment quality comparisons
- disclosure disparity trends

### Reports / analyst notebooks

- saved analyst views
- export-ready briefing layouts
- reproducible research notebooks from API payloads

## Reporting and Research Directions

### Source disclosure disparity

Now that canonical provenance stores which source filled which field, future work should quantify:

- which fields are most inconsistently disclosed across sources
- which source families publish richer victim-impact details
- whether selected primary sources are the same ones contributing the most incident detail

### Diamond-model analysis

Current production already exposes Diamond aggregates, but future research can go further by measuring:

- completeness by vertex
- confidence by vertex
- campaign clustering quality using Diamond pivots
- differences between education sub-sectors

### MITRE reporting quality

With current ATT&CK support and future optimization, we can study:

- most common ATT&CK tactics in education incidents
- which attack types have the widest ATT&CK spread
- whether public reporting under-discloses certain tactics

### STIX-oriented exports

Future work should add:

- STIX-style incident export
- ATT&CK + actor + victim relation export
- provenance-aware report packages for external analysis

## Recommended Implementation Order

1. IOC retention in canonical `v2`
2. IOC analytics endpoint and detail rendering
3. ingestion/source-provenance analytics
4. related-incident scoring endpoint
5. graph / investigations workspace
6. ATT&CK materialization if JSONB aggregation becomes a performance bottleneck
7. STIX-oriented exports and richer report generation

## Why This Sequencing Matters

The current redesign keeps the dashboard honest: it only visualizes what the data can already support. The roadmap above extends the platform toward a deeper education-sector CTI research environment without introducing mock analytics or weakly-backed product surfaces too early.
