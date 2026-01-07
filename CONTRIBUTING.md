# Contributing to EduThreat-CTI

Thank you for your interest in contributing to EduThreat-CTI! This document provides comprehensive guidelines for contributing to the project as a **developer** and **CTI analyst**.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Adding New Sources](#adding-new-sources)
- [Testing Your Contribution](#testing-your-contribution)
- [Development Setup](#development-setup)
- [Pull Request Process](#pull-request-process)
- [Code Style](#code-style)
- [CTI Data Quality Standards](#cti-data-quality-standards)

---

## Code of Conduct

This project adheres to a code of conduct. By participating, you are expected to uphold this code. We prioritize:
- **Ethical OSINT collection** (public sources only)
- **Respectful communication**
- **Privacy awareness** (no PII beyond what's publicly disclosed)

---

## Getting Started

### 1. Fork and Clone

```bash
# Fork the repository on GitHub, then:
git clone https://github.com/YOUR_USERNAME/EduThreat-CTI.git
cd EduThreat-CTI
git remote add upstream https://github.com/sagarkishore-7/EduThreat-CTI.git
```

### 2. Set Up Development Environment

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e ".[dev]"
```

### 3. Verify Setup

```bash
# Run tests to ensure everything works
pytest tests/ -v

# Test Phase 1 (quick)
python -m src.edu_cti.pipeline.phase1.orchestrator --groups news --news-max-pages 1
```

---

## Adding New Sources

Adding data sources is one of the most valuable contributions. This section provides a complete workflow.

### Source Types

| Type | Description | Examples |
|------|-------------|----------|
| **Curated** | Dedicated education sector sections | KonBriefing, DataBreaches.net education archive |
| **News** | Keyword-based search sources | The Hacker News, SecurityWeek, Dark Reading |
| **RSS** | RSS feed sources | DataBreaches RSS, security blog feeds |

### Step-by-Step Guide

#### 1. Create Source Builder

```bash
# Create file in appropriate directory
# Curated: src/edu_cti/sources/curated/<source_name>.py
# News: src/edu_cti/sources/news/<source_name>.py
# RSS: src/edu_cti/sources/rss/<source_name>.py
```

**Template:**

```python
"""
<Source Name> source implementation for EduThreat-CTI.

Collects education sector cyber incidents from <source>.
"""

import logging
from typing import List, Optional, Callable

from edu_cti.core.models import BaseIncident, make_incident_id
from edu_cti.core.http import HttpClient, build_http_client

logger = logging.getLogger(__name__)

SOURCE_NAME = "mysource"  # lowercase, alphanumeric + underscore only


def build_mysource_incidents(
    *,
    max_pages: Optional[int] = None,
    client: Optional[HttpClient] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
) -> List[BaseIncident]:
    """
    Build incidents from MySource.
    
    Args:
        max_pages: Maximum pages to fetch (None = all)
        client: HTTP client (creates default if not provided)
        save_callback: Optional callback for incremental saving
        
    Returns:
        List of BaseIncident objects
    """
    if client is None:
        client = build_http_client()
    
    incidents = []
    page = 1
    
    while True:
        if max_pages and page > max_pages:
            break
            
        # Fetch page
        url = f"https://mysource.com/education/page/{page}"
        soup = client.get_soup(url)
        
        if soup is None:
            break
        
        # Extract incidents
        articles = soup.select("article.incident")
        if not articles:
            break
            
        for article in articles:
            # Extract data
            title = article.select_one("h2").get_text(strip=True)
            link = article.select_one("a")["href"]
            date_str = article.select_one(".date").get_text(strip=True)
            
            # Create incident
            incident = BaseIncident(
                incident_id=make_incident_id(SOURCE_NAME, link),
                source=SOURCE_NAME,
                source_event_id=None,
                title=title,
                subtitle=None,
                primary_url=None,  # IMPORTANT: Always None in Phase 1
                all_urls=[link],   # REQUIRED: At least one URL
                university_name=None,
                victim_raw_name=None,
                institution_type="Unknown",
                country=None,
                region=None,
                city=None,
                incident_date=parse_date(date_str),  # Implement date parsing
                date_precision="day",  # or "month", "year", "unknown"
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",  # "low", "medium", or "high"
                source_published_date=date_str,
                ingested_at=None,  # Set automatically
                notes=f"news_source={SOURCE_NAME}",
            )
            incidents.append(incident)
        
        # Incremental save
        if save_callback and incidents:
            save_callback(incidents)
        
        page += 1
    
    logger.info(f"Built {len(incidents)} incidents from {SOURCE_NAME}")
    return incidents
```

#### 2. Register Source

Edit `src/edu_cti/core/sources.py`:

```python
# Add import
from edu_cti.sources.news.mysource import build_mysource_incidents

# Add to appropriate registry
NEWS_SOURCE_REGISTRY: Dict[str, Callable[..., List[BaseIncident]]] = {
    # ... existing sources ...
    "mysource": build_mysource_incidents,
}
```

#### 3. Test Your Source

```bash
# Run contributor test suite for your source
pytest tests/phase1/test_source_contribution.py -v --source-name mysource --max-pages 2

# This runs:
# - test_source_builds_incidents: Verifies incidents are created
# - test_source_incidents_ingestable: Verifies DB insertion works
# - test_source_incidents_queryable: Verifies Phase 2 compatibility
# - test_phase2_readiness: Verifies incidents have required fields
```

#### 4. Verify Full Pipeline

```bash
# Test ingestion
python -m src.edu_cti.pipeline.phase1.orchestrator --groups news --news-sources mysource --news-max-pages 5

# Verify database
python -c "
import sqlite3
conn = sqlite3.connect('data/eduthreat.db')
cur = conn.execute(\"SELECT COUNT(*) FROM incidents WHERE source = 'mysource'\")
print(f'Ingested: {cur.fetchone()[0]} incidents')
"

# Run full regression
pytest tests/phase1/ -v
```

#### 5. Document Your Source

Add to `docs/SOURCES.md`:

```markdown
### MySource

- **Type**: News
- **URL**: https://mysource.com
- **Description**: Security news site with education sector coverage
- **Implementation**: `src/edu_cti/sources/news/mysource.py`
- **Confidence**: Medium (keyword-based search)
```

---

## Testing Your Contribution

### Required Tests

Before submitting a PR, ensure all tests pass:

```bash
# 1. Run all unit tests
pytest tests/ -v

# 2. Run Phase 1 tests specifically
pytest tests/phase1/ -v

# 3. For new sources, run contributor tests
pytest tests/phase1/test_source_contribution.py -v --source-name <your_source>

# 4. Run mock LLM test (no API key needed)
python tests/phase2/test_comprehensive_llm_extraction.py --mock
```

### Test Checklist

For new sources:

- [ ] `test_source_builds_incidents` passes
- [ ] `test_source_incidents_ingestable` passes
- [ ] `test_source_incidents_queryable` passes
- [ ] `test_phase2_readiness` passes
- [ ] All existing tests still pass
- [ ] `primary_url` is `None` for all incidents
- [ ] All incidents have at least one URL in `all_urls`
- [ ] `incident_id` format is `<source>_<hash>`

For bug fixes/features:

- [ ] Existing tests pass
- [ ] New tests added for the change
- [ ] No regressions in Phase 1 or Phase 2

---

## Development Setup

### Environment Variables

```bash
# Copy example env file
cp .env.example .env

# Edit with your settings
# Required for Phase 2:
# OLLAMA_API_KEY=your_api_key
```

### Project Structure

```
src/edu_cti/
├── core/              # Shared functionality
│   ├── models.py      # BaseIncident model
│   ├── config.py      # Configuration
│   ├── db.py          # Database operations
│   ├── sources.py     # Source registry
│   └── http.py        # HTTP client
├── sources/           # Source implementations
│   ├── curated/       # Curated sources
│   ├── news/          # News sources
│   └── rss/           # RSS sources
└── pipeline/          # Processing pipelines
    ├── phase1/        # Ingestion
    └── phase2/        # LLM Enrichment
```

---

## Pull Request Process

### Before Submitting

1. **Run all tests**: `pytest tests/ -v`
2. **Format code**: `black src/`
3. **Lint code**: `flake8 src/`
4. **Update CHANGELOG.md** with your changes
5. **Update documentation** if needed

### PR Checklist

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] New source
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation update

## Testing
- [ ] All existing tests pass
- [ ] New tests added for changes
- [ ] Contributor tests pass (for new sources)
- [ ] Manual testing completed

## Documentation
- [ ] CHANGELOG.md updated
- [ ] docs/SOURCES.md updated (for new sources)
- [ ] Code docstrings added

## Data Quality (for new sources)
- [ ] Incidents have valid URLs
- [ ] Dates are properly formatted
- [ ] Deduplication works correctly
- [ ] Phase 2 can consume the data
```

---

## Code Style

### Python Style

- Follow PEP 8
- Use type hints
- Write docstrings for public functions
- Keep functions focused and modular

```bash
# Format code
black src/

# Check formatting
black src/ --check

# Lint
flake8 src/
```

### Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Source name | lowercase, alphanumeric + underscore | `darkreading`, `the_record` |
| Builder function | `build_<source>_incidents` | `build_darkreading_incidents` |
| Source constant | `SOURCE_NAME` | `SOURCE_NAME = "darkreading"` |
| File name | `<source>.py` | `darkreading.py` |

---

## CTI Data Quality Standards

As a CTI project, we maintain high data quality standards:

### Incident Requirements

| Field | Requirement |
|-------|-------------|
| `incident_id` | Unique, deterministic hash: `<source>_<hash>` |
| `source` | Must match source name in registry |
| `title` | Non-empty, describes the incident |
| `all_urls` | At least one valid URL for Phase 2 |
| `primary_url` | **Must be None** in Phase 1 (set by Phase 2) |
| `source_confidence` | Appropriate for source type |

### Date Handling

```python
# Preferred format: YYYY-MM-DD
incident_date = "2024-03-15"
date_precision = "day"

# For month-only dates
incident_date = "2024-03-01"  # First of month
date_precision = "month"

# For year-only dates
incident_date = "2024-01-01"  # First of year
date_precision = "year"

# Unknown dates
incident_date = None
date_precision = "unknown"
```

### Source Confidence Levels

| Level | When to Use |
|-------|-------------|
| `high` | Curated sources with verified data (KonBriefing) |
| `medium` | News sources with editorial review |
| `low` | Automated feeds, social media, unverified sources |

---

## Data Integrity & Database Protection

This project maintains a production SQLite database (`data/eduthreat.db`) with curated CTI data. **Protecting data integrity is critical** since contributors may inadvertently introduce errors through:

1. **LLM extraction errors** - Models may hallucinate or produce invalid data
2. **Source parsing bugs** - Incorrect scraping logic can corrupt data
3. **Schema violations** - Invalid field values can break downstream analysis

### ⚠️ Critical Rules for Contributors

#### 1. **Never Commit Database Files**

The `.gitignore` already excludes `data/*.db`, but please verify:

```bash
# Check if db files are tracked
git status data/

# Should show: "nothing to commit" or files in .gitignore
```

**If you accidentally commit a database file**, contact maintainers immediately.

#### 2. **Use a Sandbox Database for Development**

Always work with a separate test database:

```bash
# Create a sandbox database
export EDU_CTI_DB_PATH="data/eduthreat_sandbox.db"

# Or use pytest which creates temporary databases automatically
pytest tests/ -v
```

#### 3. **Run Validation Tests Before PRs**

```bash
# Validate your source doesn't produce schema violations
pytest tests/phase1/test_source_contribution.py -v --source-name <your_source>

# Validate LLM extraction doesn't produce validation errors
pytest tests/phase2/test_llm_response_validation.py -v
```

### LLM Enrichment Safeguards

The enrichment pipeline has built-in safeguards, but contributors should be aware:

#### Validation Layers

1. **JSON Schema Validation** - LLM output must match `EXTRACTION_SCHEMA`
2. **Pydantic Validation** - Results are validated via `CTIEnrichmentResult`
3. **Enum Normalization** - Invalid values are normalized to valid options
4. **Null Handling** - Missing fields default to `None`, not invalid values

#### Adding New Extraction Fields

When extending the schema:

```python
# In extraction_schema.py - add with validation
"new_field": {
    "type": ["string", "null"],
    "enum": ["valid_option1", "valid_option2", null],  # Always include null
    "description": "Clear description for LLM"
}

# In json_to_schema_mapper.py - add normalization
NEW_FIELD_NORMALIZATION = {
    "variation1": "valid_option1",
    "variation2": "valid_option2",
}
```

### Testing LLM Extraction Locally

Before running enrichment against production data:

```bash
# 1. Test with mock LLM (no API calls)
python tests/phase2/test_comprehensive_llm_extraction.py --mock

# 2. Test with real LLM on sandbox
export EDU_CTI_DB_PATH="data/eduthreat_sandbox.db"
python -m src.edu_cti.pipeline.phase2 --limit 5

# 3. Validate enrichment results
python -c "
import sqlite3
conn = sqlite3.connect('data/eduthreat_sandbox.db')
cur = conn.execute('''
    SELECT COUNT(*) as total,
           SUM(CASE WHEN llm_enriched = 1 THEN 1 ELSE 0 END) as enriched
    FROM incidents
''')
print(cur.fetchone())
"
```

### Production Database Access

The production database is maintained by project maintainers:

- **Contributors**: Use sandbox databases only
- **Maintainers**: Follow the release checklist before updating production
- **Automated CI**: Runs on clean databases, never production

### Rollback Procedures

If bad data enters the database:

```bash
# Revert specific incidents
python -c "
from src.edu_cti.pipeline.phase2.utils.revert_enrichments import revert_enrichments
revert_enrichments(incident_ids=['bad_incident_id_1', 'bad_incident_id_2'])
"

# Revert all enrichments from a specific time
python -m src.edu_cti.pipeline.phase2.utils.revert_enrichments --after "2024-01-15T00:00:00"
```

### Dashboard Data Quality

The dashboard (`EduThreat-CTI-Dashboard`) displays data from the API. Data quality issues here indicate upstream problems:

1. **Missing data** → Check if source is scraping correctly
2. **Invalid values** → Check LLM normalization maps
3. **Duplicate incidents** → Check deduplication logic

Report data quality issues as GitHub Issues with:
- Screenshot of the problem
- Incident ID(s) affected
- Expected vs actual values

---

## Questions?

- Check existing source implementations for examples
- Open an issue for questions
- Review `docs/ARCHITECTURE.md` for system design
- See `tests/README.md` for test documentation
- For data integrity concerns, tag issue with `data-integrity` label

Thank you for contributing to EduThreat-CTI!
