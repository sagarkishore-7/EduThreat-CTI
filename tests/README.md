# EduThreat-CTI Test Suite

Comprehensive test suite for verifying Phase 1 ingestion and Phase 2 LLM enrichment pipelines.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src.edu_cti --cov-report=html
```

---

## Test Organization

```
tests/
├── README.md                    # This file
├── phase1/                      # Phase 1: Ingestion tests
│   ├── test_models.py           # BaseIncident data model tests
│   ├── test_deduplication.py    # Cross-source deduplication tests
│   ├── test_csv_output.py       # CSV export tests
│   ├── test_pipeline.py         # Pipeline integration tests
│   ├── test_rss_pipeline.py     # RSS feed ingestion tests
│   ├── test_incremental_save.py # Incremental saving tests
│   └── test_source_contribution.py  # 🆕 Contributor test suite
└── phase2/                      # Phase 2: LLM Enrichment tests
    ├── README.md                # Phase 2 specific documentation
    ├── test_comprehensive_llm_extraction.py  # Full pipeline test
    ├── test_phase2_enrichment.py    # Enrichment unit tests
    ├── test_phase2_deduplication.py # Post-enrichment dedup tests
    ├── test_phase2_llm_client.py    # Ollama client tests
    └── test_llm_response_validation.py  # Response parsing tests
```

---

## For Contributors

### Adding a New Source

After adding a new source (see `docs/ADDING_SOURCES.md`), verify it works correctly:

```bash
# 1. Test your source specifically
pytest tests/phase1/test_source_contribution.py -v -k "test_source" --source-name your_source_name

# 2. Verify database ingestion
pytest tests/phase1/test_source_contribution.py -v -k "test_source_incidents_ingestable" --source-name your_source_name

# 3. Verify Phase 2 readiness
pytest tests/phase1/test_source_contribution.py -v -k "test_phase2_readiness" --source-name your_source_name

# 4. Run all Phase 1 tests to ensure no regressions
pytest tests/phase1/ -v
```

### Source Contribution Checklist

Before submitting a PR for a new source:

- [ ] Source builder function follows naming convention: `build_<source_name>_incidents()`
- [ ] Source registered in `core/sources.py`
- [ ] All tests pass: `pytest tests/phase1/test_source_contribution.py -v --source-name <name>`
- [ ] Incidents have valid `incident_id` format: `<source>_<hash>`
- [ ] Incidents have at least one URL in `all_urls`
- [ ] `primary_url` is `None` (set by Phase 2)
- [ ] Source documented in `docs/SOURCES.md`
- [ ] CHANGELOG.md updated

---

## Phase 1 Tests

### Unit Tests

| Test File | Description | When to Run |
|-----------|-------------|-------------|
| `test_models.py` | BaseIncident model validation | After modifying data models |
| `test_deduplication.py` | URL normalization, cross-source dedup | After modifying dedup logic |
| `test_csv_output.py` | CSV export format verification | After modifying export |
| `test_incremental_save.py` | Batch saving during collection | After modifying save logic |

### Integration Tests

| Test File | Description | When to Run |
|-----------|-------------|-------------|
| `test_pipeline.py` | Full Phase 1 pipeline | Before releases |
| `test_rss_pipeline.py` | RSS feed ingestion | After modifying RSS sources |
| `test_source_contribution.py` | Source verification | When adding new sources |

### Running Phase 1 Tests

```bash
# All Phase 1 tests
pytest tests/phase1/ -v

# Specific test file
pytest tests/phase1/test_deduplication.py -v

# Tests matching pattern
pytest tests/phase1/ -v -k "test_url"

# With coverage
pytest tests/phase1/ --cov=src.edu_cti.pipeline.phase1
```

---

## Phase 2 Tests

### Test Modes

The comprehensive LLM test (`test_comprehensive_llm_extraction.py`) supports three modes:

| Mode | Command | API Key Required | Description |
|------|---------|------------------|-------------|
| Mock | `--mock` | No | Verifies data flow with simulated LLM response |
| LLM | (default) | Yes | Tests actual LLM extraction |
| E2E | `--e2e` | Yes | Full flow: LLM → DB → CSV |

```bash
# Mock test (no API key needed)
python tests/phase2/test_comprehensive_llm_extraction.py --mock

# LLM test (requires API key)
export OLLAMA_API_KEY="your_key"
python tests/phase2/test_comprehensive_llm_extraction.py

# E2E test (full pipeline)
python tests/phase2/test_comprehensive_llm_extraction.py --e2e
```

### Unit Tests

```bash
# All Phase 2 unit tests
pytest tests/phase2/ -v

# Specific tests
pytest tests/phase2/test_phase2_enrichment.py -v
pytest tests/phase2/test_llm_response_validation.py -v
```

### Data Flow Tested

```
Article Content
      ↓
LLM Extraction (JSON Schema) ← test_comprehensive_llm_extraction.py
      ↓
json_to_cti_enrichment() → CTIEnrichmentResult
      ↓
save_enrichment_result() → DB (JSON + Flattened)
      ↓
export_enriched_dataset() → CSV Export
```

---

## Local Testing Workflow

### Complete Pipeline Verification

```bash
# 1. Activate environment
source .venv/bin/activate

# 2. Run all unit tests
pytest tests/ -v

# 3. Test Phase 1 with limited pages (fast)
python -m src.edu_cti.pipeline.phase1.orchestrator --groups news --news-max-pages 2

# 4. Verify database state
python -c "
import sqlite3
conn = sqlite3.connect('data/eduthreat.db')
cur = conn.execute('SELECT COUNT(*) FROM incidents')
print(f'Incidents in DB: {cur.fetchone()[0]}')
cur = conn.execute('SELECT COUNT(*) FROM incidents WHERE llm_enriched = 0')
print(f'Ready for Phase 2: {cur.fetchone()[0]}')
"

# 5. Test Phase 2 with limit (requires API key)
export OLLAMA_API_KEY="your_key"
python -m src.edu_cti.pipeline.phase2 --limit 5

# 6. Verify enrichment and export
python -c "
from src.edu_cti.pipeline.phase2.csv_export import export_enriched_dataset
result = export_enriched_dataset()
print(f'Exported to: {result}')
"
```

### Testing a New Source

```bash
# 1. Create source file
vim src/edu_cti/sources/news/my_new_source.py

# 2. Register in sources.py
vim src/edu_cti/core/sources.py

# 3. Run contributor tests
pytest tests/phase1/test_source_contribution.py -v --source-name my_new_source --max-pages 2

# 4. Test ingestion
python -m src.edu_cti.pipeline.phase1.orchestrator --groups news --news-sources my_new_source --news-max-pages 5

# 5. Verify in database
python -c "
import sqlite3
conn = sqlite3.connect('data/eduthreat.db')
cur = conn.execute(\"SELECT COUNT(*) FROM incidents WHERE source = 'my_new_source'\")
print(f'Incidents from new source: {cur.fetchone()[0]}')
"

# 6. Run full Phase 1 regression
pytest tests/phase1/ -v
```

---

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: pip install -r requirements.txt
      
      - name: Run unit tests
        run: pytest tests/ -v --ignore=tests/phase2/test_comprehensive_llm_extraction.py
      
      - name: Run mock LLM test
        run: python tests/phase2/test_comprehensive_llm_extraction.py --mock
```

---

## Troubleshooting

### Common Issues

**Test imports fail**
```bash
# Ensure you're in the project root and have installed the package
pip install -e .
```

**Network errors in CI**
```bash
# Source tests that require network will skip gracefully in CI
pytest tests/phase1/test_source_contribution.py -v
# Tests with network errors are marked as skipped, not failed
```

**Database locked**
```bash
# Close any open database connections and retry
# Tests use temporary databases to avoid conflicts
```

**LLM tests require API key**
```bash
export OLLAMA_API_KEY="your_key"
# Or use mock mode: python tests/phase2/test_comprehensive_llm_extraction.py --mock
```

---

## Coverage Goals

| Component | Target Coverage |
|-----------|-----------------|
| Core models | 90%+ |
| Deduplication | 85%+ |
| Database operations | 80%+ |
| Source builders | 70%+ |
| Pipeline orchestration | 75%+ |

Generate coverage report:
```bash
pytest tests/ --cov=src.edu_cti --cov-report=html
open htmlcov/index.html
```

---

## Questions?

- Check existing tests for patterns
- Open an issue for questions
- See `docs/ARCHITECTURE.md` for system design
