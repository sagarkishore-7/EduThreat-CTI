# Test Suite

This directory contains tests for the EduThreat-CTI project, organized by phase.

## Test Organization

### Phase 1 Tests (`phase1/`)
- `test_models.py` - BaseIncident model tests
- `test_deduplication.py` - Cross-source deduplication tests
- `test_csv_output.py` - CSV output tests
- `test_pipeline.py` - Phase 1 pipeline integration tests
- `test_rss_pipeline.py` - RSS pipeline tests
- `test_incremental_save.py` - Incremental saving tests

### Phase 2 Tests (`phase2/`)
- `test_phase2_enrichment.py` - LLM enrichment pipeline tests
- `test_phase2_deduplication.py` - Post-enrichment deduplication tests
- `test_phase2_llm_client.py` - Ollama LLM client tests

## Running Tests

### Prerequisites
```bash
# Install dependencies
pip install -r requirements.txt

# Install test dependencies
pip install -e ".[dev]"
```

### Run All Tests
```bash
pytest
```

### Run Specific Test Suite
```bash
# Phase 1 tests
pytest tests/phase1/test_models.py
pytest tests/phase1/test_deduplication.py
pytest tests/phase1/test_pipeline.py

# Phase 2 tests
pytest tests/phase2/test_phase2_enrichment.py
pytest tests/phase2/test_phase2_deduplication.py
pytest tests/phase2/test_phase2_llm_client.py

# All Phase 1 tests
pytest tests/phase1/

# All Phase 2 tests
pytest tests/phase2/
```

### Run with Coverage
```bash
pytest --cov=src.edu_cti --cov-report=html
```

## Test Data

Tests use temporary databases and mock data. No real API calls or external dependencies are required for most tests.

## Phase 2 Testing

For Phase 2 tests that require Ollama Cloud API:
1. Set `OLLAMA_API_KEY` environment variable
2. Tests will skip if API key is not available
3. Mock implementations are available for unit testing

