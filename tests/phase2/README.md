# Phase 2: LLM Enrichment Pipeline Tests

Comprehensive test suite for the Phase 2 LLM enrichment pipeline, which extracts structured Cyber Threat Intelligence (CTI) from articles about educational sector security incidents.

---

## Quick Start

```bash
# Run all Phase 2 unit tests (no API key needed)
pytest tests/phase2/ -v

# Run mock LLM test (no API key needed)
python tests/phase2/test_comprehensive_llm_extraction.py --mock

# Run actual LLM test (requires API key)
export OLLAMA_API_KEY="your_key"
python tests/phase2/test_comprehensive_llm_extraction.py
```

---

## Test Files

| File | Description | API Key Required |
|------|-------------|------------------|
| `test_comprehensive_llm_extraction.py` | Full pipeline test with all modes | Optional |
| `test_phase2_enrichment.py` | `IncidentEnricher` class tests | No |
| `test_phase2_llm_client.py` | `OllamaLLMClient` tests | No |
| `test_phase2_deduplication.py` | Institution name normalization | No |
| `test_llm_response_validation.py` | LLM response parsing/validation | No |

---

## Test Modes

The comprehensive test (`test_comprehensive_llm_extraction.py`) supports three modes:

### Mock Mode (Recommended for CI)

```bash
python tests/phase2/test_comprehensive_llm_extraction.py --mock
```

- **No API key required**
- Verifies data flow with simulated LLM response
- Tests JSON-to-Pydantic mapping, DB storage, CSV export
- Fast execution (~2 seconds)

### LLM Mode (Integration Test)

```bash
export OLLAMA_API_KEY="your_key"
python tests/phase2/test_comprehensive_llm_extraction.py
```

- **Requires API key**
- Tests actual LLM extraction with comprehensive test article
- Verifies schema coverage and standardization
- Execution time: ~30-60 seconds

### E2E Mode (Full Pipeline)

```bash
export OLLAMA_API_KEY="your_key"
python tests/phase2/test_comprehensive_llm_extraction.py --e2e
```

- **Requires API key**
- Full flow: LLM extraction → DB storage → CSV export
- Uses temporary database
- Verifies entire data pipeline

---

## Data Flow Tested

```
                                ┌─────────────────────┐
                                │   Article Content   │
                                └──────────┬──────────┘
                                           │
                                           ▼
                            ┌──────────────────────────────┐
                            │  LLM Extraction (JSON Schema)│
                            │  Using EXTRACTION_SCHEMA     │
                            └──────────────┬───────────────┘
                                           │
                                           ▼
                            ┌──────────────────────────────┐
                            │ json_to_cti_enrichment()     │
                            │ → CTIEnrichmentResult        │
                            └──────────────┬───────────────┘
                                           │
                                           ▼
                            ┌──────────────────────────────┐
                            │ save_enrichment_result()     │
                            │ → incident_enrichments (JSON)│
                            │ → incident_enrichments_flat  │
                            └──────────────┬───────────────┘
                                           │
                                           ▼
                            ┌──────────────────────────────┐
                            │ export_enriched_dataset()    │
                            │ → CSV Export                 │
                            └──────────────────────────────┘
```

---

## Comprehensive Test Article

The test suite includes a crafted article covering **all 200+ schema fields** for comprehensive verification:

**Fields Covered:**
- Education relevance and institution identification
- Attack mechanics (vectors, MITRE ATT&CK tactics)
- Threat actor and ransomware attribution (40+ families)
- Data impact (records affected, 25+ data types)
- User impact (students, faculty, staff, alumni)
- System impact (35+ system categories)
- Operational impact (25+ impact types)
- Financial impact (ransom, recovery costs)
- Regulatory impact (GDPR, HIPAA, FERPA, fines)
- Recovery and remediation metrics (25+ security improvements)
- Transparency and disclosure tracking
- Research impact assessment
- Cross-incident analysis fields

---

## Enhanced CTI Schema (v2.0)

The extraction schema has been enhanced for comprehensive threat intelligence:

### Attack Categories (50+ types)
```
RANSOMWARE: ransomware_encryption, ransomware_double_extortion, ransomware_triple_extortion
PHISHING: phishing_credential_harvest, spear_phishing, whaling, business_email_compromise
DATA BREACH: data_breach_external, data_breach_internal, data_exposure_misconfiguration
MALWARE: malware_trojan, malware_infostealer, malware_cryptominer, malware_rat
ACCESS: credential_stuffing, brute_force, account_takeover, unauthorized_access
```

### Attack Vectors (60+ types)
```
CREDENTIAL: stolen_credentials, credential_stuffing, password_spraying, session_hijacking
VULNERABILITY: vulnerability_exploit_known, vulnerability_exploit_zero_day, unpatched_system
EXPOSED: exposed_rdp, exposed_vpn, exposed_ssh, exposed_database, exposed_api
CLOUD: cloud_misconfiguration, api_key_exposure, storage_bucket_exposure
SUPPLY CHAIN: supply_chain_compromise, third_party_vendor, trusted_relationship
```

### Ransomware Families (35+)
```
lockbit, lockbit_2, lockbit_3, blackcat_alphv, cl0p_clop, akira, play, 8base,
bianlian, royal, black_basta, medusa, rhysida, hunters_international, inc_ransom,
vice_society, hive, conti, ryuk, revil_sodinokibi, darkside, blackmatter, maze, etc.
```

### Data Categories (30+ types)
```
STUDENT: student_pii, student_ssn, student_grades, student_transcripts, student_health_records
EMPLOYEE: employee_pii, employee_ssn, employee_payroll, employee_benefits
RESEARCH: research_data, research_grants, research_ip, research_unpublished
CREDENTIALS: usernames_passwords, api_keys, certificates
```

### Value Standardization
All extracted values are automatically standardized:
- **Ransom amounts**: `$4.75 million` → `4750000` (numeric USD)
- **Durations**: `2 weeks` → `14` days or `336` hours
- **Record counts**: `2.8 million records` → `2800000`
- **Dates**: All ISO format `YYYY-MM-DD`

---

## Database Tables Verified

| Table | Purpose | Fields Verified |
|-------|---------|-----------------|
| `incidents` | Base incident with enrichment flags | `llm_enriched`, `llm_enriched_at`, `primary_url` |
| `incident_enrichments` | Full JSON enrichment | Complete `CTIEnrichmentResult` |
| `incident_enrichments_flat` | Flattened for CSV | 88+ columns |
| `articles` | Fetched article content | `content`, `fetch_successful`, `is_primary` |

---

## CSV Export Verification

Tests verify correct population of CSV columns:

### Core Fields
- `incident_id`, `source`, `university_name`
- `victim_raw_name`, `victim_raw_name_normalized`
- `institution_type`, `country`, `region`, `city`

### Standardization Verified
- Ransom amounts: `$4.75 million` → `4750000`
- Durations: `13 days` → `312` hours
- User counts: `52,500 students` → `52500`
- Records: `2,847,293 records` → `2847293`

### Field Categories (150+ columns)
- Attack mechanics (`attack_vector`, `ransomware_family`)
- Data impact (`data_records_affected_exact`, `data_exfiltrated`)
- User impact (`user_students_affected`, `user_staff_affected`)
- Operational impact (`operational_downtime_days`)
- Financial impact (`financial_ransom_amount_exact`)
- Regulatory impact (`regulatory_fine_amount`)
- Recovery metrics (`recovery_recovery_timeframe_days`)
- Transparency (`transparency_disclosure_delay_days`)

---

## Running Tests

### Unit Tests

```bash
# All Phase 2 unit tests
pytest tests/phase2/ -v

# Specific test file
pytest tests/phase2/test_phase2_enrichment.py -v

# Tests matching pattern
pytest tests/phase2/ -v -k "test_dedup"
```

### Coverage

```bash
pytest tests/phase2/ --cov=src.edu_cti.pipeline.phase2 --cov-report=html
open htmlcov/index.html
```

### With Verbose Logging

```bash
pytest tests/phase2/ -v --log-cli-level=DEBUG
```

---

## Error Handling Verified

The test suite verifies proper handling of:

| Scenario | Expected Behavior |
|----------|-------------------|
| JSON parsing failure | Marked as error, will retry |
| Not education-related | Marked as skipped with reason |
| No articles fetched | Marked as error, will retry |
| Selenium fails | Tries archive.org fallback |
| Invalid escape characters | Pre-processed before parsing |

---

## Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `OLLAMA_API_KEY` | API key for Ollama Cloud | Required for LLM/E2E |
| `OLLAMA_MODEL` | LLM model to use | `deepseek-v3.1:671b-cloud` |
| `EDU_CTI_DB_PATH` | Database path | `data/eduthreat.db` |

---

## Troubleshooting

### "OLLAMA_API_KEY not provided"

```bash
export OLLAMA_API_KEY="your_key"
# Or use mock mode:
python tests/phase2/test_comprehensive_llm_extraction.py --mock
```

### "No module named 'src.edu_cti'"

```bash
pip install -e .
```

### JSON Parse Errors

The pipeline now handles:
- `Invalid \escape` sequences
- Truncated JSON responses
- Missing required fields

These are logged but don't fail tests unless the whole pipeline fails.

---

## Contributing New Phase 2 Tests

When adding tests:

1. Use temporary databases (`tempfile.NamedTemporaryFile`)
2. Mock LLM calls for unit tests
3. Use the comprehensive test article for schema coverage
4. Verify both JSON and flattened storage
5. Check CSV export mapping

Example pattern:

```python
import tempfile
from src.edu_cti.pipeline.phase2.storage.db import get_connection, init_incident_enrichments_table

def test_my_feature():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    try:
        conn = get_connection(db_path)
        init_incident_enrichments_table(conn)
        # Your test code here
        conn.close()
    finally:
        os.unlink(db_path)
```

---

## Questions?

- Check `tests/phase2/test_comprehensive_llm_extraction.py` for patterns
- Review `src/edu_cti/pipeline/phase2/extraction/` for schema details
- See `docs/ARCHITECTURE.md` for system design
