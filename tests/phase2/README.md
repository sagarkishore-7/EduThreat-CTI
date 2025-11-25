# Phase 2 Enrichment Coverage Test

## Overview

This test verifies that the Phase 2 enrichment pipeline can extract all fields from the JSON schema when provided with a comprehensive article that mentions all possible information.

## Test File

**`test_enrichment_coverage.py`** - Comprehensive coverage test for Phase 2 enrichment

## Test Article

The test uses a prepared article (`COMPREHENSIVE_TEST_ARTICLE`) that covers **all fields** in the JSON schema, including:

### Core Information
- Institution details (Metropolitan State University, New York City, United States)
- Incident dates (incident, discovery, publication)
- Education relevance confirmation

### Attack Details
- Attack vector (spear-phishing, vulnerability exploit)
- Attack chain (full kill chain)
- MITRE ATT&CK techniques (7 techniques with sub-techniques)
- Threat actor (LockBit ransomware group)
- Initial access description

### Ransom Information
- Ransom demanded: $5.2 million USD
- Ransom paid: Yes, full amount
- Payment method: Bitcoin
- Communication channels: Email, Tor leak site

### Data Impact
- Records affected: 2,547,832 (exact number)
- Data types: Student records, faculty data, alumni data, financial data, research data, medical records, PII, credentials
- Data exfiltrated: Yes
- Data encrypted: Yes

### System Impact
- Systems affected: Email, student portal, LMS, HPC clusters, Active Directory, VPN, WiFi, network infrastructure, DNS, firewall, ERP, HR, admissions, library, payment, cloud storage, file shares, research lab instruments, phone/VoIP, printing, backups, security tools
- Infrastructure context: Hybrid cloud/on-premises
- Critical systems affected: Yes
- Network compromised: Yes
- Third-party vendor impact: Yes (EduTech Solutions Inc.)

### User Impact
- Students affected: 45,000
- Faculty affected: 3,200
- Staff affected: 8,500
- Alumni affected: 125,000
- Parents affected: 12,000
- Applicants affected: 5,500
- Patients affected: 2,800
- Total users: ~200,000

### Operational Impact
- Teaching disrupted: Yes (classes cancelled)
- Research disrupted: Yes (projects halted)
- Admissions disrupted: Yes
- Enrollment disrupted: Yes
- Payroll disrupted: Yes
- Clinical operations disrupted: Yes
- Online learning disrupted: Yes
- Classes cancelled: Yes
- Exams postponed: Yes
- Graduation delayed: Yes
- Downtime: 10 days (240 hours)
- Partial service: 5 days

### Financial Impact
- Recovery costs: $3.5-4.2 million USD
- Legal costs: $850,000 USD
- Notification costs: $125,000 USD
- Credit monitoring: $2.1 million USD
- Insurance claim: $8.5 million USD
- Total impact: $12-15 million USD

### Regulatory Impact
- GDPR breach: Yes
- UK DPA notification: Yes
- HIPAA breach: Yes
- FERPA breach: Yes
- PCI-DSS concerns: Yes
- Regulators notified: Multiple (NY AG, DoE, HHS, ICO, EU DPAs)
- Notifications sent: 200,000 individuals
- Fine imposed: $2.5 million USD
- Lawsuits filed: 13 (1 class action + 12 individual)
- Investigation opened: Yes

### Recovery & Remediation
- Recovery started: March 13, 2024
- Recovery completed: April 5, 2024
- Recovery timeframe: 23 days
- Recovery phases: Containment, eradication, recovery, lessons learned, post-incident review
- From backup: Yes (7 days old)
- Clean rebuild: Some systems
- Incident response firm: Mandiant
- Forensics firm: CrowdStrike
- Law firm: Jones Day
- Security improvements: MFA, network segmentation, firewall, IDS/IPS, monitoring, training, penetration testing, audit
- Response measures: Multiple (password reset, backup restoration, system rebuild, network isolation, etc.)
- MTTD: 18 hours
- MTTR: 4 hours
- MTTRecovery: 23 days

### Transparency
- Public disclosure: Yes (March 13, 2024)
- Disclosure delay: 3 days
- Transparency level: High
- Official statement URL: Provided
- Detailed report URL: Provided
- Updates provided: 8 updates

### Research Impact
- Research projects affected: 47
- Research data compromised: Yes (sensitive data)
- Publications delayed: 12
- Grants affected: 8 ($15 million)
- Collaborations affected: 15
- Research area: Biomedical Sciences, Quantum Computing

### Timeline
- Complete timeline with 15+ events from March 10 to April 5, 2024
- Event types: initial_access, discovery, exploitation, impact, containment, recovery, disclosure, notification
- Actor attribution: LockBit ransomware group
- Indicators: Multiple IOCs mentioned

## Running the Test

```bash
# Activate virtual environment
source .venv/bin/activate

# Set API key
export OLLAMA_API_KEY="your_api_key_here"

# Run the test
python tests/phase2/test_enrichment_coverage.py
```

## Test Output

The test generates:

1. **Console Output**: Detailed logging of the test process and results
2. **`coverage_test_results.json`**: JSON file with:
   - Coverage statistics (total fields, extracted, missing, null)
   - List of extracted fields
   - List of missing fields
   - List of null fields
   - Sample of extracted enrichment data

## Expected Results

The test verifies:
- ✅ All 192 schema fields are analyzed
- ✅ Enrichment completes successfully
- ✅ Data is saved to database
- ✅ Field coverage is calculated
- ✅ Results are saved to JSON file

## Notes

- The test uses the comprehensive enrichment method as a fallback if JSON schema parsing fails
- Coverage percentage indicates how many schema fields were successfully extracted
- Missing fields are those in the schema but not extracted from the article
- Null fields are those extracted but set to null/None

## Test Article Location

The comprehensive test article is embedded in `test_enrichment_coverage.py` as `COMPREHENSIVE_TEST_ARTICLE`. It's a 13,342-character article that mentions all possible fields in the JSON schema.

## Coverage Metrics

The test tracks:
- **Total Schema Fields**: 192 fields
- **Extracted Fields**: Fields with non-null values
- **Missing Fields**: Schema fields not present in enrichment data
- **Null Fields**: Fields present but set to null
- **Coverage Percentage**: (Extracted / Total) * 100

## Usage

This test can be run:
- During development to verify extraction capabilities
- After schema changes to ensure all fields are still extractable
- As a regression test to catch extraction regressions
- To benchmark extraction quality improvements

