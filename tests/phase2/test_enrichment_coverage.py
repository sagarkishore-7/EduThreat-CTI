#!/usr/bin/env python3
"""
Comprehensive coverage test for Phase 2 enrichment pipeline.

This test uses a prepared article that covers all fields in the JSON schema
to verify that the LLM extraction captures all available information.
"""

import sys
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Set

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.edu_cti.core.db import get_connection, init_db
from src.edu_cti.pipeline.phase2.enrichment import IncidentEnricher
from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti.pipeline.phase2.storage.db import save_enrichment_result, get_enrichment_flat
from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleContent
from src.edu_cti.core.config import OLLAMA_API_KEY, OLLAMA_HOST, OLLAMA_MODEL
from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase2.extraction.extraction_schema import EXTRACTION_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Comprehensive test article covering all schema fields
COMPREHENSIVE_TEST_ARTICLE = """
Major Cyber Attack Strikes Prestigious Research University: LockBit Ransomware Group Claims Responsibility

Published: March 15, 2024
Author: Cybersecurity News Team
Source: https://example.com/cyber-attack-university-2024

BREAKING: Metropolitan State University, a leading research institution in New York City, New York, United States, has fallen victim to a sophisticated ransomware attack orchestrated by the LockBit ransomware group. The incident, which began on March 10, 2024, has resulted in widespread system encryption, data exfiltration, and operational disruption affecting over 45,000 students, 3,200 faculty members, and 8,500 staff members.

INITIAL ACCESS AND ATTACK VECTOR

The attack began on March 10, 2024, when threat actors gained initial access through a spear-phishing email campaign targeting university IT administrators. The attackers exploited a zero-day vulnerability (CVE-2024-12345) in the university's student information system (SIS) vendor software, allowing them to establish a foothold in the network. The attack chain followed a classic kill chain: reconnaissance, weaponization, delivery, exploitation, installation, command and control, actions on objectives, exfiltration, and impact.

The LockBit ransomware group, also known as LockBit 3.0, claimed responsibility for the attack on March 12, 2024, posting evidence of the breach on their Tor leak site at http://lockbit7z2jwcskxpb.onion/claim/metropolitan-state-university. The group communicated via email and their dark web forum, demanding a ransom payment.

RANSOM DEMANDS AND PAYMENT

On March 12, 2024, the attackers demanded a ransom of $5.2 million USD in Bitcoin. The university initially refused to pay, but after assessing the full scope of the damage and the critical nature of the encrypted research data, the board of directors authorized payment of the full $5.2 million on March 14, 2024. The payment was made in Bitcoin to the wallet address provided by the attackers. The ransom note was delivered via email and posted on the university's encrypted systems.

SYSTEMS AFFECTED

The attack had a devastating impact on the university's infrastructure, which operates on a hybrid cloud and on-premises architecture. The following systems were compromised:

- Email systems (Microsoft Exchange) - fully encrypted
- Student portal and learning management system (LMS) - encrypted
- Research high-performance computing (HPC) clusters - encrypted
- Active Directory and identity/SSO systems - compromised
- VPN and remote access systems - disabled
- WiFi and wired network infrastructure - partially compromised
- DNS and DHCP servers - affected
- Firewall and gateway systems - bypassed
- ERP, finance, and HR systems - encrypted
- Admissions and enrollment systems - down
- Library systems - offline
- Payment and billing systems - compromised
- Cloud storage (Microsoft OneDrive, Google Drive) - encrypted
- On-premises file shares - encrypted
- Research lab instruments and equipment - network access blocked
- Phone and VoIP systems - down
- Printing and copy services - offline
- Backup infrastructure - partially encrypted
- Security tools and monitoring systems - disabled

The university's third-party vendor, EduTech Solutions Inc., which provides the SIS platform, was also impacted. The vendor's cloud-hosted services were compromised, affecting multiple educational institutions.

DATA BREACH AND EXFILTRATION

The attackers successfully exfiltrated approximately 2.5 million records before encrypting the systems. The compromised data includes:

- Personal information (PII) of 45,000 current students
- Student records including grades, transcripts, and academic history
- Faculty data including employment records and research information
- Alumni data including contact information and donation records
- Financial data including tuition payments and scholarship information
- Research data including sensitive research projects and intellectual property
- Medical records from the university hospital (HIPAA-protected data)
- Administrative data including HR records and payroll information
- Special category GDPR data including health information and biometric data
- Credentials including password hashes and authentication tokens

The exact number of records affected is 2,547,832. The data was exfiltrated to external servers controlled by the attackers before encryption began.

OPERATIONAL IMPACT

The attack caused severe operational disruption:

- Teaching was completely disrupted - all classes cancelled from March 11-20, 2024
- Research activities were severely impacted - multiple research projects halted
- Admissions processes were disrupted - application processing delayed
- Enrollment systems were down - new student registration impossible
- Payroll was disrupted - staff payments delayed by 3 days
- Clinical operations at the university hospital were disrupted
- Online learning platforms were completely offline
- Network-wide outage lasted 10 days (240 hours)
- Partial service restoration took an additional 5 days
- Exams scheduled for March 15-25 were postponed
- Spring graduation ceremony scheduled for May 15 was delayed to June 1

USER IMPACT

The attack affected:
- 45,000 students (current enrollment)
- 3,200 faculty members
- 8,500 staff members
- 125,000 alumni (contact information compromised)
- 12,000 parents (guardian information exposed)
- 5,500 applicants (application data breached)
- 2,800 patients at the university hospital

Total users affected: approximately 200,000 individuals.

FINANCIAL IMPACT

Beyond the $5.2 million ransom payment, the university incurred significant recovery costs:

- Recovery and restoration costs: $3.5-4.2 million USD
- Legal costs: $850,000 USD
- Breach notification costs: $125,000 USD
- Credit monitoring services for affected individuals: $2.1 million USD
- Insurance claim filed: Yes, for $8.5 million USD

The total financial impact is estimated at $12-15 million USD.

REGULATORY AND COMPLIANCE

The breach triggered multiple regulatory requirements:

- GDPR breach confirmed - EU students' data affected
- UK Data Protection Act (DPA) notification required
- HIPAA breach confirmed - medical records compromised
- FERPA breach - student educational records exposed
- PCI-DSS concerns - payment card data potentially exposed

The university notified regulators on March 16, 2024:
- New York State Attorney General
- U.S. Department of Education
- U.S. Department of Health and Human Services (HHS)
- UK Information Commissioner's Office (ICO)
- EU Data Protection Authorities

Data breach notifications were sent to all affected individuals on March 18, 2024. A total of 200,000 notification letters were sent.

Regulatory investigations were opened by:
- New York State Attorney General's Office
- U.S. Department of Education
- HHS Office for Civil Rights

A fine of $2.5 million USD was imposed by the New York State Attorney General for failure to implement adequate security measures.

Multiple lawsuits were filed:
- Class action lawsuit filed on March 25, 2024
- Individual lawsuits: 12 filed by affected students and staff
- Total lawsuit count: 13

RECOVERY AND REMEDIATION

Recovery efforts began on March 13, 2024, and were completed on April 5, 2024, taking 23 days. The recovery process involved multiple phases:

1. Containment - Isolated affected systems
2. Eradication - Removed malware and backdoors
3. Recovery - Restored systems from backups
4. Lessons learned - Post-incident review
5. Post-incident review - Security audit

The university was able to restore most systems from backups, though the backups were 7 days old. Some systems required a clean rebuild due to persistent malware. The university engaged several third-party firms:

- Incident response: Mandiant (FireEye)
- Forensics investigation: CrowdStrike
- Legal counsel: Jones Day law firm

Security improvements implemented:
- Multi-factor authentication (MFA) enabled for all accounts
- Network segmentation implemented
- Enhanced firewall rules
- Intrusion detection and prevention systems (IDS/IPS) deployed
- Security monitoring enhanced
- Security training conducted for all staff
- Penetration testing performed
- Security audit completed

Response measures taken:
- Password reset for all accounts
- Account lockout for compromised accounts
- Credential rotation for all service accounts
- Backup restoration from clean backups
- System rebuild for critical systems
- Network isolation of compromised segments
- Endpoint containment
- Malware removal
- Patch application for all vulnerabilities
- Vulnerability remediation
- Access revocation for compromised accounts
- Incident response team activation
- Forensics investigation
- Law enforcement notification (FBI Cyber Division)
- Regulatory notification
- User notification
- Public disclosure

The incident was detected by the internal security team on March 11, 2024, at 2:30 AM. Mean time to detect (MTTD): 18 hours. Mean time to respond (MTTR): 4 hours. Mean time to recover (MTTR): 23 days.

TRANSPARENCY AND DISCLOSURE

The university provided high transparency throughout the incident:

- Public disclosure made on March 13, 2024 (3 days after discovery)
- Disclosure delay: 3 days
- Official statement URL: https://www.metropolitan-state.edu/security-incident
- Detailed report URL: https://www.metropolitan-state.edu/security-incident-report
- Regular updates provided: 8 updates total
- Updates posted on university website and social media

The disclosure was made by the institution itself, with additional information provided by regulators and media coverage.

RESEARCH IMPACT

The attack had severe consequences for research activities:

- 47 active research projects affected
- Sensitive research data compromised, including:
  - Biomedical research on cancer treatments
  - Climate change modeling data
  - Quantum computing research
- 12 research publications delayed
- 8 research grants affected (totaling $15 million)
- 15 international research collaborations disrupted
- Primary research area: Biomedical Sciences and Quantum Computing

MITRE ATT&CK TECHNIQUES

The attackers used multiple MITRE ATT&CK techniques:

1. T1566.001 - Phishing: Spear-phishing Attachment (Initial Access)
2. T1078 - Valid Accounts: Domain Accounts (Persistence, Privilege Escalation)
3. T1055 - Process Injection: Dynamic-link Library Injection (Defense Evasion)
4. T1021.001 - Remote Services: Remote Desktop Protocol (Lateral Movement)
5. T1003.001 - OS Credential Dumping: LSASS Memory (Credential Access)
6. T1048 - Exfiltration Over Alternative Protocol (Exfiltration)
7. T1486 - Data Encrypted for Impact (Impact)

Sub-techniques used:
- T1566.001 (Phishing: Spear-phishing Attachment)
- T1055.001 (Process Injection: Dynamic-link Library Injection)
- T1021.001 (Remote Services: Remote Desktop Protocol)
- T1003.001 (OS Credential Dumping: LSASS Memory)

TIMELINE OF EVENTS

March 10, 2024 (Day 1):
- 8:00 AM: Spear-phishing email sent to IT administrators
- 10:30 AM: Malicious attachment opened, initial access gained
- 2:15 PM: Vulnerability exploited in SIS vendor software
- 4:45 PM: Command and control (C2) channel established
- 6:20 PM: Lateral movement begins across network

March 11, 2024 (Day 2):
- 12:00 AM: Data exfiltration begins
- 2:30 AM: Internal security team detects anomalous activity
- 4:30 AM: Incident response team activated
- 8:00 AM: Systems begin encryption process
- 10:00 AM: Full network encryption completed
- 2:00 PM: Ransom note delivered via email

March 12, 2024 (Day 3):
- 9:00 AM: LockBit group claims responsibility on leak site
- 11:00 AM: Ransom demand of $5.2 million USD received
- 3:00 PM: University board meeting to discuss response

March 13, 2024 (Day 4):
- 8:00 AM: Recovery efforts begin
- 10:00 AM: Public disclosure made
- 2:00 PM: Mandiant incident response team engaged
- 4:00 PM: FBI Cyber Division notified

March 14, 2024 (Day 5):
- 11:00 AM: Board authorizes ransom payment
- 3:00 PM: $5.2 million Bitcoin payment made
- 6:00 PM: Decryption keys received from attackers

March 15-20, 2024 (Days 6-11):
- System restoration from backups
- Malware removal and system hardening
- Network segmentation implemented

March 21-April 5, 2024 (Days 12-27):
- Gradual service restoration
- Security improvements implemented
- Staff training conducted

April 5, 2024:
- Full recovery completed
- All systems operational
- Post-incident review begins

BUSINESS IMPACT

The business impact of this incident was classified as CRITICAL, affecting:
- Core educational operations
- Research activities
- Financial operations
- Regulatory compliance
- Reputation and trust

The operational impact included complete shutdown of teaching, research, and administrative functions for 10 days, with partial operations for an additional 5 days.

CONCLUSION

This attack represents one of the most severe cyber incidents affecting a higher education institution in recent years. The combination of sophisticated attack techniques, extensive data exfiltration, and operational disruption highlights the critical importance of robust cybersecurity measures in the education sector.

The university has committed to implementing comprehensive security improvements and has established a dedicated cybersecurity task force to prevent future incidents.
"""


def get_test_incident() -> BaseIncident:
    """Create a test incident for the comprehensive article."""
    from datetime import datetime, timezone
    return BaseIncident(
        incident_id="test_coverage_001",
        source="test_coverage",
        source_event_id="test_001",
        ingested_at=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        university_name="Metropolitan State University",
        victim_raw_name="Metropolitan State University",
        institution_type="University",
        country="United States",
        region="New York",
        city="New York City",
        incident_date="2024-03-10",
        date_precision="day",
        source_published_date="2024-03-15",
        title="Major Cyber Attack Strikes Prestigious Research University",
        subtitle="LockBit Ransomware Group Claims Responsibility",
        primary_url="https://example.com/cyber-attack-university-2024",
        all_urls=["https://example.com/cyber-attack-university-2024"],
        attack_type_hint="ransomware",
        status="confirmed",
        source_confidence="high",
    )


def get_test_article() -> Dict[str, ArticleContent]:
    """Get the comprehensive test article."""
    article = ArticleContent(
        url="https://example.com/cyber-attack-university-2024",
        title="Major Cyber Attack Strikes Prestigious Research University: LockBit Ransomware Group Claims Responsibility",
        content=COMPREHENSIVE_TEST_ARTICLE,
        author="Cybersecurity News Team",
        publish_date="2024-03-15",
        fetch_successful=True,
    )
    return {article.url: article}


def get_all_schema_fields(schema: Dict[str, Any], prefix: str = "") -> Set[str]:
    """Recursively extract all field names from the JSON schema."""
    fields = set()
    
    if "properties" in schema:
        for field_name, field_def in schema["properties"].items():
            full_name = f"{prefix}.{field_name}" if prefix else field_name
            fields.add(full_name)
            
            # Recursively process nested objects
            if field_def.get("type") == "object" and "properties" in field_def:
                fields.update(get_all_schema_fields(field_def, full_name))
            elif field_def.get("type") == "array" and "items" in field_def:
                items = field_def["items"]
                if isinstance(items, dict) and items.get("type") == "object" and "properties" in items:
                    fields.update(get_all_schema_fields(items, full_name))
    
    return fields


def verify_field_coverage(enrichment_data: Dict[str, Any], schema_fields: Set[str]) -> Dict[str, Any]:
    """Verify which schema fields were extracted."""
    extracted_fields = set()
    missing_fields = set()
    null_fields = set()
    
    def check_field(value: Any, field_path: str):
        """Recursively check if field has a value."""
        if field_path in schema_fields:
            if value is None:
                null_fields.add(field_path)
            elif isinstance(value, (dict, list)):
                if value:  # Non-empty dict/list
                    extracted_fields.add(field_path)
                    # Recursively check nested fields
                    if isinstance(value, dict):
                        for k, v in value.items():
                            check_field(v, f"{field_path}.{k}")
                    elif isinstance(value, list) and value and isinstance(value[0], dict):
                        for item in value[:1]:  # Check first item
                            for k, v in item.items():
                                check_field(v, f"{field_path}.{k}")
                else:
                    null_fields.add(field_path)
            else:
                extracted_fields.add(field_path)
    
    # Check all fields in enrichment data
    for key, value in enrichment_data.items():
        check_field(value, key)
    
    # Find fields in schema but not in enrichment data
    for field in schema_fields:
        if field not in extracted_fields and field not in null_fields:
            # Check if it's a nested field
            base_field = field.split(".")[0]
            if base_field not in enrichment_data:
                missing_fields.add(field)
    
    return {
        "extracted": extracted_fields,
        "missing": missing_fields,
        "null": null_fields,
        "total_schema_fields": len(schema_fields),
        "extracted_count": len(extracted_fields),
        "missing_count": len(missing_fields),
        "null_count": len(null_fields),
        "coverage_percent": (len(extracted_fields) / len(schema_fields) * 100) if schema_fields else 0,
    }


def main():
    """Run the comprehensive coverage test."""
    logger.info("=" * 80)
    logger.info("PHASE 2 ENRICHMENT COVERAGE TEST")
    logger.info("=" * 80)
    
    # Get all schema fields
    logger.info("\n[STEP 1] Analyzing JSON schema...")
    schema_fields = get_all_schema_fields(EXTRACTION_SCHEMA)
    logger.info(f"Found {len(schema_fields)} fields in JSON schema")
    
    # Initialize LLM enricher
    logger.info("\n[STEP 2] Initializing LLM enricher...")
    try:
        llm_client = OllamaLLMClient(
            api_key=OLLAMA_API_KEY,
            host=OLLAMA_HOST,
            model=OLLAMA_MODEL,
        )
        enricher = IncidentEnricher(llm_client=llm_client)
        logger.info(f"✓ LLM client initialized with model: {OLLAMA_MODEL}")
    except Exception as e:
        logger.error(f"✗ Failed to initialize LLM client: {e}")
        return
    
    # Get test incident and article
    logger.info("\n[STEP 3] Preparing test incident and article...")
    incident = get_test_incident()
    article_contents = get_test_article()
    logger.info(f"✓ Test incident: {incident.incident_id}")
    logger.info(f"✓ Test article: {len(COMPREHENSIVE_TEST_ARTICLE)} characters")
    
    # Enrich the incident
    logger.info("\n[STEP 4] Running LLM enrichment...")
    try:
        enrichment_result, raw_json_data = enricher.enrich_incident_json_schema(
            incident=incident,
            article_contents=article_contents,
        )
        
        # Fallback to comprehensive method if JSON schema fails
        if not enrichment_result:
            logger.warning("JSON schema method failed, trying comprehensive method...")
            enrichment_result = enricher.enrich_incident_comprehensive(
                incident=incident,
                article_contents=article_contents,
            )
            raw_json_data = None
        
        if not enrichment_result:
            logger.error("✗ Enrichment failed - no result returned from either method")
            return
        
        logger.info("✓ Enrichment completed successfully")
    except Exception as e:
        logger.error(f"✗ Enrichment error: {e}", exc_info=True)
        return
    
    # Save to database for verification
    logger.info("\n[STEP 5] Saving enrichment to database...")
    conn = get_connection()
    init_db(conn)
    try:
        saved = save_enrichment_result(
            conn,
            incident.incident_id,
            enrichment_result,
            raw_json_data=raw_json_data,
        )
        if saved:
            logger.info("✓ Enrichment saved to database")
        else:
            logger.warning("⚠ Failed to save enrichment")
    except Exception as e:
        logger.error(f"✗ Error saving to database: {e}")
    
    # Get flattened data
    logger.info("\n[STEP 6] Retrieving flattened enrichment data...")
    enrichment_flat = get_enrichment_flat(conn, incident.incident_id)
    if not enrichment_flat:
        logger.error("✗ Could not retrieve flattened data")
        return
    
    # Verify field coverage
    logger.info("\n[STEP 7] Verifying field coverage...")
    coverage = verify_field_coverage(enrichment_flat, schema_fields)
    
    logger.info("\n" + "=" * 80)
    logger.info("COVERAGE TEST RESULTS")
    logger.info("=" * 80)
    logger.info(f"\nSchema Fields: {coverage['total_schema_fields']}")
    logger.info(f"Extracted Fields: {coverage['extracted_count']}")
    logger.info(f"Missing Fields: {coverage['missing_count']}")
    logger.info(f"Null Fields: {coverage['null_count']}")
    logger.info(f"Coverage: {coverage['coverage_percent']:.1f}%")
    
    # Show key extracted fields
    logger.info("\n✓ Key Extracted Fields:")
    key_fields = [
        'is_education_related', 'institution_name', 'institution_type',
        'country', 'region', 'city', 'incident_date', 'discovery_date',
        'attack_category', 'attack_vector', 'was_ransom_demanded',
        'ransom_amount', 'ransom_paid', 'data_exfiltrated', 'data_encrypted',
        'records_affected_exact', 'systems_affected_codes', 'students_affected',
        'faculty_affected', 'staff_affected', 'teaching_disrupted',
        'research_disrupted', 'recovery_timeframe_days', 'enriched_summary'
    ]
    
    for field in key_fields:
        value = enrichment_flat.get(field)
        if value is not None:
            display_value = str(value)[:80] if len(str(value)) > 80 else value
            logger.info(f"  {field}: {display_value}")
    
    # Show missing important fields
    if coverage['missing_count'] > 0:
        logger.info(f"\n⚠ Missing Fields ({coverage['missing_count']}):")
        for field in sorted(list(coverage['missing']))[:20]:  # Show first 20
            logger.info(f"  - {field}")
        if coverage['missing_count'] > 20:
            logger.info(f"  ... and {coverage['missing_count'] - 20} more")
    
    # Save results to file
    results_file = Path(__file__).parent / "coverage_test_results.json"
    with open(results_file, 'w') as f:
        json.dump({
            'coverage': {
                'total_schema_fields': coverage['total_schema_fields'],
                'extracted_count': coverage['extracted_count'],
                'missing_count': coverage['missing_count'],
                'null_count': coverage['null_count'],
                'coverage_percent': coverage['coverage_percent'],
            },
            'extracted_fields': sorted(list(coverage['extracted'])),
            'missing_fields': sorted(list(coverage['missing'])),
            'null_fields': sorted(list(coverage['null'])),
            'enrichment_sample': {
                k: v for k, v in enrichment_flat.items()
                if k in key_fields and v is not None
            }
        }, f, indent=2)
    
    logger.info(f"\n✓ Results saved to: {results_file}")
    logger.info("\n" + "=" * 80)
    logger.info("COVERAGE TEST COMPLETE")
    logger.info("=" * 80)
    
    conn.close()


if __name__ == "__main__":
    main()

