"""
Cyber Threat Intelligence (CTI) Report Generator.

Generates professional CTI reports following industry-standard frameworks:
- MITRE ATT&CK mapping
- NIST Cybersecurity Framework alignment
- STIX 2.1 compatible structure
- ISO/IEC 27001 incident reporting format

Reports are designed for security researchers, practitioners, and analysts.
"""

import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path


def generate_cti_report(incident: Dict[str, Any]) -> str:
    """
    Generate a comprehensive CTI report for an incident.
    
    Follows industry-standard frameworks for security incident reporting:
    - Executive Summary
    - Incident Overview
    - MITRE ATT&CK Mapping
    - Threat Actor Analysis
    - Impact Assessment
    - Timeline
    - IOCs (Indicators of Compromise)
    - Recommendations
    
    Args:
        incident: Full incident data dictionary from database
    
    Returns:
        Formatted report as string (markdown format)
    """
    report_lines = []
    
    # Header
    report_lines.append("# CYBER THREAT INTELLIGENCE REPORT")
    report_lines.append("")
    report_lines.append(f"**Incident ID:** {incident.get('incident_id', 'Unknown')}")
    report_lines.append(f"**Report Generated:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    report_lines.append(f"**Classification:** UNCLASSIFIED")
    report_lines.append("")
    report_lines.append("---")
    report_lines.append("")
    
    # Executive Summary
    report_lines.append("## EXECUTIVE SUMMARY")
    report_lines.append("")
    institution_name = incident.get('university_name') or incident.get('victim_raw_name') or 'Unknown Institution'
    country = incident.get('country', 'Unknown')
    country_code = incident.get('country_code', '')
    incident_date = incident.get('incident_date', 'Unknown')
    
    report_lines.append(f"This report details a cyber security incident affecting **{institution_name}** "
                       f"located in **{country}** ({country_code if country_code else 'N/A'}). "
                       f"The incident was discovered on **{incident_date}**.")
    
    if incident.get('enriched_summary'):
        report_lines.append("")
        report_lines.append(incident.get('enriched_summary'))
    
    report_lines.append("")
    report_lines.append("### Key Findings")
    report_lines.append("")
    
    # Key findings
    findings = []
    if incident.get('attack_category'):
        findings.append(f"- **Attack Type:** {incident.get('attack_category', '').replace('_', ' ').title()}")
    if incident.get('ransomware_family'):
        findings.append(f"- **Ransomware Family:** {incident.get('ransomware_family', '').replace('_', ' ').title()}")
    if incident.get('threat_actor_name'):
        findings.append(f"- **Threat Actor:** {incident.get('threat_actor_name')}")
    if incident.get('data_breached') or incident.get('data_exfiltrated'):
        findings.append("- **Data Impact:** Confirmed data breach/exfiltration")
    if incident.get('was_ransom_demanded'):
        findings.append("- **Ransom:** Ransom demand confirmed")
    
    if findings:
        report_lines.extend(findings)
    else:
        report_lines.append("- Incident details pending analysis")
    
    report_lines.append("")
    report_lines.append("---")
    report_lines.append("")
    
    # Incident Overview
    report_lines.append("## INCIDENT OVERVIEW")
    report_lines.append("")
    report_lines.append("### Victim Information")
    report_lines.append("")
    report_lines.append(f"- **Institution:** {institution_name}")
    if incident.get('institution_type'):
        report_lines.append(f"- **Institution Type:** {incident.get('institution_type')}")
    report_lines.append(f"- **Location:** {country} ({country_code if country_code else 'N/A'})")
    if incident.get('region'):
        report_lines.append(f"- **Region:** {incident.get('region')}")
    if incident.get('city'):
        report_lines.append(f"- **City:** {incident.get('city')}")
    report_lines.append(f"- **Incident Date:** {incident_date}")
    if incident.get('date_precision'):
        report_lines.append(f"- **Date Precision:** {incident.get('date_precision')}")
    if incident.get('discovery_date'):
        report_lines.append(f"- **Discovery Date:** {incident.get('discovery_date')}")
    report_lines.append("")
    
    # Attack Details
    report_lines.append("### Attack Details")
    report_lines.append("")
    if incident.get('attack_category'):
        report_lines.append(f"- **Attack Category:** {incident.get('attack_category', '').replace('_', ' ').title()}")
    if incident.get('attack_vector'):
        report_lines.append(f"- **Attack Vector:** {incident.get('attack_vector', '').replace('_', ' ').title()}")
    if incident.get('initial_access_vector'):
        report_lines.append(f"- **Initial Access:** {incident.get('initial_access_vector', '').replace('_', ' ').title()}")
    if incident.get('initial_access_description'):
        report_lines.append(f"- **Initial Access Description:** {incident.get('initial_access_description')}")
    if incident.get('ransomware_family'):
        report_lines.append(f"- **Ransomware Family:** {incident.get('ransomware_family', '').replace('_', ' ').title()}")
    if incident.get('status'):
        report_lines.append(f"- **Status:** {incident.get('status', '').title()}")
    report_lines.append("")
    
    # Threat Actor
    if incident.get('threat_actor_name'):
        report_lines.append("### Threat Actor")
        report_lines.append("")
        report_lines.append(f"- **Name:** {incident.get('threat_actor_name')}")
        if incident.get('threat_actor_category'):
            report_lines.append(f"- **Category:** {incident.get('threat_actor_category', '').replace('_', ' ').title()}")
        if incident.get('threat_actor_motivation'):
            report_lines.append(f"- **Motivation:** {incident.get('threat_actor_motivation', '').replace('_', ' ').title()}")
        if incident.get('threat_actor_claim_url'):
            report_lines.append(f"- **Claim URL:** {incident.get('threat_actor_claim_url')}")
        report_lines.append("")
    
    # MITRE ATT&CK Mapping
    if incident.get('mitre_attack_techniques'):
        report_lines.append("## MITRE ATT&CK MAPPING")
        report_lines.append("")
        report_lines.append("This incident has been mapped to the MITRE ATT&CK framework:")
        report_lines.append("")
        
        techniques = incident.get('mitre_attack_techniques', [])
        if isinstance(techniques, str):
            try:
                techniques = json.loads(techniques)
            except:
                techniques = []
        
        if techniques:
            for tech in techniques:
                if isinstance(tech, dict):
                    tech_id = tech.get('technique_id', 'N/A')
                    tech_name = tech.get('technique_name', 'N/A')
                    tactic = tech.get('tactic', 'N/A')
                    description = tech.get('description', '')
                    
                    report_lines.append(f"### {tech_id}: {tech_name}")
                    report_lines.append("")
                    report_lines.append(f"- **Tactic:** {tactic}")
                    if description:
                        report_lines.append(f"- **Description:** {description}")
                    report_lines.append("")
        else:
            report_lines.append("*No MITRE ATT&CK techniques mapped for this incident.*")
        report_lines.append("")
    
    # Impact Assessment
    report_lines.append("## IMPACT ASSESSMENT")
    report_lines.append("")
    
    # Data Impact
    if incident.get('data_breached') or incident.get('data_exfiltrated'):
        report_lines.append("### Data Impact")
        report_lines.append("")
        if incident.get('data_breached'):
            report_lines.append("- **Data Breach:** Confirmed")
        if incident.get('data_exfiltrated'):
            report_lines.append("- **Data Exfiltration:** Confirmed")
        if incident.get('records_affected_exact'):
            report_lines.append(f"- **Records Affected:** {incident.get('records_affected_exact'):,}")
        elif incident.get('records_affected_min') or incident.get('records_affected_max'):
            min_records = incident.get('records_affected_min', 0)
            max_records = incident.get('records_affected_max', 0)
            report_lines.append(f"- **Records Affected:** {min_records:,} - {max_records:,}")
        if incident.get('pii_records_leaked'):
            report_lines.append(f"- **PII Records Leaked:** {incident.get('pii_records_leaked'):,}")
        report_lines.append("")
    
    # System Impact
    systems_affected = incident.get('systems_affected')
    if systems_affected:
        if isinstance(systems_affected, str):
            try:
                systems_affected = json.loads(systems_affected)
            except:
                systems_affected = []
        
        if systems_affected:
            report_lines.append("### System Impact")
            report_lines.append("")
            report_lines.append("**Systems Affected:**")
            for system in systems_affected:
                report_lines.append(f"- {system.replace('_', ' ').title()}")
            report_lines.append("")
    
    # Operational Impact
    operational_impacts = []
    if incident.get('teaching_impacted'):
        operational_impacts.append("Teaching operations")
    if incident.get('research_impacted'):
        operational_impacts.append("Research activities")
    if incident.get('classes_cancelled'):
        operational_impacts.append("Class cancellations")
    if incident.get('exams_postponed'):
        operational_impacts.append("Exam postponements")
    if incident.get('downtime_days'):
        operational_impacts.append(f"System downtime ({incident.get('downtime_days')} days)")
    
    if operational_impacts:
        report_lines.append("### Operational Impact")
        report_lines.append("")
        for impact in operational_impacts:
            report_lines.append(f"- {impact}")
        report_lines.append("")
    
    # Financial Impact
    financial_items = []
    if incident.get('recovery_costs_min') or incident.get('recovery_costs_max'):
        min_cost = incident.get('recovery_costs_min', 0)
        max_cost = incident.get('recovery_costs_max', 0)
        if min_cost == max_cost:
            financial_items.append(f"- **Recovery Costs:** ${min_cost:,.2f} USD")
        else:
            financial_items.append(f"- **Recovery Costs:** ${min_cost:,.2f} - ${max_cost:,.2f} USD")
    if incident.get('ransom_amount'):
        financial_items.append(f"- **Ransom Demanded:** ${incident.get('ransom_amount'):,.2f} {incident.get('ransom_currency', 'USD')}")
    if incident.get('ransom_paid'):
        financial_items.append(f"- **Ransom Paid:** Yes (${incident.get('ransom_paid_amount', 0):,.2f})")
    if incident.get('fine_amount'):
        financial_items.append(f"- **Regulatory Fine:** ${incident.get('fine_amount'):,.2f} USD")
    
    if financial_items:
        report_lines.append("### Financial Impact")
        report_lines.append("")
        report_lines.extend(financial_items)
        report_lines.append("")
    
    # Timeline
    timeline = incident.get('timeline')
    if timeline:
        if isinstance(timeline, str):
            try:
                timeline = json.loads(timeline)
            except:
                timeline = []
        
        if timeline:
            report_lines.append("## INCIDENT TIMELINE")
            report_lines.append("")
            for event in timeline:
                if isinstance(event, dict):
                    event_date = event.get('date', 'Unknown')
                    event_type = event.get('event_type', '').replace('_', ' ').title() if event.get('event_type') else 'Event'
                    event_desc = event.get('event_description', '')
                    
                    report_lines.append(f"### {event_date} - {event_type}")
                    if event_desc:
                        report_lines.append("")
                        report_lines.append(event_desc)
                    report_lines.append("")
    
    # Indicators of Compromise (IOCs)
    iocs = []
    if incident.get('threat_actor_claim_url'):
        iocs.append(f"- **Threat Actor Claim URL:** {incident.get('threat_actor_claim_url')}")
    if incident.get('leak_site_url'):
        iocs.append(f"- **Leak Site URL:** {incident.get('leak_site_url')}")
    
    # Extract IOCs from timeline
    if timeline:
        for event in timeline:
            if isinstance(event, dict) and event.get('indicators'):
                for indicator in event.get('indicators', []):
                    iocs.append(f"- **IOC:** {indicator}")
    
    if iocs:
        report_lines.append("## INDICATORS OF COMPROMISE (IOCs)")
        report_lines.append("")
        report_lines.extend(iocs)
        report_lines.append("")
    
    # Recovery & Response
    recovery_items = []
    if incident.get('recovery_timeframe_days'):
        recovery_items.append(f"- **Recovery Timeframe:** {incident.get('recovery_timeframe_days')} days")
    if incident.get('recovery_started_date'):
        recovery_items.append(f"- **Recovery Started:** {incident.get('recovery_started_date')}")
    if incident.get('recovery_completed_date'):
        recovery_items.append(f"- **Recovery Completed:** {incident.get('recovery_completed_date')}")
    if incident.get('incident_response_firm'):
        recovery_items.append(f"- **Incident Response Firm:** {incident.get('incident_response_firm')}")
    if incident.get('forensics_firm'):
        recovery_items.append(f"- **Forensics Firm:** {incident.get('forensics_firm')}")
    if incident.get('from_backup'):
        recovery_items.append("- **Recovery Method:** From backup")
    
    if recovery_items:
        report_lines.append("## RECOVERY & RESPONSE")
        report_lines.append("")
        report_lines.extend(recovery_items)
        report_lines.append("")
    
    # Security Improvements
    improvements = []
    if incident.get('mfa_implemented'):
        improvements.append("- Multi-factor authentication (MFA) implemented")
    # Add more improvements as needed
    
    if improvements:
        report_lines.append("## SECURITY IMPROVEMENTS")
        report_lines.append("")
        report_lines.extend(improvements)
        report_lines.append("")
    
    # Regulatory & Compliance
    regulatory_items = []
    if incident.get('gdpr_breach'):
        regulatory_items.append("- **GDPR:** Breach notification required")
    if incident.get('hipaa_breach'):
        regulatory_items.append("- **HIPAA:** Breach notification required")
    if incident.get('ferpa_breach'):
        regulatory_items.append("- **FERPA:** Breach notification required")
    if incident.get('breach_notification_required'):
        regulatory_items.append("- **Breach Notification:** Required")
    if incident.get('notifications_sent'):
        regulatory_items.append(f"- **Notifications Sent:** {incident.get('notifications_sent')}")
    if incident.get('lawsuits_filed'):
        regulatory_items.append("- **Lawsuits:** Filed")
    if incident.get('class_action'):
        regulatory_items.append("- **Class Action:** Filed")
    
    if regulatory_items:
        report_lines.append("## REGULATORY & COMPLIANCE")
        report_lines.append("")
        report_lines.extend(regulatory_items)
        report_lines.append("")
    
    # Sources & References
    sources = incident.get('sources', [])
    if sources:
        report_lines.append("## SOURCES & REFERENCES")
        report_lines.append("")
        for source in sources:
            if isinstance(source, dict):
                source_name = source.get('source', 'Unknown')
                source_url = source.get('source_detail_url', '')
                report_lines.append(f"- **{source_name}**")
                if source_url:
                    report_lines.append(f"  - {source_url}")
            elif isinstance(source, str):
                report_lines.append(f"- {source}")
        report_lines.append("")
    
    # URLs
    if incident.get('primary_url') or incident.get('all_urls'):
        report_lines.append("## RELATED URLS")
        report_lines.append("")
        if incident.get('primary_url'):
            report_lines.append(f"- **Primary URL:** {incident.get('primary_url')}")
        if incident.get('all_urls'):
            all_urls = incident.get('all_urls', [])
            if isinstance(all_urls, str):
                all_urls = [u.strip() for u in all_urls.split(';') if u.strip()]
            for url in all_urls:
                report_lines.append(f"- {url}")
        report_lines.append("")
    
    # Footer
    report_lines.append("---")
    report_lines.append("")
    report_lines.append("## REPORT METADATA")
    report_lines.append("")
    report_lines.append(f"- **Report Format:** Cyber Threat Intelligence (CTI)")
    report_lines.append(f"- **Framework Alignment:** MITRE ATT&CK, NIST CSF")
    report_lines.append(f"- **Generated By:** EduThreat-CTI Platform")
    report_lines.append(f"- **Report Version:** 1.0")
    report_lines.append("")
    report_lines.append("---")
    report_lines.append("")
    report_lines.append("*This report is generated for security research, threat intelligence, and educational purposes.*")
    report_lines.append("*For questions or additional information, please refer to the incident sources listed above.*")
    
    return "\n".join(report_lines)
