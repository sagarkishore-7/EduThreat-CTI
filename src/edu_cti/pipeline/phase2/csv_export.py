"""
CSV export for Phase 2 enriched dataset.

Exports enriched incidents to CSV with all schema fields as columns.
"""

import csv
import json
import sqlite3
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any

from src.edu_cti.core.db import get_connection
from src.edu_cti.pipeline.phase2.storage.db import get_enrichment_result
from src.edu_cti.pipeline.phase2.schemas import CTIEnrichmentResult, EducationRelevanceCheck
from src.edu_cti.pipeline.phase2.utils.deduplication import normalize_institution_name

logger = logging.getLogger(__name__)


def flatten_enrichment_data(enrichment: CTIEnrichmentResult) -> Dict[str, Any]:
    """
    Flatten enrichment result into a flat dictionary for CSV export.
    
    Args:
        enrichment: CTIEnrichmentResult to flatten
        
    Returns:
        Dictionary with all fields flattened (nested objects become prefixed fields)
    """
    flat = {}
    
    # Education relevance
    if enrichment.education_relevance:
        er = enrichment.education_relevance
        flat['is_education_related'] = er.is_education_related
        flat['education_reasoning'] = er.reasoning
        # institution_identified is used for victim_raw_name in CSV, not exported separately
    
    # Primary URL
    flat['primary_url'] = enrichment.primary_url
    
    # Initial access description
    flat['initial_access_description'] = enrichment.initial_access_description
    
    # Timeline (as JSON string)
    if enrichment.timeline:
        flat['timeline'] = json.dumps([event.model_dump() for event in enrichment.timeline])
        flat['timeline_events_count'] = len(enrichment.timeline)
    else:
        flat['timeline'] = None
        flat['timeline_events_count'] = 0
    
    # MITRE ATT&CK techniques (as JSON string)
    if enrichment.mitre_attack_techniques:
        flat['mitre_attack_techniques'] = json.dumps([tech.model_dump() for tech in enrichment.mitre_attack_techniques])
        flat['mitre_techniques_count'] = len(enrichment.mitre_attack_techniques)
    else:
        flat['mitre_attack_techniques'] = None
        flat['mitre_techniques_count'] = 0
    
    # Attack dynamics
    if enrichment.attack_dynamics:
        ad = enrichment.attack_dynamics
        flat['attack_vector'] = ad.attack_vector
        flat['attack_chain'] = json.dumps(ad.attack_chain) if ad.attack_chain else None
        flat['ransomware_family'] = ad.ransomware_family
        flat['data_exfiltration'] = ad.data_exfiltration
        flat['encryption_impact'] = ad.encryption_impact
        flat['impact_scope'] = json.dumps(ad.impact_scope) if ad.impact_scope else None
        flat['ransom_demanded'] = ad.ransom_demanded
        flat['ransom_amount'] = ad.ransom_amount
        flat['ransom_paid'] = ad.ransom_paid
        flat['recovery_timeframe_days'] = ad.recovery_timeframe_days
        flat['business_impact'] = ad.business_impact
        flat['operational_impact'] = json.dumps(ad.operational_impact) if ad.operational_impact else None
    else:
        flat['attack_vector'] = None
        flat['attack_chain'] = None
        flat['ransomware_family'] = None
        flat['data_exfiltration'] = None
        flat['encryption_impact'] = None
        flat['impact_scope'] = None
        flat['ransom_demanded'] = None
        flat['ransom_amount'] = None
        flat['ransom_paid'] = None
        flat['recovery_timeframe_days'] = None
        flat['business_impact'] = None
        flat['operational_impact'] = None
    
    # Data impact metrics
    if enrichment.data_impact:
        di = enrichment.data_impact
        flat['data_personal_information'] = di.get("personal_information")
        flat['data_student_data'] = di.get("student_data")
        flat['data_faculty_data'] = di.get("faculty_data")
        flat['data_alumni_data'] = di.get("alumni_data")
        flat['data_financial_data'] = di.get("financial_data")
        flat['data_research_data'] = di.get("research_data")
        flat['data_intellectual_property'] = di.get("intellectual_property")
        flat['data_medical_records'] = di.get("medical_records")
        flat['data_administrative_data'] = di.get("administrative_data")
        flat['data_records_affected_min'] = di.get("records_affected_min")
        flat['data_records_affected_max'] = di.get("records_affected_max")
        flat['data_records_affected_exact'] = di.get("records_affected_exact")
        flat['data_types_affected'] = json.dumps(di.get("data_types_affected")) if di.get("data_types_affected") else None
        flat['data_encrypted'] = di.get("data_encrypted")
        flat['data_exfiltrated'] = di.get("data_exfiltrated")
    else:
        for field in ['data_personal_information', 'data_student_data', 'data_faculty_data', 'data_alumni_data',
                      'data_financial_data', 'data_research_data', 'data_intellectual_property', 'data_medical_records',
                      'data_administrative_data', 'data_records_affected_min', 'data_records_affected_max',
                      'data_records_affected_exact', 'data_types_affected', 'data_encrypted', 'data_exfiltrated']:
            flat[field] = None
    
    # System impact metrics
    if enrichment.system_impact:
        si = enrichment.system_impact
        flat['system_systems_affected'] = json.dumps(si.get("systems_affected")) if si.get("systems_affected") else None
        flat['system_critical_systems_affected'] = si.get("critical_systems_affected")
        flat['system_network_compromised'] = si.get("network_compromised")
        flat['system_email_system_affected'] = si.get("email_system_affected")
        flat['system_student_portal_affected'] = si.get("student_portal_affected")
        flat['system_research_systems_affected'] = si.get("research_systems_affected")
        flat['system_hospital_systems_affected'] = si.get("hospital_systems_affected")
        flat['system_cloud_services_affected'] = si.get("cloud_services_affected")
        flat['system_third_party_vendor_impact'] = si.get("third_party_vendor_impact")
        flat['system_vendor_name'] = si.get("vendor_name")
    else:
        for field in ['system_systems_affected', 'system_critical_systems_affected', 'system_network_compromised',
                      'system_email_system_affected', 'system_student_portal_affected', 'system_research_systems_affected',
                      'system_hospital_systems_affected', 'system_cloud_services_affected', 'system_third_party_vendor_impact',
                      'system_vendor_name']:
            flat[field] = None
    
    # User impact metrics
    if enrichment.user_impact:
        ui = enrichment.user_impact
        flat['user_students_affected'] = ui.get("students_affected")
        flat['user_faculty_affected'] = ui.get("faculty_affected")
        flat['user_staff_affected'] = ui.get("staff_affected")
        flat['user_alumni_affected'] = ui.get("alumni_affected")
        flat['user_parents_affected'] = ui.get("parents_affected")
        flat['user_applicants_affected'] = ui.get("applicants_affected")
        flat['user_patients_affected'] = ui.get("patients_affected")
        flat['user_users_affected_min'] = ui.get("users_affected_min")
        flat['user_users_affected_max'] = ui.get("users_affected_max")
        flat['user_users_affected_exact'] = ui.get("users_affected_exact")
    else:
        for field in ['user_students_affected', 'user_faculty_affected', 'user_staff_affected', 'user_alumni_affected',
                      'user_parents_affected', 'user_applicants_affected', 'user_patients_affected',
                      'user_users_affected_min', 'user_users_affected_max', 'user_users_affected_exact']:
            flat[field] = None
    
    # Operational impact metrics
    if enrichment.operational_impact_metrics:
        oim = enrichment.operational_impact_metrics
        flat['operational_teaching_disrupted'] = oim.get("teaching_disrupted")
        flat['operational_research_disrupted'] = oim.get("research_disrupted")
        flat['operational_admissions_disrupted'] = oim.get("admissions_disrupted")
        flat['operational_payroll_disrupted'] = oim.get("payroll_disrupted")
        flat['operational_enrollment_disrupted'] = oim.get("enrollment_disrupted")
        flat['operational_clinical_operations_disrupted'] = oim.get("clinical_operations_disrupted")
        flat['operational_online_learning_disrupted'] = oim.get("online_learning_disrupted")
        flat['operational_downtime_days'] = oim.get("downtime_days")
        flat['operational_partial_service_days'] = oim.get("partial_service_days")
        flat['operational_classes_cancelled'] = oim.get("classes_cancelled")
        flat['operational_exams_postponed'] = oim.get("exams_postponed")
        flat['operational_graduation_delayed'] = oim.get("graduation_delayed")
    else:
        for field in ['operational_teaching_disrupted', 'operational_research_disrupted', 'operational_admissions_disrupted',
                      'operational_payroll_disrupted', 'operational_enrollment_disrupted', 'operational_clinical_operations_disrupted',
                      'operational_online_learning_disrupted', 'operational_downtime_days', 'operational_partial_service_days',
                      'operational_classes_cancelled', 'operational_exams_postponed', 'operational_graduation_delayed']:
            flat[field] = None
    
    # Financial impact metrics
    if enrichment.financial_impact:
        fi = enrichment.financial_impact
        flat['financial_ransom_demanded'] = fi.get("ransom_demanded")
        flat['financial_ransom_amount_min'] = fi.get("ransom_amount_min")
        flat['financial_ransom_amount_max'] = fi.get("ransom_amount_max")
        flat['financial_ransom_amount_exact'] = fi.get("ransom_amount_exact")
        flat['financial_ransom_currency'] = fi.get("ransom_currency")
        flat['financial_ransom_paid'] = fi.get("ransom_paid")
        flat['financial_ransom_paid_amount'] = fi.get("ransom_paid_amount")
        flat['financial_recovery_costs_min'] = fi.get("recovery_costs_min")
        flat['financial_recovery_costs_max'] = fi.get("recovery_costs_max")
        flat['financial_legal_costs'] = fi.get("legal_costs")
        flat['financial_notification_costs'] = fi.get("notification_costs")
        flat['financial_credit_monitoring_costs'] = fi.get("credit_monitoring_costs")
        flat['financial_insurance_claim'] = fi.get("insurance_claim")
        flat['financial_insurance_claim_amount'] = fi.get("insurance_claim_amount")
    else:
        for field in ['financial_ransom_demanded', 'financial_ransom_amount_min', 'financial_ransom_amount_max',
                      'financial_ransom_amount_exact', 'financial_ransom_currency', 'financial_ransom_paid',
                      'financial_ransom_paid_amount', 'financial_recovery_costs_min', 'financial_recovery_costs_max',
                      'financial_legal_costs', 'financial_notification_costs', 'financial_credit_monitoring_costs',
                      'financial_insurance_claim', 'financial_insurance_claim_amount']:
            flat[field] = None
    
    # Regulatory impact metrics
    if enrichment.regulatory_impact:
        ri = enrichment.regulatory_impact
        flat['regulatory_breach_notification_required'] = ri.get("breach_notification_required")
        flat['regulatory_notifications_sent'] = ri.get("notifications_sent")
        flat['regulatory_notifications_sent_date'] = ri.get("notifications_sent_date")
        flat['regulatory_regulators_notified'] = json.dumps(ri.get("regulators_notified")) if ri.get("regulators_notified") else None
        flat['regulatory_regulators_notified_date'] = ri.get("regulators_notified_date")
        flat['regulatory_gdpr_breach'] = ri.get("gdpr_breach")
        flat['regulatory_dpa_notified'] = ri.get("dpa_notified")
        flat['regulatory_hipaa_breach'] = ri.get("hipaa_breach")
        flat['regulatory_ferc_breach'] = ri.get("ferc_breach")
        flat['regulatory_investigation_opened'] = ri.get("investigation_opened")
        flat['regulatory_fine_imposed'] = ri.get("fine_imposed")
        flat['regulatory_fine_amount'] = ri.get("fine_amount")
        flat['regulatory_lawsuits_filed'] = ri.get("lawsuits_filed")
        flat['regulatory_lawsuit_count'] = ri.get("lawsuit_count")
        flat['regulatory_class_action'] = ri.get("class_action")
    else:
        for field in ['regulatory_breach_notification_required', 'regulatory_notifications_sent', 'regulatory_notifications_sent_date',
                      'regulatory_regulators_notified', 'regulatory_regulators_notified_date', 'regulatory_gdpr_breach',
                      'regulatory_dpa_notified', 'regulatory_hipaa_breach', 'regulatory_ferc_breach', 'regulatory_investigation_opened',
                      'regulatory_fine_imposed', 'regulatory_fine_amount', 'regulatory_lawsuits_filed', 'regulatory_lawsuit_count',
                      'regulatory_class_action']:
            flat[field] = None
    
    # Recovery metrics
    if enrichment.recovery_metrics:
        rm = enrichment.recovery_metrics
        flat['recovery_recovery_started_date'] = rm.get("recovery_started_date")
        flat['recovery_recovery_completed_date'] = rm.get("recovery_completed_date")
        flat['recovery_recovery_timeframe_days'] = rm.get("recovery_timeframe_days")
        flat['recovery_recovery_phases'] = json.dumps(rm.get("recovery_phases")) if rm.get("recovery_phases") else None
        flat['recovery_from_backup'] = rm.get("from_backup")
        flat['recovery_backup_age_days'] = rm.get("backup_age_days")
        flat['recovery_clean_rebuild'] = rm.get("clean_rebuild")
        flat['recovery_incident_response_firm'] = rm.get("incident_response_firm")
        flat['recovery_forensics_firm'] = rm.get("forensics_firm")
        flat['recovery_law_firm'] = rm.get("law_firm")
        flat['recovery_security_improvements'] = json.dumps(rm.get("security_improvements")) if rm.get("security_improvements") else None
        flat['recovery_mfa_implemented'] = rm.get("mfa_implemented")
        flat['recovery_security_training_conducted'] = rm.get("security_training_conducted")
        flat['recovery_response_measures'] = json.dumps(rm.get("response_measures")) if rm.get("response_measures") else None
    else:
        for field in ['recovery_recovery_started_date', 'recovery_recovery_completed_date', 'recovery_recovery_timeframe_days',
                      'recovery_recovery_phases', 'recovery_from_backup', 'recovery_backup_age_days', 'recovery_clean_rebuild',
                      'recovery_incident_response_firm', 'recovery_forensics_firm', 'recovery_law_firm', 'recovery_security_improvements',
                      'recovery_mfa_implemented', 'recovery_security_training_conducted', 'recovery_response_measures']:
            flat[field] = None
    
    # Transparency metrics
    if enrichment.transparency_metrics:
        tm = enrichment.transparency_metrics
        flat['transparency_disclosure_timeline'] = json.dumps(tm.get("disclosure_timeline")) if tm.get("disclosure_timeline") else None
        flat['transparency_public_disclosure'] = tm.get("public_disclosure")
        flat['transparency_public_disclosure_date'] = tm.get("public_disclosure_date")
        flat['transparency_disclosure_delay_days'] = tm.get("disclosure_delay_days")
        flat['transparency_transparency_level'] = tm.get("transparency_level")
        flat['transparency_official_statement_url'] = tm.get("official_statement_url")
        flat['transparency_detailed_report_url'] = tm.get("detailed_report_url")
        flat['transparency_updates_provided'] = tm.get("updates_provided")
        flat['transparency_update_count'] = tm.get("update_count")
    else:
        for field in ['transparency_disclosure_timeline', 'transparency_public_disclosure', 'transparency_public_disclosure_date',
                      'transparency_disclosure_delay_days', 'transparency_transparency_level', 'transparency_official_statement_url',
                      'transparency_detailed_report_url', 'transparency_updates_provided', 'transparency_update_count']:
            flat[field] = None
    
    # Research impact metrics
    if enrichment.research_impact:
        rim = enrichment.research_impact
        flat['research_research_projects_affected'] = rim.get("research_projects_affected")
        flat['research_research_data_compromised'] = rim.get("research_data_compromised")
        flat['research_sensitive_research_impact'] = rim.get("sensitive_research_impact")
        flat['research_publications_delayed'] = rim.get("publications_delayed")
        flat['research_grants_affected'] = rim.get("grants_affected")
        flat['research_collaborations_affected'] = rim.get("collaborations_affected")
        flat['research_research_area'] = rim.get("research_area")
    else:
        for field in ['research_research_projects_affected', 'research_research_data_compromised', 'research_sensitive_research_impact',
                      'research_publications_delayed', 'research_grants_affected', 'research_collaborations_affected', 'research_research_area']:
            flat[field] = None
    
    # Summary and notes
    flat['enriched_summary'] = enrichment.enriched_summary
    flat['extraction_notes'] = enrichment.extraction_notes
    
    return flat


def load_enriched_incidents_from_db(conn: sqlite3.Connection, use_flat_table: bool = True) -> List[Dict]:
    """
    Load all enriched incidents from database with enrichment data.
    
    Uses the flattened table for faster queries if available, falls back to JSON parsing.
    
    Args:
        conn: Database connection
        use_flat_table: If True, use incident_enrichments_flat for faster queries
        
    Returns:
        List of incident dictionaries with enrichment data
    """
    # Check if flat table exists
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='incident_enrichments_flat'"
    )
    flat_table_exists = cur.fetchone() is not None
    
    if use_flat_table and flat_table_exists:
        # Use flattened table for much faster queries
        query = """
            SELECT 
                i.*,
                ief.*,
                GROUP_CONCAT(DISTINCT isrc.source) as sources
            FROM incidents i
            INNER JOIN incident_enrichments_flat ief ON i.incident_id = ief.incident_id
            LEFT JOIN incident_sources isrc ON i.incident_id = isrc.incident_id
            WHERE i.llm_enriched = 1
            GROUP BY i.incident_id
            ORDER BY i.llm_enriched_at DESC
        """
    else:
        # Fallback to JSON parsing
        query = """
        SELECT 
            i.*,
            ie.enrichment_data,
            GROUP_CONCAT(DISTINCT isrc.source) as sources
        FROM incidents i
        INNER JOIN incident_enrichments ie ON i.incident_id = ie.incident_id
        LEFT JOIN incident_sources isrc ON i.incident_id = isrc.incident_id
        WHERE i.llm_enriched = 1
        GROUP BY i.incident_id
        ORDER BY i.llm_enriched_at DESC
    """
    
    cur = conn.execute(query)
    rows = cur.fetchall()
    
    # Helper function to safely get values from sqlite3.Row
    def safe_get(row, key, default=None):
        """Safely get value from sqlite3.Row, returning default if key doesn't exist."""
        try:
            value = row[key]
            return default if value is None else value
        except (KeyError, IndexError):
            return default
    
    incidents = []
    for row in rows:
        # Parse all_urls
        all_urls_str = safe_get(row, "all_urls", "") or ""
        all_urls = [url.strip() for url in all_urls_str.split(";") if url.strip()]
        
        # Get sources (comma-separated from GROUP_CONCAT)
        sources_str = safe_get(row, "sources", "") or ""
        sources = [s.strip() for s in sources_str.split(",") if s.strip()] if sources_str else []
        primary_source = sources[0] if sources else "unknown"
        
        # Check if we're using flattened table (has direct columns) or JSON
        using_flat_table = 'institution_name' in row.keys() and 'attack_vector' in row.keys()
        
        if using_flat_table:
            # Use flattened table data directly - much faster!
            # Convert flat table row to flattened dict format
            flattened = {}
            institution_from_enrichment = None
            
            # Map flat table column names to CSV column names
            # The flat table uses shorter names, CSV uses prefixed names for clarity
            flat_to_csv_mapping = {
                # Attack dynamics (no prefix needed for attack_vector, ransomware_family)
                'attack_vector': 'attack_vector',
                'ransomware_family': 'ransomware_family',
                'was_ransom_demanded': 'ransom_demanded',
                'ransom_paid': 'ransom_paid',
                'data_exfiltrated': 'data_exfiltration',
                'recovery_timeframe_days': 'recovery_timeframe_days',
                'business_impact': 'business_impact',
                
                # User impact - map to user_ prefix
                'students_affected': 'user_students_affected',
                'staff_affected': 'user_staff_affected',
                'faculty_affected': 'user_faculty_affected',
                'users_affected_exact': 'user_users_affected_exact',
                'users_affected_min': 'user_users_affected_min',
                'users_affected_max': 'user_users_affected_max',
                
                # Operational impact - map to operational_ prefix
                'teaching_disrupted': 'operational_teaching_disrupted',
                'research_disrupted': 'operational_research_disrupted',
                'admissions_disrupted': 'operational_admissions_disrupted',
                'enrollment_disrupted': 'operational_enrollment_disrupted',
                'payroll_disrupted': 'operational_payroll_disrupted',
                'classes_cancelled': 'operational_classes_cancelled',
                'exams_postponed': 'operational_exams_postponed',
                'downtime_days': 'operational_downtime_days',
                
                # System impact - map to system_ prefix
                'systems_affected_codes': 'system_systems_affected',
                'critical_systems_affected': 'system_critical_systems_affected',
                'network_compromised': 'system_network_compromised',
                'email_system_affected': 'system_email_system_affected',
                'student_portal_affected': 'system_student_portal_affected',
                'research_systems_affected': 'system_research_systems_affected',
                'hospital_systems_affected': 'system_hospital_systems_affected',
                'cloud_services_affected': 'system_cloud_services_affected',
                'third_party_vendor_impact': 'system_third_party_vendor_impact',
                'vendor_name': 'system_vendor_name',
                
                # Data impact - map to data_ prefix
                'data_breached': 'data_encrypted',  # Not exact match but closest
                'records_affected_exact': 'data_records_affected_exact',
                'records_affected_min': 'data_records_affected_min',
                'records_affected_max': 'data_records_affected_max',
                
                # Financial impact - map to financial_ prefix
                'ransom_amount': 'financial_ransom_amount_exact',
                'ransom_currency': 'financial_ransom_currency',
                'ransom_paid_amount': 'financial_ransom_paid_amount',
                'recovery_costs_min': 'financial_recovery_costs_min',
                'recovery_costs_max': 'financial_recovery_costs_max',
                'legal_costs': 'financial_legal_costs',
                'notification_costs': 'financial_notification_costs',
                'insurance_claim': 'financial_insurance_claim',
                'insurance_claim_amount': 'financial_insurance_claim_amount',
                
                # Regulatory impact - map to regulatory_ prefix
                'gdpr_breach': 'regulatory_gdpr_breach',
                'hipaa_breach': 'regulatory_hipaa_breach',
                'ferpa_breach': 'regulatory_ferc_breach',  # Note: DB uses ferpa, CSV uses ferc
                'breach_notification_required': 'regulatory_breach_notification_required',
                'notifications_sent': 'regulatory_notifications_sent',
                'fine_imposed': 'regulatory_fine_imposed',
                'fine_amount': 'regulatory_fine_amount',
                'lawsuits_filed': 'regulatory_lawsuits_filed',
                'class_action': 'regulatory_class_action',
                
                # Recovery metrics - map to recovery_ prefix
                'recovery_timeframe_days': 'recovery_recovery_timeframe_days',
                'recovery_started_date': 'recovery_recovery_started_date',
                'recovery_completed_date': 'recovery_recovery_completed_date',
                'from_backup': 'recovery_from_backup',
                'mfa_implemented': 'recovery_mfa_implemented',
                'incident_response_firm': 'recovery_incident_response_firm',
                'forensics_firm': 'recovery_forensics_firm',
                
                # Transparency metrics - map to transparency_ prefix
                'public_disclosure': 'transparency_public_disclosure',
                'public_disclosure_date': 'transparency_public_disclosure_date',
                'disclosure_delay_days': 'transparency_disclosure_delay_days',
                'transparency_level': 'transparency_transparency_level',
                
                # Timeline and MITRE - map JSON columns
                'timeline_json': 'timeline',
                'mitre_techniques_json': 'mitre_attack_techniques',
            }
            
            # Map flat table columns to flattened dict keys
            for key in row.keys():
                if key.startswith('i.') or key in ['incident_id', 'sources']:
                    continue  # Skip incident table columns
                value = safe_get(row, key)
                # Convert integer booleans back to proper booleans for flattening function
                if isinstance(value, int) and key in [
                    'is_education_related', 'was_ransom_demanded', 'ransom_paid',
                    'data_breached', 'data_exfiltrated', 'critical_systems_affected',
                    'network_compromised', 'email_system_affected', 'student_portal_affected',
                    'research_systems_affected', 'hospital_systems_affected',
                    'cloud_services_affected', 'third_party_vendor_impact',
                    'teaching_impacted', 'teaching_disrupted', 'research_impacted',
                    'research_disrupted', 'admissions_disrupted', 'enrollment_disrupted',
                    'payroll_disrupted', 'classes_cancelled', 'exams_postponed',
                    'faculty_affected', 'insurance_claim', 'gdpr_breach', 'hipaa_breach',
                    'ferpa_breach', 'breach_notification_required', 'notifications_sent',
                    'fine_imposed', 'lawsuits_filed', 'class_action', 'from_backup',
                    'mfa_implemented', 'public_disclosure'
                ]:
                    value = bool(value) if value is not None else None
                
                # Store with original key
                flattened[key] = value
                
                # Also store with mapped CSV key if different
                if key in flat_to_csv_mapping:
                    csv_key = flat_to_csv_mapping[key]
                    flattened[csv_key] = value
            
            institution_from_enrichment = safe_get(row, "institution_name")
        else:
            # Fallback: Parse JSON (slower but works with old data)
            enrichment_data = None
        enrichment_data_str = safe_get(row, "enrichment_data")
        if enrichment_data_str:
            try:
                enrichment_data = json.loads(enrichment_data_str)
            except json.JSONDecodeError as e:
                logger.warning(f"Error parsing enrichment data for {safe_get(row, 'incident_id')}: {e}")
        
            # Extract and flatten enrichment data
        institution_from_enrichment = None
        if enrichment_data:
            try:
                enrichment = CTIEnrichmentResult.model_validate(enrichment_data)
                # Get institution_identified from enrichment
                if enrichment.education_relevance and enrichment.education_relevance.institution_identified:
                    institution_from_enrichment = enrichment.education_relevance.institution_identified
                flattened = flatten_enrichment_data(enrichment)
            except Exception as e:
                incident_id = safe_get(row, "incident_id", "unknown")
                logger.warning(f"Error extracting enrichment details for {incident_id}: {e}")
                flattened = flatten_enrichment_data(CTIEnrichmentResult(
                    education_relevance=EducationRelevanceCheck(
                        is_education_related=False,
                        reasoning="",
                        institution_identified=None
                    ),
                    enriched_summary=""
                ))
        else:
            flattened = flatten_enrichment_data(CTIEnrichmentResult(
                education_relevance=EducationRelevanceCheck(
                    is_education_related=False,
                    reasoning="",
                    institution_identified=None
                ),
                enriched_summary=""
            ))
        
        # Use institution_identified from enrichment for victim_raw_name if available
        victim_raw_name = institution_from_enrichment or safe_get(row, "victim_raw_name")
        victim_raw_name_normalized = None
        if victim_raw_name:
            victim_raw_name_normalized = normalize_institution_name(victim_raw_name)
        
        # Base incident dict with Phase 1 fields
        incident_dict = {
            # Core Phase 1 fields
            "incident_id": safe_get(row, "incident_id", ""),
            "source": primary_source,
            "sources": ";".join(sources) if sources else "",
            "university_name": victim_raw_name or safe_get(row, "university_name") or None,
            "victim_raw_name": victim_raw_name,
            "victim_raw_name_normalized": victim_raw_name_normalized,
            "institution_type": safe_get(row, "institution_type"),
            "country": safe_get(row, "country"),
            "region": safe_get(row, "region"),
            "city": safe_get(row, "city"),
            "incident_date": safe_get(row, "incident_date"),
            "date_precision": safe_get(row, "date_precision", "unknown"),
            "source_published_date": safe_get(row, "source_published_date"),
            "ingested_at": safe_get(row, "ingested_at"),
            "title": safe_get(row, "title"),
            "subtitle": safe_get(row, "subtitle"),
            "primary_url": safe_get(row, "primary_url"),
            "all_urls": ";".join(all_urls) if all_urls else "",
            "attack_type_hint": safe_get(row, "attack_type_hint"),
            "status": safe_get(row, "status", "suspected"),
            "source_confidence": safe_get(row, "source_confidence", "medium"),
            "notes": safe_get(row, "notes"),
            "llm_enriched_at": safe_get(row, "llm_enriched_at"),
        }
        
        # Add flattened enrichment data (but remove fields we handle separately)
        keys_to_remove = ['institution_identified', 'victim_raw_name', 'university_name', 'victim_raw_name_normalized']
        for key in keys_to_remove:
            if key in flattened:
                del flattened[key]
        incident_dict.update(flattened)
        
        # Re-set victim fields AFTER update to ensure enrichment data takes precedence
        if victim_raw_name:
            incident_dict['victim_raw_name'] = victim_raw_name
            incident_dict['university_name'] = victim_raw_name
            incident_dict['victim_raw_name_normalized'] = victim_raw_name_normalized
        
        incidents.append(incident_dict)
    
    return incidents


def write_enriched_csv(output_path: Path, incidents: List[Dict]) -> None:
    """
    Write enriched incidents to CSV file with all schema fields as columns.
    
    Args:
        output_path: Path to output CSV file
        incidents: List of incident dictionaries with enrichment data
    """
    if not incidents:
        logger.warning("No enriched incidents to write")
        return
    
    # Define CSV columns - Phase 1 fields first, then all enrichment fields
    fieldnames = [
        # Phase 1 core fields
        "incident_id",
        "source",
        "sources",
        "university_name",
        "victim_raw_name",
        "victim_raw_name_normalized",
        "institution_type",
        "country",
        "region",
        "city",
        "incident_date",
        "date_precision",
        "source_published_date",
        "ingested_at",
        "title",
        "subtitle",
        "primary_url",
        "all_urls",
        "attack_type_hint",
        "status",
        "source_confidence",
        "notes",
        "llm_enriched_at",
        # Education relevance
        "is_education_related",
        "education_reasoning",
        # Initial access
        "initial_access_description",
        # Timeline
        "timeline",
        "timeline_events_count",
        # MITRE ATT&CK
        "mitre_attack_techniques",
        "mitre_techniques_count",
        # Attack dynamics
        "attack_vector",
        "attack_chain",
        "ransomware_family",
        "data_exfiltration",
        "encryption_impact",
        "impact_scope",
        "ransom_demanded",
        "ransom_amount",
        "ransom_paid",
        "recovery_timeframe_days",
        "business_impact",
        "operational_impact",
        # Data impact
        "data_personal_information",
        "data_student_data",
        "data_faculty_data",
        "data_alumni_data",
        "data_financial_data",
        "data_research_data",
        "data_intellectual_property",
        "data_medical_records",
        "data_administrative_data",
        "data_records_affected_min",
        "data_records_affected_max",
        "data_records_affected_exact",
        "data_types_affected",
        "data_encrypted",
        "data_exfiltrated",
        # System impact
        "system_systems_affected",
        "system_critical_systems_affected",
        "system_network_compromised",
        "system_email_system_affected",
        "system_student_portal_affected",
        "system_research_systems_affected",
        "system_hospital_systems_affected",
        "system_cloud_services_affected",
        "system_third_party_vendor_impact",
        "system_vendor_name",
        # User impact
        "user_students_affected",
        "user_faculty_affected",
        "user_staff_affected",
        "user_alumni_affected",
        "user_parents_affected",
        "user_applicants_affected",
        "user_patients_affected",
        "user_users_affected_min",
        "user_users_affected_max",
        "user_users_affected_exact",
        # Operational impact
        "operational_teaching_disrupted",
        "operational_research_disrupted",
        "operational_admissions_disrupted",
        "operational_payroll_disrupted",
        "operational_enrollment_disrupted",
        "operational_clinical_operations_disrupted",
        "operational_online_learning_disrupted",
        "operational_downtime_days",
        "operational_partial_service_days",
        "operational_classes_cancelled",
        "operational_exams_postponed",
        "operational_graduation_delayed",
        # Financial impact
        "financial_ransom_demanded",
        "financial_ransom_amount_min",
        "financial_ransom_amount_max",
        "financial_ransom_amount_exact",
        "financial_ransom_currency",
        "financial_ransom_paid",
        "financial_ransom_paid_amount",
        "financial_recovery_costs_min",
        "financial_recovery_costs_max",
        "financial_legal_costs",
        "financial_notification_costs",
        "financial_credit_monitoring_costs",
        "financial_insurance_claim",
        "financial_insurance_claim_amount",
        # Regulatory impact
        "regulatory_breach_notification_required",
        "regulatory_notifications_sent",
        "regulatory_notifications_sent_date",
        "regulatory_regulators_notified",
        "regulatory_regulators_notified_date",
        "regulatory_gdpr_breach",
        "regulatory_dpa_notified",
        "regulatory_hipaa_breach",
        "regulatory_ferc_breach",
        "regulatory_investigation_opened",
        "regulatory_fine_imposed",
        "regulatory_fine_amount",
        "regulatory_lawsuits_filed",
        "regulatory_lawsuit_count",
        "regulatory_class_action",
        # Recovery metrics
        "recovery_recovery_started_date",
        "recovery_recovery_completed_date",
        "recovery_recovery_timeframe_days",
        "recovery_recovery_phases",
        "recovery_from_backup",
        "recovery_backup_age_days",
        "recovery_clean_rebuild",
        "recovery_incident_response_firm",
        "recovery_forensics_firm",
        "recovery_law_firm",
        "recovery_security_improvements",
        "recovery_mfa_implemented",
        "recovery_security_training_conducted",
        "recovery_response_measures",
        # Transparency metrics
        "transparency_disclosure_timeline",
        "transparency_public_disclosure",
        "transparency_public_disclosure_date",
        "transparency_disclosure_delay_days",
        "transparency_transparency_level",
        "transparency_official_statement_url",
        "transparency_detailed_report_url",
        "transparency_updates_provided",
        "transparency_update_count",
        # Research impact
        "research_research_projects_affected",
        "research_research_data_compromised",
        "research_sensitive_research_impact",
        "research_publications_delayed",
        "research_grants_affected",
        "research_collaborations_affected",
        "research_research_area",
        # Summary and notes
        "enriched_summary",
        "extraction_notes",
    ]
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        
        for incident in incidents:
            # Convert None to empty string for CSV, but keep actual None values for JSON fields
            row = {}
            for field in fieldnames:
                value = incident.get(field)
                if value is None:
                    row[field] = ""  # Empty string for None values in CSV
                elif isinstance(value, (dict, list)):
                    row[field] = json.dumps(value) if value else ""
                else:
                    row[field] = value
            writer.writerow(row)
    
    logger.info(f"Wrote {len(incidents)} enriched incidents to {output_path}")


def export_enriched_dataset(
    db_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
) -> Optional[Path]:
    """
    Export enriched incidents to CSV.
    
    Args:
        db_path: Path to database (default: from config)
        output_path: Path to output CSV (default: data/processed/enriched_dataset.csv)
        
    Returns:
        Path to output CSV file, or None if no enriched incidents found
    """
    from pathlib import Path
    from src.edu_cti.core.config import DB_PATH
    from src.edu_cti.pipeline.phase1.base_io import PROC_DIR
    
    if db_path is None:
        db_path = DB_PATH
    elif isinstance(db_path, str):
        db_path = Path(db_path)
    
    if output_path is None:
        output_path = PROC_DIR / "enriched_dataset.csv"
    elif isinstance(output_path, str):
        output_path = Path(output_path)
    
    conn = get_connection(db_path)
    
    try:
        # Check if incident_enrichments table exists
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='incident_enrichments'"
        )
        table_exists = cur.fetchone() is not None
        
        if not table_exists:
            logger.warning("No enriched incidents found - incident_enrichments table does not exist")
            return None
        
        # Load enriched incidents
        logger.info("Loading enriched incidents from database...")
        incidents = load_enriched_incidents_from_db(conn)
        
        logger.info(f"Found {len(incidents)} enriched incidents")
        
        # Write to CSV
        if incidents:
            write_enriched_csv(output_path, incidents)
            logger.info(f"Enriched dataset exported to: {output_path}")
            return output_path
        else:
            logger.warning("No enriched incidents found to export")
            return None
        
    except Exception as e:
        logger.error(f"Error exporting enriched dataset: {e}", exc_info=True)
        return None
    finally:
        conn.close()
