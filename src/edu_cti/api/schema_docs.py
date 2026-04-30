"""
DB schema documentation for the /api/schema endpoint.

Single source of truth for all tables, columns, types, descriptions,
pipeline layer mapping, and which API endpoints expose each field.
"""

from typing import List, Optional
from pydantic import BaseModel


class ColumnDoc(BaseModel):
    name: str
    type: str
    nullable: bool = True
    description: str
    extraction_field: Optional[str] = None  # LLM extraction_schema.py field name
    populated_by: Optional[str] = None       # "llm" | "pipeline" | "ingestion" | "derived"
    example: Optional[str] = None
    api_endpoints: List[str] = []


class TableDoc(BaseModel):
    name: str
    description: str
    row_count_note: str
    columns: List[ColumnDoc]


class PipelineLayerDoc(BaseModel):
    layer: int
    name: str
    file: str
    description: str


class SchemaResponse(BaseModel):
    version: str
    description: str
    pipeline_layers: List[PipelineLayerDoc]
    tables: List[TableDoc]
    analytics_endpoints: List[dict]
    notes: List[str]


PIPELINE_LAYERS: List[PipelineLayerDoc] = [
    PipelineLayerDoc(
        layer=1,
        name="Extraction Schema (LLM prompt)",
        file="src/edu_cti/pipeline/phase2/extraction/extraction_schema.py",
        description="Defines every field the LLM is asked to extract from article text. ~120 fields as a JSON grammar.",
    ),
    PipelineLayerDoc(
        layer=2,
        name="Pydantic Schema (CTIEnrichmentResult)",
        file="src/edu_cti/pipeline/phase2/schemas.py",
        description="Validates and structures the LLM output. Nested impact groups stored as Dict[str,Any]; "
                    "new typed fields: vulnerabilities_exploited, malware_families, attacker_tools, etc.",
    ),
    PipelineLayerDoc(
        layer=3,
        name="Mapper (json_to_cti_enrichment)",
        file="src/edu_cti/pipeline/phase2/extraction/json_to_schema_mapper.py",
        description="Normalises raw LLM JSON to CTIEnrichmentResult. Handles enum normalisation, "
                    "list coercion, derived boolean fields, and fallback chains.",
    ),
    PipelineLayerDoc(
        layer=4,
        name="Flat Table (_flatten_enrichment_for_db + save_enrichment_result)",
        file="src/edu_cti/pipeline/phase2/storage/db.py",
        description="Writes all scalar values to incident_enrichments_flat for fast SQL analytics. "
                    "JSON arrays stored as serialised TEXT in the current canonical schema.",
    ),
]


# ── incident_enrichments_flat columns ────────────────────────────────────────

FLAT_COLUMNS: List[ColumnDoc] = [
    # Identity
    ColumnDoc(name="incident_id", type="TEXT PK", nullable=False,
              description="Unique incident identifier: {source}_{sha256_of_url[:16]}",
              populated_by="ingestion",
              api_endpoints=["/api/incidents", "/api/incidents/{id}"]),

    # Education flag
    ColumnDoc(name="is_education_related", type="INTEGER (0/1)",
              description="1 if LLM confirmed this is an education-sector cyber incident.",
              extraction_field="is_edu_cyber_incident", populated_by="llm",
              api_endpoints=["/api/analytics/*", "/api/incidents?enriched_only=true"]),

    # Institution
    ColumnDoc(name="institution_name", type="TEXT",
              description="Cleaned institution name (LLM-extracted, then scored against raw name).",
              extraction_field="institution_name", populated_by="llm",
              api_endpoints=["/api/incidents", "/api/analytics/institution-types"]),
    ColumnDoc(name="institution_type", type="TEXT",
              description="Category: university, k12_school, community_college, research_institute, "
                          "government_education, hospital_medical_school, other.",
              extraction_field="institution_type", populated_by="llm",
              api_endpoints=["/api/analytics/institution-types"]),
    ColumnDoc(name="institution_size", type="TEXT",
              description="Size band: small_under_5k / medium_5k_20k / large_20k_50k / very_large_over_50k.",
              extraction_field="institution_size", populated_by="llm"),
    ColumnDoc(name="incident_severity", type="TEXT",
              description="Severity: critical / high / medium / low.",
              extraction_field="incident_severity", populated_by="llm"),

    # Geography
    ColumnDoc(name="country", type="TEXT",
              description="Full country name (ISO-normalised, e.g. 'United States', 'United Kingdom').",
              extraction_field="country", populated_by="llm",
              api_endpoints=["/api/analytics/countries", "/api/incidents?country=..."]),
    ColumnDoc(name="country_code", type="TEXT",
              description="ISO 3166-1 alpha-2 code (e.g. 'US', 'GB'). Used by choropleth map.",
              populated_by="derived",
              api_endpoints=["/api/analytics/countries"]),
    ColumnDoc(name="region", type="TEXT",
              description="Sub-national region or state.", extraction_field="region", populated_by="llm"),
    ColumnDoc(name="city", type="TEXT",
              description="City where the institution is located.", extraction_field="city", populated_by="llm"),

    # Attack taxonomy
    ColumnDoc(name="attack_category", type="TEXT",
              description="Primary attack category: ransomware, data_breach_external, phishing, ddos, etc.",
              extraction_field="attack_category", populated_by="llm",
              api_endpoints=["/api/analytics/attack-types", "/api/analytics/attack-trends"]),
    ColumnDoc(name="attack_vector", type="TEXT",
              description="Initial access vector enum (50+ values): phishing_email, vulnerability_exploit, "
                          "stolen_credentials, exposed_rdp, etc.",
              extraction_field="attack_vector", populated_by="llm",
              api_endpoints=["/api/analytics/attack-vectors"]),
    ColumnDoc(name="access_vector", type="TEXT",
              description="Canonical initial access vector used for analytics. Mirrors the schema's attack_vector field.",
              extraction_field="attack_vector", populated_by="llm",
              api_endpoints=["/api/analytics/initial-access"]),
    ColumnDoc(name="initial_access_description", type="TEXT",
              description="1–3 sentence narrative of how the attacker gained initial access.",
              extraction_field="initial_access_description", populated_by="llm"),

    # Threat actor
    ColumnDoc(name="ransomware_family", type="TEXT",
              description="Ransomware family name (e.g. LockBit, BlackCat, Cl0p).",
              extraction_field="ransomware_family_or_group", populated_by="llm",
              api_endpoints=["/api/analytics/ransomware", "/api/analytics/ransomware-timeline"]),
    ColumnDoc(name="threat_actor_name", type="TEXT",
              description="Named threat actor or group (e.g. Vice Society, TA505).",
              extraction_field="threat_actor_name", populated_by="llm",
              api_endpoints=["/api/analytics/threat-actor-categories"]),
    ColumnDoc(name="threat_actor_category", type="TEXT",
              description="Actor category: ransomware_gang / apt_nation_state / hacktivist / cybercriminal / insider.",
              extraction_field="threat_actor_category", populated_by="llm",
              api_endpoints=["/api/analytics/threat-actor-categories"]),
    ColumnDoc(name="threat_actor_motivation", type="TEXT",
              description="Primary motivation: financial / espionage / hacktivism / disruption / unknown.",
              extraction_field="threat_actor_motivation", populated_by="llm",
              api_endpoints=["/api/analytics/threat-actor-motivations"]),
    ColumnDoc(name="threat_actor_origin_country", type="TEXT",
              description="Country of origin of the threat actor (when attributed).",
              extraction_field="threat_actor_origin_country", populated_by="llm"),
    ColumnDoc(name="threat_actor_claim_url", type="TEXT",
              description="URL of the threat actor's public leak site or claim post.",
              extraction_field="threat_actor_claim_url", populated_by="llm"),

    # Ransom
    ColumnDoc(name="was_ransom_demanded", type="INTEGER (0/1)",
              description="Whether a ransom was demanded.",
              extraction_field="was_ransom_demanded", populated_by="llm",
              api_endpoints=["/api/analytics/ransom-economics"]),
    ColumnDoc(name="ransom_amount", type="REAL",
              description="Ransom amount demanded in USD (or converted). Used in total_financial_impact.",
              extraction_field="ransom_amount_exact", populated_by="llm",
              api_endpoints=["/api/analytics/ransom-economics"]),
    ColumnDoc(name="ransom_currency", type="TEXT",
              description="Currency of ransom demand (USD, BTC, XMR, etc.).",
              extraction_field="ransom_currency", populated_by="llm"),
    ColumnDoc(name="ransom_paid", type="INTEGER (0/1)",
              description="Whether the ransom was paid.",
              extraction_field="ransom_paid", populated_by="llm"),
    ColumnDoc(name="ransom_paid_amount", type="REAL",
              description="Amount actually paid if different from demanded.",
              extraction_field="ransom_paid_amount", populated_by="llm"),

    # Data breach
    ColumnDoc(name="data_breached", type="INTEGER (0/1)",
              description="Whether data was exfiltrated or exposed (derived from attack_category + explicit field).",
              extraction_field="data_breached", populated_by="derived"),
    ColumnDoc(name="data_exfiltrated", type="INTEGER (0/1)",
              description="Whether confirmed exfiltration occurred.",
              extraction_field="data_exfiltrated", populated_by="llm"),
    ColumnDoc(name="records_affected_exact", type="INTEGER",
              description="Exact count of records compromised.",
              extraction_field="records_affected_exact", populated_by="llm"),
    ColumnDoc(name="records_affected_min", type="INTEGER",
              description="Lower bound of affected records range.", populated_by="llm"),
    ColumnDoc(name="records_affected_max", type="INTEGER",
              description="Upper bound of affected records range.", populated_by="llm"),
    ColumnDoc(name="pii_records_leaked", type="INTEGER",
              description="Number of PII records specifically leaked.",
              extraction_field="pii_records_leaked", populated_by="llm"),
    ColumnDoc(name="data_categories", type="TEXT (JSON array)",
              description="JSON array of data category codes compromised: student_pii, employee_ssn, "
                          "research_data, financial_data, health_data, etc.",
              extraction_field="data_categories", populated_by="llm"),

    # System impact
    ColumnDoc(name="systems_affected_codes", type="TEXT (JSON array)",
              description="JSON array of affected system codes: email_system, student_portal, "
                          "lms, erp_finance_hr, research_hpc, etc.",
              extraction_field="systems_affected", populated_by="llm",
              api_endpoints=["/api/analytics/system-impact"]),
    ColumnDoc(name="critical_systems_affected", type="INTEGER (0/1)", populated_by="derived",
              description="True if any systems_affected_codes is non-empty."),
    ColumnDoc(name="network_compromised", type="INTEGER (0/1)", populated_by="derived",
              description="True if core_network, wifi_network, or datacenter in systems_affected."),
    ColumnDoc(name="email_system_affected", type="INTEGER (0/1)", populated_by="derived",
              description="True if email_system in systems_affected."),
    ColumnDoc(name="student_portal_affected", type="INTEGER (0/1)", populated_by="derived",
              description="True if student_portal or SIS in systems_affected."),
    ColumnDoc(name="research_systems_affected", type="INTEGER (0/1)", populated_by="derived",
              description="True if research_hpc or research_storage in systems_affected."),
    ColumnDoc(name="hospital_systems_affected", type="INTEGER (0/1)", populated_by="derived",
              description="True if ehr_emr or medical_devices in systems_affected."),
    ColumnDoc(name="cloud_services_affected", type="INTEGER (0/1)", populated_by="derived",
              description="True if cloud_storage in systems_affected."),
    ColumnDoc(name="third_party_vendor_impact", type="INTEGER (0/1)", populated_by="derived",
              description="True if third_parties_involved is non-empty."),
    ColumnDoc(name="vendor_name", type="TEXT", populated_by="llm",
              description="Name(s) of third-party vendors involved.",
              extraction_field="third_parties_involved"),

    # Operational impact
    ColumnDoc(name="teaching_impacted", type="INTEGER (0/1)", populated_by="llm",
              description="Whether teaching activities were disrupted.", extraction_field="teaching_impacted"),
    ColumnDoc(name="teaching_disrupted", type="INTEGER (0/1)", populated_by="derived",
              description="Derived from teaching_impacted or operational_impacts containing classes_cancelled etc."),
    ColumnDoc(name="research_impacted", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="research_impacted",
              description="Whether research operations were disrupted."),
    ColumnDoc(name="research_disrupted", type="INTEGER (0/1)", populated_by="derived",
              description="Derived from research_impacted or operational_impacts containing research_halted."),
    ColumnDoc(name="admissions_disrupted", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="admissions_disrupted",
              description="Admissions or enrollment impacted."),
    ColumnDoc(name="enrollment_disrupted", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="enrollment_disrupted",
              description="Registration or enrollment portal disrupted."),
    ColumnDoc(name="payroll_disrupted", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="payroll_disrupted",
              description="Payroll processing disrupted."),
    ColumnDoc(name="classes_cancelled", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="classes_cancelled",
              description="Physical or online classes cancelled."),
    ColumnDoc(name="exams_postponed", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="exams_postponed",
              description="Exams delayed or cancelled."),
    ColumnDoc(name="downtime_days", type="REAL", populated_by="llm",
              extraction_field="downtime_days",
              description="System downtime in days. Used as recovery fallback when recovery_timeframe_days is null.",
              api_endpoints=["/api/analytics/recovery-by-attack-type"]),
    ColumnDoc(name="outage_duration_hours", type="REAL", populated_by="llm",
              extraction_field="outage_duration_hours",
              description="Outage duration in hours. Converted to days (÷24) as secondary recovery fallback."),

    # User impact
    ColumnDoc(name="students_affected", type="INTEGER", populated_by="llm",
              extraction_field="students_affected",
              description="Number of students affected."),
    ColumnDoc(name="staff_affected", type="INTEGER", populated_by="llm",
              extraction_field="staff_affected",
              description="Number of staff affected."),
    ColumnDoc(name="faculty_affected", type="INTEGER", populated_by="llm",
              extraction_field="faculty_affected",
              description="Number of faculty affected."),
    ColumnDoc(name="users_affected_exact", type="INTEGER", populated_by="llm",
              extraction_field="users_affected_exact",
              description="Total users affected (sum of students+staff+faculty if not explicit)."),
    ColumnDoc(name="users_affected_min", type="INTEGER", populated_by="llm",
              description="Lower bound of affected users."),
    ColumnDoc(name="users_affected_max", type="INTEGER", populated_by="llm",
              description="Upper bound of affected users."),

    # Financial
    ColumnDoc(name="recovery_costs_min", type="REAL", populated_by="llm",
              extraction_field="recovery_cost_usd",
              description="Minimum estimated recovery cost in USD."),
    ColumnDoc(name="recovery_costs_max", type="REAL", populated_by="llm",
              description="Maximum estimated recovery cost in USD."),
    ColumnDoc(name="legal_costs", type="REAL", populated_by="llm",
              extraction_field="legal_cost_usd",
              description="Legal/counsel fees in USD."),
    ColumnDoc(name="notification_costs", type="REAL", populated_by="llm",
              extraction_field="notification_cost_usd",
              description="Breach notification costs in USD."),
    ColumnDoc(name="insurance_claim", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="insurance_claim",
              description="Whether a cyber insurance claim was filed."),
    ColumnDoc(name="insurance_claim_amount", type="REAL", populated_by="llm",
              extraction_field="insurance_payout_usd",
              description="Insurance payout amount in USD."),
    ColumnDoc(name="business_impact", type="TEXT", populated_by="llm",
              extraction_field="business_impact",
              description="Business impact severity: critical / severe / moderate / limited / minimal."),

    # Regulatory
    ColumnDoc(name="gdpr_breach", type="INTEGER (0/1)", populated_by="derived",
              description="GDPR reportable breach (from gdpr_breach field or 'GDPR' in applicable_regulations)."),
    ColumnDoc(name="hipaa_breach", type="INTEGER (0/1)", populated_by="derived",
              description="HIPAA reportable breach."),
    ColumnDoc(name="ferpa_breach", type="INTEGER (0/1)", populated_by="derived",
              description="FERPA violation (student records breach in the US)."),
    ColumnDoc(name="breach_notification_required", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="breach_notification_required",
              description="Whether legal notification to affected parties was required."),
    ColumnDoc(name="notifications_sent", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="notification_sent",
              description="Whether notifications were actually sent."),
    ColumnDoc(name="fine_imposed", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="fine_imposed",
              description="Whether a regulatory fine was imposed."),
    ColumnDoc(name="fine_amount", type="REAL", populated_by="llm",
              extraction_field="fine_amount_usd",
              description="Fine amount in USD."),
    ColumnDoc(name="lawsuits_filed", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="lawsuits_filed",
              description="Whether lawsuits were filed."),
    ColumnDoc(name="class_action", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="class_action_filed",
              description="Whether a class action lawsuit was initiated."),

    # Recovery
    ColumnDoc(name="recovery_timeframe_days", type="REAL", populated_by="llm",
              extraction_field="recovery_duration_days",
              description="Days from incident to full service restoration. "
                          "Dashboard falls back to downtime_days → outage_duration_hours/24 when null.",
              api_endpoints=["/api/analytics/recovery-by-attack-type", "/api/stats"]),
    ColumnDoc(name="recovery_started_date", type="TEXT", populated_by="llm",
              extraction_field="recovery_started_date",
              description="ISO date when recovery operations started."),
    ColumnDoc(name="recovery_completed_date", type="TEXT", populated_by="llm",
              extraction_field="recovery_completed_date",
              description="ISO date when full service was restored."),
    ColumnDoc(name="from_backup", type="INTEGER (0/1)", populated_by="derived",
              extraction_field="recovery_method",
              description="True if recovery_method is backup_restore or partial_backup_partial_rebuild."),
    ColumnDoc(name="mfa_implemented", type="INTEGER (0/1)", populated_by="derived",
              description="True if mfa_implemented in security_improvements post-incident."),
    ColumnDoc(name="incident_response_firm", type="TEXT", populated_by="llm",
              extraction_field="ir_firm_engaged",
              description="External IR firm hired (e.g. CrowdStrike, Mandiant)."),
    ColumnDoc(name="forensics_firm", type="TEXT", populated_by="llm",
              extraction_field="forensics_firm_engaged",
              description="Digital forensics firm engaged."),

    # Transparency
    ColumnDoc(name="public_disclosure", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="public_disclosure",
              description="Whether the institution publicly disclosed the incident."),
    ColumnDoc(name="public_disclosure_date", type="TEXT", populated_by="llm",
              description="ISO date of public disclosure."),
    ColumnDoc(name="disclosure_delay_days", type="REAL", populated_by="llm",
              extraction_field="disclosure_delay_days",
              description="Days between incident discovery and public disclosure."),
    ColumnDoc(name="transparency_level", type="TEXT", populated_by="llm",
              extraction_field="transparency_level",
              description="Transparency tier: full / partial / minimal / none."),

    # Timeline & MITRE (stored as JSON)
    ColumnDoc(name="timeline_json", type="TEXT (JSON array of TimelineEvent)",
              description="Full incident timeline serialised as JSON. "
                          "Each event: {date, date_precision, event_description, event_type, actor_attribution}. "
                          "Flattened to 16 columns in the research CSV export.",
              populated_by="llm",
              api_endpoints=["/api/analytics/recovery-metrics", "/admin/export/research-csv"]),
    ColumnDoc(name="timeline_events_count", type="INTEGER",
              description="Count of timeline events extracted.", populated_by="derived"),
    ColumnDoc(name="mitre_techniques_json", type="TEXT (JSON array of MITREAttackTechnique)",
              description="MITRE ATT&CK techniques as JSON. "
                          "Each: {technique_id, technique_name, tactic, description, sub_techniques}. "
                          "Flattened to 4 columns (ids, names, tactics, sub_techniques) in research CSV.",
              populated_by="llm",
              api_endpoints=["/api/analytics/mitre-tactics"]),
    ColumnDoc(name="mitre_techniques_count", type="INTEGER",
              description="Count of MITRE techniques identified.", populated_by="derived"),

    # NEW threat intelligence columns (added 2026-04 — auto-migrated on deploy)
    ColumnDoc(name="malware_families", type="TEXT (JSON array)", populated_by="llm",
              extraction_field="malware_families",
              description="[NEW] Malware families or strains used (e.g. ['LockBit 3.0', 'Cobalt Strike'])."),
    ColumnDoc(name="attacker_tools", type="TEXT (JSON array)", populated_by="llm",
              extraction_field="attacker_tools",
              description="[NEW] Attacker tools and frameworks (e.g. ['Mimikatz', 'PsExec'])."),
    ColumnDoc(name="threat_actor_aliases", type="TEXT (JSON array)", populated_by="llm",
              extraction_field="threat_actor_aliases",
              description="[NEW] Known aliases of the threat actor."),
    ColumnDoc(name="attack_campaign_name", type="TEXT", populated_by="llm",
              extraction_field="attack_campaign_name",
              description="[NEW] Named attack campaign or operation."),
    ColumnDoc(name="cloud_provider", type="TEXT", populated_by="llm",
              extraction_field="cloud_provider",
              description="[NEW] Cloud provider targeted (AWS, Azure, GCP)."),
    ColumnDoc(name="infrastructure_type", type="TEXT", populated_by="llm",
              extraction_field="infrastructure_type",
              description="[NEW] Infrastructure type: on_prem / cloud / hybrid."),
    ColumnDoc(name="dwell_time_days", type="REAL", populated_by="llm",
              extraction_field="dwell_time_days",
              description="[NEW] Days between initial access and detection."),
    ColumnDoc(name="mttd_hours", type="REAL", populated_by="llm",
              extraction_field="mttd_hours",
              description="[NEW] Mean Time To Detect in hours."),
    ColumnDoc(name="mttr_hours", type="REAL", populated_by="llm",
              extraction_field="mttr_hours",
              description="[NEW] Mean Time To Recover in hours."),

    # NEW vulnerability columns
    ColumnDoc(name="cve_ids", type="TEXT (JSON array)", populated_by="llm",
              extraction_field="vulnerabilities_exploited[].cve_id",
              description="[NEW] CVE identifiers exploited (e.g. ['CVE-2021-44228'])."),
    ColumnDoc(name="cvss_scores", type="TEXT (JSON array of REAL)", populated_by="llm",
              extraction_field="vulnerabilities_exploited[].cvss_score",
              description="[NEW] CVSS scores corresponding to cve_ids."),
    ColumnDoc(name="vulnerability_names", type="TEXT (JSON array)", populated_by="llm",
              extraction_field="vulnerabilities_exploited[].vulnerability_name",
              description="[NEW] Human-readable vulnerability names (e.g. ['Log4Shell'])."),
    ColumnDoc(name="affected_products", type="TEXT (JSON array)", populated_by="llm",
              extraction_field="vulnerabilities_exploited[].affected_product",
              description="[NEW] Products/software containing the exploited vulnerability."),

    # NEW financial
    ColumnDoc(name="total_cost_estimate", type="REAL", populated_by="llm",
              extraction_field="currency_normalized_cost_usd",
              description="[NEW] Total estimated incident cost in USD (all-in: ransom + recovery + legal)."),

    # NEW operational
    ColumnDoc(name="partial_service_days", type="REAL", populated_by="llm",
              extraction_field="partial_service_days",
              description="[NEW] Days operating at reduced capacity."),
    ColumnDoc(name="clinical_operations_disrupted", type="INTEGER (0/1)", populated_by="llm",
              description="[NEW] Clinical/patient-care operations disrupted (teaching hospitals)."),
    ColumnDoc(name="graduation_delayed", type="INTEGER (0/1)", populated_by="llm",
              description="[NEW] Graduation ceremonies or ceremonies delayed."),
    ColumnDoc(name="online_learning_disrupted", type="INTEGER (0/1)", populated_by="llm",
              description="[NEW] Online/remote learning disrupted."),

    # NEW recovery
    ColumnDoc(name="backup_status", type="TEXT", populated_by="llm",
              extraction_field="backup_status",
              description="[NEW] Backup availability: available_and_used / available_not_used / unavailable / unknown."),
    ColumnDoc(name="backup_age_days", type="REAL", populated_by="llm",
              extraction_field="backup_age_days",
              description="[NEW] Age of the most recent backup used in recovery (days)."),
    ColumnDoc(name="law_enforcement_involved", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="law_enforcement_involved",
              description="[NEW] Whether law enforcement (FBI, Europol, etc.) was notified/involved."),
    ColumnDoc(name="law_enforcement_agency", type="TEXT", populated_by="llm",
              extraction_field="law_enforcement_agency",
              description="[NEW] Name of law enforcement agency involved."),
    # NEW transparency
    ColumnDoc(name="official_statement_url", type="TEXT", populated_by="llm",
              extraction_field="official_statement_url",
              description="[NEW] URL of official institutional statement about the incident."),

    # NEW research impact
    ColumnDoc(name="research_projects_affected", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="research_projects_affected",
              description="[NEW] Whether research projects were specifically impacted."),
    ColumnDoc(name="research_data_compromised", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="research_data_compromised",
              description="[NEW] Whether research data (datasets, IP, unpublished results) was compromised."),
    ColumnDoc(name="publications_delayed", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="publications_delayed",
              description="[NEW] Whether academic publications were delayed."),
    ColumnDoc(name="grants_affected", type="INTEGER (0/1)", populated_by="llm",
              extraction_field="grants_affected",
              description="[NEW] Whether research grants or funded projects were affected."),
    ColumnDoc(name="research_area", type="TEXT", populated_by="llm",
              extraction_field="research_area",
              description="[NEW] Research domain affected (e.g. genomics, AI, defence)."),

    # NEW regulatory
    ColumnDoc(name="regulatory_context", type="TEXT (JSON array)", populated_by="llm",
              extraction_field="applicable_regulations",
              description="[NEW] Applicable regulations as JSON array (e.g. ['FERPA', 'HIPAA', 'GDPR'])."),

    # NEW data volume
    ColumnDoc(name="data_volume_gb", type="REAL", populated_by="llm",
              extraction_field="data_volume_gb",
              description="[NEW] Volume of data stolen or encrypted in gigabytes."),

    # Enrichment metadata
    ColumnDoc(name="enriched_summary", type="TEXT", populated_by="llm",
              description="LLM-generated narrative summary of the incident.",
              api_endpoints=["/api/incidents", "/api/incidents/{id}"]),
    ColumnDoc(name="extraction_notes", type="TEXT", populated_by="llm",
              description="Notes from the extraction process; includes threat_actor_claim_url when found."),
    ColumnDoc(name="confidence", type="REAL", populated_by="llm",
              description="LLM extraction confidence score (0–1)."),
    ColumnDoc(name="created_at", type="TEXT", populated_by="pipeline",
              description="ISO timestamp when the enrichment record was first created."),
    ColumnDoc(name="updated_at", type="TEXT", populated_by="pipeline",
              description="ISO timestamp of the most recent enrichment update."),
    ColumnDoc(name="enriched_at", type="TEXT", populated_by="pipeline",
              description="ISO timestamp when LLM enrichment completed."),
    ColumnDoc(name="skip_reason", type="TEXT", populated_by="pipeline",
              description="Reason the incident was skipped during enrichment (too short, irrelevant, etc.)."),
]


ANALYTICS_ENDPOINTS: List[dict] = [
    {"path": "/api/dashboard", "tag": "Dashboard",
     "fields_used": ["is_education_related", "attack_category", "data_breached", "country",
                     "threat_actor_name", "ransomware_family", "mitre_techniques_count",
                     "recovery_timeframe_days", "downtime_days", "outage_duration_hours",
                     "ransom_amount", "recovery_costs_max", "recovery_costs_min", "legal_costs"],
     "description": "Primary dashboard: stats + country map + attack types + ransomware + timeline."},
    {"path": "/api/stats", "tag": "Dashboard",
     "fields_used": ["same as /api/dashboard"],
     "description": "Dashboard stats only (no chart data)."},
    {"path": "/api/analytics/countries", "tag": "Analytics",
     "fields_used": ["country", "country_code", "is_education_related"],
     "description": "Incident counts by country for choropleth map."},
    {"path": "/api/analytics/attack-types", "tag": "Analytics",
     "fields_used": ["attack_category"],
     "description": "Distribution of attack categories."},
    {"path": "/api/analytics/attack-vectors", "tag": "Analytics",
     "fields_used": ["attack_vector", "access_vector"],
     "description": "Initial access vector breakdown."},
    {"path": "/api/analytics/attack-trends", "tag": "Analytics",
     "fields_used": ["attack_category", "incidents.incident_date"],
     "description": "Monthly attack category trend over time."},
    {"path": "/api/analytics/ransomware", "tag": "Analytics",
     "fields_used": ["ransomware_family", "attack_category"],
     "description": "Top ransomware families by incident count."},
    {"path": "/api/analytics/ransomware-timeline", "tag": "Analytics",
     "fields_used": ["ransomware_family", "incidents.incident_date"],
     "description": "Ransomware family timeline."},
    {"path": "/api/analytics/ransom-economics", "tag": "Analytics",
     "fields_used": ["ransom_amount", "ransom_paid_amount", "was_ransom_demanded", "ransom_paid"],
     "description": "Ransom demand/payment economics."},
    {"path": "/api/analytics/mitre-tactics", "tag": "Analytics",
     "fields_used": ["mitre_techniques_json", "mitre_techniques_count"],
     "description": "MITRE ATT&CK technique and tactic frequency."},
    {"path": "/api/analytics/system-impact", "tag": "Analytics",
     "fields_used": ["systems_affected_codes"],
     "description": "Systems affected frequency (email, portal, LMS, etc.)."},
    {"path": "/api/analytics/financial-impact", "tag": "Analytics",
     "fields_used": ["ransom_amount", "recovery_costs_min", "recovery_costs_max",
                     "legal_costs", "notification_costs", "incidents.incident_date"],
     "description": "Financial impact aggregated by year."},
    {"path": "/api/analytics/data-impact", "tag": "Analytics",
     "fields_used": ["records_affected_exact", "records_affected_min", "pii_records_leaked",
                     "data_categories"],
     "description": "Data breach scale statistics."},
    {"path": "/api/analytics/regulatory-impact", "tag": "Analytics",
     "fields_used": ["gdpr_breach", "hipaa_breach", "ferpa_breach", "fine_imposed",
                     "fine_amount", "lawsuits_filed", "class_action"],
     "description": "Regulatory and legal consequence breakdown."},
    {"path": "/api/analytics/recovery-metrics", "tag": "Analytics",
     "fields_used": ["recovery_timeframe_days", "downtime_days", "outage_duration_hours",
                     "from_backup", "incident_response_firm", "forensics_firm", "mfa_implemented"],
     "description": "Recovery effectiveness stats."},
    {"path": "/api/analytics/recovery-by-attack-type", "tag": "Analytics",
     "fields_used": ["attack_category", "recovery_timeframe_days", "downtime_days",
                     "outage_duration_hours"],
     "description": "Average recovery days by attack category (fallback chain)."},
    {"path": "/api/analytics/transparency-metrics", "tag": "Analytics",
     "fields_used": ["public_disclosure", "public_disclosure_date", "disclosure_delay_days",
                     "transparency_level"],
     "description": "Disclosure behaviour and transparency level breakdown."},
    {"path": "/api/analytics/operational-impact", "tag": "Analytics",
     "fields_used": ["teaching_disrupted", "research_disrupted", "admissions_disrupted",
                     "classes_cancelled", "exams_postponed", "downtime_days"],
     "description": "Operational disruption frequency."},
    {"path": "/api/analytics/institution-types", "tag": "Analytics",
     "fields_used": ["institution_type"],
     "description": "Incident distribution by institution type."},
    {"path": "/api/analytics/user-impact", "tag": "Analytics",
     "fields_used": ["students_affected", "staff_affected", "faculty_affected",
                     "users_affected_exact"],
     "description": "Aggregated user impact counts."},
    {"path": "/api/analytics/threat-actor-categories", "tag": "Analytics",
     "fields_used": ["threat_actor_category"],
     "description": "Threat actor category distribution."},
    {"path": "/api/analytics/threat-actor-motivations", "tag": "Analytics",
     "fields_used": ["threat_actor_motivation"],
     "description": "Threat actor motivation distribution."},
    {"path": "/admin/export/research-csv", "tag": "Admin",
     "fields_used": ["all flat columns", "+ timeline flattened", "+ mitre flattened"],
     "description": "Full research dataset export as CSV (education incidents only)."},
]


SCHEMA_DOC = SchemaResponse(
    version="2.1.0",
    description=(
        "EduThreat-CTI database schema reference. "
        "All analytics are sourced from incident_enrichments_flat — a denormalised "
        "flat table populated by the LLM enrichment pipeline. "
        "The base incidents table is joined for date/source fields."
    ),
    pipeline_layers=PIPELINE_LAYERS,
    tables=[
        TableDoc(
            name="incidents",
            description="Base incident table. Every ingested cyber incident (all sources). "
                        "Fields populated at ingestion time; LLM fields updated after enrichment.",
            row_count_note="~11,000+ candidates; ~1,000+ education-confirmed after enrichment",
            columns=[
                ColumnDoc(name="incident_id", type="TEXT PK", nullable=False,
                          description="Unique ID: {source}_{sha256[:16]}", populated_by="ingestion"),
                ColumnDoc(name="source", type="TEXT",
                          description="Data source name (konbriefing, ransomware_live, databreaches, etc.)",
                          populated_by="ingestion"),
                ColumnDoc(name="institution_name", type="TEXT",
                          description="Institution name (raw from source; overwritten by LLM-resolved name).",
                          populated_by="ingestion"),
                ColumnDoc(name="incident_date", type="TEXT",
                          description="Best-known incident date (ISO). Updated by LLM with 90-day guard vs source_published_date.",
                          populated_by="ingestion"),
                ColumnDoc(name="source_published_date", type="TEXT",
                          description="Date the source article/report was published. Used as upper-bound guard for incident_date.",
                          populated_by="ingestion"),
                ColumnDoc(name="primary_url", type="TEXT",
                          description="Best source URL for this incident. Must be in all_urls; guarded against SERP override.",
                          populated_by="pipeline"),
                ColumnDoc(name="all_urls", type="TEXT",
                          description="Semicolon-separated list of all discovered URLs for this incident.",
                          populated_by="ingestion"),
                ColumnDoc(name="llm_enriched", type="INTEGER (0/1)",
                          description="1 after LLM enrichment completes successfully.",
                          populated_by="pipeline"),
                ColumnDoc(name="llm_summary", type="TEXT",
                          description="Short LLM-generated summary stored in the base incidents table.",
                          populated_by="llm"),
                ColumnDoc(name="country", type="TEXT",
                          description="Normalised country name. Updated by LLM enrichment.",
                          populated_by="pipeline"),
                ColumnDoc(name="country_code", type="TEXT",
                          description="ISO alpha-2 country code.", populated_by="pipeline"),
            ],
        ),
        TableDoc(
            name="incident_enrichments_flat",
            description="Denormalised flat table — one row per enriched incident. "
                        "Source for all analytics API endpoints and the research CSV export.",
            row_count_note="One row per llm_enriched incident. ~36 edu confirmed in snapshot; grows with each pipeline run.",
            columns=FLAT_COLUMNS,
        ),
        TableDoc(
            name="incident_enrichments",
            description="Canonical enrichment artifact store. Keeps the exact raw LLM response payload, "
                        "the schema-shaped raw extraction JSON, the final post-processed enrichment JSON, "
                        "and provenance metadata for each incident.",
            row_count_note="Same cardinality as incident_enrichments_flat.",
            columns=[
                ColumnDoc(name="incident_id", type="TEXT PK", nullable=False, populated_by="pipeline",
                          description="References incidents.incident_id."),
                ColumnDoc(name="raw_response_payload", type="TEXT (JSON)",
                          description="Exact text returned by the primary extraction/summary model calls.",
                          populated_by="llm"),
                ColumnDoc(name="raw_extraction_json", type="TEXT (JSON)",
                          description="Parsed schema-shaped extraction payload with explicit nulls for unknown fields.",
                          populated_by="pipeline"),
                ColumnDoc(name="final_enrichment_json", type="TEXT (JSON)",
                          description="Canonical post-processed enrichment record, including top-level final fields plus typed/raw/debug layers.",
                          populated_by="pipeline"),
                ColumnDoc(name="storage_metadata", type="TEXT (JSON)",
                          description="Provider/model/prompt/schema/mapper/post-processing metadata for the saved enrichment.",
                          populated_by="pipeline"),
                ColumnDoc(name="enrichment_version", type="TEXT",
                          description="Storage-layer version that created this record (e.g. '3.1').",
                          populated_by="pipeline"),
                ColumnDoc(name="enrichment_confidence", type="REAL",
                          description="Overall confidence score.", populated_by="pipeline"),
                ColumnDoc(name="llm_provider", type="TEXT",
                          description="Provider that produced the saved enrichment.",
                          populated_by="pipeline"),
                ColumnDoc(name="llm_model", type="TEXT",
                          description="Model identifier used for extraction.",
                          populated_by="pipeline"),
                ColumnDoc(name="extraction_mode", type="TEXT",
                          description="Single-pass or split extraction mode used for the record.",
                          populated_by="pipeline"),
                ColumnDoc(name="prompt_version", type="TEXT",
                          description="Prompt version used when the record was generated.",
                          populated_by="pipeline"),
                ColumnDoc(name="schema_version", type="TEXT",
                          description="Extraction-schema version used when the record was generated.",
                          populated_by="pipeline"),
                ColumnDoc(name="mapper_version", type="TEXT",
                          description="Mapper version used to normalize the raw extraction JSON.",
                          populated_by="pipeline"),
                ColumnDoc(name="post_processing_version", type="TEXT",
                          description="Post-processing version used to derive analytics fields.",
                          populated_by="pipeline"),
            ],
        ),
        TableDoc(
            name="incident_enrichment_runs",
            description="Append-only history of enrichment executions. Each re-enrichment inserts a new row "
                        "so raw responses, raw extraction JSON, and final canonical outputs remain auditable over time.",
            row_count_note="Zero or more rows per incident; grows with each rerun or benchmarking pass.",
            columns=[
                ColumnDoc(name="id", type="INTEGER PK", nullable=False, populated_by="pipeline",
                          description="Append-only surrogate key for each enrichment execution."),
                ColumnDoc(name="incident_id", type="TEXT", nullable=False, populated_by="pipeline",
                          description="References incidents.incident_id."),
                ColumnDoc(name="raw_response_payload", type="TEXT (JSON)",
                          description="Exact text returned by the primary extraction/summary model calls in that run.",
                          populated_by="llm"),
                ColumnDoc(name="raw_extraction_json", type="TEXT (JSON)",
                          description="Parsed schema-shaped extraction payload with explicit nulls for that run.",
                          populated_by="pipeline"),
                ColumnDoc(name="final_enrichment_json", type="TEXT (JSON)",
                          description="Canonical post-processed enrichment record saved for that run, including final fields plus typed/raw/debug layers.",
                          populated_by="pipeline"),
                ColumnDoc(name="storage_metadata", type="TEXT (JSON)",
                          description="Provider/model/prompt/schema/mapper/post-processing metadata for that run.",
                          populated_by="pipeline"),
            ],
        ),
        TableDoc(
            name="articles",
            description="Raw fetched article content. One row per (incident_id, article_url) pair. "
                        "Used by the LLM enricher as input text.",
            row_count_note="Multiple articles per incident possible. Fetch chain: newspaper3k → HttpClient → Oxylabs → archive.org",
            columns=[
                ColumnDoc(name="incident_id", type="TEXT", populated_by="pipeline",
                          description="References incidents.incident_id."),
                ColumnDoc(name="article_url", type="TEXT", populated_by="pipeline",
                          description="URL of the fetched article."),
                ColumnDoc(name="fetch_successful", type="INTEGER (0/1)", populated_by="pipeline",
                          description="1 if article content was successfully retrieved."),
                ColumnDoc(name="article_content", type="TEXT", populated_by="pipeline",
                          description="Full text of the article. Passed to LLM for enrichment."),
            ],
        ),
    ],
    analytics_endpoints=ANALYTICS_ENDPOINTS,
    notes=[
        "recovery_timeframe_days is often NULL in news articles; dashboard uses downtime_days → outage_duration_hours/24 as fallback.",
        "total_financial_impact = SUM(ransom_amount + recovery_costs + legal_costs + notification_costs).",
        "country_code is used by the choropleth map; always check NUMERIC_TO_ALPHA2 mapping in WorldHeatmap.tsx.",
        "This documentation reflects the current canonical schema for fresh DB initialization.",
        "JSON array columns (malware_families, cve_ids, etc.) are TEXT in SQLite. Parse with json.loads().",
        "The research CSV export (/admin/export/research-csv) flattens timeline_json to 16 date/duration columns "
        "and mitre_techniques_json to 4 columns (ids, names, tactics, sub_techniques).",
    ],
)
