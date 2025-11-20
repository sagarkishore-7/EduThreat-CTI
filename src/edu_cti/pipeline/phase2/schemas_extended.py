"""
Extended Pydantic schemas for comprehensive CTI analytics.

These schemas provide extensive fields for analytics across incidents
and detailed metrics for the education sector.
"""

from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field
from datetime import datetime


# ============================================================================
# Extended Impact Metrics
# ============================================================================

class DataImpactMetrics(BaseModel):
    """Metrics for data types affected and scope."""
    
    personal_information: bool = Field(
        description="Whether personal information (PII) was affected"
    )
    student_data: bool = Field(
        description="Whether student data was compromised"
    )
    faculty_data: bool = Field(
        description="Whether faculty/staff data was compromised"
    )
    alumni_data: bool = Field(
        description="Whether alumni data was compromised"
    )
    financial_data: bool = Field(
        description="Whether financial data (payment info, donations) was affected"
    )
    research_data: bool = Field(
        description="Whether research data was compromised"
    )
    intellectual_property: bool = Field(
        description="Whether intellectual property was affected"
    )
    medical_records: bool = Field(
        description="Whether medical/health records were affected (for university hospitals)"
    )
    administrative_data: bool = Field(
        description="Whether administrative data was affected"
    )
    
    # Counts (if disclosed)
    records_affected_min: Optional[int] = Field(
        default=None,
        description="Minimum number of records affected"
    )
    records_affected_max: Optional[int] = Field(
        default=None,
        description="Maximum number of records affected (if range disclosed)"
    )
    records_affected_exact: Optional[int] = Field(
        default=None,
        description="Exact number of records affected (if disclosed)"
    )
    
    # Data types details
    data_types_affected: List[str] = Field(
        default_factory=list,
        description="Specific data types: names, emails, SSNs, grades, research data, etc."
    )
    data_encrypted: bool = Field(
        description="Whether stolen data was encrypted"
    )
    data_exfiltrated: bool = Field(
        description="Whether data was exfiltrated"
    )


class SystemImpactMetrics(BaseModel):
    """Metrics for systems and infrastructure affected."""
    
    systems_affected: List[str] = Field(
        default_factory=list,
        description="Types of systems affected: email, student portal, research servers, etc."
    )
    critical_systems_affected: bool = Field(
        description="Whether critical systems (payroll, admissions, etc.) were affected"
    )
    network_compromised: bool = Field(
        description="Whether the network was compromised"
    )
    email_system_affected: bool = Field(
        description="Whether email system was affected"
    )
    student_portal_affected: bool = Field(
        description="Whether student portal/learning management system was affected"
    )
    research_systems_affected: bool = Field(
        description="Whether research computing systems were affected"
    )
    hospital_systems_affected: bool = Field(
        description="Whether hospital systems were affected (for universities with hospitals)"
    )
    cloud_services_affected: bool = Field(
        description="Whether cloud services were affected"
    )
    third_party_vendor_impact: bool = Field(
        description="Whether third-party vendor systems were involved"
    )
    vendor_name: Optional[str] = Field(
        default=None,
        description="Name of affected vendor if applicable"
    )


class UserImpactMetrics(BaseModel):
    """Metrics for users/people affected."""
    
    students_affected: bool = Field(
        description="Whether students were affected"
    )
    faculty_affected: bool = Field(
        description="Whether faculty were affected"
    )
    staff_affected: bool = Field(
        description="Whether staff were affected"
    )
    alumni_affected: bool = Field(
        description="Whether alumni were affected"
    )
    parents_affected: bool = Field(
        description="Whether parents/guardians were affected"
    )
    applicants_affected: bool = Field(
        description="Whether prospective students/applicants were affected"
    )
    patients_affected: bool = Field(
        description="Whether patients were affected (for university hospitals)"
    )
    
    # Counts (if disclosed)
    users_affected_min: Optional[int] = Field(
        default=None,
        description="Minimum number of users affected"
    )
    users_affected_max: Optional[int] = Field(
        default=None,
        description="Maximum number of users affected (if range disclosed)"
    )
    users_affected_exact: Optional[int] = Field(
        default=None,
        description="Exact number of users affected (if disclosed)"
    )


class OperationalImpactMetrics(BaseModel):
    """Metrics for operational disruptions."""
    
    teaching_disrupted: bool = Field(
        description="Whether teaching activities were disrupted"
    )
    research_disrupted: bool = Field(
        description="Whether research activities were disrupted"
    )
    admissions_disrupted: bool = Field(
        description="Whether admissions processes were disrupted"
    )
    payroll_disrupted: bool = Field(
        description="Whether payroll was disrupted"
    )
    enrollment_disrupted: bool = Field(
        description="Whether enrollment was disrupted"
    )
    clinical_operations_disrupted: bool = Field(
        description="Whether clinical operations were disrupted (hospitals)"
    )
    online_learning_disrupted: bool = Field(
        description="Whether online learning was disrupted"
    )
    
    downtime_days: Optional[float] = Field(
        default=None,
        description="Number of days systems were down"
    )
    partial_service_days: Optional[float] = Field(
        default=None,
        description="Number of days with partial service"
    )
    
    classes_cancelled: bool = Field(
        description="Whether classes were cancelled"
    )
    exams_postponed: bool = Field(
        description="Whether exams were postponed"
    )
    graduation_delayed: bool = Field(
        description="Whether graduation ceremonies were delayed"
    )


class FinancialImpactMetrics(BaseModel):
    """Metrics for financial impact."""
    
    ransom_demanded: bool = Field(
        description="Whether a ransom was demanded"
    )
    ransom_amount_min: Optional[float] = Field(
        default=None,
        description="Minimum ransom amount (in USD if currency not specified)"
    )
    ransom_amount_max: Optional[float] = Field(
        default=None,
        description="Maximum ransom amount (if range)"
    )
    ransom_amount_exact: Optional[float] = Field(
        default=None,
        description="Exact ransom amount"
    )
    ransom_currency: Optional[str] = Field(
        default="USD",
        description="Currency of ransom (default: USD)"
    )
    ransom_paid: Optional[bool] = Field(
        default=None,
        description="Whether ransom was paid (True/False/None if unknown)"
    )
    ransom_paid_amount: Optional[float] = Field(
        default=None,
        description="Amount of ransom paid (if different from demanded)"
    )
    
    # Additional costs
    recovery_costs_min: Optional[float] = Field(
        default=None,
        description="Minimum recovery costs (in USD)"
    )
    recovery_costs_max: Optional[float] = Field(
        default=None,
        description="Maximum recovery costs (if range)"
    )
    legal_costs: Optional[float] = Field(
        default=None,
        description="Legal costs incurred"
    )
    notification_costs: Optional[float] = Field(
        default=None,
        description="Costs for notifying affected individuals"
    )
    credit_monitoring_costs: Optional[float] = Field(
        default=None,
        description="Costs for credit monitoring services offered"
    )
    insurance_claim: Optional[bool] = Field(
        default=None,
        description="Whether cyber insurance was claimed"
    )
    insurance_claim_amount: Optional[float] = Field(
        default=None,
        description="Insurance claim amount"
    )


class RegulatoryImpactMetrics(BaseModel):
    """Metrics for regulatory and compliance impact."""
    
    breach_notification_required: bool = Field(
        description="Whether breach notification was required"
    )
    notifications_sent: Optional[bool] = Field(
        default=None,
        description="Whether notifications were sent to affected individuals"
    )
    notifications_sent_date: Optional[str] = Field(
        default=None,
        description="Date when notifications were sent (YYYY-MM-DD)"
    )
    
    regulators_notified: List[str] = Field(
        default_factory=list,
        description="Regulators notified: CISA, state AG, GDPR authority, etc."
    )
    regulators_notified_date: Optional[str] = Field(
        default=None,
        description="Date regulators were notified (YYYY-MM-DD)"
    )
    
    # GDPR-specific
    gdpr_breach: bool = Field(
        description="Whether this is a GDPR breach (EU/UK data affected)"
    )
    dpa_notified: Optional[bool] = Field(
        default=None,
        description="Whether Data Protection Authority was notified"
    )
    
    # US-specific
    hipaa_breach: bool = Field(
        description="Whether this is a HIPAA breach (medical data affected)"
    )
    ferc_breach: bool = Field(
        description="Whether this is a FERPA breach (student records affected)"
    )
    
    # Enforcement actions
    investigation_opened: Optional[bool] = Field(
        default=None,
        description="Whether regulatory investigation was opened"
    )
    fine_imposed: Optional[bool] = Field(
        default=None,
        description="Whether a fine was imposed"
    )
    fine_amount: Optional[float] = Field(
        default=None,
        description="Fine amount (in USD)"
    )
    
    # Lawsuits
    lawsuits_filed: Optional[bool] = Field(
        default=None,
        description="Whether lawsuits were filed"
    )
    lawsuit_count: Optional[int] = Field(
        default=None,
        description="Number of lawsuits filed"
    )
    class_action: Optional[bool] = Field(
        default=None,
        description="Whether a class action lawsuit was filed"
    )


class RecoveryMetrics(BaseModel):
    """Metrics for recovery and remediation."""
    
    recovery_started_date: Optional[str] = Field(
        default=None,
        description="Date recovery efforts started (YYYY-MM-DD)"
    )
    recovery_completed_date: Optional[str] = Field(
        default=None,
        description="Date recovery was completed (YYYY-MM-DD)"
    )
    recovery_timeframe_days: Optional[float] = Field(
        default=None,
        description="Total recovery time in days"
    )
    
    recovery_phases: List[str] = Field(
        default_factory=list,
        description="Recovery phases: containment, eradication, recovery, lessons learned"
    )
    
    # Recovery methods
    from_backup: bool = Field(
        description="Whether recovery was from backup"
    )
    backup_age_days: Optional[float] = Field(
        default=None,
        description="Age of backup used (days old)"
    )
    clean_rebuild: bool = Field(
        description="Whether systems were rebuilt from scratch"
    )
    
    # External help
    incident_response_firm: Optional[str] = Field(
        default=None,
        description="Name of incident response firm hired"
    )
    forensics_firm: Optional[str] = Field(
        default=None,
        description="Name of forensics firm hired"
    )
    law_firm: Optional[str] = Field(
        default=None,
        description="Name of law firm hired for legal support"
    )
    
    # Security improvements
    security_improvements: List[str] = Field(
        default_factory=list,
        description="Security improvements implemented post-incident"
    )
    mfa_implemented: Optional[bool] = Field(
        default=None,
        description="Whether MFA was implemented after incident"
    )
    security_training_conducted: Optional[bool] = Field(
        default=None,
        description="Whether additional security training was conducted"
    )


class TransparencyMetrics(BaseModel):
    """Metrics for transparency and disclosure."""
    
    disclosure_timeline: Dict[str, Optional[str]] = Field(
        default_factory=dict,
        description="Timeline of disclosures: discovered_date, disclosed_date, notified_date"
    )
    public_disclosure: bool = Field(
        description="Whether incident was publicly disclosed"
    )
    public_disclosure_date: Optional[str] = Field(
        default=None,
        description="Date of public disclosure (YYYY-MM-DD)"
    )
    disclosure_delay_days: Optional[float] = Field(
        default=None,
        description="Days between discovery and disclosure"
    )
    
    transparency_level: Literal["high", "medium", "low", "none"] = Field(
        description="Overall transparency level"
    )
    official_statement_url: Optional[str] = Field(
        default=None,
        description="URL of official statement"
    )
    detailed_report_url: Optional[str] = Field(
        default=None,
        description="URL of detailed incident report if published"
    )
    
    updates_provided: Optional[bool] = Field(
        default=None,
        description="Whether regular updates were provided"
    )
    update_count: Optional[int] = Field(
        default=None,
        description="Number of updates provided"
    )


class ResearchImpactMetrics(BaseModel):
    """Metrics for research-specific impact."""
    
    research_projects_affected: bool = Field(
        description="Whether research projects were affected"
    )
    research_data_compromised: bool = Field(
        description="Whether research data was compromised"
    )
    sensitive_research_impact: bool = Field(
        description="Whether sensitive research (defense, medical) was affected"
    )
    
    publications_delayed: Optional[bool] = Field(
        default=None,
        description="Whether publications were delayed"
    )
    grants_affected: Optional[bool] = Field(
        default=None,
        description="Whether grant activities were affected"
    )
    collaborations_affected: Optional[bool] = Field(
        default=None,
        description="Whether research collaborations were affected"
    )
    
    research_area: Optional[str] = Field(
        default=None,
        description="Primary research area affected (if specified)"
    )


# ============================================================================
# Extended CTI Enrichment Schema
# ============================================================================

class ExtendedCTIEnrichmentResult(BaseModel):
    """Extended CTI enrichment result with comprehensive analytics fields."""
    
    # Inherit from base schema - education relevance, primary_url, url_scores, timeline, MITRE, attack_dynamics
    # We'll compose this in the enrichment module
    
    # Extended impact metrics
    data_impact: Optional[DataImpactMetrics] = Field(
        default=None,
        description="Detailed data impact metrics"
    )
    system_impact: Optional[SystemImpactMetrics] = Field(
        default=None,
        description="Detailed system impact metrics"
    )
    user_impact: Optional[UserImpactMetrics] = Field(
        default=None,
        description="Detailed user impact metrics"
    )
    operational_impact: Optional[OperationalImpactMetrics] = Field(
        default=None,
        description="Detailed operational impact metrics"
    )
    financial_impact: Optional[FinancialImpactMetrics] = Field(
        default=None,
        description="Detailed financial impact metrics"
    )
    regulatory_impact: Optional[RegulatoryImpactMetrics] = Field(
        default=None,
        description="Detailed regulatory and compliance impact metrics"
    )
    recovery_metrics: Optional[RecoveryMetrics] = Field(
        default=None,
        description="Detailed recovery and remediation metrics"
    )
    transparency_metrics: Optional[TransparencyMetrics] = Field(
        default=None,
        description="Detailed transparency and disclosure metrics"
    )
    research_impact: Optional[ResearchImpactMetrics] = Field(
        default=None,
        description="Detailed research-specific impact metrics"
    )
    
    # Cross-incident analytics fields
    institution_size_category: Optional[Literal["large", "medium", "small"]] = Field(
        default=None,
        description="Institution size category based on enrollment"
    )
    institution_type_detailed: Optional[str] = Field(
        default=None,
        description="Detailed institution type: public_4yr, private_4yr, community_college, k12, etc."
    )
    
    # Incident categorization for analytics
    incident_severity: Optional[Literal["critical", "high", "medium", "low"]] = Field(
        default=None,
        description="Overall incident severity rating"
    )
    incident_category: Optional[str] = Field(
        default=None,
        description="Primary incident category: ransomware, data_breach, phishing, etc."
    )
    incident_tags: List[str] = Field(
        default_factory=list,
        description="Tags for filtering and analytics: education_sector, ransomware, healthcare, etc."
    )
    
    # Geographic analytics
    region_category: Optional[str] = Field(
        default=None,
        description="Regional category: north_america, europe, asia, etc."
    )
    country_risk_level: Optional[Literal["high", "medium", "low"]] = Field(
        default=None,
        description="Country risk level for cyber incidents"
    )
    
    # Temporal analytics
    incident_month: Optional[int] = Field(
        default=None,
        description="Month of incident (1-12) for seasonal analysis"
    )
    incident_quarter: Optional[int] = Field(
        default=None,
        description="Quarter of incident (1-4) for quarterly analysis"
    )
    incident_year: Optional[int] = Field(
        default=None,
        description="Year of incident for trend analysis"
    )
    
    # Summary for analytics
    analytics_summary: Optional[str] = Field(
        default=None,
        description="Structured summary optimized for analytics and reporting"
    )

