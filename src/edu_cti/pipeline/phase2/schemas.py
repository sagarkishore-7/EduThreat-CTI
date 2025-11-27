"""
Pydantic schemas for LLM enrichment output.

Simplified schema - nested impact metrics are stored as Dict structures
instead of separate Pydantic models for simplicity and flexibility.
"""

from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field


class EducationRelevanceCheck(BaseModel):
    """Schema for education relevance check - simple Yes/No with reasoning."""
    
    is_education_related: bool = Field(
        description="Whether this incident is related to the education sector (true/false)"
    )
    reasoning: str = Field(
        description="Brief explanation (1-2 sentences) of why this is or isn't education-related"
    )
    institution_identified: Optional[str] = Field(
        default=None,
        description="Specific educational institution name if identified"
    )


class TimelineEvent(BaseModel):
    """Schema for a single timeline event."""
    
    date: Optional[str] = Field(
        default=None,
        description="Date of the event in YYYY-MM-DD format (or best approximation)"
    )
    date_precision: Optional[Literal["day", "month", "year", "approximate"]] = Field(
        default=None,
        description="Precision level: 'day', 'month', 'year', or 'approximate'"
    )
    event_description: Optional[str] = Field(
        default=None,
        description="Description of what happened at this time"
    )
    event_type: Optional[Literal[
        "initial_access",
        "reconnaissance",
        "lateral_movement",
        "privilege_escalation",
        "data_exfiltration",
        "encryption_started",
        "ransom_demand",
        "discovery",
        "exploitation",
        "impact",
        "operational_impact",
        "containment",
        "eradication",
        "recovery",
        "disclosure",
        "notification",
        "investigation",
        "remediation",
        "law_enforcement_contact",
        "public_statement",
        "systems_restored",
        "response_action",
        "security_improvement",
        "other"
    ]] = Field(
        default=None,
        description="Type of event in the incident timeline"
    )
    actor_attribution: Optional[str] = Field(
        default=None,
        description="Attributed threat actor or group name if identified"
    )
    indicators: Optional[List[str]] = Field(
        default=None,
        description="List of indicators of compromise (IOCs) or artifacts from this event"
    )


class MITREAttackTechnique(BaseModel):
    """Schema for a MITRE ATT&CK technique."""
    
    technique_id: Optional[str] = Field(
        default=None,
        description="MITRE ATT&CK technique ID (e.g., 'T1055.001')"
    )
    technique_name: Optional[str] = Field(
        default=None,
        description="Name of the technique"
    )
    tactic: Optional[str] = Field(
        default=None,
        description="MITRE ATT&CK tactic name (e.g., 'Defense Evasion', 'Initial Access')"
    )
    description: Optional[str] = Field(
        default=None,
        description="How this technique was used in the incident"
    )
    sub_techniques: Optional[List[str]] = Field(
        default=None,
        description="List of sub-technique IDs if applicable"
    )


class AttackDynamics(BaseModel):
    """Schema for attack dynamics and modeling."""
    
    attack_vector: Optional[Literal[
        # Email-based
        "phishing_email",
        "spear_phishing_email",
        "malicious_attachment",
        "malicious_link",
        "business_email_compromise",
        # Credential-based
        "stolen_credentials",
        "credential_stuffing",
        "brute_force",
        "password_spraying",
        "credential_phishing",
        "session_hijacking",
        # Vulnerability exploitation
        "vulnerability_exploit_known",
        "vulnerability_exploit_zero_day",
        "unpatched_system",
        "misconfiguration",
        "default_credentials",
        # Web-based
        "drive_by_download",
        "watering_hole",
        "malvertising",
        "sql_injection",
        "xss",
        "csrf",
        "ssrf",
        "path_traversal",
        # Network-based
        "exposed_service",
        "exposed_rdp",
        "exposed_vpn",
        "exposed_ssh",
        "exposed_database",
        "exposed_api",
        "man_in_the_middle",
        # Supply chain
        "supply_chain_compromise",
        "third_party_vendor",
        "software_update_compromise",
        "trusted_relationship",
        # Physical/Social
        "social_engineering",
        "pretexting",
        "baiting",
        "tailgating",
        "usb_drop",
        # Insider
        "insider_access",
        "former_employee",
        # Cloud-specific
        "cloud_misconfiguration",
        "api_key_exposure",
        "storage_bucket_exposure",
        # Other/Legacy (for backwards compatibility)
        "phishing",
        "spear_phishing",
        "vulnerability_exploit",
        "credential_theft",
        "malware",
        "ransomware",
        "insider_threat",
        "supply_chain",
        "third_party_breach",
        "ddos",
        "dns_hijacking",
        "sim_swapping",
        "unknown",
        "other"
    ]] = Field(
        default=None,
        description="Primary attack vector used for initial access"
    )
    attack_chain: Optional[List[Literal[
        # MITRE ATT&CK Tactics (Enterprise)
        "reconnaissance",
        "resource_development",
        "initial_access",
        "execution",
        "persistence",
        "privilege_escalation",
        "defense_evasion",
        "credential_access",
        "discovery",
        "lateral_movement",
        "collection",
        "command_and_control",
        "exfiltration",
        "impact",
        # Legacy Cyber Kill Chain (for backwards compatibility)
        "weaponization",
        "delivery",
        "exploitation",
        "installation",
        "actions_on_objectives",
        # LLM variations (mapped from common LLM outputs)
        "vulnerability_discovery",
        "credential_harvesting",
        "data_analysis",
        "data_theft",
        "encryption",
        "ransom_demand",
        "data_exfiltration",
        # Incident response phases (LLM sometimes includes these)
        "containment",
        "eradication",
        "recovery",
        "lessons_learned",
        "notification",
        "disclosure",
        "detection",
        "investigation"
    ]]] = Field(
        default=None,
        description="MITRE ATT&CK tactics or Kill chain stages observed"
    )
    ransomware_family: Optional[str] = Field(
        default=None,
        description="Ransomware family name if applicable (e.g., 'LockBit', 'BlackCat')"
    )
    data_exfiltration: Optional[bool] = Field(
        default=None,
        description="Whether data exfiltration occurred"
    )
    encryption_impact: Optional[Literal["full", "partial", "none"]] = Field(
        default=None,
        description="Encryption impact. Use exact tags: 'full', 'partial', 'none'"
    )
    impact_scope: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Impact scope details: systems affected, data types, user count, etc."
    )
    ransom_demanded: Optional[bool] = Field(
        default=None,
        description="Whether a ransom was demanded"
    )
    ransom_amount: Optional[str] = Field(
        default=None,
        description="Ransom amount if disclosed (as string to preserve format)"
    )
    ransom_paid: Optional[bool] = Field(
        default=None,
        description="Whether ransom was paid (True/False/None if unknown)"
    )
    recovery_timeframe_days: Optional[float] = Field(
        default=None,
        description="Recovery timeframe in days (convert from weeks/months to days if mentioned)"
    )
    business_impact: Optional[Literal["critical", "severe", "moderate", "limited", "minimal"]] = Field(
        default=None,
        description="Business impact assessment. Use exact tags: 'critical', 'severe', 'moderate', 'limited', 'minimal'"
    )
    operational_impact: Optional[List[Literal[
        "classes_cancelled",
        "classes_moved_online",
        "exams_postponed",
        "exams_cancelled",
        "graduation_delayed",
        "semester_extended",
        "campus_closed",
        "research_halted",
        "research_data_lost",
        "payroll_delayed",
        "financial_aid_delayed",
        "admissions_suspended",
        "registration_suspended",
        "email_unavailable",
        "website_down",
        "student_portal_down",
        "lms_unavailable",
        "network_offline",
        "vpn_unavailable",
        "library_closed",
        "it_helpdesk_overwhelmed",
        "manual_processes_required",
        "clinical_operations_disrupted",
        "patient_care_affected",
        # Legacy values for backwards compatibility
        "teaching_disrupted",
        "research_disrupted",
        "admissions_disrupted",
        "enrollment_disrupted",
        "payroll_disrupted",
        "online_learning_disrupted",
        "email_system_down",
        "network_down",
        "other"
    ]]] = Field(
        default=None,
        description="Operational impacts observed during the incident"
    )


class CTIEnrichmentResult(BaseModel):
    """
    Complete CTI enrichment result schema.
    
    Nested impact metrics are stored as Dict[str, Any] for flexibility.
    This allows direct mapping from JSON without intermediate Pydantic models.
    """
    
    # Education relevance (required - simple Yes/No check)
    education_relevance: EducationRelevanceCheck = Field(
        description="Education relevance assessment - is this an education sector cyber attack?"
    )
    
    # Primary URL (optional - just the URL, no scoring)
    primary_url: Optional[str] = Field(
        default=None,
        description="Primary URL for the incident article"
    )
    
    # Initial access description (1-3 sentences on how attacker gained access)
    initial_access_description: Optional[str] = Field(
        default=None,
        description="1-3 sentences describing how the attacker gained initial access (if mentioned in article)"
    )
    
    # Timeline
    timeline: Optional[List[TimelineEvent]] = Field(
        default=None,
        description="Chronological timeline of events in the incident"
    )
    
    # MITRE ATT&CK
    mitre_attack_techniques: Optional[List[MITREAttackTechnique]] = Field(
        default=None,
        description="MITRE ATT&CK techniques identified in the incident"
    )
    
    # Attack dynamics
    attack_dynamics: Optional[AttackDynamics] = Field(
        default=None,
        description="Attack dynamics and modeling details"
    )
    
    # Extended impact metrics (stored as Dict for flexibility)
    data_impact: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Detailed data impact metrics as dictionary"
    )
    system_impact: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Detailed system impact metrics as dictionary"
    )
    user_impact: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Detailed user impact metrics as dictionary"
    )
    operational_impact_metrics: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Detailed operational impact metrics as dictionary"
    )
    financial_impact: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Detailed financial impact metrics as dictionary"
    )
    regulatory_impact: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Detailed regulatory and compliance impact metrics as dictionary"
    )
    recovery_metrics: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Detailed recovery and remediation metrics as dictionary"
    )
    transparency_metrics: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Detailed transparency and disclosure metrics as dictionary"
    )
    research_impact: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Detailed research-specific impact metrics as dictionary"
    )
    
    # Summary
    enriched_summary: str = Field(
        description="Comprehensive summary of the incident with all extracted details"
    )
    
    # Additional metadata
    extraction_notes: Optional[str] = Field(
        default=None,
        description="Additional notes about the extraction process or limitations"
    )
