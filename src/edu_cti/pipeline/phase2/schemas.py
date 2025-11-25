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
        "discovery",
        "exploitation",
        "impact",
        "containment",
        "eradication",
        "recovery",
        "disclosure",
        "notification",
        "investigation",
        "remediation",
        "other"
    ]] = Field(
        default=None,
        description="Type of event. Use exact tags: initial_access, discovery, exploitation, impact, containment, eradication, recovery, disclosure, notification, investigation, remediation, other"
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
        "phishing",
        "spear_phishing",
        "vulnerability_exploit",
        "credential_stuffing",
        "credential_theft",
        "malware",
        "ransomware",
        "insider_threat",
        "social_engineering",
        "supply_chain",
        "third_party_breach",
        "misconfiguration",
        "brute_force",
        "ddos",
        "sql_injection",
        "xss",
        "other"
    ]] = Field(
        default=None,
        description="Primary attack vector. Use exact tags: phishing, spear_phishing, vulnerability_exploit, credential_stuffing, credential_theft, malware, ransomware, insider_threat, social_engineering, supply_chain, third_party_breach, misconfiguration, brute_force, ddos, sql_injection, xss, other"
    )
    attack_chain: Optional[List[Literal[
        "reconnaissance",
        "weaponization",
        "delivery",
        "exploitation",
        "installation",
        "command_and_control",
        "actions_on_objectives",
        "exfiltration",
        "impact"
    ]]] = Field(
        default=None,
        description="Kill chain stages observed. Use exact tags: reconnaissance, weaponization, delivery, exploitation, installation, command_and_control, actions_on_objectives, exfiltration, impact"
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
        "teaching_disrupted",
        "research_disrupted",
        "admissions_disrupted",
        "enrollment_disrupted",
        "payroll_disrupted",
        "clinical_operations_disrupted",
        "online_learning_disrupted",
        "classes_cancelled",
        "exams_postponed",
        "graduation_delayed",
        "email_system_down",
        "student_portal_down",
        "network_down",
        "website_down",
        "other"
    ]]] = Field(
        default=None,
        description="Operational impacts. Use exact tags: teaching_disrupted, research_disrupted, admissions_disrupted, enrollment_disrupted, payroll_disrupted, clinical_operations_disrupted, online_learning_disrupted, classes_cancelled, exams_postponed, graduation_delayed, email_system_down, student_portal_down, network_down, website_down, other"
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
