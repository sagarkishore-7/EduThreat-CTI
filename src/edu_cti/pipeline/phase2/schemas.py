"""
Pydantic schemas for LLM enrichment output.

These schemas ensure structured, validated output from the LLM for CTI enrichment.
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime


class EducationRelevanceCheck(BaseModel):
    """Schema for education relevance check."""
    
    is_education_related: bool = Field(
        description="Whether this incident is related to the education sector"
    )
    confidence: float = Field(
        ge=0.0, le=1.0,
        description="Confidence score (0.0 to 1.0) in the relevance assessment"
    )
    reasoning: str = Field(
        description="Brief explanation of why this is or isn't education-related"
    )
    institution_identified: Optional[str] = Field(
        default=None,
        description="Specific educational institution name if identified"
    )


class URLConfidenceScore(BaseModel):
    """Schema for URL confidence scoring."""
    
    url: str = Field(description="The URL being scored")
    confidence_score: float = Field(
        ge=0.0, le=1.0,
        description="Confidence score (0.0 to 1.0) indicating quality/completeness"
    )
    reasoning: str = Field(
        description="Brief explanation of why this URL scores this way"
    )
    article_quality: str = Field(
        description="Quality assessment: 'excellent', 'good', 'fair', or 'poor'"
    )
    content_completeness: str = Field(
        description="Completeness: 'complete', 'partial', or 'minimal'"
    )
    source_reliability: str = Field(
        description="Source reliability: 'highly_reliable', 'reliable', 'moderate', or 'unknown'"
    )


class TimelineEvent(BaseModel):
    """Schema for a single timeline event."""
    
    date: str = Field(
        description="Date of the event in YYYY-MM-DD format (or best approximation)"
    )
    date_precision: str = Field(
        description="Precision level: 'day', 'month', 'year', or 'approximate'"
    )
    event_description: str = Field(
        description="Description of what happened at this time"
    )
    event_type: str = Field(
        description="Type of event: 'initial_access', 'discovery', 'impact', 'containment', 'recovery', 'disclosure', 'other'"
    )
    actor_attribution: Optional[str] = Field(
        default=None,
        description="Attributed threat actor or group name if identified"
    )
    indicators: List[str] = Field(
        default_factory=list,
        description="List of indicators of compromise (IOCs) or artifacts from this event"
    )


class MITREAttackTechnique(BaseModel):
    """Schema for a MITRE ATT&CK technique."""
    
    technique_id: str = Field(
        description="MITRE ATT&CK technique ID (e.g., 'T1055.001')"
    )
    technique_name: str = Field(
        description="Name of the technique"
    )
    tactic: str = Field(
        description="MITRE ATT&CK tactic name (e.g., 'Defense Evasion', 'Initial Access')"
    )
    confidence: str = Field(
        description="Confidence level: 'confirmed', 'likely', or 'possible'"
    )
    description: str = Field(
        description="How this technique was used in the incident"
    )
    sub_techniques: List[str] = Field(
        default_factory=list,
        description="List of sub-technique IDs if applicable"
    )


class AttackDynamics(BaseModel):
    """Schema for attack dynamics and modeling."""
    
    attack_vector: str = Field(
        description="Primary attack vector: 'phishing', 'vulnerability_exploit', 'credential_stuffing', 'insider_threat', 'unknown', or 'other'"
    )
    attack_chain: List[str] = Field(
        description="Kill chain stages observed: e.g., ['Reconnaissance', 'Weaponization', 'Delivery', 'Exploitation', 'Installation', 'C2', 'Actions on Objectives']"
    )
    ransomware_family: Optional[str] = Field(
        default=None,
        description="Ransomware family name if applicable (e.g., 'LockBit', 'BlackCat')"
    )
    data_exfiltration: bool = Field(
        description="Whether data exfiltration occurred"
    )
    encryption_impact: Optional[str] = Field(
        default=None,
        description="Encryption impact: 'full', 'partial', 'none', or None if not applicable"
    )
    impact_scope: Dict[str, Any] = Field(
        default_factory=dict,
        description="Impact scope details: systems affected, data types, user count, etc."
    )
    ransom_demanded: bool = Field(
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
    recovery_timeframe: Optional[str] = Field(
        default=None,
        description="Recovery timeframe if mentioned (e.g., '2 weeks', '1 month')"
    )
    business_impact: str = Field(
        description="Business impact assessment: 'critical', 'severe', 'moderate', 'limited', or 'unknown'"
    )
    operational_impact: List[str] = Field(
        default_factory=list,
        description="Operational impacts: e.g., ['teaching_disrupted', 'research_affected', 'admissions_delayed', 'payroll_disrupted']"
    )


class CTIEnrichmentResult(BaseModel):
    """Complete CTI enrichment result schema."""
    
    # Education relevance
    education_relevance: EducationRelevanceCheck = Field(
        description="Education relevance assessment"
    )
    
    # Primary URL selection
    primary_url: Optional[str] = Field(
        default=None,
        description="Selected primary URL for the incident (best URL from all_urls)"
    )
    url_scores: List[URLConfidenceScore] = Field(
        default_factory=list,
        description="Confidence scores for all URLs evaluated"
    )
    
    # Timeline
    timeline: List[TimelineEvent] = Field(
        default_factory=list,
        description="Chronological timeline of events in the incident"
    )
    
    # MITRE ATT&CK
    mitre_attack_techniques: List[MITREAttackTechnique] = Field(
        default_factory=list,
        description="MITRE ATT&CK techniques identified in the incident"
    )
    
    # Attack dynamics
    attack_dynamics: Optional[AttackDynamics] = Field(
        default=None,
        description="Attack dynamics and modeling details"
    )
    
    # Summary
    enriched_summary: str = Field(
        description="Comprehensive summary of the incident with all extracted details"
    )
    
    # Additional metadata
    extraction_confidence: float = Field(
        ge=0.0, le=1.0,
        description="Overall confidence in the extraction quality (0.0 to 1.0)"
    )
    extraction_notes: Optional[str] = Field(
        default=None,
        description="Additional notes about the extraction process or limitations"
    )

