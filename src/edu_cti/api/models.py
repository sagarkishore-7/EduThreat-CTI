"""
Pydantic models for API responses.

Provides structured response models for the REST API endpoints.
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime


# ============================================================
# Pagination Models
# ============================================================

class PaginationMeta(BaseModel):
    """Pagination metadata for list responses."""
    page: int
    per_page: int
    total: int
    total_pages: int
    has_next: bool
    has_prev: bool


# ============================================================
# Incident Models
# ============================================================

class IncidentSource(BaseModel):
    """Source attribution for an incident."""
    source: str
    source_event_id: Optional[str] = None
    first_seen_at: str
    confidence: Optional[str] = None


class IncidentSummary(BaseModel):
    """Summary view of an incident for list endpoints."""
    incident_id: str
    university_name: str
    victim_raw_name: Optional[str] = None
    institution_type: Optional[str] = None
    country: Optional[str] = None
    country_code: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    incident_date: Optional[str] = None
    date_precision: Optional[str] = None
    title: Optional[str] = None
    attack_type_hint: Optional[str] = None
    attack_category: Optional[str] = None
    ransomware_family: Optional[str] = None
    threat_actor_name: Optional[str] = None
    status: str = "suspected"
    source_confidence: str = "medium"
    llm_enriched: bool = False
    llm_enriched_at: Optional[str] = None
    ingested_at: Optional[str] = None
    sources: List[str] = []


class TimelineEvent(BaseModel):
    """A single event in the incident timeline."""
    date: Optional[str] = None
    date_precision: Optional[str] = None
    event_description: Optional[str] = None
    event_type: Optional[str] = None
    actor_attribution: Optional[str] = None
    indicators: Optional[List[str]] = None


class MITRETechnique(BaseModel):
    """A MITRE ATT&CK technique."""
    technique_id: Optional[str] = None
    technique_name: Optional[str] = None
    tactic: Optional[str] = None
    description: Optional[str] = None
    sub_techniques: Optional[List[str]] = None


class AttackDynamics(BaseModel):
    """Attack dynamics and kill chain information."""
    attack_vector: Optional[str] = None
    attack_chain: Optional[List[str]] = None
    ransomware_family: Optional[str] = None
    data_exfiltration: Optional[bool] = None
    encryption_impact: Optional[str] = None
    ransom_demanded: Optional[bool] = None
    ransom_amount: Optional[float] = None
    ransom_paid: Optional[bool] = None
    recovery_timeframe_days: Optional[float] = None
    business_impact: Optional[str] = None
    operational_impact: Optional[List[str]] = None


class DataImpact(BaseModel):
    """Data breach impact metrics."""
    data_breached: Optional[bool] = None
    data_exfiltrated: Optional[bool] = None
    data_categories: Optional[List[str]] = None
    records_affected_exact: Optional[int] = None
    records_affected_min: Optional[int] = None
    records_affected_max: Optional[int] = None
    pii_records_leaked: Optional[int] = None


class SystemImpact(BaseModel):
    """System and infrastructure impact."""
    systems_affected: Optional[List[str]] = None
    critical_systems_affected: Optional[bool] = None
    network_compromised: Optional[bool] = None
    email_system_affected: Optional[bool] = None
    student_portal_affected: Optional[bool] = None
    research_systems_affected: Optional[bool] = None


class UserImpact(BaseModel):
    """User impact metrics."""
    students_affected: Optional[int] = None
    staff_affected: Optional[int] = None
    faculty_affected: Optional[int] = None
    alumni_affected: Optional[int] = None
    total_individuals_affected: Optional[int] = None


class FinancialImpact(BaseModel):
    """Financial impact metrics."""
    estimated_total_cost_usd: Optional[float] = None
    ransom_cost_usd: Optional[float] = None
    recovery_cost_usd: Optional[float] = None
    legal_cost_usd: Optional[float] = None
    insurance_claim: Optional[bool] = None
    insurance_payout_usd: Optional[float] = None


class RegulatoryImpact(BaseModel):
    """Regulatory and compliance impact."""
    applicable_regulations: Optional[List[str]] = None
    breach_notification_required: Optional[bool] = None
    notification_sent: Optional[bool] = None
    fine_imposed: Optional[bool] = None
    fine_amount_usd: Optional[float] = None
    lawsuits_filed: Optional[bool] = None
    class_action_filed: Optional[bool] = None


class RecoveryMetrics(BaseModel):
    """Recovery and response metrics."""
    recovery_method: Optional[str] = None
    recovery_duration_days: Optional[float] = None
    law_enforcement_involved: Optional[bool] = None
    ir_firm_engaged: Optional[str] = None
    security_improvements: Optional[List[str]] = None


class TransparencyMetrics(BaseModel):
    """Disclosure and transparency metrics."""
    public_disclosure: Optional[bool] = None
    public_disclosure_date: Optional[str] = None
    disclosure_delay_days: Optional[float] = None
    transparency_level: Optional[str] = None


class IncidentDetail(BaseModel):
    """Full incident detail with all enrichment data."""
    # Core identification
    incident_id: str
    university_name: str
    victim_raw_name: Optional[str] = None
    institution_type: Optional[str] = None
    institution_size: Optional[str] = None
    
    # Location
    country: Optional[str] = None
    country_code: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    
    # Dates
    incident_date: Optional[str] = None
    date_precision: Optional[str] = None
    discovery_date: Optional[str] = None
    source_published_date: Optional[str] = None
    ingested_at: Optional[str] = None
    
    # Content
    title: Optional[str] = None
    subtitle: Optional[str] = None
    enriched_summary: Optional[str] = None
    initial_access_description: Optional[str] = None
    
    # URLs
    primary_url: Optional[str] = None
    all_urls: List[str] = []
    leak_site_url: Optional[str] = None
    
    # Classification
    attack_type_hint: Optional[str] = None
    attack_category: Optional[str] = None
    incident_severity: Optional[str] = None
    status: str = "suspected"
    source_confidence: str = "medium"
    
    # Threat actor
    threat_actor_name: Optional[str] = None
    threat_actor_category: Optional[str] = None
    threat_actor_motivation: Optional[str] = None
    
    # Timeline & MITRE
    timeline: Optional[List[TimelineEvent]] = None
    mitre_attack_techniques: Optional[List[MITRETechnique]] = None
    
    # Attack dynamics
    attack_dynamics: Optional[AttackDynamics] = None
    
    # Impact metrics
    data_impact: Optional[DataImpact] = None
    system_impact: Optional[SystemImpact] = None
    user_impact: Optional[UserImpact] = None
    financial_impact: Optional[FinancialImpact] = None
    regulatory_impact: Optional[RegulatoryImpact] = None
    
    # Recovery & transparency
    recovery_metrics: Optional[RecoveryMetrics] = None
    transparency_metrics: Optional[TransparencyMetrics] = None
    
    # Enrichment status
    llm_enriched: bool = False
    llm_enriched_at: Optional[str] = None
    
    # Source attribution
    sources: List[IncidentSource] = []
    
    # Notes
    notes: Optional[str] = None


class IncidentListResponse(BaseModel):
    """Response for incident list endpoint."""
    incidents: List[IncidentSummary]
    pagination: PaginationMeta


# ============================================================
# Statistics & Analytics Models
# ============================================================

class CountByCategory(BaseModel):
    """Count of incidents by a category."""
    category: str
    count: int
    percentage: float = 0.0


class TimeSeriesPoint(BaseModel):
    """A single point in a time series."""
    date: str
    count: int


class DashboardStats(BaseModel):
    """Overall dashboard statistics."""
    total_incidents: int
    enriched_incidents: int
    unenriched_incidents: int
    incidents_with_ransomware: int
    incidents_with_data_breach: int
    countries_affected: int
    unique_threat_actors: int
    unique_ransomware_families: int
    last_updated: str


class IncidentsByCountry(BaseModel):
    """Incidents aggregated by country."""
    data: List[CountByCategory]
    total: int


class IncidentsByAttackType(BaseModel):
    """Incidents aggregated by attack type."""
    data: List[CountByCategory]
    total: int


class IncidentsByRansomwareFamily(BaseModel):
    """Incidents aggregated by ransomware family."""
    data: List[CountByCategory]
    total: int


class IncidentsByMonth(BaseModel):
    """Incidents aggregated by month."""
    data: List[TimeSeriesPoint]
    total: int


class ThreatActorSummary(BaseModel):
    """Summary of threat actor activity."""
    name: str
    incident_count: int
    countries_targeted: List[str]
    ransomware_families: List[str]
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None


class ThreatActorsResponse(BaseModel):
    """Response for threat actors endpoint."""
    threat_actors: List[ThreatActorSummary]
    total: int


class RecentIncident(BaseModel):
    """A recent incident for the dashboard feed."""
    incident_id: str
    university_name: str
    country: Optional[str] = None
    attack_category: Optional[str] = None
    ransomware_family: Optional[str] = None
    incident_date: Optional[str] = None
    title: Optional[str] = None
    threat_actor_name: Optional[str] = None


class DashboardResponse(BaseModel):
    """Complete dashboard data response."""
    stats: DashboardStats
    incidents_by_country: List[CountByCategory]
    incidents_by_attack_type: List[CountByCategory]
    incidents_by_ransomware: List[CountByCategory]
    incidents_over_time: List[TimeSeriesPoint]
    recent_incidents: List[RecentIncident]


# ============================================================
# Filter/Search Models
# ============================================================

class FilterOptions(BaseModel):
    """Available filter options for the incidents list."""
    countries: List[str]
    attack_categories: List[str]
    ransomware_families: List[str]
    threat_actors: List[str]
    institution_types: List[str]
    years: List[int]

