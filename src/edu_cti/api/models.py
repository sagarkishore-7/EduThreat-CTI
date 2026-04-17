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
    country_code: Optional[str] = None
    flag_emoji: Optional[str] = None


class TimeSeriesPoint(BaseModel):
    """A single point in a time series."""
    date: str
    count: int


class DashboardStats(BaseModel):
    """Overall dashboard statistics."""
    total_incidents: int
    education_incidents: int = 0
    enriched_incidents: int = 0
    unenriched_incidents: int = 0
    incidents_with_ransomware: int = 0
    incidents_with_data_breach: int = 0
    countries_affected: int = 0
    unique_threat_actors: int = 0
    unique_ransomware_families: int = 0
    data_sources: int = 0
    avg_recovery_days: Optional[float] = None
    total_financial_impact: float = 0
    incidents_with_mitre: int = 0
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

# ============================================================
# Advanced Analytics Models
# ============================================================

class AttackTrendPoint(BaseModel):
    """A point in the attack trend time series."""
    month: str
    attack_category: Optional[str] = None
    count: int


class AttackTrendsResponse(BaseModel):
    """Attack trends over time by category."""
    data: List[AttackTrendPoint]
    total: int


class MitreTacticItem(BaseModel):
    """A MITRE ATT&CK tactic with count."""
    tactic: str
    count: int
    techniques: List[str] = []


class RansomwareTimelineItem(BaseModel):
    """Ransomware family activity period."""
    family: str
    incident_count: int
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None


class RansomwareFamilyDetail(BaseModel):
    """Enhanced ransomware family stats."""
    family: str
    incident_count: int
    exfiltration_count: int = 0
    exfiltration_rate: float = 0.0
    avg_ransom: Optional[float] = None
    countries: List[str] = []
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None


class RansomEconomics(BaseModel):
    """Ransom economics aggregate."""
    total_ransomware: int = 0
    demanded_count: int = 0
    paid_count: int = 0
    payment_rate: float = 0.0
    total_demanded: Optional[float] = None
    avg_demanded: Optional[float] = None
    max_demanded: Optional[float] = None
    total_paid: Optional[float] = None
    avg_paid: Optional[float] = None


class RecoveryComparison(BaseModel):
    """Recovery metrics for a category."""
    avg_recovery_days: float = 0
    avg_downtime_days: float = 0
    backup_rate: float = 0
    ir_firm_rate: float = 0
    forensics_rate: float = 0
    total: int = 0


class RecoveryComparisonResponse(BaseModel):
    """Ransomware vs other recovery comparison."""
    ransomware: RecoveryComparison
    other: RecoveryComparison


class RansomwareGeoItem(BaseModel):
    """Per-family geographic targeting."""
    family: str
    countries: List[CountByCategory] = []


class ActorTimelinePoint(BaseModel):
    """Monthly activity for a threat actor."""
    actor: str
    month: str
    count: int


class ActorRansomwareMatrixResponse(BaseModel):
    """Actor-to-ransomware cross-tabulation."""
    actors: List[str]
    families: List[str]
    matrix: List[Dict[str, Any]]


class ActorTargetingItem(BaseModel):
    """Per-actor country targeting."""
    actor: str
    countries: List[CountByCategory] = []


class DataImpactStats(BaseModel):
    """Data breach impact statistics."""
    total: int = 0
    breached_count: int = 0
    exfiltrated_count: int = 0
    breach_rate: float = 0.0
    exfiltration_rate: float = 0.0
    total_records: Optional[int] = None
    avg_records: Optional[float] = None
    max_records: Optional[int] = None
    total_pii_leaked: Optional[int] = None


class RegulatoryImpactStats(BaseModel):
    """Regulatory impact statistics."""
    total: int = 0
    gdpr_count: int = 0
    hipaa_count: int = 0
    ferpa_count: int = 0
    notification_required: int = 0
    notifications_sent: int = 0
    fines_imposed: int = 0
    total_fines: Optional[float] = None
    lawsuits_count: int = 0
    class_action_count: int = 0


class RecoveryEffectiveness(BaseModel):
    """Recovery effectiveness metrics."""
    total: int = 0
    avg_recovery_days: Optional[float] = None
    avg_downtime_days: Optional[float] = None
    backup_count: int = 0
    backup_rate: float = 0.0
    ir_firm_count: int = 0
    ir_firm_rate: float = 0.0
    forensics_count: int = 0
    forensics_rate: float = 0.0
    mfa_post_count: int = 0
    mfa_adoption_rate: float = 0.0


class TransparencyLevel(BaseModel):
    """A transparency level with count."""
    level: str
    count: int


class TransparencyStats(BaseModel):
    """Transparency and disclosure metrics."""
    total: int = 0
    disclosed_count: int = 0
    disclosure_rate: float = 0.0
    avg_delay_days: Optional[float] = None
    levels: List[TransparencyLevel] = []


class UserImpactTotals(BaseModel):
    """User category impact totals."""
    students: Optional[int] = None
    staff: Optional[int] = None
    faculty: Optional[int] = None
    total_individuals: Optional[int] = None
    incidents_with_data: int = 0


class FinancialImpactByYear(BaseModel):
    """Financial breakdown for a year."""
    year: Optional[str] = None
    ransom_cost: Optional[float] = None
    recovery_cost: Optional[float] = None
    legal_cost: Optional[float] = None
    notification_cost: Optional[float] = None
    incident_count: int = 0


class OperationalImpactItem(BaseModel):
    """Operational impact metric."""
    category: str
    count: int
    percentage: float = 0.0


# ============================================================
# Extended Cross-Dimensional Analytics Models
# ============================================================

class InstitutionRiskItem(BaseModel):
    """Institution type × attack category cross-reference."""
    institution_type: str
    attack_category: str
    count: int


class RecoveryByAttackTypeItem(BaseModel):
    """Recovery/downtime stats per attack category."""
    attack_category: str
    avg_recovery_days: Optional[float] = None
    avg_downtime_days: Optional[float] = None
    incident_count: int


class AttackVectorByInstitutionResponse(BaseModel):
    """Attack vectors per institution type."""
    institution_types: List[str]
    vectors: List[str]
    data: List[Dict[str, Any]]


class BreachSeverityPoint(BaseModel):
    """Monthly breach severity data point."""
    month: str
    incident_count: int
    avg_records: Optional[float] = None
    breach_count: int = 0


class RansomPaymentByYearItem(BaseModel):
    """Ransom demanded vs paid per year."""
    year: Optional[str] = None
    total_incidents: int = 0
    demanded_count: int = 0
    paid_count: int = 0
    total_demanded: Optional[float] = None
    total_paid: Optional[float] = None
    payment_rate: float = 0.0


class RansomwareFamilyTrendResponse(BaseModel):
    """Ransomware family trends over time."""
    families: List[str]
    data: List[Dict[str, Any]]


class ActorInstitutionResponse(BaseModel):
    """Actor × institution type targeting matrix."""
    actors: List[str]
    institution_types: List[str]
    data: List[Dict[str, Any]]


class ActorTTPResponse(BaseModel):
    """Actor MITRE ATT&CK tactic profiles."""
    actors: List[str]
    tactics: List[str]
    data: List[Dict[str, Any]]


class DisclosureTimelinePoint(BaseModel):
    """Single disclosure delay data point."""
    incident_date: str
    disclosure_delay_days: int
    country: str
    transparency_level: Optional[str] = None


class BreachByInstitutionItem(BaseModel):
    """Breach stats per institution type."""
    institution_type: str
    total_incidents: int
    breach_count: int
    breach_rate: float
    avg_records: Optional[float] = None
    total_records: Optional[int] = None


# ============================================================
# Interactive Nivo Visualization Models
# ============================================================


class SankeyNode(BaseModel):
    id: str

class SankeyLink(BaseModel):
    source: str
    target: str
    value: float

class AttackFlowResponse(BaseModel):
    """Sankey flow: Attack Vector → Category → Impact Outcome."""
    nodes: List[SankeyNode]
    links: List[SankeyLink]

class MitreSunburstChild(BaseModel):
    id: str
    value: Optional[int] = None
    children: Optional[List["MitreSunburstChild"]] = None

class MitreSunburstResponse(BaseModel):
    """Hierarchical MITRE tree for sunburst chart."""
    id: str
    children: List[MitreSunburstChild]

class NetworkNode(BaseModel):
    id: str
    radius: int
    count: int
    families: List[str]

class NetworkLink(BaseModel):
    source: str
    target: str
    distance: int
    shared_families: List[str]

class ActorNetworkResponse(BaseModel):
    """Force-directed network graph data."""
    nodes: List[NetworkNode]
    links: List[NetworkLink]

class RansomFlowResponse(BaseModel):
    """Sankey flow: Institution → Family → Payment Outcome."""
    nodes: List[SankeyNode]
    links_by_count: List[SankeyLink]
    links_by_amount: List[SankeyLink]

class CountryAttackMatrixResponse(BaseModel):
    """Country × Attack Category chord diagram data."""
    keys: List[str]
    matrix: List[List[int]]


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

