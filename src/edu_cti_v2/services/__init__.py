"""Lazy service exports for the Postgres-backed v2 runtime."""

from __future__ import annotations

from importlib import import_module

__all__ = [
    "V2AnalyticsRefreshService",
    "V2CanonicalizationService",
    "V2CanonicalReadService",
    "V2CampaignService",
    "V2DataQualityService",
    "V2EnrichmentService",
    "V2FetchService",
    "V2IntakeService",
    "V2OperationsService",
    "V2PreflightService",
    "V2ResearchMetricsService",
    "V2ResolveUrlService",
    "V2SourceHealthService",
    "V2TaskRuntime",
    "build_source_projection",
    "determine_initial_task_type",
    "source_incident_to_base_incident",
]

_LAZY_IMPORTS = {
    "V2AnalyticsRefreshService": ("src.edu_cti_v2.services.analytics", "V2AnalyticsRefreshService"),
    "V2CanonicalizationService": ("src.edu_cti_v2.services.canonicalization", "V2CanonicalizationService"),
    "V2CanonicalReadService": ("src.edu_cti_v2.services.read_models", "V2CanonicalReadService"),
    "V2CampaignService": ("src.edu_cti_v2.services.campaigns", "V2CampaignService"),
    "V2DataQualityService": ("src.edu_cti_v2.services.data_quality", "V2DataQualityService"),
    "V2EnrichmentService": ("src.edu_cti_v2.services.enrichment", "V2EnrichmentService"),
    "V2FetchService": ("src.edu_cti_v2.services.fetching", "V2FetchService"),
    "V2IntakeService": ("src.edu_cti_v2.services.intake", "V2IntakeService"),
    "V2OperationsService": ("src.edu_cti_v2.services.operations", "V2OperationsService"),
    "V2PreflightService": ("src.edu_cti_v2.services.preflight", "V2PreflightService"),
    "V2ResearchMetricsService": ("src.edu_cti_v2.services.research_metrics", "V2ResearchMetricsService"),
    "V2ResolveUrlService": ("src.edu_cti_v2.services.resolution", "V2ResolveUrlService"),
    "V2SourceHealthService": ("src.edu_cti_v2.services.source_health", "V2SourceHealthService"),
    "V2TaskRuntime": ("src.edu_cti_v2.services.task_runtime", "V2TaskRuntime"),
    "build_source_projection": ("src.edu_cti_v2.services.canonicalization", "build_source_projection"),
    "determine_initial_task_type": ("src.edu_cti_v2.services.intake", "determine_initial_task_type"),
    "source_incident_to_base_incident": ("src.edu_cti_v2.services.enrichment", "source_incident_to_base_incident"),
}


def __getattr__(name: str):
    if name not in _LAZY_IMPORTS:
        raise AttributeError(name)
    module_name, attr_name = _LAZY_IMPORTS[name]
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
