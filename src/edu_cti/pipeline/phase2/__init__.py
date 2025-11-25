"""
Phase 2: LLM Enrichment Pipeline

Enriches incidents with structured CTI data using LLM-based extraction.
"""

from .enrichment import IncidentEnricher
from .llm_client import OllamaLLMClient
from .schemas import CTIEnrichmentResult, EducationRelevanceCheck
from .csv_export import export_enriched_dataset

# Re-export from submodules for convenience
from .storage import (
    ArticleFetcher,
    ArticleContent,
    ArticleProcessor,
    get_enrichment_result,
    save_enrichment_result,
)
from .extraction import (
    EXTRACTION_SCHEMA,
    PROMPT_TEMPLATE,
    json_to_cti_enrichment,
)
from .utils import (
    normalize_institution_name,
    deduplicate_by_institution,
)

__all__ = [
    # Core classes
    'IncidentEnricher',
    'OllamaLLMClient',
    'CTIEnrichmentResult',
    'EducationRelevanceCheck',
    
    # Storage
    'ArticleFetcher',
    'ArticleContent',
    'ArticleProcessor',
    'get_enrichment_result',
    'save_enrichment_result',
    
    # Extraction
    'EXTRACTION_SCHEMA',
    'PROMPT_TEMPLATE',
    'json_to_cti_enrichment',
    
    # Utils
    'normalize_institution_name',
    'deduplicate_by_institution',
    
    # Export
    'export_enriched_dataset',
]
