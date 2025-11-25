"""
Extraction module for Phase 2.

Contains JSON schema-based extraction components:
- extraction_schema: JSON schema definition
- extraction_prompt: Prompt template
- json_to_schema_mapper: Maps JSON response to CTIEnrichmentResult
"""

from .extraction_schema import EXTRACTION_SCHEMA
from .extraction_prompt import PROMPT_TEMPLATE
from .json_to_schema_mapper import json_to_cti_enrichment

__all__ = [
    'EXTRACTION_SCHEMA',
    'PROMPT_TEMPLATE',
    'json_to_cti_enrichment',
]

