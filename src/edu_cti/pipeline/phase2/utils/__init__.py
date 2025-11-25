"""
Utility module for Phase 2.

Contains utility functions:
- deduplication: Post-enrichment deduplication
- fetching_strategy: Smart article fetching with rate limiting
- revert_enrichments: CLI script to revert enrichment operations
"""

from .deduplication import (
    normalize_institution_name,
    deduplicate_by_institution,
)
from .fetching_strategy import (
    DomainRateLimiter,
    SmartArticleFetchingStrategy,
)

__all__ = [
    'normalize_institution_name',
    'deduplicate_by_institution',
    'DomainRateLimiter',
    'SmartArticleFetchingStrategy',
]

