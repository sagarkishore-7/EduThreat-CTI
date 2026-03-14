"""
Enhanced deduplication with fuzzy matching and entity resolution.

Supplements the existing URL-based deduplication with:
- Fuzzy institution name matching (Levenshtein distance)
- Temporal clustering (same institution + similar date = likely same incident)
- Name normalization (MIT vs Massachusetts Institute of Technology)

Uses thefuzz (formerly fuzzywuzzy) for fuzzy string matching.
"""

import logging
import re
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

try:
    from thefuzz import fuzz, process
    FUZZY_AVAILABLE = True
except ImportError:
    FUZZY_AVAILABLE = False
    logger.debug("thefuzz not available. Install with: pip install thefuzz python-Levenshtein")


# Common institution name variations to normalize
INSTITUTION_ALIASES: Dict[str, str] = {
    # US institutions
    "MIT": "Massachusetts Institute of Technology",
    "M.I.T.": "Massachusetts Institute of Technology",
    "UCLA": "University of California, Los Angeles",
    "UCSF": "University of California, San Francisco",
    "UCSD": "University of California, San Diego",
    "UC Berkeley": "University of California, Berkeley",
    "USC": "University of Southern California",
    "NYU": "New York University",
    "UPenn": "University of Pennsylvania",
    "UMich": "University of Michigan",
    "OSU": "Ohio State University",
    "PSU": "Penn State University",
    "UT Austin": "University of Texas at Austin",
    "ASU": "Arizona State University",
    "MSU": "Michigan State University",
    "UNC": "University of North Carolina",
    "TAMU": "Texas A&M University",
    "CU Boulder": "University of Colorado Boulder",
    "UW": "University of Washington",
    "JHU": "Johns Hopkins University",
    "CMU": "Carnegie Mellon University",
    "GT": "Georgia Institute of Technology",
    "Georgia Tech": "Georgia Institute of Technology",
    "Caltech": "California Institute of Technology",
    # UK institutions
    "UCL": "University College London",
    "LSE": "London School of Economics",
    "KCL": "King's College London",
    "Imperial": "Imperial College London",
    "Oxbridge": "University of Oxford",
    # German
    "TU Munich": "Technical University of Munich",
    "TUM": "Technical University of Munich",
    "TU Berlin": "Technical University of Berlin",
    "LMU": "Ludwig Maximilian University of Munich",
    # Indian
    "IIT": "Indian Institute of Technology",
    "IISc": "Indian Institute of Science",
    "IIIT": "International Institute of Information Technology",
}

# Common prefixes/suffixes to normalize
STRIP_PATTERNS = [
    r'\s*\(.*?\)\s*',  # Remove parenthetical info
    r'\s*-\s*.*$',  # Remove everything after dash
    r'^\s*The\s+',  # Remove leading "The "
    r'\s+University$',  # Normalize trailing "University"
    r'\s+College$',
    r'\s+School$',
    r'\s+Institute$',
]


def normalize_institution_name(name: str) -> str:
    """
    Normalize an institution name for comparison.

    Steps:
    1. Check alias table
    2. Strip common patterns
    3. Lowercase and strip whitespace
    """
    if not name:
        return ""

    name = name.strip()

    # Check alias table (case-insensitive)
    for alias, canonical in INSTITUTION_ALIASES.items():
        if name.lower() == alias.lower():
            return canonical.lower()

    # Apply strip patterns
    normalized = name
    for pattern in STRIP_PATTERNS:
        normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE)

    return normalized.strip().lower()


def fuzzy_match_institution(
    name: str,
    candidates: List[str],
    threshold: int = 85,
) -> Optional[Tuple[str, int]]:
    """
    Find the best fuzzy match for an institution name.

    Args:
        name: Institution name to match
        candidates: List of existing institution names
        threshold: Minimum similarity score (0-100)

    Returns:
        Tuple of (best_match, score) or None if no match above threshold
    """
    if not FUZZY_AVAILABLE:
        return None
    if not name or not candidates:
        return None

    normalized = normalize_institution_name(name)
    normalized_candidates = [normalize_institution_name(c) for c in candidates]

    # Use token_sort_ratio which handles word reordering
    # e.g., "University of California" vs "California University"
    result = process.extractOne(
        normalized,
        normalized_candidates,
        scorer=fuzz.token_sort_ratio,
    )

    if result and result[1] >= threshold:
        # Map back to original candidate
        idx = normalized_candidates.index(result[0])
        return (candidates[idx], result[1])

    return None


def are_likely_same_incident(
    name1: str,
    name2: str,
    date1: Optional[str],
    date2: Optional[str],
    name_threshold: int = 80,
    date_window_days: int = 14,
) -> bool:
    """
    Determine if two incidents are likely the same based on fuzzy name
    matching and temporal proximity.

    Args:
        name1: First institution name
        name2: Second institution name
        date1: First incident date (YYYY-MM-DD or None)
        date2: Second incident date (YYYY-MM-DD or None)
        name_threshold: Minimum name similarity (0-100)
        date_window_days: Max days apart for temporal match

    Returns:
        True if likely the same incident
    """
    if not FUZZY_AVAILABLE:
        return False

    # Compare names
    n1 = normalize_institution_name(name1)
    n2 = normalize_institution_name(name2)

    if not n1 or not n2:
        return False

    # Exact match after normalization
    if n1 == n2:
        name_match = True
    else:
        score = fuzz.token_sort_ratio(n1, n2)
        name_match = score >= name_threshold

    if not name_match:
        return False

    # If both dates are available, check temporal proximity
    if date1 and date2:
        try:
            from datetime import datetime
            d1 = datetime.strptime(date1[:10], "%Y-%m-%d")
            d2 = datetime.strptime(date2[:10], "%Y-%m-%d")
            days_apart = abs((d1 - d2).days)
            return days_apart <= date_window_days
        except (ValueError, IndexError):
            pass

    # Names match but no dates to compare — assume same
    return True
